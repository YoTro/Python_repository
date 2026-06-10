"""
CookieBrowserPool — manages N (session, browser) slots for concurrent Amazon scraping.

Each slot encapsulates one Amazon identity:
  - An independent curl_cffi AsyncSession for Tier 1/2 (AJAX / HTML)
  - A lazily-initialised ChromiumPage for Tier 3, serialised by a per-slot asyncio.Lock

Concurrency model
-----------------
Tier 1/2 (curl_cffi):   Round-robin slot selection.  No blocking — each session is an
                         independent object; asyncio cooperative scheduling prevents races.

Tier 3 (browser):       Caller acquires ``slot.browser_lock`` before entering
                         ``_fetch_reviews_via_browser``.  Lock is released when the
                         browser scrape finishes, allowing the next queued Tier-3 call
                         on the same slot to proceed.

Usage
-----
    # One-time init at server / bot startup
    CookieBrowserPool.init([
        {"cookies": {"session-id": "...", ...}, "user_agent": "...", "proxy": "http://..."},
        {"cookies": {...}},
    ])

    # Or load from existing AmazonCookieHelper cache files
    CookieBrowserPool.from_cookie_files([
        "data/cookies/account_0.json",
        "data/cookies/account_1.json",
    ])

    # CommentsExtractor picks this up automatically; no call-site changes needed.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import tempfile
import time

from curl_cffi import requests

from src.core.utils.cookie_helper import AMAZON_UA, AmazonCookieHelper, _nearest_cffi_target

logger = logging.getLogger(__name__)


class SlotCircuit:
    """
    Per-slot circuit breaker.

    State machine
    -------------
    closed  → failures accumulate on each ``record_failure()`` call
    open    → tripped when failures >= threshold; slot skipped by ``next_slot()``
    half-open → cooldown has elapsed; ``is_closed()`` returns True so the slot
                gets one trial request; a success resets it, a failure re-opens it

    The distinction between THROTTLED and EXPIRED is left to callers via logging;
    the circuit itself only cares about closed vs open, which is all ``next_slot``
    needs.
    """

    __slots__ = ("failures", "open_until", "threshold", "cooldown")

    def __init__(self, threshold: int = 3, cooldown: float = 300.0) -> None:
        self.failures: int = 0
        self.open_until: float = 0.0   # monotonic timestamp; 0 means closed
        self.threshold: int = threshold
        self.cooldown: float = cooldown

    def is_closed(self) -> bool:
        """True when the slot should receive traffic (closed or half-open)."""
        return time.monotonic() >= self.open_until

    def record_failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.open_until = time.monotonic() + self.cooldown
            logger.warning(
                "[SlotCircuit] Circuit opened after %d consecutive failures; "
                "cooldown %.0fs.",
                self.failures,
                self.cooldown,
            )

    def record_success(self) -> None:
        if self.failures:
            logger.info("[SlotCircuit] Circuit reset after successful request.")
        self.failures = 0
        self.open_until = 0.0


# Base CDP port for slot browsers.  Slot N listens on _BASE_PORT + N.
# Avoids collision with the legacy single-browser debug port used by live tests (19222).
_BASE_BROWSER_PORT = 19300


class CookieSlot:
    """
    One Amazon identity: a curl_cffi session (Tier 1/2) plus a lazily-launched
    Chrome browser (Tier 3).

    Attributes
    ----------
    slot_id           : zero-based index in the pool
    cookies           : snapshot of cookies used to seed the browser at launch
    headers           : base HTTP headers (includes User-Agent)
    session           : curl_cffi AsyncSession — shared across Tier 1/2 calls for this slot
    proxy             : optional paired proxy URL; applied to both session and browser
    cache_file        : path to this slot's persistent cookie JSON (e.g. config/cookies_slot_0.json)
    browser           : ChromiumPage instance, None until first Tier-3 use
    browser_lock      : asyncio.Lock — exactly one Tier-3 operation per slot at a time
    browser_use_count : Tier-3 invocation counter; triggers tab recycling at _RECYCLE_AFTER
    page1_html        : per-slot CSRF-token cache (consumed once per AJAX call chain)
    circuit           : SlotCircuit — tracks consecutive failures for next_slot routing
    """

    # Recycle the active tab after this many Tier-3 browser invocations.
    # Destroying and recreating the tab releases V8 heap, detached DOM nodes,
    # and the renderer's JS closure graph without restarting the browser process
    # (so WAF session cookies are preserved).
    _RECYCLE_AFTER: int = 200

    def __init__(
        self,
        slot_id: int,
        cookies: dict[str, str],
        headers: dict[str, str],
        session: requests.AsyncSession,
        proxy: str | None = None,
        cache_file: str = "",
    ) -> None:
        self.slot_id = slot_id
        self.cookies = cookies
        self.headers = headers
        self.session = session
        self.proxy = proxy
        self.cache_file: str = cache_file or f"config/cookies_slot_{slot_id}.json"
        self.browser = None
        self.browser_lock = asyncio.Lock()
        self.browser_use_count: int = 0
        self.page1_html: str | None = None
        self.circuit = SlotCircuit()

    # ------------------------------------------------------------------
    # Browser lifecycle
    # ------------------------------------------------------------------

    def get_or_init_browser(self):
        """
        Return the live browser for this slot, launching Chrome if necessary.

        Increments ``browser_use_count`` on each call.  When the count reaches
        ``_RECYCLE_AFTER``, the active tab is closed and a fresh blank tab is
        opened on the same browser process.  This releases the old renderer's V8
        heap without losing the WAF / session cookies stored in the profile.
        If recycling fails, the browser is invalidated and re-launched.
        """
        if self.browser is not None:
            try:
                _ = self.browser.url  # probe — raises if the tab/process is dead
                self.browser_use_count += 1
                if self.browser_use_count >= self._RECYCLE_AFTER:
                    self._recycle_tab()  # may set self.browser = None on failure
            except Exception:
                self.browser = None
                self.browser_use_count = 0

        if self.browser is None:
            self.browser = _launch_browser(self)
            self.browser_use_count = 1

        return self.browser

    def _recycle_tab(self) -> None:
        """
        Replace the current tab with a fresh blank one on the same browser process.

        The browser process (and its cookie store) is kept alive, so the WAF
        session token and Amazon login cookies survive the recycle.  The old
        renderer context is destroyed, which releases:
          - V8 old-generation heap accumulated over hundreds of page loads
          - Detached DOM nodes and JS event-listener graphs
          - The renderer's in-memory resource cache

        On any failure the browser is invalidated so ``get_or_init_browser``
        falls back to a clean re-launch on the next call.
        """
        try:
            bp = self.browser
            stale_tabs = bp.get_tabs()          # snapshot before opening the replacement
            bp.new_tab()                         # new blank tab becomes active on bp
            for tab in stale_tabs:
                try:
                    tab.close()
                except Exception:
                    pass
            self.browser_use_count = 0
            logger.info("[CookieBrowserPool] Slot %d tab recycled.", self.slot_id)
        except Exception as exc:
            logger.warning(
                "[CookieBrowserPool] Slot %d tab recycle failed (%s) — restarting browser.",
                self.slot_id,
                exc,
            )
            self.invalidate_browser()
            self.browser_use_count = 0

    def invalidate_browser(self) -> None:
        """Mark the browser as dead so the next call re-launches it."""
        self.browser = None


class CookieBrowserPool:
    """
    Singleton pool of N CookieSlots.

    Tier 1/2 callers use ``next_slot()`` (round-robin, never blocks).
    Tier 3 callers do:

        async with slot.browser_lock:
            bp = slot.get_or_init_browser()
            ...
    """

    _instance: CookieBrowserPool | None = None

    def __init__(self, slots: list[CookieSlot]) -> None:
        if not slots:
            raise ValueError("CookieBrowserPool requires at least one cookie entry.")
        self._slots = slots
        self._rr_idx = 0

    # ------------------------------------------------------------------
    # Factory / singleton
    # ------------------------------------------------------------------

    @classmethod
    def init(cls, cookie_entries: list[dict]) -> CookieBrowserPool:
        """
        Initialise (or replace) the singleton from a list of cookie-entry dicts::

            {
                "cookies":    {"session-id": "...", ...},   # required
                "user_agent": "Mozilla/5.0 ...",             # optional
                "proxy":      "http://user:pass@host:port",  # optional
            }
        """
        slots = [_build_slot(i, e) for i, e in enumerate(cookie_entries)]
        cls._instance = cls(slots)
        logger.info("[CookieBrowserPool] Initialised with %d slot(s).", len(slots))
        return cls._instance

    @classmethod
    def from_cookie_files(cls, paths: list[str]) -> CookieBrowserPool:
        """
        Load from cookie-cache JSON files (same format as AmazonCookieHelper).
        Each file becomes one slot; the file path is stored as ``slot.cache_file``
        so fresh browser cookies are written back to the originating file.
        """
        entries: list[dict] = []
        for p in paths:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            data["_cache_file"] = p   # carry the source path through to _build_slot
            entries.append(data)
        return cls.init(entries)

    @classmethod
    def from_cookie_helper(cls, *helpers: AmazonCookieHelper) -> CookieBrowserPool:
        """
        Build a pool directly from one or more ``AmazonCookieHelper`` instances.

        Each helper represents one Amazon account.  Its ``cache_file`` path is
        preserved as the slot's ``cache_file`` so the browser tier writes fresh
        cookies back to the correct per-account file.

        Example — single account::

            CookieBrowserPool.from_cookie_helper(AmazonCookieHelper())

        Example — three accounts with separate cache files::

            CookieBrowserPool.from_cookie_helper(
                AmazonCookieHelper("config/cookies_a.json"),
                AmazonCookieHelper("config/cookies_b.json"),
                AmazonCookieHelper("config/cookies_c.json"),
            )
        """
        entries: list[dict] = []
        for helper in helpers:
            data = helper.get_cookie_data()
            data["_cache_file"] = helper.cache_file
            entries.append(data)
        return cls.init(entries)

    @classmethod
    def get_instance(cls) -> CookieBrowserPool | None:
        return cls._instance

    @classmethod
    def clear(cls) -> None:
        """Tear down the singleton — primarily for tests."""
        cls._instance = None

    # ------------------------------------------------------------------
    # Slot selection
    # ------------------------------------------------------------------

    def next_slot(self) -> CookieSlot:
        """
        Round-robin among closed (healthy) slots.

        Scans up to N slots starting from the current round-robin position and
        returns the first whose circuit is closed.  If every circuit is open
        (all accounts throttled), falls back to the slot whose cooldown expires
        soonest rather than blocking or raising.
        """
        n = len(self._slots)
        for i in range(n):
            idx = (self._rr_idx + i) % n
            slot = self._slots[idx]
            if slot.circuit.is_closed():
                self._rr_idx = (idx + 1) % n
                return slot

        best = min(self._slots, key=lambda s: s.circuit.open_until)
        logger.warning(
            "[CookieBrowserPool] All %d slot(s) have open circuits; "
            "using least-recently-failed slot %d.",
            n,
            best.slot_id,
        )
        return best

    def slot_count(self) -> int:
        return len(self._slots)

    def __len__(self) -> int:
        return len(self._slots)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_slot(slot_id: int, entry: dict) -> CookieSlot:
    cookies: dict[str, str] = entry.get("cookies", {})
    ua: str = entry.get("user_agent", AMAZON_UA)
    proxy: str | None = entry.get("proxy")
    # _cache_file is injected by from_cookie_files / from_cookie_helper so fresh
    # browser cookies are written back to the originating per-account file rather
    # than to the shared config/cookies.json.
    cache_file: str = entry.get("_cache_file", f"config/cookies_slot_{slot_id}.json")

    headers = {
        "User-Agent": ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    m = re.search(r"Chrome/(\d+)", ua)
    major = int(m.group(1)) if m else 146
    impersonate = f"chrome{_nearest_cffi_target(major)}"

    proxies = {"https": proxy, "http": proxy} if proxy else None
    session = requests.AsyncSession(
        headers=headers,
        cookies=cookies,
        impersonate=impersonate,
        proxies=proxies,
    )

    return CookieSlot(
        slot_id=slot_id,
        cookies=cookies,
        headers=headers,
        session=session,
        proxy=proxy,
        cache_file=cache_file,
    )


def _launch_browser(slot: CookieSlot):
    """
    Launch a Chrome instance for *slot*.

    Design decisions
    ----------------
    - Deterministic port (_BASE_BROWSER_PORT + slot_id) avoids random collisions
      between slots and with any other DrissionPage processes.
    - Dedicated --user-data-dir per slot ensures cookie jars never bleed across slots;
      Chrome stores the WAF session token inside this profile.
    - Linux: ``headless=True`` (Chrome 112+ headless-new keeps the same TLS JA3 hash
      as a headed browser, so Amazon WAF cannot distinguish it from a real window).
    - macOS: visible window for manual inspection / login during development.
    """
    import time as _time

    try:
        from DrissionPage import ChromiumOptions, ChromiumPage
    except ImportError:
        raise RuntimeError("[CookieBrowserPool] DrissionPage is not installed.")

    co = ChromiumOptions()
    co.set_local_port(_BASE_BROWSER_PORT + slot.slot_id)

    data_dir = os.path.join(tempfile.gettempdir(), f"amazon_slot_{slot.slot_id}")
    os.makedirs(data_dir, exist_ok=True)
    co.set_argument(f"--user-data-dir={data_dir}")

    for candidate in (
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
    ):
        if os.path.isfile(candidate):
            co.set_browser_path(candidate)
            break

    co.set_argument("--no-sandbox")
    co.set_argument("--disable-dev-shm-usage")
    co.set_argument("--disable-gpu")

    # ── Memory caps ────────────────────────────────────────────────────────────
    # Limit V8 old-generation heap per renderer process.  Amazon review pages
    # are heavy React apps; without a cap, V8 old-gen grows unboundedly over
    # hundreds of navigations.  256 MB is sufficient for review page execution.
    co.set_argument("--js-flags=--max-old-space-size=256")
    # Disable the HTTP disk cache so the --user-data-dir under /tmp does not
    # accumulate Amazon's large JS bundles over hours of operation.
    co.set_argument("--disk-cache-size=1")
    co.set_argument("--media-cache-size=1")
    # Disable the legacy Application Cache (AppCache) API; unused by Amazon but
    # Chrome still allocates storage for it when enabled.
    co.set_argument("--disable-application-cache")

    if sys.platform != "darwin":
        co.headless(True)
    else:
        co.headless(False)

    if slot.proxy:
        co.set_proxy(slot.proxy)

    co.set_user_agent(slot.headers.get("User-Agent", AMAZON_UA))

    bp = ChromiumPage(co)
    bp.set.load_mode.normal()

    bp.get("https://www.amazon.com/", timeout=30)
    _time.sleep(3)
    for name, value in slot.cookies.items():
        try:
            bp.set.cookies({"name": name, "value": value, "domain": ".amazon.com"})
        except Exception:
            pass

    logger.info(
        "[CookieBrowserPool] Slot %d browser launched on port %d.",
        slot.slot_id,
        _BASE_BROWSER_PORT + slot.slot_id,
    )
    return bp
