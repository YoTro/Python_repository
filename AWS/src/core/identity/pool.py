"""
IdentityPool — generic multi-account (session, browser) slot pool.

Each slot encapsulates one web identity:
  - An independent curl_cffi AsyncSession for lightweight HTTP tiers
  - A lazily-initialised ChromiumPage for JavaScript-heavy tiers, serialised
    by a per-slot asyncio.Lock
  - A BaseIdentityStrategy that supplies domain-specific policy (warmup URL,
    cookie domain, default UA, hard-block detection)

Concurrency model
-----------------
HTTP tiers (curl_cffi):   Round-robin slot selection.  No blocking — each
                          session is an independent object.  curl_cffi's
                          set_curl_options() bakes the session cookie jar
                          into the libcurl easy handle synchronously (via
                          CURLOPT_COOKIELIST) before the transfer starts, so
                          concurrent jar mutations only affect future requests.

Browser tier (DrissionPage): Caller acquires ``slot.browser_lock`` before
                          entering any browser scrape.  Lock is released when
                          the scrape finishes, allowing the next queued call
                          on the same slot to proceed.

Multi-process safety
--------------------
Two pool instances on the same host collide on the default CDP port range and
Chrome profile dirs.  Pass a distinct ``base_port`` to each process::

    IdentityPool.init(entries, strategy)                  # ports 19300…
    IdentityPool.init(entries, strategy, base_port=19400) # ports 19400…

``--user-data-dir`` paths are created with stable home dir at pool init
time, so they are always unique across processes regardless of ``base_port``.

Environment overrides
---------------------
``CHROME_EXECUTABLE``  Absolute path to the Chrome/Chromium binary.  Bypasses
                       the built-in candidate list entirely.
``CHROME_HEADLESS``    ``1``/``true`` → always headless; ``0``/``false`` →
                       always headed; absent → platform default (headed on
                       macOS, headless everywhere else).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import socket
import sys
import time
from typing import TYPE_CHECKING, Any

from curl_cffi import requests

if TYPE_CHECKING:
    from curl_cffi.requests.session import ProxySpec

from src.core.identity.strategy import BaseIdentityStrategy
from src.core.utils.cookie_helper import _nearest_cffi_target

logger = logging.getLogger(__name__)


class SlotCircuit:
    """
    Per-slot circuit breaker.

    State machine
    -------------
    closed    → failures accumulate on each ``record_failure()`` call
    open      → tripped when failures >= threshold; slot skipped by ``next_slot()``
    half-open → cooldown elapsed; ``is_closed()`` returns True for one trial;
                success resets, failure re-opens
    """

    __slots__ = ("failures", "open_until", "threshold", "cooldown")

    def __init__(self, threshold: int = 3, cooldown: float = 300.0) -> None:
        self.failures: int = 0
        self.open_until: float = 0.0  # monotonic timestamp; 0 means closed
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
                "[SlotCircuit] Circuit opened after %d consecutive failures; cooldown %.0fs.",
                self.failures,
                self.cooldown,
            )

    def record_success(self) -> None:
        if self.failures:
            logger.info("[SlotCircuit] Circuit reset after successful request.")
        self.failures = 0
        self.open_until = 0.0


# Base CDP port for slot browsers.  Slot N listens on _BASE_BROWSER_PORT + N.
_BASE_BROWSER_PORT = 19300

# Ordered candidate paths; first existing file wins.
# Override entirely via the CHROME_EXECUTABLE environment variable.
_CHROME_CANDIDATES: tuple[str, ...] = (
    # macOS
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    # Linux (package-manager installs + snap)
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    "/snap/bin/chromium",
    # Windows (64-bit and 32-bit install locations)
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files\Chromium\Application\chrome.exe",
    r"C:\Program Files (x86)\Chromium\Application\chrome.exe",
)


def resolve_chrome_path() -> str | None:
    """Return the Chrome binary path to pass to DrissionPage, or None to auto-detect."""
    env = os.environ.get("CHROME_EXECUTABLE", "").strip()
    if env:
        return env
    for candidate in _CHROME_CANDIDATES:
        if os.path.isfile(candidate):
            return candidate
    return None  # DrissionPage will attempt its own detection


def _resolve_headless() -> bool:
    """
    Return whether Chrome should run headless.

    Resolution order:
    1. ``CHROME_HEADLESS`` env var (``1``/``true`` → True; ``0``/``false`` → False)
    2. Platform default: headless on Linux/Windows (servers); headed on macOS (dev)
    """
    env = os.environ.get("CHROME_HEADLESS", "").strip().lower()
    if env in ("1", "true", "yes"):
        return True
    if env in ("0", "false", "no"):
        return False
    return sys.platform != "darwin"


def _find_free_port(preferred: int, search_range: int = 20) -> int:
    """
    Return the first TCP port that is not actively bound, starting at *preferred*.

    Probes up to *search_range* consecutive ports.  Uses ``SO_REUSEADDR`` so
    ports left in TIME_WAIT by a recently-crashed Chrome process are considered
    available.  Falls back to an OS-assigned ephemeral port if every port in
    the range is actively occupied.

    Note: there is an inherent TOCTOU window between this check and Chrome
    actually binding the port.  In practice the window is negligible because
    Chrome binds its CDP port within milliseconds of launch.
    """
    for port in range(preferred, preferred + search_range):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    # All preferred ports are actively occupied — let the OS assign one.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class IdentitySlot:
    """
    One web identity: a curl_cffi session plus a lazily-launched Chrome browser.

    Attributes
    ----------
    slot_id           : zero-based index in the pool
    strategy          : domain policy (warmup URL, cookie domain, UA, block detection)
    cookies           : snapshot of cookies used to seed the browser at launch
    headers           : base HTTP headers (includes User-Agent)
    session           : curl_cffi AsyncSession — shared across HTTP-tier calls
    proxy             : optional paired proxy URL; applied to both session and browser
    cache_file        : path to this slot's persistent cookie JSON
    browser_port      : CDP debug port assigned to this slot's Chrome process
    browser_data_dir  : unique ``--user-data-dir`` path created by stable home-dir path at pool init;
                        stable across browser restarts so WAF session tokens survive crashes
    browser           : ChromiumPage instance, None until first browser-tier use
    browser_lock      : asyncio.Lock — exactly one browser operation per slot at a time
    browser_use_count : browser invocation counter; triggers tab recycling at _RECYCLE_AFTER
    circuit           : SlotCircuit — tracks consecutive failures for next_slot routing
    """

    # Recycle the active tab after this many browser invocations.
    # Destroying and recreating the tab releases V8 heap, detached DOM nodes,
    # and the renderer's JS closure graph without restarting the browser process
    # (so WAF session cookies stored in the profile are preserved).
    _RECYCLE_AFTER: int = 200

    def __init__(
        self,
        slot_id: int,
        strategy: BaseIdentityStrategy,
        cookies: dict[str, str],
        headers: dict[str, str],
        session: requests.AsyncSession,
        proxy: str | None = None,
        cache_file: str = "",
        browser_port: int = 0,
        browser_data_dir: str = "",
    ) -> None:
        self.slot_id = slot_id
        self.strategy = strategy
        self.cookies = cookies
        self.headers = headers
        self.session = session
        self.proxy = proxy
        self.cache_file: str = cache_file or f"config/cookies_slot_{slot_id}.json"
        self.browser_port: int = browser_port or (_BASE_BROWSER_PORT + slot_id)
        self.browser_data_dir: str = browser_data_dir
        self.browser: Any = None  # ChromiumPage when active; Any avoids importing optional dep
        self.browser_lock = asyncio.Lock()
        self.browser_use_count: int = 0
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

        The browser process (and its cookie store) is kept alive, so WAF session
        tokens survive.  The old renderer context is destroyed, releasing V8
        old-generation heap and detached DOM nodes accumulated over many page loads.

        On any failure the browser is invalidated so ``get_or_init_browser``
        falls back to a clean re-launch on the next call.
        """
        try:
            bp = self.browser
            stale_tabs = bp.get_tabs()  # snapshot before opening the replacement
            bp.new_tab()  # new blank tab becomes active on bp
            for tab in stale_tabs:
                try:
                    tab.close()
                except Exception:
                    pass
            self.browser_use_count = 0
            logger.info("[IdentityPool] Slot %d tab recycled.", self.slot_id)
        except Exception as exc:
            logger.warning(
                "[IdentityPool] Slot %d tab recycle failed (%s) — restarting browser.",
                self.slot_id,
                exc,
            )
            self.invalidate_browser()
            self.browser_use_count = 0

    def invalidate_browser(self) -> None:
        """Mark the browser as dead so the next call re-launches it."""
        self.browser = None


class IdentityPool:
    """
    Named registry of IdentityPool instances — one entry per logical domain or tenant.

    HTTP-tier callers use ``next_slot()`` (round-robin, never blocks).
    Browser-tier callers do::

        async with slot.browser_lock:
            bp = slot.get_or_init_browser()
            ...

    Multiple pools coexist via the ``name`` key::

        IdentityPool.init(us_entries, strategy, name="amazon_us")
        IdentityPool.init(jp_entries, strategy, name="amazon_jp", base_port=19400)

        us_pool = IdentityPool.get_instance("amazon_us")
        jp_pool = IdentityPool.get_instance("amazon_jp")

    Replacing a named pool is safe for in-flight callers: they hold a reference
    to an ``IdentitySlot`` from the old pool, which Python keeps alive until
    their request completes.  Only new ``get_instance()`` calls receive the
    replacement.
    """

    _DEFAULT_NAME: str = "__default__"
    _registry: dict[str, IdentityPool] = {}

    def __init__(self, slots: list[IdentitySlot]) -> None:
        if not slots:
            raise ValueError("IdentityPool requires at least one entry.")
        self._slots = slots
        self._rr_idx = 0

    # ------------------------------------------------------------------
    # Factory / registry
    # ------------------------------------------------------------------

    @classmethod
    def init(
        cls,
        cookie_entries: list[dict],
        strategy: BaseIdentityStrategy,
        *,
        name: str = "",
        base_port: int = _BASE_BROWSER_PORT,
    ) -> IdentityPool:
        """
        Create (or replace) a named pool from a list of entry dicts::

            {
                "cookies":    {"session-id": "...", ...},   # required
                "user_agent": "Mozilla/5.0 ...",             # optional; falls back to strategy.user_agent()
                "proxy":      "http://user:pass@host:port",  # optional
            }

        ``name`` identifies the pool within this process.  Omit to use the
        class default (``_DEFAULT_NAME``).  Use distinct names for separate
        domains or tenants; use distinct ``base_port`` values for each pool
        on the same host to avoid CDP port collisions.
        """
        key = name or cls._DEFAULT_NAME
        slots = [
            _build_slot(i, e, strategy, base_port=base_port) for i, e in enumerate(cookie_entries)
        ]
        instance = cls(slots)
        cls._registry[key] = instance
        logger.info(
            "[IdentityPool] Registered pool %r with %d slot(s), base_port=%d.",
            key,
            len(slots),
            base_port,
        )
        return instance

    @classmethod
    def get_instance(cls, name: str = "") -> IdentityPool | None:
        return cls._registry.get(name or cls._DEFAULT_NAME)

    @classmethod
    def all_instances(cls) -> dict[str, IdentityPool]:
        """Return a snapshot of all registered pools for this class."""
        return dict(cls._registry)

    @classmethod
    def clear(cls, name: str = "") -> None:
        """Remove a named pool (or all pools when name is omitted) — primarily for tests."""
        if name:
            cls._registry.pop(name, None)
        else:
            cls._registry.clear()

    # ------------------------------------------------------------------
    # Slot selection
    # ------------------------------------------------------------------

    def next_slot(self) -> IdentitySlot:
        """
        Round-robin among closed (healthy) slots.

        Scans up to N slots starting from the current round-robin position and
        returns the first whose circuit is closed.  If every circuit is open,
        falls back to the slot whose cooldown expires soonest rather than
        blocking or raising.
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
            "[IdentityPool] All %d slot(s) have open circuits; "
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


def _build_slot(
    slot_id: int,
    entry: dict,
    strategy: BaseIdentityStrategy,
    base_port: int = _BASE_BROWSER_PORT,
) -> IdentitySlot:
    cookies: dict[str, str] = entry.get("cookies", {})
    ua: str = entry.get("user_agent", strategy.user_agent())
    proxy: str | None = entry.get("proxy")
    # _cache_file is injected by domain-specific factory methods (from_cookie_files,
    # from_cookie_helper) so fresh browser cookies are written back to the
    # originating per-account file.
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

    proxies: ProxySpec | None = {"https": proxy, "http": proxy} if proxy else None
    session: requests.AsyncSession = requests.AsyncSession(
        headers=headers,
        cookies=cookies,
        impersonate=impersonate,
        proxies=proxies,
    )

    # Per-slot port override: an entry may pin a specific CDP port via
    # "browser_port" to avoid conflicts with other services on the host.
    preferred_port: int = int(entry.get("browser_port", base_port + slot_id))

    # Use a stable home-dir path instead of /tmp:
    #   - snap-confined Chromium (Ubuntu 22.04+) allows $HOME but AppArmor-blocks /tmp
    #   - stable path means WAF session tokens survive Python process restarts
    # Keyed by base_port + slot_id so two pool instances on the same host
    # (different base_port) get different dirs, preventing Chrome profile lock conflicts.
    _pool_dir = os.path.join(os.path.expanduser("~"), ".local", "share", "identity_pool")
    os.makedirs(_pool_dir, exist_ok=True)
    browser_data_dir = os.path.join(_pool_dir, f"slot_{slot_id}_p{base_port}")
    os.makedirs(browser_data_dir, exist_ok=True)

    return IdentitySlot(
        slot_id=slot_id,
        strategy=strategy,
        cookies=cookies,
        headers=headers,
        session=session,
        proxy=proxy,
        cache_file=cache_file,
        browser_port=preferred_port,
        browser_data_dir=browser_data_dir,
    )


def _launch_browser(slot: IdentitySlot):
    """
    Launch a Chrome instance for *slot* and seed it with the slot's cookies.

    Design decisions
    ----------------
    - ``slot.browser_port`` (base_port + slot_id) is deterministic within a pool.
      Pass a distinct ``base_port`` to each pool process on the same host to
      avoid cross-process port conflicts.
    - ``slot.browser_data_dir`` is created once by ``_build_slot`` via stable home-dir path,
      so it is unique across processes.  Reusing the same dir on re-launch
      preserves WAF session tokens stored in the Chrome profile across crashes.
    - Warmup URL and cookie domain come from ``slot.strategy`` so this function
      contains no domain-specific hardcoding.
    """
    import time as _time

    try:
        from DrissionPage import ChromiumOptions, ChromiumPage
    except ImportError as exc:
        raise RuntimeError("[IdentityPool] DrissionPage is not installed.") from exc

    actual_port = _find_free_port(slot.browser_port)
    if actual_port != slot.browser_port:
        logger.warning(
            "[IdentityPool] Slot %d preferred port %d is occupied; using %d instead.",
            slot.slot_id,
            slot.browser_port,
            actual_port,
        )
        slot.browser_port = actual_port  # keep slot state consistent for diagnostics

    co = ChromiumOptions()
    co.set_local_port(actual_port)
    co.set_argument(f"--user-data-dir={slot.browser_data_dir}")

    chrome_path = resolve_chrome_path()
    if chrome_path:
        co.set_browser_path(chrome_path)

    co.set_argument("--no-sandbox")
    co.set_argument("--disable-dev-shm-usage")
    co.set_argument("--disable-gpu")

    # Limit V8 old-generation heap per renderer process.  Without a cap, V8
    # old-gen grows unboundedly over hundreds of navigations.  256 MB is
    # sufficient for most heavy React pages.
    co.set_argument("--js-flags=--max-old-space-size=256")
    # Disable the HTTP disk cache so the profile dir does not accumulate
    # large JS bundles over hours of operation.
    co.set_argument("--disk-cache-size=1")
    co.set_argument("--media-cache-size=1")
    co.set_argument("--disable-application-cache")

    # co.headless(True) in DrissionPage ≤ 4.1.x sets the deprecated --headless
    # flag; Chrome 112+ requires --headless=new (headed-mode renderer pipeline
    # with compositing). Using the deprecated flag causes Chrome to fail
    # silently in headless environments, breaking the CDP connection.
    if _resolve_headless():
        co.set_argument("--headless=new")
        co.set_argument("--disable-setuid-sandbox")

    if slot.proxy:
        co.set_proxy(slot.proxy)

    co.set_user_agent(slot.headers.get("User-Agent", slot.strategy.user_agent()))

    bp = ChromiumPage(co)
    bp.set.load_mode.normal()

    bp.get(slot.strategy.warmup_url(), timeout=30)
    _time.sleep(3)
    for name, value in slot.cookies.items():
        try:
            bp.set.cookies({"name": name, "value": value, "domain": slot.strategy.cookie_domain()})
        except Exception:
            pass

    logger.info(
        "[IdentityPool] Slot %d browser launched on port %d, data_dir=%s.",
        slot.slot_id,
        slot.browser_port,
        slot.browser_data_dir,
    )
    return bp
