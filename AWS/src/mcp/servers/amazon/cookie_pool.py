"""
CookieBrowserPool — Amazon-specific identity pool (thin shim over IdentityPool).

Wraps ``src.core.identity.IdentityPool`` with:
  - ``AmazonIdentityStrategy`` pre-wired (warmup URL, cookie domain, UA, block detection)
  - Amazon-specific factory methods (``from_cookie_files``, ``from_cookie_helper``)
  - Backward-compatible public names (``CookieSlot``, ``SlotCircuit``)

All mechanism (slot circuit-breaker, Chrome launch, port probing, round-robin
selection) lives in ``src.core.identity.pool``.  See that module for full
concurrency / multi-process / environment-override documentation.

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

import json
import logging

from src.core.identity.pool import (
    _BASE_BROWSER_PORT,
    IdentityPool,
    IdentitySlot,
    SlotCircuit,
    _resolve_chrome_path,
)
from src.core.utils.cookie_helper import AmazonCookieHelper
from src.mcp.servers.amazon.identity import AmazonIdentityStrategy

logger = logging.getLogger(__name__)

# Backward-compatible alias: callers that type-annotate CookieSlot continue to work.
CookieSlot = IdentitySlot

__all__ = [
    "CookieBrowserPool",
    "CookieSlot",
    "SlotCircuit",
    "_resolve_chrome_path",
]


class CookieBrowserPool(IdentityPool):
    """
    Singleton pool of N Amazon identity slots.

    Tier 1/2 callers use ``next_slot()`` (round-robin, never blocks).
    Tier 3 callers do::

        async with slot.browser_lock:
            bp = slot.get_or_init_browser()
            ...
    """

    _instance: CookieBrowserPool | None = None  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Factory / singleton
    # ------------------------------------------------------------------

    @classmethod
    def init(
        cls,
        cookie_entries: list[dict],
        *,
        base_port: int = _BASE_BROWSER_PORT,
    ) -> CookieBrowserPool:
        """
        Initialise (or replace) the singleton from a list of cookie-entry dicts::

            {
                "cookies":    {"session-id": "...", ...},   # required
                "user_agent": "Mozilla/5.0 ...",             # optional
                "proxy":      "http://user:pass@host:port",  # optional
            }

        ``base_port`` sets the first CDP debug port; slot N uses ``base_port + N``.
        Pass a different value for each pool process on the same host to avoid
        port collisions (default 19300; second process → 19400, etc.).
        """
        strategy = AmazonIdentityStrategy()
        pool = super().init(cookie_entries, strategy, base_port=base_port)
        logger.info(
            "[CookieBrowserPool] Initialised with %d Amazon slot(s), base_port=%d.",
            len(pool),
            base_port,
        )
        return pool  # type: ignore[return-value]

    @classmethod
    def from_cookie_files(
        cls,
        paths: list[str],
        *,
        base_port: int = _BASE_BROWSER_PORT,
    ) -> CookieBrowserPool:
        """
        Load from cookie-cache JSON files (same format as AmazonCookieHelper).
        Each file becomes one slot; the file path is stored as ``slot.cache_file``
        so fresh browser cookies are written back to the originating file.
        """
        entries: list[dict] = []
        for p in paths:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            data["_cache_file"] = p
            entries.append(data)
        return cls.init(entries, base_port=base_port)

    @classmethod
    def from_cookie_helper(
        cls,
        *helpers: AmazonCookieHelper,
        base_port: int = _BASE_BROWSER_PORT,
    ) -> CookieBrowserPool:
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
        return cls.init(entries, base_port=base_port)

    @classmethod
    def get_instance(cls) -> CookieBrowserPool | None:
        return cls._instance  # type: ignore[return-value]
