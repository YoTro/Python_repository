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
]


class CookieBrowserPool(IdentityPool):
    """
    Named registry of Amazon identity pools.

    Tier 1/2 callers use ``next_slot()`` (round-robin, never blocks).
    Tier 3 callers do::

        async with slot.browser_lock:
            bp = slot.get_or_init_browser()
            ...

    Multiple pools can coexist by name (e.g. ``"amazon_us"``, ``"amazon_jp"``).
    The default name is ``"amazon"`` so existing no-arg callers are unaffected.
    """

    _DEFAULT_NAME: str = "amazon"
    _registry: dict[str, CookieBrowserPool] = {}  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Factory / registry
    # ------------------------------------------------------------------

    @classmethod
    def init(
        cls,
        cookie_entries: list[dict],
        *,
        name: str = "",
        base_port: int = _BASE_BROWSER_PORT,
    ) -> CookieBrowserPool:
        """
        Create (or replace) a named pool from a list of cookie-entry dicts::

            {
                "cookies":    {"session-id": "...", ...},   # required
                "user_agent": "Mozilla/5.0 ...",             # optional
                "proxy":      "http://user:pass@host:port",  # optional
            }

        ``name`` identifies the pool (default ``"amazon"``).  Use distinct
        names for separate marketplaces or tenants; use distinct ``base_port``
        values for each pool on the same host to avoid CDP port collisions.
        """
        strategy = AmazonIdentityStrategy()
        return super().init(
            cookie_entries, strategy, name=name or cls._DEFAULT_NAME, base_port=base_port
        )  # type: ignore[return-value]

    @classmethod
    def from_cookie_files(
        cls,
        paths: list[str],
        *,
        name: str = "",
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
        return cls.init(entries, name=name, base_port=base_port)

    @classmethod
    def from_cookie_helper(
        cls,
        *helpers: AmazonCookieHelper,
        name: str = "",
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
        return cls.init(entries, name=name, base_port=base_port)

    @classmethod
    def get_instance(cls, name: str = "") -> CookieBrowserPool | None:
        return cls._registry.get(name or cls._DEFAULT_NAME)  # type: ignore[return-value]
