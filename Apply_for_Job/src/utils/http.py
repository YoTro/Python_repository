"""
http.py - Shared HTTP session factory with retry + exponential back-off.

Usage:
    from src.utils.http import build_session

    session = build_session()                 # default: 3 retries
    session = build_session(retries=5, backoff_factor=1.0)
    resp = session.get(url, timeout=15)

Under the hood this uses urllib3's Retry + HTTPAdapter so it works with the
standard requests library without any extra dependencies.

If the optional `tenacity` package is installed, the helper
`with_tenacity(fn, *args, **kwargs)` provides application-level retries
(useful for DrissionPage calls that don't go through requests).
"""
from __future__ import annotations

import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Status codes that warrant a retry
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def build_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    proxies: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> requests.Session:
    """
    Return a requests.Session pre-configured with:
      - Automatic retry on transient errors (connection / read timeout / 5xx)
      - Exponential back-off: 0s, 0.5s, 1s, 2s … (backoff_factor × 2^(n-1))
      - Optional proxy and default headers
    """
    retry_cfg = Retry(
        total             = retries,
        backoff_factor    = backoff_factor,
        status_forcelist  = _RETRY_STATUSES,
        allowed_methods   = {"GET", "POST", "HEAD"},
        raise_on_status   = False,
    )
    adapter = HTTPAdapter(max_retries=retry_cfg)

    session = requests.Session()
    session.mount("http://",  adapter)
    session.mount("https://", adapter)

    if proxies:
        session.proxies.update(proxies)
    if headers:
        session.headers.update(headers)

    logger.debug("HTTP session created — retries=%d backoff=%.1f", retries, backoff_factor)
    return session


# ── Optional tenacity wrapper for non-requests call sites ────────────

def with_tenacity(fn, *args, max_attempts: int = 3, wait_seconds: float = 1.0, **kwargs):
    """
    Call `fn(*args, **kwargs)` with tenacity-based retry.
    Falls back to a simple loop if tenacity is not installed.
    """
    try:
        from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=wait_seconds, min=wait_seconds, max=30),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        def _call():
            return fn(*args, **kwargs)

        return _call()

    except ImportError:
        last_exc: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_exc = e
                import time
                wait = wait_seconds * (2 ** (attempt - 1))
                logger.warning("Attempt %d/%d failed (%s); retrying in %.1fs",
                               attempt, max_attempts, e, wait)
                time.sleep(wait)
        raise last_exc  # type: ignore[misc]
