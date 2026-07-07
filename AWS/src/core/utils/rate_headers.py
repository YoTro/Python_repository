"""
Parse API rate-limit headers and feed observed limits back into the token bucket.

Call observe_response() after every HTTP response from an external API.
On 2xx: the rate-limit header is present — update the in-memory bucket to
        match the server's actual limit so we self-calibrate over time.
On 4xx/429: the rate-limit header may be absent — still extract retry_after
            so callers can propagate the wait time via RetryableError.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# source → (rate_limit_header, unit, request_id_header, retry_after_header)
# unit: "rps" (requests per second) | "rpm" (requests per minute — will be /60)
_SOURCE_HEADER_CFG: dict[str, tuple[str | None, str, str | None, str | None]] = {
    "sp_api": (
        "x-amzn-RateLimit-Limit",  # float RPS; present on 2xx, ABSENT on 429
        "rps",
        "x-amzn-RequestId",
        None,  # Amazon does NOT include Retry-After on SP-API 429s
    ),
    "amazon_ads": (
        "x-ratelimit-limit",  # float RPM → divide by 60 for RPS
        "rpm",
        "x-amzn-RequestId",
        "Retry-After",
    ),
    "sellersprite": (
        None,
        "rps",
        None,
        "Retry-After",
    ),
    "xiyouzhaoci": (
        None,
        "rps",
        None,
        "Retry-After",
    ),
    "tiktok": (
        None,
        "rps",
        None,
        "Retry-After",
    ),
}


@dataclass
class ApiRateLimitHeaders:
    source: str
    store_id: str
    operation: str
    limit_rps: float | None  # None when header absent (e.g. on 429)
    request_id: str | None
    retry_after: float | None  # seconds; None when header absent


def observe_response(
    resp,  # requests.Response (or any object with .headers and .status_code)
    source: str,
    store_id: str = "default",
    operation: str = "default",
) -> ApiRateLimitHeaders:
    """
    Parse rate-limit headers from an API response and dynamically update
    the source's token-bucket rate when the server reports its actual limit.

    Always safe to call — never raises; any parse or update failure is
    swallowed and logged at DEBUG level so it can never crash the caller.
    """
    cfg = _SOURCE_HEADER_CFG.get(source, (None, "rps", None, None))
    rate_header, rate_unit, req_id_header, retry_after_header = cfg

    # ── rate limit ────────────────────────────────────────────────────────
    limit_rps: float | None = None
    if rate_header:
        raw = resp.headers.get(rate_header)
        if raw is not None:
            try:
                limit = float(raw)
                limit_rps = limit / 60.0 if rate_unit == "rpm" else limit
            except (ValueError, TypeError):
                logger.debug(f"[observe_response] Cannot parse {rate_header}={raw!r}")

    # ── request ID ────────────────────────────────────────────────────────
    request_id: str | None = None
    if req_id_header:
        request_id = resp.headers.get(req_id_header)

    # ── retry-after ───────────────────────────────────────────────────────
    retry_after: float | None = None
    if retry_after_header:
        raw = resp.headers.get(retry_after_header)
        if raw is not None:
            try:
                retry_after = float(raw)
            except (ValueError, TypeError):
                logger.debug(f"[observe_response] Cannot parse {retry_after_header}={raw!r}")

    # ── feed server-reported limit back into the token bucket ─────────────
    if limit_rps is not None:
        try:
            from src.gateway.rate_limit import RateLimiter

            RateLimiter().update_source_rate(
                source=source,
                rps=limit_rps,
                store_id=store_id,
                operation=operation,
            )
        except Exception as exc:
            logger.debug(f"[observe_response] update_source_rate failed: {exc}")

    if request_id:
        logger.debug(
            f"[{source}:{store_id}:{operation}] status={resp.status_code} "
            f"RequestId={request_id} limit_rps={limit_rps} retry_after={retry_after}"
        )

    return ApiRateLimitHeaders(
        source=source,
        store_id=store_id,
        operation=operation,
        limit_rps=limit_rps,
        request_id=request_id,
        retry_after=retry_after,
    )
