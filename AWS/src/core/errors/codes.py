from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    """
    Canonical error codes for the AWS V2 platform.

    Format: "{category}.{condition}"

    All provider-specific codes (HTTP status, API string codes, ERP integer codes)
    are mapped to one of these values via classify_http() / classify_api_code() /
    classify_response_message(). Use is_retryable() / is_auth_error() to drive
    retry and re-auth logic without scattering provider-specific checks across clients.

    Sources:
      Amazon Ads  — advertising.amazon.com/API/docs/en-us/reference/concepts/errors
      DeepSeek    — platform.deepseek.com/api-docs/error-codes
      Gemini Exch — docs.gemini.com/rest-api/#errors
      Feishu      — upload error map (src/entry/feishu/const.py)
      Lingxing    — ERP token expiry codes (src/mcp/servers/erp/lingxing/client.py)
      Sellersprite — auth codes (src/mcp/servers/market/sellersprite/client.py)
    """

    # ── Auth / Token ──────────────────────────────────────────────────────────
    AUTH_TOKEN_EXPIRED = "auth.token_expired"  # HTTP 401 generic; Lingxing {401,"401",-1,-999}
    AUTH_SCOPE_MISSING = "auth.scope_missing"  # Amazon Ads: Scope header is missing
    AUTH_SCOPE_INVALID = (
        "auth.scope_invalid"  # Amazon Ads: no matching advertiser / not authorized for scope
    )
    AUTH_REQUIRED = "auth.required"  # interactive re-auth needed (QR, SMS)
    AUTH_IP_BLOCKED = "auth.ip_blocked"  # Sellersprite ERR_GLOBAL_403; HTTP 403 (generic)
    AUTH_FAILED = "auth.failed"  # wrong credentials, permanent (DeepSeek 401; Gemini 403)

    # ── Billing ───────────────────────────────────────────────────────────────
    BILLING_INSUFFICIENT = (
        "billing.insufficient_balance"  # HTTP 402 (DeepSeek); HTTP 406 (Gemini Exchange)
    )

    # ── Rate Limiting ─────────────────────────────────────────────────────────
    RATE_LIMITED = "rate.limited"  # HTTP 429, all external APIs

    # ── Resource ──────────────────────────────────────────────────────────────
    NOT_FOUND = "resource.not_found"  # HTTP 404
    DUPLICATE_REQUEST = "resource.duplicate"  # Amazon Ads HTTP/JSON 425

    # ── Validation ────────────────────────────────────────────────────────────
    INVALID_PARAMS = "validation.invalid_params"  # HTTP 400/422; Feishu 234001
    INVALID_HEADER = "validation.invalid_header"  # HTTP 406 (Accept), 415 (Content-Type)
    FILE_TOO_LARGE = "validation.file_too_large"  # Feishu 234006
    FILE_EMPTY = "validation.file_empty"  # Feishu 234010
    UNAUTHORIZED_APP = "validation.unauthorized_app"  # Feishu 234002, 234007

    # ── Server / Transient ────────────────────────────────────────────────────
    SERVER_ERROR = "server.error"  # HTTP 500 / 502 / 503 / 504
    TIMEOUT = "server.timeout"  # request or poll timeout

    # ── Content / Parsing ─────────────────────────────────────────────────────
    RESPONSE_TRUNCATED = "content.truncated"  # LLM finish_reason == max_tokens
    PARSE_ERROR = "content.parse_error"  # JSON decode failure, malformed body
    SOFT_BLOCKED = "content.soft_blocked"  # anti-bot / captcha wall

    # ── Storage ───────────────────────────────────────────────────────────────
    STORAGE_FULL = "storage.full"  # Feishu 234042
    STORAGE_ERROR = "storage.error"  # Feishu 234041, 232096

    # ── Catch-all ─────────────────────────────────────────────────────────────
    UNKNOWN = "unknown"


# ── Global HTTP status → canonical code ──────────────────────────────────────
# Default mapping used when no provider-specific override exists.
# 401 → AUTH_TOKEN_EXPIRED; call classify_response_message() to refine sub-variants.
# 406 → INVALID_HEADER here; Gemini Exchange overrides this to BILLING_INSUFFICIENT.
_HTTP_STATUS_MAP: dict[int, ErrorCode] = {
    400: ErrorCode.INVALID_PARAMS,  # Bad Request — general client error
    401: ErrorCode.AUTH_TOKEN_EXPIRED,  # Unauthorized — refined by response message
    402: ErrorCode.BILLING_INSUFFICIENT,  # Payment Required (DeepSeek: out of credits)
    403: ErrorCode.AUTH_IP_BLOCKED,  # Forbidden — refined per provider (see overrides)
    404: ErrorCode.NOT_FOUND,  # Not Found
    406: ErrorCode.INVALID_HEADER,  # Not Acceptable — bad Accept header (standard)
    415: ErrorCode.INVALID_HEADER,  # Unsupported Media Type
    422: ErrorCode.INVALID_PARAMS,  # Unprocessable Entity — wrong parameters
    425: ErrorCode.DUPLICATE_REQUEST,  # Amazon Ads duplicate report
    429: ErrorCode.RATE_LIMITED,  # Too Many Requests
    500: ErrorCode.SERVER_ERROR,  # Internal Error
    502: ErrorCode.SERVER_ERROR,  # Bad Gateway
    503: ErrorCode.SERVER_ERROR,  # Service Unavailable / Overloaded
    504: ErrorCode.SERVER_ERROR,  # Gateway Timeout
}

# ── Provider-specific HTTP status overrides ───────────────────────────────────
# Some providers assign non-standard meanings to standard HTTP status codes.
# These take priority over _HTTP_STATUS_MAP when a provider is specified.
#
# Gemini Exchange (crypto) reuses HTTP codes in non-standard ways:
#   406 → Insufficient Funds  (standard 406 = "Not Acceptable")
#   403 → Missing API key role (standard 403 = IP/access block; here it's a key permission issue)
_PROVIDER_HTTP_OVERRIDES: dict[str, dict[int, ErrorCode]] = {
    "gemini_exchange": {
        403: ErrorCode.AUTH_FAILED,  # API key missing required role — fix the key, not a block
        406: ErrorCode.BILLING_INSUFFICIENT,  # Insufficient Funds — non-standard reuse of 406
    },
}

# ── Provider API code → canonical code ───────────────────────────────────────
# Keys are exact values returned in API response bodies (int or str).
# Provider names match the source_limits keys in config/settings.json.
_API_CODE_MAP: dict[str, dict[int | str, ErrorCode]] = {
    "lingxing": {
        # src/mcp/servers/erp/lingxing/client.py: _TOKEN_EXPIRED_CODES
        401: ErrorCode.AUTH_TOKEN_EXPIRED,
        "401": ErrorCode.AUTH_TOKEN_EXPIRED,
        -1: ErrorCode.AUTH_TOKEN_EXPIRED,
        -999: ErrorCode.AUTH_TOKEN_EXPIRED,
    },
    "feishu": {
        # src/entry/feishu/const.py: UPLOAD_ERROR_MAP
        232096: ErrorCode.STORAGE_ERROR,
        234001: ErrorCode.INVALID_PARAMS,
        234002: ErrorCode.UNAUTHORIZED_APP,
        234006: ErrorCode.FILE_TOO_LARGE,
        234007: ErrorCode.UNAUTHORIZED_APP,
        234010: ErrorCode.FILE_EMPTY,
        234041: ErrorCode.STORAGE_ERROR,
        234042: ErrorCode.STORAGE_FULL,
    },
    "sellersprite": {
        # src/mcp/servers/market/sellersprite/client.py
        "ERR_GLOBAL_403": ErrorCode.AUTH_IP_BLOCKED,
    },
    "amazon_ads": {
        # HTTP 200 with JSON body {"code": "425"} — duplicate report creation
        "425": ErrorCode.DUPLICATE_REQUEST,
    },
}

# ── Provider response message → canonical code ────────────────────────────────
# Used when the same HTTP status carries multiple distinct meanings distinguishable
# only by the response body text. First match wins — order most-specific first.
_API_MESSAGE_MAP: dict[str, list[tuple[str, ErrorCode]]] = {
    "amazon_ads": [
        # Three 401 sub-variants (official error reference)
        ("scope header is missing", ErrorCode.AUTH_SCOPE_MISSING),
        ("no matching advertiser found", ErrorCode.AUTH_SCOPE_INVALID),
        ("not authorized to access scope", ErrorCode.AUTH_SCOPE_INVALID),
        ("not authorized to manage this profile", ErrorCode.AUTH_SCOPE_INVALID),
    ],
    "lingxing": [
        # src/mcp/servers/erp/lingxing/client.py: _TOKEN_EXPIRED_KEYWORDS
        ("未登录", ErrorCode.AUTH_TOKEN_EXPIRED),
        ("登录已过期", ErrorCode.AUTH_TOKEN_EXPIRED),
        ("token", ErrorCode.AUTH_TOKEN_EXPIRED),
    ],
    "deepseek": [
        # DeepSeek 401 = wrong API key (permanent) — not a refreshable token
        ("authentication fails", ErrorCode.AUTH_FAILED),
        ("wrong api key", ErrorCode.AUTH_FAILED),
        ("invalid api key", ErrorCode.AUTH_FAILED),
    ],
    "gemini_exchange": [
        # 400 bundles three distinct causes; refine from the message body
        ("market not open", ErrorCode.SERVER_ERROR),  # transient, retry later
        ("missing private api key", ErrorCode.AUTH_TOKEN_EXPIRED),  # auth header absent
        ("invalid signature", ErrorCode.AUTH_TOKEN_EXPIRED),  # HMAC mismatch
    ],
}

# ── Retry semantics ───────────────────────────────────────────────────────────
_RETRYABLE: frozenset[ErrorCode] = frozenset(
    {
        ErrorCode.RATE_LIMITED,
        ErrorCode.SERVER_ERROR,
        ErrorCode.TIMEOUT,
        ErrorCode.AUTH_TOKEN_EXPIRED,  # retryable after token refresh
        # NOT retryable (require human action or config fix):
        # AUTH_SCOPE_MISSING / AUTH_SCOPE_INVALID — missing header or wrong profile ID
        # AUTH_FAILED        — wrong API key or wrong role
        # BILLING_INSUFFICIENT — add funds first
        # INVALID_HEADER     — fix Accept / Content-Type header in code
    }
)

_AUTH_ERRORS: frozenset[ErrorCode] = frozenset(
    {
        ErrorCode.AUTH_TOKEN_EXPIRED,
        ErrorCode.AUTH_SCOPE_MISSING,
        ErrorCode.AUTH_SCOPE_INVALID,
        ErrorCode.AUTH_REQUIRED,
        ErrorCode.AUTH_IP_BLOCKED,
        ErrorCode.AUTH_FAILED,
    }
)

# Suggested wait (seconds) before retrying. Prefer Retry-After headers;
# use this as a fallback floor.
_DEFAULT_RETRY_AFTER: dict[ErrorCode, float] = {
    ErrorCode.RATE_LIMITED: 60.0,
    ErrorCode.SERVER_ERROR: 5.0,
    ErrorCode.TIMEOUT: 2.0,
    ErrorCode.AUTH_TOKEN_EXPIRED: 0.0,  # immediate after token refresh
}


# ── Public helpers ────────────────────────────────────────────────────────────


def classify_http(status: int, provider: str = "") -> ErrorCode:
    """Map an HTTP status code to a canonical ErrorCode.

    Provider-specific overrides are checked first — some APIs reuse standard
    HTTP codes with non-standard meanings (e.g. Gemini Exchange uses 406 for
    Insufficient Funds instead of the standard "Not Acceptable").

    Falls back to SERVER_ERROR for any unmapped 5xx, UNKNOWN otherwise.
    Call classify_response_message() afterwards to refine 401 sub-variants
    for amazon_ads, deepseek, gemini_exchange, and lingxing.
    """
    if provider:
        override = _PROVIDER_HTTP_OVERRIDES.get(provider, {}).get(status)
        if override is not None:
            return override
    if status in _HTTP_STATUS_MAP:
        return _HTTP_STATUS_MAP[status]
    if status >= 500:
        return ErrorCode.SERVER_ERROR
    return ErrorCode.UNKNOWN


def classify_api_code(code: int | str, provider: str) -> ErrorCode:
    """Map a provider-specific API response code to a canonical ErrorCode.

    ``provider`` must match a key in _API_CODE_MAP (e.g. "lingxing", "feishu").
    Returns UNKNOWN for unrecognised codes or unknown providers.
    """
    return _API_CODE_MAP.get(provider, {}).get(code, ErrorCode.UNKNOWN)


def classify_response_message(message: str, provider: str) -> ErrorCode:
    """Refine an error code from the response body message text.

    Use after classify_http() when the same HTTP status can carry multiple
    meanings (e.g. Amazon Ads 401 has three sub-variants; Gemini Exchange 400
    covers market-closed, malformed request, and missing auth headers).

    Returns UNKNOWN if no pattern matches — leave the caller's existing code intact.

    Example::

        code = classify_http(resp.status_code, provider="amazon_ads")
        if code in (ErrorCode.AUTH_TOKEN_EXPIRED, ErrorCode.INVALID_PARAMS):
            refined = classify_response_message(
                resp.json().get("message", ""), "amazon_ads"
            )
            if refined != ErrorCode.UNKNOWN:
                code = refined
    """
    patterns = _API_MESSAGE_MAP.get(provider, [])
    lower = message.lower()
    for substring, error_code in patterns:
        if substring.lower() in lower:
            return error_code
    return ErrorCode.UNKNOWN


def is_retryable(code: ErrorCode) -> bool:
    """Return True if the error is transient and the call should be retried."""
    return code in _RETRYABLE


def is_auth_error(code: ErrorCode) -> bool:
    """Return True if the error is auth-related and may require re-login."""
    return code in _AUTH_ERRORS


def default_retry_after(code: ErrorCode) -> float:
    """Suggested minimum wait (seconds) before retrying.

    Prefer Retry-After response headers; treat this as a fallback floor.
    """
    return _DEFAULT_RETRY_AFTER.get(code, 0.0)
