from __future__ import annotations

"""
Slack Web API error code mappings.
Reference: https://api.slack.com/methods/chat.postMessage#errors

Display strings only — canonical ErrorCode classification lives in
src/core/errors/codes._API_CODE_MAP["slack"]. Keep both tables in sync
when adding new codes.
"""

# chat.postMessage / files.upload error strings — human-readable descriptions for logging/UI.
# Canonical classification (ErrorCode): use classify_api_code(code, "slack").
UPLOAD_ERROR_MAP: dict[str, str] = {
    "channel_not_found": "Channel/user not found — check the target id.",
    "not_in_channel": "Bot is not a member of this channel — invite it first.",
    "invalid_auth": "Unauthorized — check SLACK_*_BOT_TOKEN.",
    "account_inactive": "Bot token has been revoked or the workspace deactivated.",
    "missing_scope": "Bot Token is missing a required OAuth scope for this call.",
    "msg_too_long": "Message text exceeds Slack's length limit — split or attach as a file.",
    "ratelimited": "Slack API rate limit hit — back off and retry (see Retry-After header).",
    "file_uploads_disabled": "File uploads are disabled for this Slack workspace.",
    "invalid_blocks": "Block Kit payload failed Slack's schema validation.",
}


def slack_error_msg(code: str, fallback: str = "") -> str:
    """Return a human-readable description for a Slack error string."""
    return UPLOAD_ERROR_MAP.get(code, fallback or f"Slack error (code={code})")
