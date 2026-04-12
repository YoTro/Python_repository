from __future__ import annotations
"""
Feishu (Lark) API error code mappings.
Reference: https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/im-v1/file/create
"""

# IM / file upload error codes
UPLOAD_ERROR_MAP: dict[int, str] = {
    232096: "App meta writing has stopped, please try again later.",
    234001: "Invalid request param — check file_type (opus/mp4/pdf/doc/xls/ppt/stream) and file_name.",
    234002: "Unauthorized — Feishu API authentication failed.",
    234006: "File size exceeds limit (file ≤ 30 MB, image ≤ 10 MB).",
    234007: "App does not have bot capability enabled — enable it in Feishu Open Platform console.",
    234010: "File is empty (0 bytes) — do not upload zero-byte files.",
    234041: "Tenant master key deleted — contact the Feishu tenant administrator.",
    234042: "Tenant storage error or full — contact the Feishu tenant administrator.",
}


def feishu_error_msg(code: int, fallback: str = "") -> str:
    """Return a human-readable description for a Feishu error code."""
    return UPLOAD_ERROR_MAP.get(code, fallback or f"Feishu error (code={code})")
