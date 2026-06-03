from __future__ import annotations

import io
import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as _fm
import matplotlib.pyplot as plt


def _first_available_font(names: list[str]) -> str | None:
    """Return the first font name that matplotlib can resolve on this system."""
    for name in names:
        try:
            _fm.findfont(_fm.FontProperties(family=name), fallback_to_default=False)
            return name
        except Exception:
            continue
    return None


# Prefer a Unicode font that covers both ASCII and CJK; fall back to DejaVu Sans.
# Arial Unicode MS (macOS), WenQuanYi / Noto CJK (Linux) cover the ideographic range.
_cjk_font = _first_available_font(
    [
        "Arial Unicode MS",
        "PingFang HK",
        "Hiragino Sans GB",
        "WenQuanYi Micro Hei",
        "Noto Sans CJK SC",
    ]
)
plt.rcParams["font.sans-serif"] = [_cjk_font, "DejaVu Sans"] if _cjk_font else ["DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

logger = logging.getLogger(__name__)

CHART_PALETTE: dict[str, str] = {
    "blue": "#2563EB",
    "orange": "#F59E0B",
    "red": "#EF4444",
    "green": "#10B981",
    "purple": "#8B5CF6",
    "grey": "#9CA3AF",
    "light_blue": "#BFDBFE",
    "light_red": "#FEE2E2",
    "bg": "#F9FAFB",
}


def fig_to_png(fig: plt.Figure) -> bytes:
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
        return buf.getvalue()
    finally:
        plt.close(fig)


def chart_upload(png: bytes, key: str) -> str | None:
    try:
        from src.core.storage import get_storage_backend

        return get_storage_backend().upload(key, png, "image/png")
    except Exception as e:
        logger.warning(f"[charts] upload failed for {key}: {e}")
        return None
