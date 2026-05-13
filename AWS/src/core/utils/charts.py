from __future__ import annotations

import io
import logging
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

CHART_PALETTE: dict[str, str] = {
    "blue":       "#2563EB",
    "orange":     "#F59E0B",
    "red":        "#EF4444",
    "green":      "#10B981",
    "purple":     "#8B5CF6",
    "grey":       "#9CA3AF",
    "light_blue": "#BFDBFE",
    "light_red":  "#FEE2E2",
    "bg":         "#F9FAFB",
}


def fig_to_png(fig: plt.Figure) -> bytes:
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        return buf.getvalue()
    finally:
        plt.close(fig)


def chart_upload(png: bytes, key: str) -> Optional[str]:
    try:
        from src.core.storage import get_storage_backend
        return get_storage_backend().upload(key, png, "image/png")
    except Exception as e:
        logger.warning(f"[charts] upload failed for {key}: {e}")
        return None
