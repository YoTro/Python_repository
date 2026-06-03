from __future__ import annotations

from ..registry import register_provider
from .client import LingxingClient

register_provider("lingxing", LingxingClient)

__all__ = ["LingxingClient"]
