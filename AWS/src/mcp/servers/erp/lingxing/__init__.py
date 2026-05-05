from __future__ import annotations
from .client import LingxingClient
from ..registry import register_provider

register_provider("lingxing", LingxingClient)

__all__ = ["LingxingClient"]
