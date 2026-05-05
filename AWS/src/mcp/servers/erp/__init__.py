from __future__ import annotations

# Register all built-in providers on import
from . import lingxing  # noqa: F401 — triggers register_provider("lingxing", LingxingClient)

from .base import ERPClient
from .registry import get_erp_client, register_provider

__all__ = ["ERPClient", "get_erp_client", "register_provider"]
