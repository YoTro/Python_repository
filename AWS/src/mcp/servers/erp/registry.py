from __future__ import annotations

from .base import ERPClient

_PROVIDERS: dict[str, type[ERPClient]] = {}


def register_provider(name: str, cls: type[ERPClient]) -> None:
    _PROVIDERS[name] = cls


def get_erp_client(provider: str = "lingxing", **kwargs) -> ERPClient:
    """
    Instantiate the ERPClient for the given provider name.

    Usage:
        client = get_erp_client("lingxing")
        client = get_erp_client("lingxing", account="...", password="...")

    To add a new ERP:
        1. Create src/mcp/servers/erp/<name>/ with a client.py subclassing ERPClient
        2. Call register_provider("<name>", MyClient) in that module's __init__.py
    """
    cls = _PROVIDERS.get(provider)
    if cls is None:
        available = list(_PROVIDERS.keys())
        raise ValueError(f"Unknown ERP provider '{provider}'. Available: {available}")
    return cls(**kwargs)
