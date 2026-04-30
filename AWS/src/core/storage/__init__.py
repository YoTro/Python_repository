from __future__ import annotations
"""
Storage abstraction layer.

Usage:
    from src.core.storage import get_storage_backend
    storage = get_storage_backend()
    url = storage.upload("images/chart.png", png_bytes, "image/png")

Backend is selected by STORAGE_BACKEND env var:
    s3_compatible  (default) — R2 / S3 / MinIO
    local_http               — VPS directory + nginx/caddy
"""
import os
from .base import StorageBackend


def get_storage_backend() -> StorageBackend:
    backend = os.getenv("STORAGE_BACKEND", "s3_compatible").lower()
    if backend == "s3_compatible":
        from .s3_compatible import S3CompatibleBackend
        return S3CompatibleBackend()
    if backend == "local_http":
        from .local_http import LocalHTTPBackend
        return LocalHTTPBackend()
    raise ValueError(
        f"Unknown STORAGE_BACKEND={backend!r}. "
        "Supported: 's3_compatible', 'local_http'."
    )


__all__ = ["StorageBackend", "get_storage_backend"]
