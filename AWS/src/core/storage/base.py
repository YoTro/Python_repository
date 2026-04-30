from __future__ import annotations
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """
    Minimal interface for public-URL file storage.

    Implementations: S3CompatibleBackend (R2 / S3 / MinIO)
                     LocalHTTPBackend    (VPS dir + nginx/caddy)
    """

    @abstractmethod
    def upload(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        """Upload bytes and return the public HTTPS URL."""

    @abstractmethod
    def upload_file(self, key: str, file_path: str, content_type: str = "application/octet-stream") -> str:
        """Upload a local file and return the public HTTPS URL."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete an object (best-effort; implementations may no-op)."""
