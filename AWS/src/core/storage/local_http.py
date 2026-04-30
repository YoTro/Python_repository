from __future__ import annotations
"""
Local-HTTP storage backend.

Files are written to a directory on the VPS that is served by nginx/caddy
over HTTPS.  No S3 dependency needed.

Required env vars:
  STORAGE_LOCAL_DIR    absolute path served by nginx, e.g. /var/www/files
  STORAGE_PUBLIC_URL   matching public base URL,      e.g. https://files.yourdomain.com

nginx config example:
  server {
      listen 443 ssl;
      server_name files.yourdomain.com;
      root /var/www/files;
      location / { autoindex off; }
  }
"""
import logging
import os
from .base import StorageBackend

logger = logging.getLogger(__name__)


class LocalHTTPBackend(StorageBackend):
    def __init__(
        self,
        local_dir: str | None = None,
        public_url: str | None = None,
    ):
        self._dir        = local_dir  or os.environ["STORAGE_LOCAL_DIR"]
        self._public_url = (public_url or os.environ["STORAGE_PUBLIC_URL"]).rstrip("/")
        os.makedirs(self._dir, exist_ok=True)

    def upload(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        dest = os.path.join(self._dir, key)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "wb") as f:
            f.write(data)
        url = f"{self._public_url}/{key}"
        logger.info(f"[storage] written {key} ({len(data)} bytes) → {url}")
        return url

    def upload_file(self, key: str, file_path: str, content_type: str = "application/octet-stream") -> str:
        with open(file_path, "rb") as f:
            return self.upload(key, f.read(), content_type)

    def delete(self, key: str) -> None:
        path = os.path.join(self._dir, key)
        try:
            os.remove(path)
            logger.info(f"[storage] deleted {path}")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning(f"[storage] delete {path} failed (ignored): {e}")
