from __future__ import annotations
"""
S3-compatible storage backend.

Works with:
  - Cloudflare R2  endpoint_url = https://{account_id}.r2.cloudflarestorage.com
  - AWS S3         endpoint_url = (omit, or https://s3.amazonaws.com)
  - MinIO (VPS)    endpoint_url = https://minio.yourdomain.com
  - Backblaze B2   endpoint_url = https://s3.us-west-004.backblazeb2.com

Required env vars:
  STORAGE_ACCESS_KEY_ID
  STORAGE_SECRET_ACCESS_KEY
  STORAGE_BUCKET_NAME
  STORAGE_PUBLIC_URL         base URL for public access, e.g. https://pub-xxx.r2.dev

Optional (pick one endpoint strategy):
  CLOUDFLARE_R2_ACCOUNT_ID  auto-builds endpoint https://{id}.r2.cloudflarestorage.com
  STORAGE_ENDPOINT_URL      explicit override (MinIO / Backblaze / custom S3)
                            omit both for AWS S3 (boto3 default endpoint)
  STORAGE_REGION            default "auto" (R2/MinIO); use real region for AWS S3
"""
import logging
import os
from .base import StorageBackend

logger = logging.getLogger(__name__)


class S3CompatibleBackend(StorageBackend):
    def __init__(
        self,
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        bucket_name: str | None = None,
        public_url: str | None = None,
        region: str = "auto",
    ):
        self._bucket     = bucket_name       or os.environ["STORAGE_BUCKET_NAME"]
        self._public_url = (public_url       or os.environ["STORAGE_PUBLIC_URL"]).rstrip("/")
        _key             = access_key_id     or os.environ["STORAGE_ACCESS_KEY_ID"]
        _secret          = secret_access_key or os.environ["STORAGE_SECRET_ACCESS_KEY"]
        _region          = region            or os.getenv("STORAGE_REGION", "auto")

        # Endpoint priority:
        #   1. constructor arg
        #   2. STORAGE_ENDPOINT_URL (explicit, e.g. MinIO)
        #   3. CLOUDFLARE_R2_ACCOUNT_ID → auto-build R2 endpoint
        #   4. None → boto3 default (AWS S3)
        _r2_account = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
        _endpoint = (
            endpoint_url
            or os.getenv("STORAGE_ENDPOINT_URL")
            or (f"https://{_r2_account}.r2.cloudflarestorage.com" if _r2_account else None)
        )

        import boto3
        self._s3 = boto3.client(
            "s3",
            endpoint_url=_endpoint,          # None → standard AWS S3
            aws_access_key_id=_key,
            aws_secret_access_key=_secret,
            region_name=_region,
        )

    def upload(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        url = f"{self._public_url}/{key}"
        logger.info(f"[storage] uploaded {key} ({len(data)} bytes) → {url}")
        return url

    def upload_file(self, key: str, file_path: str, content_type: str = "application/octet-stream") -> str:
        with open(file_path, "rb") as f:
            return self.upload(key, f.read(), content_type)

    def delete(self, key: str) -> None:
        try:
            self._s3.delete_object(Bucket=self._bucket, Key=key)
            logger.info(f"[storage] deleted {key}")
        except Exception as e:
            logger.warning(f"[storage] delete {key} failed (ignored): {e}")
