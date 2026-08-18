from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------


class _CacheBackend(ABC):
    @abstractmethod
    def get_raw(self, domain: str, key: str) -> dict[str, Any] | None:
        """Return the raw envelope {data, updated_at} or None."""

    @abstractmethod
    def set_raw(
        self, domain: str, key: str, envelope: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        """Persist the raw envelope. ttl_seconds, when given, lets the backend expire the
        key on its own (e.g. Redis EX) instead of relying solely on the read-time check in
        DataCache.get()."""

    @abstractmethod
    def exists(self, domain: str, key: str) -> bool:
        pass


class _JsonFileBackend(_CacheBackend):
    """Single-process, file-backed store. One JSON file per domain."""

    def __init__(self, cache_dir: str):
        self._cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._mem: dict[str, dict[str, Any]] = {}

    def _path(self, domain: str) -> str:
        return os.path.join(self._cache_dir, f"{domain}.json")

    def _load(self, domain: str) -> None:
        if domain in self._mem:
            return
        path = self._path(domain)
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self._mem[domain] = json.load(f)
            except Exception as e:
                logger.error(f"[cache] corrupt file for domain '{domain}': {e} — resetting")
                self._mem[domain] = {}
                try:
                    os.remove(path)
                except OSError:
                    pass
        else:
            self._mem[domain] = {}

    def get_raw(self, domain: str, key: str) -> dict[str, Any] | None:
        self._load(domain)
        return self._mem[domain].get(key)

    def set_raw(
        self, domain: str, key: str, envelope: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        # No native expiry mechanism for a plain JSON file; staleness is still enforced by
        # DataCache.get()'s updated_at check. ttl_seconds is accepted for interface parity
        # with _RedisBackend but not acted on here.
        self._load(domain)
        self._mem[domain][key] = envelope
        try:
            with open(self._path(domain), "w", encoding="utf-8") as f:
                json.dump(self._mem[domain], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[cache] failed to persist domain '{domain}': {e}")

    def exists(self, domain: str, key: str) -> bool:
        self._load(domain)
        return key in self._mem[domain]


class _RedisBackend(_CacheBackend):
    """Distributed, Redis-backed store."""

    def __init__(self, url: str):
        import redis

        self._url = url
        # decode_responses=True returns strings instead of bytes
        self._r = redis.from_url(url, decode_responses=True)
        logger.info(f"[cache] RedisBackend initialized with {url}")

    def _key(self, domain: str, key: str) -> str:
        return f"aws:cache:{domain}:{key}"

    def get_raw(self, domain: str, key: str) -> dict[str, Any] | None:
        try:
            raw = self._r.get(self._key(domain, key))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.error(f"[cache] Redis get failed: {e}")
            return None

    def set_raw(
        self, domain: str, key: str, envelope: dict[str, Any], ttl_seconds: int | None = None
    ) -> None:
        try:
            payload = json.dumps(envelope, ensure_ascii=False)
            # ex=ttl_seconds lets Redis reclaim the key on its own once it goes stale,
            # instead of relying on DataCache.get()'s read-time check — which only skips
            # stale data, it never deletes it, so unread keys would otherwise accumulate
            # in Redis forever.
            self._r.set(self._key(domain, key), payload, ex=ttl_seconds)
        except Exception as e:
            logger.error(f"[cache] Redis set failed: {e}")

    def exists(self, domain: str, key: str) -> bool:
        try:
            return bool(self._r.exists(self._key(domain, key)))
        except Exception as e:
            logger.error(f"[cache] Redis exists failed: {e}")
            return False


# ---------------------------------------------------------------------------
# Public DataCache — interface unchanged
# ---------------------------------------------------------------------------


class DataCache:
    """
    Core Data Cache for L1/L2 orchestration.

    L1 Servers (Amazon, Market, Social) write raw scraped data here.
    L2 Servers (Finance, Compliance) read from here to perform calculations.

    Backend is swappable (JsonFile by default, Redis if REDIS_URL is set).
    """

    def __init__(self, backend: _CacheBackend | None = None, cache_dir: str | None = None):
        if backend is not None:
            self._backend = backend
        else:
            redis_url = os.getenv("REDIS_URL")
            if redis_url:
                try:
                    self._backend = _RedisBackend(redis_url)
                except Exception as e:
                    logger.error(
                        f"[cache] Failed to initialize RedisBackend: {e} — falling back to JsonFile"
                    )
                    self._backend = self._init_json_backend(cache_dir)
            else:
                self._backend = self._init_json_backend(cache_dir)

    def _init_json_backend(self, cache_dir: str | None = None) -> _JsonFileBackend:
        _dir = cache_dir or os.path.join(os.path.dirname(__file__), "..", "..", "data", "cache")
        return _JsonFileBackend(_dir)

    @staticmethod
    def _to_serializable(value: Any) -> Any:
        """Recursively convert Pydantic models to plain dicts so json.dump never fails."""
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "dict"):
            return value.dict()
        if isinstance(value, list):
            return [DataCache._to_serializable(v) for v in value]
        if isinstance(value, dict):
            return {k: DataCache._to_serializable(v) for k, v in value.items()}
        return value

    def set(self, domain: str, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        envelope = {
            "data": self._to_serializable(value),
            "updated_at": datetime.utcnow().isoformat(),
        }
        self._backend.set_raw(domain, key, envelope, ttl_seconds=ttl_seconds)

    def get(self, domain: str, key: str, ttl_seconds: int | None = None) -> Any | None:
        envelope = self._backend.get_raw(domain, key)
        if not envelope:
            return None
        if ttl_seconds:
            age = (
                datetime.utcnow() - datetime.fromisoformat(envelope["updated_at"])
            ).total_seconds()
            if age > ttl_seconds:
                logger.info(f"[cache] {domain}:{key} expired ({int(age)}s > {ttl_seconds}s ttl)")
                return None
        return envelope["data"]

    def get_model(
        self, domain: str, key: str, model_class: Any, ttl_seconds: int | None = None
    ) -> Any | None:
        data = self.get(domain, key, ttl_seconds=ttl_seconds)
        if data is None:
            return None
        try:
            if isinstance(data, list):
                return [model_class.model_validate(item) for item in data]
            return model_class.model_validate(data)
        except Exception as e:
            logger.error(f"[cache] failed to reconstruct {model_class.__name__}: {e}")
            return None

    def exists(self, domain: str, key: str) -> bool:
        return self._backend.exists(domain, key)


# Global singleton
data_cache = DataCache()
