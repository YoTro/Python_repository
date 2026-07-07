from __future__ import annotations

import contextlib
import logging
import os
import threading
import time
from dataclasses import dataclass, field

from src.core.utils.config_helper import ConfigHelper

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lua script for atomic Redis token-bucket (registered once on startup).
# Returns [granted (0|1), wait_ms (int)] so the caller knows whether to
# wait and for how long before retrying without a second round-trip.
# ---------------------------------------------------------------------------
_BUCKET_LUA = """
local key          = KEYS[1]
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local now          = tonumber(ARGV[3])
local h = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(h[1])
local ts     = tonumber(h[2])
if not tokens then tokens = capacity; ts = now end
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill_rate)
local granted = 0
local wait_ms = 0
if tokens >= 1.0 then
    tokens  = tokens - 1.0
    granted = 1
else
    wait_ms = math.ceil((1.0 - tokens) / refill_rate * 1000)
end
local ttl = math.ceil(capacity / refill_rate) + 60
redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', key, ttl)
return {granted, wait_ms}
"""


@dataclass
class _TokenBucket:
    """In-memory token bucket for a single (source, store_id, operation) triple."""

    capacity: float
    tokens: float
    refill_rate: float  # tokens per second
    last_refill: float = field(default_factory=time.monotonic)
    lock: threading.Lock = field(default_factory=threading.Lock)


def _bucket_key(source: str, store_id: str, operation: str) -> str:
    """Composite key used for both in-memory dict and Redis namespace."""
    return f"rate:{source}:{store_id}:{operation}"


class RateLimiter:
    """
    Three-layer rate limiter (singleton).

    RL-1 (Entry)   — cooldown debounce + concurrent slot per entry-type/chat
                      RL-1a: per-chat cooldown  RL-1b: concurrency slot
    RL-2 (Tenant)  — daily request quota per tenant/plan-tier
    RL-3 (Source)  — token-bucket throttling for each external API

    RL-3 supports two backends:
      • In-memory  — default; fast, single-process.
      • Redis      — enabled when REDIS_URL is set; atomic across workers.

    Bucket keys use the composite format  rate:{source}:{store_id}:{operation}
    so different stores (US/DE) and different SP-API endpoints each get their
    own independent bucket.  Callers that do not supply store_id or operation
    fall back to the "default" placeholder for full backward compatibility.

    Concurrency design:
      - check_limit()     : fast gate at dispatch time (cooldown + daily quota)
      - concurrent_slot() : async context manager used inside JobManager._run_job()
                            Acquires slot on entry, releases in finally — no counter leaks
                            even if the job crashes mid-execution.
    """

    _instance: RateLimiter | None = None
    _init_lock = threading.Lock()

    def __new__(cls) -> RateLimiter:
        with cls._init_lock:
            if cls._instance is None:
                obj = super().__new__(cls)
                obj._setup()
                cls._instance = obj
        return cls._instance

    # ── Initialisation ────────────────────────────────────────────────────

    def _setup(self) -> None:
        self._config: dict = ConfigHelper.get_section("rate_limits")
        logger.debug("[RateLimiter] Loaded rate_limits config from ConfigHelper")

        # RL-3: token buckets keyed by composite string
        self._source_buckets: dict[str, _TokenBucket] = {}
        self._bucket_lock = threading.Lock()  # guards dynamic bucket creation

        for source, cfg in self._config.get("source_limits", {}).items():
            if "requests_per_minute" in cfg:
                # Flat format — one bucket per source
                rpm = float(cfg["requests_per_minute"])
                burst = float(cfg.get("burst", max(1, rpm // 10)))
                key = _bucket_key(source, "default", "default")
                self._source_buckets[key] = _TokenBucket(
                    capacity=burst, tokens=burst, refill_rate=rpm / 60.0
                )
            elif "requests_per_second" in cfg:
                # Flat RPS format — one bucket per source
                rps = float(cfg["requests_per_second"])
                burst = float(cfg.get("burst", max(1.0, rps)))
                key = _bucket_key(source, "default", "default")
                self._source_buckets[key] = _TokenBucket(
                    capacity=burst, tokens=burst, refill_rate=rps
                )
            else:
                # Per-operation nested format — one bucket per operation
                for operation, op_cfg in cfg.items():
                    if not isinstance(op_cfg, dict):
                        continue
                    rps = float(
                        op_cfg.get(
                            "requests_per_second",
                            op_cfg.get("requests_per_minute", 30) / 60.0,
                        )
                    )
                    burst = float(op_cfg.get("burst", max(1.0, rps)))
                    key = _bucket_key(source, "default", operation)
                    self._source_buckets[key] = _TokenBucket(
                        capacity=burst, tokens=burst, refill_rate=rps
                    )

        # Optional Redis backend
        self._redis = None
        self._lua_script = None
        redis_url = os.getenv("REDIS_URL")
        if redis_url:
            try:
                import redis

                self._redis = redis.from_url(redis_url, decode_responses=True)
                self._lua_script = self._redis.register_script(_BUCKET_LUA)
                self._redis.ping()
                logger.info("[RateLimiter] Redis token-bucket backend active")
            except Exception as exc:
                logger.warning(f"[RateLimiter] Redis unavailable ({exc}); using in-memory buckets")
                self._redis = None
                self._lua_script = None

        # RL-2: daily counters  {tenant_id: {"YYYY-MM-DD": count}}
        self._tenant_counters: dict[str, dict[str, int]] = {}
        self._tenant_lock = threading.Lock()

        # RL-1a: last trigger timestamp per chat_id  {chat_id: monotonic_ts}
        self._chat_last: dict[str, float] = {}
        self._chat_lock = threading.Lock()

        # RL-1b: concurrency counters (single dict, two key patterns)
        #   global:   entry_type            → int
        #   per-chat: f"{entry_type}:{chat_id}" → int
        self._concurrent: dict[str, int] = {}
        self._concurrent_lock = threading.Lock()

    # ── RL-3 helpers ───────────────────────────────────────────────────

    def _get_bucket(self, source: str, store_id: str, operation: str) -> _TokenBucket | None:
        """
        Look up a bucket with store/operation specificity, falling back through:
          rate:{source}:{store_id}:{operation}
          rate:{source}:default:{operation}
          rate:{source}:default:default
        Returns None when this source has no configured limit at all.
        """
        for key in (
            _bucket_key(source, store_id, operation),
            _bucket_key(source, "default", operation),
            _bucket_key(source, "default", "default"),
        ):
            b = self._source_buckets.get(key)
            if b is not None:
                return b
        return None

    def _redis_acquire(
        self, source: str, store_id: str, operation: str, timeout: float
    ) -> tuple[bool, float]:
        """
        Execute the Lua token-bucket script on Redis.
        Returns (granted, wait_seconds).  Falls back to (True, 0) on error.
        """
        bucket = self._get_bucket(source, store_id, operation)
        if bucket is None:
            return True, 0.0

        key = _bucket_key(source, store_id, operation)
        deadline = time.monotonic() + timeout
        while True:
            try:
                result = self._lua_script(
                    keys=[key],
                    args=[bucket.capacity, bucket.refill_rate, time.time()],
                )
                granted, wait_ms = int(result[0]), int(result[1])
                if granted:
                    return True, 0.0
                wait_sec = wait_ms / 1000.0
                if time.monotonic() + wait_sec > deadline:
                    logger.warning(
                        f"[RateLimiter] Redis bucket '{key}' timeout after {timeout:.0f}s"
                    )
                    return False, 0.0
                return False, wait_sec
            except Exception as exc:
                logger.debug(f"[RateLimiter] Redis acquire error ({exc}); allow through")
                return True, 0.0

    # ── RL-3: Source token bucket (public API) ─────────────────────────

    def get_source_config(self, source: str) -> dict:
        """Return the raw config dict for a source (empty dict if unconfigured)."""
        return self._config.get("source_limits", {}).get(source, {})

    def update_source_rate(
        self,
        source: str,
        rps: float,
        store_id: str = "default",
        operation: str = "default",
    ) -> None:
        """
        Dynamically update a bucket's refill_rate to match the server-reported
        rate limit (from the x-amzn-RateLimit-Limit response header).

        Creates a new in-memory bucket on the fly if one doesn't exist yet.
        The update only adjusts refill_rate; existing token count is preserved
        to avoid artificially granting extra burst capacity.
        """
        key = _bucket_key(source, store_id, operation)
        with self._bucket_lock:
            bucket = self._source_buckets.get(key)
            if bucket is None:
                # First observation for this (source, store, operation) triple
                burst = max(1.0, rps)
                self._source_buckets[key] = _TokenBucket(
                    capacity=burst, tokens=burst, refill_rate=rps
                )
                logger.debug(
                    f"[RateLimiter] Created bucket '{key}' rps={rps:.4f} burst={burst:.1f}"
                )
            elif abs(bucket.refill_rate - rps) > 0.001:
                # Server-reported rate differs — self-calibrate
                logger.debug(
                    f"[RateLimiter] Updated bucket '{key}' rps {bucket.refill_rate:.4f} → {rps:.4f}"
                )
                bucket.refill_rate = rps
                bucket.capacity = max(bucket.capacity, rps)

    def acquire_source(
        self,
        source: str,
        timeout: float = 30.0,
        store_id: str = "default",
        operation: str = "default",
    ) -> bool:
        """
        Synchronous blocking acquire.  Use only from non-async call sites.
        Prefer async_acquire_source() inside coroutines to avoid blocking the event loop.
        """
        if self._redis is not None and self._lua_script is not None:
            deadline = time.monotonic() + timeout
            while True:
                granted, wait_sec = self._redis_acquire(source, store_id, operation, timeout)
                if granted:
                    return True
                if not wait_sec or time.monotonic() + wait_sec > deadline:
                    return False
                time.sleep(min(wait_sec, 0.5))
            # unreachable
            return False

        # In-memory path
        bucket = self._get_bucket(source, store_id, operation)
        if bucket is None:
            return True

        deadline = time.monotonic() + timeout
        while True:
            with bucket.lock:
                now = time.monotonic()
                bucket.tokens = min(
                    bucket.capacity,
                    bucket.tokens + (now - bucket.last_refill) * bucket.refill_rate,
                )
                bucket.last_refill = now
                if bucket.tokens >= 1.0:
                    bucket.tokens -= 1.0
                    return True
                wait = (1.0 - bucket.tokens) / bucket.refill_rate

            if time.monotonic() + wait > deadline:
                logger.warning(
                    f"[RateLimiter] '{_bucket_key(source, store_id, operation)}' "
                    f"token-bucket timeout after {timeout:.0f}s"
                )
                return False
            time.sleep(min(wait, 0.5))

    async def async_acquire_source(
        self,
        source: str,
        timeout: float = 30.0,
        store_id: str = "default",
        operation: str = "default",
    ) -> bool:
        """
        Async-native token-bucket acquire for use inside coroutines.
        Yields to the event loop with asyncio.sleep() instead of blocking.
        Returns False if the timeout is exceeded.
        """
        import asyncio

        if self._redis is not None and self._lua_script is not None:
            deadline = time.monotonic() + timeout
            while True:
                granted, wait_sec = self._redis_acquire(source, store_id, operation, timeout)
                if granted:
                    return True
                if not wait_sec or time.monotonic() + wait_sec > deadline:
                    return False
                await asyncio.sleep(min(wait_sec, 0.5))
            # unreachable
            return False

        # In-memory path
        bucket = self._get_bucket(source, store_id, operation)
        if bucket is None:
            return True

        deadline = time.monotonic() + timeout
        while True:
            with bucket.lock:
                now = time.monotonic()
                bucket.tokens = min(
                    bucket.capacity,
                    bucket.tokens + (now - bucket.last_refill) * bucket.refill_rate,
                )
                bucket.last_refill = now
                if bucket.tokens >= 1.0:
                    bucket.tokens -= 1.0
                    return True
                wait = (1.0 - bucket.tokens) / bucket.refill_rate

            if time.monotonic() + wait > deadline:
                logger.warning(
                    f"[RateLimiter] '{_bucket_key(source, store_id, operation)}' "
                    f"async token-bucket timeout after {timeout:.0f}s"
                )
                return False
            await asyncio.sleep(min(wait, 0.5))

    # ── RL-2: Tenant daily quota ───────────────────────────────────────

    def _check_tenant_quota(self, tenant_id: str, plan_tier: str) -> bool:
        """Increment daily counter and return False if limit exceeded."""
        daily_limit: int = (
            self._config.get("tenant_quotas", {}).get(plan_tier, {}).get("daily_requests", -1)
        )
        if daily_limit == -1:
            return True  # unlimited

        today = time.strftime("%Y-%m-%d")
        with self._tenant_lock:
            day_counts = self._tenant_counters.setdefault(tenant_id, {})
            count = day_counts.get(today, 0)
            if count >= daily_limit:
                logger.warning(
                    f"[RateLimiter] Tenant '{tenant_id}' ({plan_tier}) hit daily limit "
                    f"{daily_limit} (used: {count})"
                )
                return False
            day_counts[today] = count + 1
        return True

    # ── RL-1a: Per-chat cooldown ───────────────────────────────────────

    def _check_chat_cooldown(self, chat_id: str, cooldown: float) -> bool:
        """Return False (and skip timestamp update) if still within cooldown window."""
        if cooldown <= 0 or not chat_id:
            return True

        with self._chat_lock:
            now = time.monotonic()
            last = self._chat_last.get(chat_id)
            if last is None:
                # First trigger for this chat — always allow
                self._chat_last[chat_id] = now
                return True
            remaining = cooldown - (now - last)
            if remaining > 0:
                logger.warning(
                    f"[RateLimiter] Chat '{chat_id}' cooldown active, {remaining:.0f}s remaining"
                )
                return False
            self._chat_last[chat_id] = now
        return True

    # ── RL-1b: Concurrent slot context manager ─────────────────────────

    @contextlib.asynccontextmanager
    async def concurrent_slot(self, entry_type: str | None, chat_id: str | None):
        """
        Async context manager that holds a concurrency slot for the duration of job execution.
        Used inside JobManager._run_job() — NOT at the gateway dispatch point.

        Why here and not at dispatch:
          Feishu gateway returns job_id immediately (fire-and-forget). If the slot were
          acquired at dispatch, it would be released before the job even starts running.
          Placing acquire/release in _run_job ensures the slot covers actual execution time.

        Deadlock prevention:
          The finally block guarantees release even when the job raises an unhandled
          exception, gets cancelled, or the worker crashes mid-execution.

        Usage:
            async with RateLimiter().concurrent_slot(record.request.entry_type,
                                                     record.request.chat_id):
                await self._run_workflow_mode(record)
        """
        # No entry_type means no concurrency config to enforce
        if not entry_type:
            yield
            return

        entry_cfg = self._config.get("entry_limits", {}).get(entry_type, {})
        global_limit: int = entry_cfg.get("concurrent_jobs", 0)
        chat_limit: int = entry_cfg.get("per_chat_concurrent", 0)

        key_global = entry_type
        key_chat = f"{entry_type}:{chat_id}" if chat_id else None

        # ── Acquire (atomic check + increment under lock) ─────────────────
        with self._concurrent_lock:
            global_count = self._concurrent.get(key_global, 0)
            chat_count = self._concurrent.get(key_chat, 0) if key_chat else 0

            if global_limit > 0 and global_count >= global_limit:
                raise RuntimeError(
                    f"Global concurrent limit reached for '{entry_type}' "
                    f"({global_count}/{global_limit}). Try again later."
                )
            if key_chat and chat_limit > 0 and chat_count >= chat_limit:
                raise RuntimeError(
                    f"Per-chat concurrent limit reached for chat '{chat_id}' "
                    f"({chat_count}/{chat_limit}). Your previous job is still running."
                )

            self._concurrent[key_global] = global_count + 1
            if key_chat:
                self._concurrent[key_chat] = chat_count + 1

        logger.debug(
            f"[RateLimiter] Slot acquired: type={entry_type} chat={chat_id} "
            f"global={self._concurrent[key_global]}/{global_limit or '∞'} "
            + (f"chat={self._concurrent.get(key_chat, 0)}/{chat_limit or '∞'}" if key_chat else "")
        )

        try:
            yield
        finally:
            # ── Release (guaranteed even on exception / cancellation) ─────
            with self._concurrent_lock:
                self._concurrent[key_global] = max(0, self._concurrent.get(key_global, 1) - 1)
                if key_chat:
                    self._concurrent[key_chat] = max(0, self._concurrent.get(key_chat, 1) - 1)
            logger.debug(
                f"[RateLimiter] Slot released: type={entry_type} chat={chat_id} "
                f"global_remaining={self._concurrent.get(key_global, 0)}"
            )

    # ── Unified entry gate (called by APIGateway at dispatch time) ────────

    def check_limit(
        self,
        identity: dict[str, str],
        request_type: str = "cli_workflow",
        chat_id: str | None = None,
    ) -> bool:
        """
        Fast gate at dispatch time — checks cooldown debounce and daily tenant quota.
        Concurrency limits are enforced separately in concurrent_slot() at execution time.

        Returns True if allowed, False if any limit exceeded.
        Callers must raise AWSBaseError when this returns False.
        """
        tenant_id = identity.get("tenant_id", "default")
        plan_tier = identity.get("plan_tier", "free")

        # RL-1a: per-chat cooldown debounce
        cooldown = (
            self._config.get("entry_limits", {}).get(request_type, {}).get("cooldown_seconds", 0)
        )
        if not self._check_chat_cooldown(chat_id or "", cooldown):
            return False

        # RL-2: tenant daily quota
        if not self._check_tenant_quota(tenant_id, plan_tier):
            return False

        logger.debug(
            f"[RateLimiter] Allowed — tenant={tenant_id} tier={plan_tier} "
            f"type={request_type} chat={chat_id}"
        )
        return True
