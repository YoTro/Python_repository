from __future__ import annotations
import contextlib
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from src.core.utils.config_helper import ConfigHelper

logger = logging.getLogger(__name__)


@dataclass
class _TokenBucket:
    """Token bucket for Layer 3 source-level throttling."""
    capacity: float
    tokens: float
    refill_rate: float          # tokens per second
    last_refill: float = field(default_factory=time.monotonic)
    lock: threading.Lock = field(default_factory=threading.Lock)


class RateLimiter:
    """
    Three-layer rate limiter (singleton).

    Layer 1 (Entry)   — cooldown debounce + concurrent slot per entry-type/chat
    Layer 2 (Tenant)  — daily request quota per tenant/plan-tier
    Layer 3 (Source)  — token-bucket throttling for each external API

    Concurrency design:
      - check_limit()     : fast gate at dispatch time (cooldown + daily quota)
      - concurrent_slot() : async context manager used inside JobManager._run_job()
                            Acquires slot on entry, releases in finally — no counter leaks
                            even if the job crashes mid-execution.
    """

    _instance: Optional[RateLimiter] = None
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

        # Layer 3: one token bucket per source (key: source_limits)
        self._source_buckets: Dict[str, _TokenBucket] = {}
        for source, cfg in self._config.get("source_limits", {}).items():
            rpm = float(cfg.get("requests_per_minute", 60))
            burst = float(cfg.get("burst", max(1, rpm // 10)))
            self._source_buckets[source] = _TokenBucket(
                capacity=burst,
                tokens=burst,
                refill_rate=rpm / 60.0,
            )

        # Layer 2: daily counters  {tenant_id: {"YYYY-MM-DD": count}}
        self._tenant_counters: Dict[str, Dict[str, int]] = {}
        self._tenant_lock = threading.Lock()

        # Layer 1a: last trigger timestamp per chat_id  {chat_id: monotonic_ts}
        self._chat_last: Dict[str, float] = {}
        self._chat_lock = threading.Lock()

        # Layer 1b: concurrency counters (single dict, two key patterns)
        #   global:   entry_type            → int
        #   per-chat: f"{entry_type}:{chat_id}" → int
        self._concurrent: Dict[str, int] = {}
        self._concurrent_lock = threading.Lock()

    # ── Layer 3: Source token bucket ─────────────────────────────────────

    def get_source_config(self, source: str) -> dict:
        """Return the raw config dict for a source (empty dict if unconfigured)."""
        return self._config.get("source_limits", {}).get(source, {})

    def acquire_source(self, source: str, timeout: float = 30.0) -> bool:
        """
        Block until a token is available for *source*, then consume it.
        Returns False if *timeout* is exceeded — caller should abort or raise.
        Call this at the start of each external API _request() method.
        """
        bucket = self._source_buckets.get(source)
        if bucket is None:
            return True  # unconfigured source — allow freely

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
                    f"[RateLimiter] Source '{source}' token-bucket timeout after {timeout:.0f}s"
                )
                return False
            time.sleep(min(wait, 0.5))

    # ── Layer 2: Tenant daily quota ───────────────────────────────────────

    def _check_tenant_quota(self, tenant_id: str, plan_tier: str) -> bool:
        """Increment daily counter and return False if limit exceeded."""
        daily_limit: int = (
            self._config.get("tenant_quotas", {})
            .get(plan_tier, {})
            .get("daily_requests", -1)
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

    # ── Layer 1a: Per-chat cooldown ───────────────────────────────────────

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

    # ── Layer 1b: Concurrent slot context manager ─────────────────────────

    @contextlib.asynccontextmanager
    async def concurrent_slot(self, entry_type: Optional[str], chat_id: Optional[str]):
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
        identity: Dict[str, str],
        request_type: str = "cli_workflow",
        chat_id: Optional[str] = None,
    ) -> bool:
        """
        Fast gate at dispatch time — checks cooldown debounce and daily tenant quota.
        Concurrency limits are enforced separately in concurrent_slot() at execution time.

        Returns True if allowed, False if any limit exceeded.
        Callers must raise AWSBaseError when this returns False.
        """
        tenant_id = identity.get("tenant_id", "default")
        plan_tier = identity.get("plan_tier", "free")

        # Layer 1a: per-chat cooldown debounce
        cooldown = (
            self._config.get("entry_limits", {})
            .get(request_type, {})
            .get("cooldown_seconds", 0)
        )
        if not self._check_chat_cooldown(chat_id or "", cooldown):
            return False

        # Layer 2: tenant daily quota
        if not self._check_tenant_quota(tenant_id, plan_tier):
            return False

        logger.debug(
            f"[RateLimiter] Allowed — tenant={tenant_id} tier={plan_tier} "
            f"type={request_type} chat={chat_id}"
        )
        return True
