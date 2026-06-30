from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from types import SimpleNamespace
from typing import Any, TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel

from src.core.data_cache import data_cache
from src.core.utils.decorators import exponential_backoff
from src.intelligence.dto import BatchJobHandle, BatchRequest, LLMResponse

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Gemini request-time processing classes (service tiers). One synchronous request
# is dispatched against exactly one tier:
#   flex     – 50% cheaper than standard, best-effort / sheddable, minutes-scale latency
#   priority – premium (75–100% over standard), low-latency, non-sheddable
#   standard – full price, the API default
# (Batch is a separate asynchronous path handled by generate_batch / poll_batch.)
SERVICE_TIERS = frozenset({"flex", "priority", "standard"})
DEFAULT_SERVICE_TIER = "standard"

# DataCache domain used to track expiry timestamps for explicit context caches.
# Key = cache_name (e.g. "cachedContents/abc123"), value = expires_at epoch float.
# Sentinel -1.0 marks a cache that has been explicitly deleted.
_CACHE_EXPIRY_DOMAIN = "gemini_cache_expiry"

# Maps content_hash → cache_name for deduplication across calls and workers.
_CACHE_LOOKUP_DOMAIN = "gemini_cache_lookup"

# Detect whether the installed SDK exposes UpdateCachedContentConfig.
_HAS_UPDATE_CACHE_CONFIG = hasattr(types, "UpdateCachedContentConfig")

# Tracks per-cache performance metrics (hits, misses, costs, etc.).
_CACHE_METRICS_DOMAIN = "gemini_cache_metrics"

# Adaptive renewal: minimum observed hits before the closed-loop logic overrides the
# formula-based break-even TTL.  Below this threshold there is not enough signal to
# trust the observed inter-hit interval, so we fall back to the static formula.
_MIN_CACHE_OBSERVATIONS = 3

# Multiply the observed inter-hit interval by this factor when computing the adaptive
# renewal TTL so that normal variance in request timing does not cause the cache to
# expire between back-to-back hits.
_ADAPTIVE_SAFETY_FACTOR = 1.5

# Bare model ids that support the premium tiers (flex and priority share the same
# support list). Matched as a substring against the fully-qualified model_name
# (e.g. "models/gemini-2.5-flash"), so dated/preview suffixes are tolerated.
# Requesting flex/priority on any other model silently downgrades to standard
# rather than erroring.
PREMIUM_TIER_MODELS = frozenset(
    {
        "gemini-3.5-flash",
        "gemini-3.1-flash-lite",
        "gemini-3.1-pro-preview",
        "gemini-3-flash-preview",
        "gemini-2.5-pro",
        "gemini-2.5-flash",  # also covers gemini-2.5-flash-lite
    }
)


def _normalize_service_tier(value: str | None) -> str | None:
    """Validate and lower-case a service tier; return None for None (no override)."""
    if value is None:
        return None
    tier = str(value).strip().lower()
    if tier not in SERVICE_TIERS:
        raise ValueError(f"Invalid service_tier {value!r}; expected one of {sorted(SERVICE_TIERS)}")
    return tier


class GeminiProvider(BaseLLMProvider):
    """
    Ultra-robust Gemini Provider with Auto-Model-Discovery and Cost Calculation.
    """

    # Context windows per model family (prefix-matched against self.model_name).
    # Gemini 1.5 Pro has a 2M window; all other current models are 1M.
    _MODEL_CONTEXT_WINDOWS = {
        "models/gemini-2.5-pro": 1_048_576,
        "models/gemini-2.5-flash": 1_048_576,
        "models/gemini-2.0-pro": 1_048_576,
        "models/gemini-1.5-pro": 2_097_152,
        "models/gemini-1.5-flash": 1_048_576,
        "models/gemini-1.0-pro": 32_760,
    }

    # Whether the installed SDK exposes service_tier as a native config field.
    # Older SDKs (<= 1.67) do not, so the tier is carried via http_options.extra_body.
    _CONFIG_HAS_SERVICE_TIER = "service_tier" in types.GenerateContentConfig.model_fields

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str | None = None,
        service_tier: str | None = None,
    ):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY missing.")

        self.client = genai.Client(api_key=self.api_key)

        discovered_model = self._discover_best_model(model_name)
        super().__init__("gemini", discovered_model)

        # Default tier for every request; overridable per call via a service_tier kwarg.
        self.service_tier = (
            _normalize_service_tier(service_tier or os.getenv("GEMINI_SERVICE_TIER"))
            or DEFAULT_SERVICE_TIER
        )

        from .config.limits import get_max_output_tokens

        _ceiling = get_max_output_tokens("gemini", self.model_name)
        _env = os.getenv("MAX_LLM_OUTPUT_TOKENS", "").strip()
        self._DEFAULT_MAX_TOKENS = min(int(_env) if _env else _ceiling, _ceiling)

        logger.info(
            f"GeminiProvider initialized with model: {self.model_name}, "
            f"max_output_tokens: {self._DEFAULT_MAX_TOKENS}, service_tier: {self.service_tier}"
        )

    def _supports_premium_tier(self) -> bool:
        """Whether the current model supports the flex/priority tiers."""
        return any(m in self.model_name for m in PREMIUM_TIER_MODELS)

    def _resolve_service_tier(self, kwargs: dict[str, Any]) -> str:
        """Pop a per-call ``service_tier`` override (if any) and fall back to the
        provider default. Mutates ``kwargs`` so the key is never forwarded to the SDK."""
        override = _normalize_service_tier(kwargs.pop("service_tier", None))
        return override or self.service_tier

    def _effective_service_tier(self, tier: str | None) -> str:
        """Resolve a tier to the one actually sent to the API.

        ``flex``/``priority`` on a model that does not support them downgrades to
        standard with a warning. Idempotent, so it is safe to call more than once.
        """
        tier = _normalize_service_tier(tier) or self.service_tier
        if tier in ("flex", "priority") and not self._supports_premium_tier():
            logger.warning(
                f"Model {self.model_name} does not support {tier} inference; "
                "falling back to standard tier."
            )
            return DEFAULT_SERVICE_TIER
        return tier

    def _service_tier_config(self, tier: str | None) -> dict[str, Any]:
        """Return GenerateContentConfig fields that carry the service tier to the API.

        The default ``standard`` tier is the API default, so nothing is emitted for it.
        The native config field is used when the SDK supports it; otherwise the tier is
        passed through the request body via ``http_options.extra_body``.
        """
        tier = self._effective_service_tier(tier)
        if tier == DEFAULT_SERVICE_TIER:
            return {}
        if self._CONFIG_HAS_SERVICE_TIER:
            return {"service_tier": tier}
        return {"http_options": types.HttpOptions(extra_body={"service_tier": tier})}

    @staticmethod
    def _served_service_tier(response: Any) -> str | None:
        """Read the ``x-gemini-service-tier`` response header, if present.

        The API echoes the tier it actually served the request under; comparing it
        to the requested tier reveals capacity-driven downgrades.
        """
        http = getattr(response, "sdk_http_response", None)
        headers = getattr(http, "headers", None) or {}
        try:
            items = headers.items()
        except AttributeError:
            return None
        for key, value in items:
            if str(key).lower() == "x-gemini-service-tier":
                return str(value).strip().lower()
        return None

    def _check_tier_downgrade(self, requested_tier: str, response: Any) -> None:
        """Warn when a flex/priority request was served as a lower tier.

        Client responsibility: monitor downgrade frequency — sustained downgrades
        mean the premium tier is shedding load and capacity should be reconsidered.
        """
        if requested_tier == DEFAULT_SERVICE_TIER:
            return
        served = self._served_service_tier(response)
        if served and served != requested_tier:
            logger.warning(
                f"Gemini service tier downgraded: requested={requested_tier}, "
                f"served={served} (model={self.model_name}). Frequent downgrades "
                "indicate the premium tier is shedding load."
            )

    # Flex is best-effort/sheddable, so DEADLINE_EXCEEDED / UNAVAILABLE / 5xx are
    # expected occasionally. The shared exponential_backoff decorator retries on
    # RetryableError; _raise_mapped_error converts transient SDK errors into one
    # (and permanent ones into FatalError, which propagates without retrying).
    @exponential_backoff(max_retries=3, base_delay=2.0, retry_on_exceptions=())
    async def _generate_content_with_retry(self, **call_kwargs: Any) -> Any:
        """Call ``models.generate_content``, mapping SDK errors so transient ones
        (DEADLINE_EXCEEDED, UNAVAILABLE, 429, …) are retried with backoff and
        permanent ones fail fast."""
        try:
            return await asyncio.to_thread(self.client.models.generate_content, **call_kwargs)
        except Exception as exc:
            self._raise_mapped_error(exc)

    # ── Context-cache TTL helpers ─────────────────────────────────────────────

    @property
    def _pricing_tier_key(self) -> str:
        """Map the instance service_tier to the pricing table tier string.

        service_tier  → pricing key segment
        ─────────────────────────────────────
        "flex"        → "flex_paid"
        "priority"    → "priority_paid"
        "standard"    → "standard_paid"
        None          → "standard_paid"  (API default)
        """
        tier = self.service_tier or DEFAULT_SERVICE_TIER
        return f"{tier}_paid"

    def _cache_prices(self, token_count: int = 0) -> tuple[float, float, float]:
        """Return (P_in, P_cr, P_storage) per 1M tokens for the current model and tier.

        Most Gemini models use flat pricing (no context-size suffix).  Only a
        small subset (e.g. gemini-2.5-pro) have lte_200k / gt_200k tiers.
        We detect which variant applies by probing the pricing table rather than
        hard-coding a model list, so new tiered models are handled automatically.

        ``token_count`` is used only for tiered models — it selects the correct
        tier (≤200k vs >200k).  Pass the cached token count from usage_metadata,
        the stored token_count from the metrics record, or 0 when unknown (which
        conservatively selects lte_200k).
        """
        model = self.price_manager.normalize_model_name(self.model_name)
        lk = self.price_manager.lookup
        pt = self._pricing_tier_key  # e.g. "standard_paid", "flex_paid", "priority_paid"

        # Determine the context-tier suffix dynamically.
        has_tiered = bool(lk.get(f"{model}#{pt}#input#text#lte_200k"))
        if has_tiered:
            tier_suffix = "#lte_200k" if token_count <= 200_000 else "#gt_200k"
        else:
            tier_suffix = ""

        def _get(direction: str) -> float:
            key = f"{model}#{pt}#{direction}#text{tier_suffix}"
            key_flat = f"{model}#{pt}#{direction}#text"
            v = lk.get(key, {}).get("price") or lk.get(key_flat, {}).get("price", 0.0)
            return float(v or 0.0)

        p_in = _get("input")
        p_cr = _get("cache_read")
        p_storage = float(lk.get(f"{model}#{pt}#cache_storage", {}).get("price", 0.0))
        return p_in, p_cr, p_storage

    def _optimal_renewal_ttl(self, token_count: int = 0) -> int:
        """Seconds to extend a cache after each hit (creation cost already sunk).

        Formula: (P_in - P_cr) / P_storage * 3600
        This is the window in which exactly one future read pays for storage.
        Minimum 60s (Gemini API lower bound). Falls back to 300s if prices are
        unavailable or the model does not support context caching.
        """
        p_in, p_cr, p_storage = self._cache_prices(token_count)
        if p_storage <= 0 or p_in <= p_cr:
            return 300
        return max(60, int((p_in - p_cr) / p_storage * 3600))

    def _optimal_initial_ttl(self, expected_hits: int, token_count: int = 0) -> int:
        """Seconds to set at cache creation, accounting for the creation fee.

        Formula: (N-1) * h_renewal
        The creation itself costs P_in (one full input), so only N-1 subsequent
        reads contribute savings relative to the uncached baseline.
        """
        return max(60, (expected_hits - 1) * self._optimal_renewal_ttl(token_count))

    def _content_hash(self, contents: list[Any], system_instruction: str | None) -> str:
        """Stable 16-char hex key identifying (model, contents, system_instruction).

        Used as the DataCache lookup key so identical content is never cached twice.
        ``default=str`` handles any non-JSON-native SDK types without raising.
        """
        payload = {
            "model": self.model_name,
            "contents": contents,
            "system": system_instruction or "",
        }
        raw = json.dumps(payload, sort_keys=True, default=str).encode()
        return hashlib.sha256(raw).hexdigest()[:16]

    def _cache_is_alive(self, cache_name: str) -> bool:
        """Return True only if our DataCache record confirms the cache is still valid.

        A missing record (never tracked) is treated as alive so callers that bypass
        create_context_cache are not penalised. A sentinel of -1.0 or an expired
        timestamp means the cache is definitively gone.
        """
        expires_at = data_cache.get(_CACHE_EXPIRY_DOMAIN, cache_name)
        if expires_at is None:
            return True
        return float(expires_at) > 0 and float(expires_at) > time.time()

    def _invalidate_cache_record(self, cache_name: str) -> None:
        """Write the -1.0 sentinel so future calls skip this cache without an API round-trip."""
        data_cache.set(_CACHE_EXPIRY_DOMAIN, cache_name, -1.0)

    @staticmethod
    def _is_cache_invalid_error(exc: Exception) -> bool:
        """Return True when an exception was caused by a missing or expired context cache.

        Works on both the raw SDK exception and the wrapped FatalError / RetryableError
        produced by _raise_mapped_error, by inspecting the full __cause__ chain.
        Requires both a "not-found" signal AND a "cache" mention to avoid false positives
        on unrelated 404s (e.g. a missing model name).
        """
        chain = [exc]
        cause = getattr(exc, "__cause__", None)
        if cause is not None:
            chain.append(cause)

        for err in chain:
            status = (
                getattr(err, "status_code", None)
                or getattr(err, "code", None)
                or getattr(getattr(err, "response", None), "status_code", None)
            )
            msg = str(err).lower()
            not_found = status == 404 or "not found" in msg or "404" in msg
            cache_related = any(kw in msg for kw in ("cachedcontent", "cached_content", "cache"))
            if not_found and cache_related:
                return True
        return False

    async def _maybe_renew_cache(self, cache_name: str) -> None:
        """Extend a context cache's TTL if it is approaching expiry.

        Called synchronously (awaited) inside generate_text / generate_structured
        after any response where cached_tokens > 0. Only fires when remaining TTL
        has dropped below 50% of the renewal window (debounce). Logs a warning on
        failure but never raises — a missed renewal is non-fatal.
        """
        expires_at = data_cache.get(_CACHE_EXPIRY_DOMAIN, cache_name)
        if expires_at is None or float(expires_at) < 0:
            return
        renewal_ttl = self._adaptive_renewal_ttl(cache_name)
        if renewal_ttl is None:
            return  # observed hit rate is unprofitable; let the cache expire naturally
        remaining = float(expires_at) - time.time()
        if remaining >= 0.5 * renewal_ttl:
            return
        try:
            if _HAS_UPDATE_CACHE_CONFIG:
                config = types.UpdateCachedContentConfig(ttl=f"{renewal_ttl}s")
            else:
                config = {"ttl": f"{renewal_ttl}s"}
            await asyncio.to_thread(self.client.caches.update, name=cache_name, config=config)
            data_cache.set(_CACHE_EXPIRY_DOMAIN, cache_name, time.time() + renewal_ttl)
            self._update_cache_metrics(cache_name, renewals=1)
            logger.info(f"Renewed Gemini context cache {cache_name} for {renewal_ttl}s")
        except Exception as e:
            logger.warning(f"Cache renewal failed for {cache_name}: {e}; cache may expire early.")

    # ── Cache metrics ─────────────────────────────────────────────────────────

    @staticmethod
    def _system_hash(system_instruction: str | None) -> str:
        """SHA-256[:16] fingerprint of a system instruction string (or empty string)."""
        return hashlib.sha256((system_instruction or "").encode()).hexdigest()[:16]

    def _cached_system_matches(self, cache_name: str, system_message: str | None) -> bool:
        """Return True when *system_message* matches the instruction embedded in *cache_name*.

        Falls back to True (assume match) when no metrics record exists — this covers
        caches created before metrics tracking was added, so we don't regress.
        Returns False only when we have a stored hash AND it differs from the current message.
        """
        record = data_cache.get(_CACHE_METRICS_DOMAIN, cache_name)
        if record is None:
            return True
        stored = record.get("system_hash")
        if stored is None:
            return True
        return stored == self._system_hash(system_message)

    def _init_cache_metrics(
        self,
        cache_name: str,
        content_hash: str,
        system_hash: str,
        token_count: int,
        expected_hits: int,
        initial_ttl: int,
        display_name: str | None,
    ) -> None:
        """Create the initial metrics record immediately after a cache is created."""
        p_in, _p_cr, _p_storage = self._cache_prices(token_count)
        now = time.time()
        data_cache.set(
            _CACHE_METRICS_DOMAIN,
            cache_name,
            {
                "cache_name": cache_name,
                "content_hash": content_hash,
                "system_hash": system_hash,
                "model": self.model_name,
                "display_name": display_name,
                "created_at": now,
                "token_count": token_count,
                "expected_hits": expected_hits,
                "initial_ttl_seconds": initial_ttl,
                "hits": 0,
                "misses": 0,
                "renewals": 0,
                "cost_creation": p_in * token_count / 1_000_000,
                "cost_storage_accrued": 0.0,
                "cost_saved": 0.0,
                "last_hit_at": None,
                "last_updated_at": now,
            },
        )

    def _update_cache_metrics(self, cache_name: str, **deltas: int) -> None:
        """Read-modify-write: accrue storage cost since last update and apply counter deltas."""
        record = data_cache.get(_CACHE_METRICS_DOMAIN, cache_name)
        if record is None:
            return
        now = time.time()
        elapsed = now - float(record.get("last_updated_at", now))
        token_count = int(record.get("token_count", 0))
        p_in, p_cr, p_storage = self._cache_prices(token_count)
        record["cost_storage_accrued"] = float(record.get("cost_storage_accrued", 0.0)) + (
            p_storage * token_count / 1_000_000 * elapsed / 3600
        )
        record["hits"] = int(record.get("hits", 0)) + int(deltas.get("hits", 0))
        record["misses"] = int(record.get("misses", 0)) + int(deltas.get("misses", 0))
        record["renewals"] = int(record.get("renewals", 0)) + int(deltas.get("renewals", 0))
        hit_tokens = int(deltas.get("hit_tokens", 0))
        if hit_tokens > 0:
            record["cost_saved"] = float(record.get("cost_saved", 0.0)) + (
                (p_in - p_cr) * hit_tokens / 1_000_000
            )
            record["last_hit_at"] = now
        record["last_updated_at"] = now
        data_cache.set(_CACHE_METRICS_DOMAIN, cache_name, record)

    def get_cache_metrics(self, cache_name: str) -> dict | None:
        """Return metrics for *cache_name* with derived fields added.

        Accrues storage cost to the current instant so the snapshot is always
        up-to-date without writing back to the store.
        Returns None when no record exists (cache pre-dates metrics tracking).
        """
        record = data_cache.get(_CACHE_METRICS_DOMAIN, cache_name)
        if record is None:
            return None
        now = time.time()
        elapsed = now - float(record.get("last_updated_at", now))
        token_count = int(record.get("token_count", 0))
        _p_in, _p_cr, p_storage = self._cache_prices(token_count)
        cost_storage = float(record.get("cost_storage_accrued", 0.0)) + (
            p_storage * token_count / 1_000_000 * elapsed / 3600
        )
        hits = int(record.get("hits", 0))
        misses = int(record.get("misses", 0))
        cost_creation = float(record.get("cost_creation", 0.0))
        cost_saved = float(record.get("cost_saved", 0.0))
        net_savings = cost_saved - cost_creation - cost_storage
        total = hits + misses
        out = dict(record)
        out["cost_storage_accrued"] = cost_storage
        out["hit_rate"] = round(hits / total, 4) if total > 0 else 0.0
        out["net_savings"] = round(net_savings, 8)
        out["cache_roi_positive"] = net_savings > 0
        return out

    def _adaptive_renewal_ttl(self, cache_name: str) -> int | None:
        """Return the cost-optimal renewal TTL (seconds) derived from observed hit frequency.

        Returns None when renewal would be unprofitable — the caller should let the cache
        expire naturally rather than paying storage for hits that no longer come.

        Decision logic
        ──────────────
        renewal_ttl_breakeven   : the maximum TTL at which one hit exactly offsets one
                                  period's storage cost  (formula-based, model-price-aware).
        inter_hit_seconds       : observed average seconds between consecutive cache hits
                                  (total elapsed / total hits).

        • inter_hit_seconds > renewal_ttl_breakeven
              → hits are too infrequent; every renewal period loses money → return None.
        • inter_hit_seconds ≤ renewal_ttl_breakeven
              → cache is profitable; set TTL = inter_hit × SAFETY_FACTOR so the cache
                stays alive long enough to absorb the next expected hit, while keeping
                storage cost below the per-hit saving.

        Falls back to the formula-based break-even TTL when fewer than
        _MIN_CACHE_OBSERVATIONS hits have been recorded (insufficient data).
        """
        record = data_cache.get(_CACHE_METRICS_DOMAIN, cache_name)
        hits = int(record.get("hits", 0)) if record else 0
        token_count = int(record.get("token_count", 0)) if record else 0
        renewal_ttl_breakeven = self._optimal_renewal_ttl(token_count)

        if hits < _MIN_CACHE_OBSERVATIONS:
            return renewal_ttl_breakeven

        created_at = float(record.get("created_at", time.time()))
        inter_hit_seconds = (time.time() - created_at) / hits

        if inter_hit_seconds > renewal_ttl_breakeven:
            logger.info(
                f"Cache {cache_name}: observed inter-hit interval {inter_hit_seconds:.0f}s "
                f"exceeds break-even TTL {renewal_ttl_breakeven}s — abandoning renewal."
            )
            return None

        return max(60, int(inter_hit_seconds * _ADAPTIVE_SAFETY_FACTOR))

    def _discover_best_model(self, preferred: str | None) -> str:
        """Query the API to find the highest-tier available model."""
        try:
            # Try newer attribute first, then fallback to older
            all_models = self.client.models.list()
            available = []
            for m in all_models:
                if (
                    hasattr(m, "supported_generation_methods")
                    and "generateContent" in m.supported_generation_methods
                    or hasattr(m, "supported_actions")
                    and "generateContent" in m.supported_actions
                ):
                    available.append(m.name)

            priorities = [
                "models/gemini-2.5-flash",
                "models/gemini-1.5-flash",
                "models/gemini-1.5-pro",
            ]

            if preferred and preferred in available:
                return preferred

            for p in priorities:
                if p in available:
                    return p

            return available[0] if available else "models/gemini-1.5-flash"
        except Exception as e:
            logger.error(f"Failed to list models: {e}. Falling back to default.")
            return "models/gemini-1.5-flash"

    async def count_tokens(self, prompt: str, system_message: str | None = None) -> int:
        try:
            full_text = f"{system_message}\n\n{prompt}" if system_message else prompt
            response = await asyncio.to_thread(
                self.client.models.count_tokens, model=self.model_name, contents=full_text
            )
            return response.total_tokens
        except Exception:
            return len(prompt) // 4

    _MAX_CONTINUATIONS = 4  # max continuation rounds when response is truncated

    @staticmethod
    def _is_truncated(response) -> bool:
        candidate = (getattr(response, "candidates", None) or [None])[0]
        finish_reason = getattr(candidate, "finish_reason", None)
        return bool(
            finish_reason and str(finish_reason) in ("FinishReason.MAX_TOKENS", "MAX_TOKENS", "2")
        )

    def _make_config(
        self,
        system_message,
        temp,
        service_tier: str | None = None,
        cached_content: str | None = None,
    ):
        fields: dict[str, Any] = {
            "temperature": temp,
            "max_output_tokens": self._DEFAULT_MAX_TOKENS,
            **self._service_tier_config(service_tier),
        }
        if cached_content:
            # system_instruction is baked into the cache; sending it again causes a 400.
            if system_message:
                logger.warning(
                    f"system_message ignored for cached request {cached_content}: "
                    "system instruction is embedded in the cache."
                )
            fields["cached_content"] = cached_content
        elif system_message:
            fields["system_instruction"] = system_message
        return types.GenerateContentConfig(**fields)

    async def generate_text(
        self, prompt: str, system_message: str | None = None, **kwargs
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            filtered_kwargs = self._filter_kwargs(kwargs)
            temp = filtered_kwargs.pop("temperature", 0.2)
            cached_content = filtered_kwargs.pop("cached_content", None)

            # Pre-flight: drop references to caches we know are expired or invalidated.
            if cached_content and not self._cache_is_alive(cached_content):
                logger.warning(
                    f"Context cache {cached_content} is expired/invalidated; proceeding uncached."
                )
                self._update_cache_metrics(cached_content, misses=1)
                cached_content = None

            # Guard: if the caller's system_message differs from what is baked into the
            # cache, using the cache would give the LLM stale instructions (e.g. an old
            # step-limit after a grace-period extension).  Drop the cache so the correct
            # system_message reaches the model; record as a miss.
            if (
                cached_content
                and system_message
                and not self._cached_system_matches(cached_content, system_message)
            ):
                logger.warning(
                    f"Context cache {cached_content} has a different system_instruction than the "
                    "current system_message; discarding cache to preserve correct LLM behavior."
                )
                self._update_cache_metrics(cached_content, misses=1)
                cached_content = None

            tier = self._effective_service_tier(self._resolve_service_tier(filtered_kwargs))
            config = self._make_config(system_message, temp, tier, cached_content=cached_content)

            try:
                response = await self._generate_content_with_retry(
                    model=self.model_name,
                    contents=prompt,
                    config=config,
                    **filtered_kwargs,
                )
            except Exception as cache_exc:
                if cached_content and self._is_cache_invalid_error(cache_exc):
                    logger.warning(
                        f"Context cache {cached_content} invalid on API call: {cache_exc}. "
                        "Invalidating record and retrying uncached."
                    )
                    self._update_cache_metrics(cached_content, misses=1)
                    self._invalidate_cache_record(cached_content)
                    cached_content = None
                    config = self._make_config(system_message, temp, tier)
                    response = await self._generate_content_with_retry(
                        model=self.model_name,
                        contents=prompt,
                        config=config,
                        **filtered_kwargs,
                    )
                else:
                    raise

            self._check_tier_downgrade(tier, response)

            usage = getattr(response, "usage_metadata", None)
            input_tokens = (
                (usage.prompt_token_count or 0)
                if usage
                else await self.count_tokens(prompt, system_message)
            )
            output_tokens = (usage.candidates_token_count or 0) if usage else 0
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            full_text = response.text

            # ── Continuation loop ────────────────────────────────────────────
            # When Gemini hits its per-call output ceiling, ask it to continue
            # from where it left off.  We build a minimal multi-turn conversation
            # (user turn = original prompt + all prior continuations; model turn =
            # last chunk) so the model has context but we don't balloon the prompt.
            for cont in range(self._MAX_CONTINUATIONS):
                if not self._is_truncated(response):
                    break
                logger.warning(
                    f"Gemini response truncated at max_output_tokens={self._DEFAULT_MAX_TOKENS} "
                    f"(continuation {cont + 1}/{self._MAX_CONTINUATIONS})…"
                )
                continuation_contents = [
                    types.Content(role="user", parts=[types.Part(text=prompt)]),
                    types.Content(role="model", parts=[types.Part(text=full_text)]),
                    types.Content(
                        role="user",
                        parts=[
                            types.Part(
                                text="Your previous response was cut off. Continue EXACTLY from where you stopped, "
                                "without repeating any prior content."
                            )
                        ],
                    ),
                ]
                response = await self._generate_content_with_retry(
                    model=self.model_name,
                    contents=continuation_contents,
                    config=config,
                    **filtered_kwargs,
                )
                cont_usage = getattr(response, "usage_metadata", None)
                if cont_usage:
                    input_tokens += cont_usage.prompt_token_count or 0
                    output_tokens += cont_usage.candidates_token_count or 0
                    thought_tokens += getattr(cont_usage, "thought_token_count", 0) or 0
                    cached_tokens += getattr(cont_usage, "cached_content_token_count", 0) or 0
                full_text += response.text
            else:
                if self._is_truncated(response):
                    logger.error(
                        f"Gemini response still truncated after {self._MAX_CONTINUATIONS} continuations. "
                        "Consider splitting the request."
                    )

            if cached_tokens > 0 and cached_content:
                self._update_cache_metrics(cached_content, hits=1, hit_tokens=cached_tokens)
                await self._maybe_renew_cache(cached_content)
            resp = self.create_response(
                text=full_text,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens,
            )
            if cached_tokens > 0:
                p_in, p_cr, _ = self._cache_prices(cached_tokens)
                resp.cache_cost_saved = (p_in - p_cr) * cached_tokens / 1_000_000
            return resp
        except Exception as e:
            logger.error(f"Gemini text generation failed: {e}")
            self._raise_mapped_error(e)

    @staticmethod
    def _clean_schema(schema: dict) -> dict:
        """Remove properties unsupported by the Gemini API (e.g. additionalProperties)."""
        UNSUPPORTED = {"additionalProperties", "$schema", "title"}
        result = {}
        for k, v in schema.items():
            if k in UNSUPPORTED:
                continue
            if isinstance(v, dict):
                result[k] = GeminiProvider._clean_schema(v)
            elif isinstance(v, list):
                result[k] = [
                    GeminiProvider._clean_schema(i) if isinstance(i, dict) else i for i in v
                ]
            else:
                result[k] = v
        return result

    # ── Batch API ─────────────────────────────────────────────────────────────

    def supports_batch(self) -> bool:
        return True

    async def generate_batch(self, requests: list[BatchRequest]) -> BatchJobHandle:
        """Submit an inline batch job. Returns immediately with a handle.

        SDK v1.67+: src accepts a list[InlinedRequest] directly; each InlinedRequest
        carries custom_id in metadata so we can map responses back by key.
        """
        self._check_batch_context_limit_sync(requests)
        try:
            inline_requests = []
            for req in requests:
                config = None
                if req.schema or req.system_message:
                    schema_dict = (
                        self._clean_schema(req.schema.model_json_schema()) if req.schema else None
                    )
                    config = types.GenerateContentConfig(
                        system_instruction=req.system_message,
                        response_mime_type="application/json" if schema_dict else None,
                        response_schema=schema_dict,
                    )
                inline_requests.append(
                    types.InlinedRequest(
                        model=self.model_name,
                        contents=req.prompt,
                        config=config,
                        metadata={"custom_id": req.custom_id},
                    )
                )

            batch_job = await asyncio.to_thread(
                self.client.batches.create,
                model=self.model_name,
                src=inline_requests,
            )
            logger.info(f"Gemini batch submitted: {batch_job.name}, {len(requests)} requests")
            return BatchJobHandle(
                job_id=batch_job.name,
                provider="gemini",
                status="pending",
            )
        except Exception as e:
            logger.error(f"Gemini batch submission failed: {e}")
            self._raise_mapped_error(e)

    async def poll_batch(self, handle: BatchJobHandle) -> dict[str, LLMResponse] | None:
        """Check batch status. Returns None while pending; dict on completion.

        SDK v1.67+: completed results are in job.dest.inlined_responses (same
        order as input requests). custom_id is recovered from resp.metadata.
        """
        _TERMINAL = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"}
        try:
            job = await asyncio.to_thread(self.client.batches.get, name=handle.job_id)
            # job.state is a JobState enum; use .name to get "JOB_STATE_SUCCEEDED" etc.
            raw_state = getattr(job, "state", None)
            state = getattr(raw_state, "name", str(raw_state)).upper()

            if state not in _TERMINAL:
                logger.debug(f"Gemini batch {handle.job_id} state={state} (raw={job.state})")
                return None

            if state != "JOB_STATE_SUCCEEDED":
                raise RuntimeError(f"Gemini batch {handle.job_id} ended with state={state}")

            inlined_responses = (job.dest.inlined_responses or []) if job.dest else []
            results: dict[str, LLMResponse] = {}
            for resp in inlined_responses:
                custom_id = (resp.metadata or {}).get("custom_id")
                if not custom_id:
                    logger.warning("Gemini batch response missing custom_id metadata, skipping")
                    continue
                if getattr(resp, "error", None):
                    logger.warning(f"Gemini batch item error custom_id={custom_id}: {resp.error}")
                    continue
                gc_response = resp.response
                usage = getattr(gc_response, "usage_metadata", None)
                input_tokens = usage.prompt_token_count if usage else 0
                output_tokens = usage.candidates_token_count if usage else 0
                thought_tokens = getattr(usage, "thought_token_count", 0) or 0
                cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
                results[custom_id] = self.create_response(
                    text=gc_response.text,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    thought_tokens=thought_tokens,
                    cached_tokens=cached_tokens,
                    is_batch=True,
                )
            logger.info(f"Gemini batch {handle.job_id} complete: {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Gemini batch poll failed: {e}")
            self._raise_mapped_error(e)

    # ─────────────────────────────────────────────────────────────────────────

    async def generate_structured(
        self, prompt: str, schema: Any, system_message: str | None = None, **kwargs
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            raw_schema = schema.model_json_schema()
            clean = self._clean_schema(raw_schema)

            # Filter out internal metadata from kwargs.
            # temperature (and any other GenerateContentConfig field) must go inside
            # the config object, not as a top-level generate_content() kwarg.
            filtered_kwargs = self._filter_kwargs(kwargs)
            temp = filtered_kwargs.pop("temperature", 0.2)
            cached_content = filtered_kwargs.pop("cached_content", None)

            # Pre-flight: drop references to caches we know are expired or invalidated.
            if cached_content and not self._cache_is_alive(cached_content):
                logger.warning(
                    f"Context cache {cached_content} is expired/invalidated; proceeding uncached."
                )
                self._update_cache_metrics(cached_content, misses=1)
                cached_content = None

            # Guard: system_instruction mismatch — same rationale as generate_text.
            if (
                cached_content
                and system_message
                and not self._cached_system_matches(cached_content, system_message)
            ):
                logger.warning(
                    f"Context cache {cached_content} has a different system_instruction than the "
                    "current system_message; discarding cache to preserve correct LLM behavior."
                )
                self._update_cache_metrics(cached_content, misses=1)
                cached_content = None

            tier = self._effective_service_tier(self._resolve_service_tier(filtered_kwargs))

            def _build_structured_config(cc: str | None) -> types.GenerateContentConfig:
                fields: dict[str, Any] = {
                    "response_mime_type": "application/json",
                    "response_schema": clean,
                    "temperature": temp,
                    **self._service_tier_config(tier),
                }
                if cc:
                    # system_instruction is baked into the cache; sending it again causes a 400.
                    if system_message:
                        logger.warning(
                            f"system_message ignored for cached request {cc}: "
                            "system instruction is embedded in the cache."
                        )
                    fields["cached_content"] = cc
                elif system_message:
                    fields["system_instruction"] = system_message
                return types.GenerateContentConfig(**fields)

            try:
                response = await self._generate_content_with_retry(
                    model=self.model_name,
                    contents=prompt,
                    config=_build_structured_config(cached_content),
                    **filtered_kwargs,
                )
            except Exception as cache_exc:
                if cached_content and self._is_cache_invalid_error(cache_exc):
                    logger.warning(
                        f"Context cache {cached_content} invalid on API call: {cache_exc}. "
                        "Invalidating record and retrying uncached."
                    )
                    self._update_cache_metrics(cached_content, misses=1)
                    self._invalidate_cache_record(cached_content)
                    cached_content = None
                    response = await self._generate_content_with_retry(
                        model=self.model_name,
                        contents=prompt,
                        config=_build_structured_config(None),
                        **filtered_kwargs,
                    )
                else:
                    raise

            self._check_tier_downgrade(tier, response)

            # Since we're asking for a schema, the text should be valid JSON
            text_response = response.text

            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0

            # Extract advanced usage stats for precise billing
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0

            if cached_tokens > 0 and cached_content:
                self._update_cache_metrics(cached_content, hits=1, hit_tokens=cached_tokens)
                await self._maybe_renew_cache(cached_content)
            resp = self.create_response(
                text=text_response,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens,
            )
            if cached_tokens > 0:
                p_in, p_cr, _ = self._cache_prices(cached_tokens)
                resp.cache_cost_saved = (p_in - p_cr) * cached_tokens / 1_000_000
            return resp
        except Exception as e:
            logger.error(f"Structured generation failed on {self.model_name}: {e}")
            self._raise_mapped_error(e)

    async def generate_vision_structured(
        self,
        image_urls: list[str],
        prompt: str,
        schema: Any,
        system_message: str | None = None,
        max_tokens: int = 1024,
        service_tier: str | None = None,
    ) -> Any:
        """
        Download image bytes in parallel, pass them as inline Blob parts alongside
        the text prompt, and return a structured Pydantic object via response_schema.
        Gemini does not support external HTTP image URLs directly — bytes required.
        """
        import aiohttp

        async def _fetch_bytes(url: str) -> bytes | None:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            return await r.read()
            except Exception as e:
                logger.warning(f"[vision] Failed to download image {url}: {e}")
            return None

        raw_bytes = await asyncio.gather(*[_fetch_bytes(u) for u in image_urls])
        parts: list = [
            types.Part(inline_data=types.Blob(mime_type="image/jpeg", data=b))
            for b in raw_bytes
            if b is not None
        ]
        if not parts:
            raise ValueError("No images could be downloaded for vision scoring.")
        parts.append(types.Part(text=prompt))

        raw_schema = schema.model_json_schema()
        clean = self._clean_schema(raw_schema)

        tier = self._effective_service_tier(service_tier)
        response = await self._generate_content_with_retry(
            model=self.model_name,
            contents=types.Content(role="user", parts=parts),
            config=types.GenerateContentConfig(
                system_instruction=system_message,
                response_mime_type="application/json",
                response_schema=clean,
                max_output_tokens=max_tokens,
                **self._service_tier_config(tier),
            ),
        )
        self._check_tier_downgrade(tier, response)

        from src.intelligence.parsers.markdown_cleaner import OutputParser

        data = OutputParser.parse_dirty_json(response.text or "")
        if not data:
            raise ValueError(
                f"Vision model returned unparsable JSON (len={len(response.text or '')}): "
                f"{(response.text or '')[:200]!r}"
            )
        return schema(**data)

    def create_context_cache(
        self,
        contents: list[Any],
        system_instruction: str | None = None,
        expected_hits: int = 2,
        display_name: str | None = None,
    ) -> Any:
        """Create a context cache with a cost-optimal TTL, reusing an existing one
        when identical content has already been cached for this model.

        Deduplication key: SHA-256[:16] of (model_name, contents, system_instruction).
        If a live cache exists for the same key, returns a SimpleNamespace(name=...)
        immediately — no API call, no creation fee.

        TTL is derived from model pricing so that storage cost breaks even against
        token savings at exactly ``expected_hits`` total uses (1 creation + N-1 reads).
        Raises ValueError if expected_hits < 2 — caching is never profitable for a
        single use since the creation fee equals one full input cost.
        """
        if expected_hits < 2:
            raise ValueError(
                f"expected_hits={expected_hits} is too low; context caching requires at least "
                "2 total uses to recover the creation cost. Pass expected_hits >= 2 or skip caching."
            )

        content_hash = self._content_hash(contents, system_instruction)
        existing_name = data_cache.get(_CACHE_LOOKUP_DOMAIN, content_hash)
        if existing_name and self._cache_is_alive(existing_name):
            logger.info(
                f"Reusing existing context cache {existing_name} (content_hash={content_hash})"
            )
            return SimpleNamespace(name=existing_name)

        ttl_seconds = self._optimal_initial_ttl(expected_hits)
        try:
            config = types.CreateCachedContentConfig(
                contents=contents,
                system_instruction=system_instruction,
                ttl=f"{ttl_seconds}s",
                display_name=display_name,
            )
            cache = self.client.caches.create(
                model=self.model_name,
                config=config,
            )
            data_cache.set(_CACHE_LOOKUP_DOMAIN, content_hash, cache.name)
            data_cache.set(_CACHE_EXPIRY_DOMAIN, cache.name, time.time() + ttl_seconds)
            usage = getattr(cache, "usage_metadata", None)
            token_count = getattr(usage, "total_token_count", 0) or 0
            self._init_cache_metrics(
                cache.name,
                content_hash,
                self._system_hash(system_instruction),
                token_count,
                expected_hits,
                ttl_seconds,
                display_name,
            )
            logger.info(
                f"Created Gemini context cache: {cache.name} (display_name={display_name}, "
                f"ttl={ttl_seconds}s, expected_hits={expected_hits}, "
                f"content_hash={content_hash})"
            )
            return cache
        except Exception as e:
            logger.error(f"Failed to create Gemini context cache: {e}")
            raise

    def delete_context_cache(self, cache_name: str) -> None:
        """Delete a context cache and invalidate its expiry record."""
        try:
            self.client.caches.delete(name=cache_name)
            self._invalidate_cache_record(cache_name)
            logger.info(f"Deleted Gemini context cache: {cache_name}")
        except Exception as e:
            logger.error(f"Failed to delete Gemini context cache {cache_name}: {e}")
            raise
