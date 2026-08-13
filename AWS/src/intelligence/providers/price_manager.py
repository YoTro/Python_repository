from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class PriceManager:
    """
    Universal LLM pricing manager.
    Supports provider-specific lookup keys and tiered pricing.
    """

    def __init__(self, provider: str = "gemini"):
        self.provider = provider.lower()
        # Map provider to its specific config file
        config_filename = f"{self.provider}_pricing.json"
        self.config_path = os.path.join(os.path.dirname(__file__), "config", config_filename)

        self.data = self._load_config()
        self.lookup = self.data.get("lookup", {})
        self.model_index = self.data.get("model_string_index", {})
        self.currency = self.data.get("metadata", {}).get("currency", "USD")

    def _load_config(self) -> dict[str, Any]:
        try:
            if not os.path.exists(self.config_path):
                logger.warning(
                    f"Pricing config for {self.provider} not found at {self.config_path}"
                )
                return {}
            with open(self.config_path, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {self.provider} pricing config: {e}")
            return {}

    def normalize_model_name(self, model_name: str) -> str:
        """Strip prefixes and map to canonical ID."""
        clean_name = model_name.strip().lower()

        # Provider specific prefix stripping
        if self.provider == "gemini":
            clean_name = clean_name.removeprefix("models/")

        # Check index
        index_entry = self.model_index.get(clean_name)
        if isinstance(index_entry, dict):
            return index_entry.get("canonical_id", clean_name)
        elif isinstance(index_entry, str):
            return index_entry

        return clean_name

    def calculate_cost(
        self,
        model_name: str,
        input_tokens: int,
        output_tokens: int,
        tier: str = "standard",
        is_batch: bool = False,
        **kwargs,
    ) -> float:
        """
        Calculates cost based on provider-specific rules.
        """
        canonical_model = self.normalize_model_name(model_name)

        if self.provider == "gemini":
            # 1. Extract detailed token counts from kwargs (flexible naming)
            cached_tokens = (
                kwargs.get("cached_content_token_count") or kwargs.get("cached_tokens") or 0
            )
            thoughts_tokens = (
                kwargs.get("thought_token_count")
                or kwargs.get("thoughts_token_count")
                or kwargs.get("thoughts_tokens")
                or 0
            )

            # 2. Prepare billing parameters
            gemini_tier = tier if "_" in tier else f"{tier}_paid"
            # Context tier is typically determined by total input (prompt) tokens
            context_tier = "gt_200k" if input_tokens > 200000 else "lte_200k"

            # 3. Construct lookup keys
            in_key = f"{canonical_model}#{gemini_tier}#input#text#{context_tier}"
            cache_key = f"{canonical_model}#{gemini_tier}#cache_read#text#{context_tier}"
            out_key = f"{canonical_model}#{gemini_tier}#output#text#{context_tier}"

            # Fallback for models without tiered pricing or missing keys
            if in_key not in self.lookup:
                in_key = f"{canonical_model}#{gemini_tier}#input#text"
            if out_key not in self.lookup:
                out_key = f"{canonical_model}#{gemini_tier}#output#text"
            if cache_key not in self.lookup:
                cache_key = f"{canonical_model}#{gemini_tier}#cache_read#text"

            # 4. Extract prices (per 1M tokens)
            in_price = self.lookup.get(in_key, {}).get("price", 0.0)
            out_price = self.lookup.get(out_key, {}).get("price", 0.0)
            cache_price = self.lookup.get(cache_key, {}).get("price", 0.0)

            # 5. Precise calculation:
            # Input = (non-cached part * input price) + (cached part * cache read price)
            input_cost = (max(0, input_tokens - cached_tokens) * in_price) + (
                cached_tokens * cache_price
            )
            # Output = (regular output + thoughts tokens) * output price
            output_cost = (output_tokens + thoughts_tokens) * out_price

            cost = round((input_cost + output_cost) / 1000000.0, 10)
            return cost * 0.5 if is_batch else cost

        elif self.provider == "claude":
            # Claude Pattern: {model}#{tier}#{dimension}
            #
            # Normalize the resolved tier before use:
            #   - "batch" is never used as a direct lookup tier here — `is_batch`
            #     already drives the 0.5x discount below, and claude_pricing.json's
            #     #batch# entries are already pre-discounted, so honoring both
            #     would double-discount a request whose usage.service_tier reports
            #     "batch" (e.g. from poll_batch results).
            #   - Any other non-standard tier (e.g. Priority Tier) falls back to
            #     "standard" with a warning if this model has no pricing data
            #     captured for it yet, instead of silently billing $0.00.
            if tier == "batch":
                tier = "standard"
            elif tier != "standard" and f"{canonical_model}#{tier}#input" not in self.lookup:
                logger.warning(
                    f"Claude pricing has no '{tier}' tier data for {canonical_model}; "
                    f"billing at the standard rate instead."
                )
                tier = "standard"

            in_key = f"{canonical_model}#{tier}#input"
            out_key = f"{canonical_model}#{tier}#output"

            # Support for long context tier switch if tokens > 200k
            if kwargs.get("total_tokens", input_tokens) > 200000:
                long_in = f"{canonical_model}#long_context_gt200k#input"
                if long_in in self.lookup:
                    in_key = long_in
                    out_key = f"{canonical_model}#long_context_gt200k#output"

            # Extract prices
            in_price = self.lookup.get(in_key, {}).get("price", 0.0)
            out_price = self.lookup.get(out_key, {}).get("price", 0.0)

            total_cost = (input_tokens * in_price / 1_000_000.0) + (
                output_tokens * out_price / 1_000_000.0
            )

            def _cache_price(dimension: str) -> float:
                key = f"{canonical_model}#standard#{dimension}"
                if key not in self.lookup:
                    logger.warning(
                        f"Claude pricing lookup miss for {dimension}: {key} — "
                        f"pricing config may be out of date. Cost reported as $0.00 for it."
                    )
                return self.lookup.get(key, {}).get("price", 0.0)

            # Prompt caching: usage.input_tokens excludes cache_creation/cache_read
            # tokens (Anthropic bills those separately, at cache_write_5m|1h and
            # cache_read rates respectively), so they must be added here or caching
            # requests are silently under-billed. Cache pricing is only published
            # under the "standard" tier — no long-context cache surcharge exists.
            cache_creation_tokens = kwargs.get("cache_creation_tokens", 0) or 0
            cache_read_tokens = kwargs.get("cache_read_tokens", 0) or 0

            # Anthropic's usage.cache_creation object reports the exact TTL split
            # (ephemeral_5m_input_tokens / ephemeral_1h_input_tokens) — bill each
            # portion at its own rate when the caller supplies it, instead of
            # guessing a single TTL for the whole cache_creation_input_tokens total.
            cache_creation_5m = kwargs.get("cache_creation_5m_tokens") or 0
            cache_creation_1h = kwargs.get("cache_creation_1h_tokens") or 0
            breakdown_total = cache_creation_5m + cache_creation_1h

            if breakdown_total:
                if cache_creation_tokens and breakdown_total != cache_creation_tokens:
                    logger.warning(
                        f"Claude cache-write TTL breakdown ({breakdown_total} tokens) doesn't "
                        f"match cache_creation_input_tokens ({cache_creation_tokens}) for "
                        f"{canonical_model}; billing the breakdown as reported."
                    )
                total_cost += cache_creation_5m * _cache_price("cache_write_5m") / 1_000_000.0
                total_cost += cache_creation_1h * _cache_price("cache_write_1h") / 1_000_000.0
            elif cache_creation_tokens:
                # No usable breakdown (not supplied, or summed to zero while the
                # flat total didn't — Anthropic's own sample responses can show
                # this) — fall back to a single guessed TTL for the whole total.
                # Defaults to "5m", matching ClaudeProvider's default ephemeral
                # cache_control, which never sets an explicit ttl; pass
                # cache_ttl="1h" explicitly for 1-hour cache_control blocks.
                cache_ttl = "1h" if kwargs.get("cache_ttl") == "1h" else "5m"
                total_cost += (
                    cache_creation_tokens * _cache_price(f"cache_write_{cache_ttl}") / 1_000_000.0
                )

            if cache_read_tokens:
                total_cost += cache_read_tokens * _cache_price("cache_read") / 1_000_000.0

            total_cost = round(total_cost, 10)
            total_cost = total_cost * 0.5 if is_batch else total_cost

            # Data residency surcharge: a request with inference_geo=US is billed
            # at 1.1x across every dimension (input/output/cache), applied last
            # after all other modifiers — see modifiers.data_residency_us in
            # claude_pricing.json (applies_to_future_models: true, so no per-model
            # allowlist check is needed here). usage.inference_geo on the response
            # reports what was actually served, e.g. "global" vs "us".
            inference_geo = str(kwargs.get("inference_geo") or "").strip().lower()
            if inference_geo == "us":
                total_cost *= 1.1

            return round(total_cost, 10)

        elif self.provider == "deepseek":
            # DeepSeek Pattern: {model}#{tier}#{dimension}
            # Server-side KV cache: cached_tokens billed at cache_hit rate, remainder at input rate.
            # Reasoning tokens (deepseek-v4-flash thinking mode) are folded into output_tokens by the API.
            import datetime as _dt

            cached_tokens = kwargs.get("cached_tokens", 0) or 0
            now_utc = _dt.datetime.now(_dt.UTC)

            # deepseek-v4-pro: 75% discount until 2026-05-31T15:59:00Z; after that, undiscounted tier = 1/4 of launch price (same value).
            _V4PRO_DISCOUNT_END = _dt.datetime(2026, 5, 31, 15, 59, 0, tzinfo=_dt.UTC)
            if canonical_model == "deepseek-v4-pro" and now_utc > _V4PRO_DISCOUNT_END:
                ds_base_tier = "undiscounted"
            else:
                ds_base_tier = "standard"

            # DeepSeek has no batch discount tier (unlike Gemini/Claude/OpenAI) — flag
            # misuse instead of silently mis-keying into a nonexistent "#batch#" price.
            if is_batch:
                logger.warning(
                    f"DeepSeek pricing has no batch tier for {canonical_model}; "
                    f"billing at the {ds_base_tier} rate instead."
                )

            # Peak-hour surcharge, effective 2026-07-15: 2x multiplier during
            # 01:00-04:00 and 06:00-10:00 UTC (see metadata.peak_pricing in
            # deepseek_pricing.json). Select the *_peak tier variant when applicable.
            _PEAK_WINDOWS_UTC = (
                (_dt.time(1, 0), _dt.time(4, 0)),
                (_dt.time(6, 0), _dt.time(10, 0)),
            )
            now_time = now_utc.time()
            is_peak = any(start <= now_time < end for start, end in _PEAK_WINDOWS_UTC)
            ds_tier = f"{ds_base_tier}_peak" if is_peak else ds_base_tier

            in_key = f"{canonical_model}#{ds_tier}#input"
            cache_hit_key = f"{canonical_model}#{ds_tier}#input_cache_hit"
            out_key = f"{canonical_model}#{ds_tier}#output"

            missing_keys = [k for k in (in_key, out_key) if k not in self.lookup]
            if missing_keys:
                logger.warning(
                    f"DeepSeek pricing lookup miss for model={canonical_model} tier={ds_tier}: "
                    f"missing {missing_keys} — pricing config may be out of date. "
                    f"Cost for the missing dimension(s) will be reported as $0.00."
                )

            in_price = self.lookup.get(in_key, {}).get("price", 0.0)
            cache_hit_price = self.lookup.get(cache_hit_key, {}).get("price", 0.0)
            out_price = self.lookup.get(out_key, {}).get("price", 0.0)

            non_cached = max(0, input_tokens - cached_tokens)
            input_cost = (non_cached * in_price + cached_tokens * cache_hit_price) / 1_000_000.0
            output_cost = output_tokens * out_price / 1_000_000.0
            return round(input_cost + output_cost, 10)

        elif self.provider == "openai":
            # OpenAI Pattern: {model}#{tier}#{dimension}, tier = standard | batch
            # (| long_context_standard | long_context_batch for models that publish
            # a long-context rate). Automatic prompt caching bills cached_tokens at
            # input_cache_hit price; reasoning ('pro') models publish no cached rate,
            # so cached tokens fall back to the full input price. Batch tier is a
            # uniform 50% discount. Long-context tier applies above 272,000 total
            # prompt tokens; only switched when the model actually publishes it.
            #
            # Service tier (pass via `tier=`, matching the API's `service_tier`
            # request parameter): "flex" reuses the batch lookup keys —
            # openai_pricing.json's gpt_5_6_coverage_note confirms flex is numerically
            # identical to batch for every model that publishes it, so no separate
            # price table is needed. "fast_mode" (2x standard/long_context_standard,
            # per fast_mode_note) is only published for gpt-5.6-sol/terra/luna; for
            # every other model it falls back to standard/batch with a warning rather
            # than fabricating a number. Priority processing was renamed Fast mode on
            # 2026-07-30 — the API accepts either service_tier: "priority" or "fast"
            # for the same tier, so both aliases (plus the internal "fast_mode" key
            # name) are accepted here.
            cached_tokens = kwargs.get("cached_tokens", 0) or 0
            cache_write_tokens = kwargs.get("cache_write_tokens", 0) or 0
            requested_tier = (tier or "standard").lower()

            if requested_tier == "flex":
                oa_tier = "batch"
            elif requested_tier in ("fast_mode", "fast", "priority"):
                if f"{canonical_model}#fast_mode#input" in self.lookup:
                    oa_tier = "fast_mode"
                else:
                    oa_tier = "batch" if is_batch else "standard"
                    logger.warning(
                        f"OpenAI fast_mode pricing is not available for {canonical_model} "
                        f"(no source data in openai_pricing.json) — billing at the "
                        f"{oa_tier} rate instead."
                    )
            else:
                oa_tier = "batch" if is_batch else "standard"

            if kwargs.get("total_tokens", input_tokens) > 272_000:
                long_tier = f"long_context_{oa_tier}"
                if f"{canonical_model}#{long_tier}#input" in self.lookup:
                    oa_tier = long_tier

            in_key = f"{canonical_model}#{oa_tier}#input"
            cache_hit_key = f"{canonical_model}#{oa_tier}#input_cache_hit"
            cache_write_key = f"{canonical_model}#{oa_tier}#cache_writes"
            out_key = f"{canonical_model}#{oa_tier}#output"

            in_price = self.lookup.get(in_key, {}).get("price", 0.0)
            cache_hit_price = self.lookup.get(cache_hit_key, {}).get("price", in_price)
            # cache_writes is only published for gpt-5.6-sol/terra/luna (see
            # cache_writes_note) — models without it are correctly billed $0.00
            # for any cache-write tokens, per that note's documented convention.
            cache_write_price = self.lookup.get(cache_write_key, {}).get("price", 0.0)
            out_price = self.lookup.get(out_key, {}).get("price", 0.0)

            non_cached = max(0, input_tokens - cached_tokens)
            input_cost = (non_cached * in_price + cached_tokens * cache_hit_price) / 1_000_000.0
            cache_write_cost = cache_write_tokens * cache_write_price / 1_000_000.0
            output_cost = output_tokens * out_price / 1_000_000.0
            return round(input_cost + cache_write_cost + output_cost, 10)

        else:
            return 0.0
