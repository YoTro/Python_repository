from __future__ import annotations

import logging
import os
import re
from typing import Any, TypeVar

import anthropic
from pydantic import BaseModel

from src.intelligence.dto import BatchJobHandle, BatchRequest, LLMResponse

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Beta header required to use output_config.effort on claude-opus-4-5 only —
# GA (no header) on Claude 4.6+ and the 5-generation models.
_EFFORT_BETA_HEADER = "effort-2025-11-24"
# effort levels, lowest to highest token spend:
#   low    - most efficient; significant token savings with some capability reduction
#   medium - balanced approach with moderate token savings (used as our default)
#   high   - equivalent to omitting the parameter; complex reasoning / agentic tasks
#   xhigh  - extended capability for long-horizon agentic/coding work (not on all models)
#   max    - absolute maximum capability, no constraint on token spending
_DEFAULT_EFFORT = "medium"

# Priority order for model selection — newest/best first.
# _active_model() validates against the live /v1/models list and falls back
# to the first entry that is actually available.
_MODEL_PRIORITIES = [
    "claude-sonnet-5",
    "claude-opus-4-8",
    "claude-sonnet-4-6",
    "claude-haiku-4-5-20251001",
    "claude-3-5-sonnet-20241022",  # legacy fallback
]


class ClaudeProvider(BaseLLMProvider):
    """
    Claude (Anthropic) provider with Cost Calculation.
    """

    # Context windows per model family (prefix-matched against self.model_name;
    # _get_context_window() returns the FIRST matching entry in this dict's
    # iteration order, so more specific prefixes must be listed before the
    # broader ones they'd otherwise be swallowed by — e.g. claude-opus-4-5
    # (200k) must precede the claude-opus-4 (1M) catch-all for opus-4-8/4-7/4-6.
    _MODEL_CONTEXT_WINDOWS = {
        "claude-opus-5": 1_000_000,
        "claude-sonnet-5": 1_000_000,
        "claude-fable-5": 1_000_000,
        "claude-mythos-5": 1_000_000,
        "claude-opus-4-5": 200_000,
        "claude-opus-4-1": 200_000,
        "claude-opus-4-0": 200_000,
        "claude-opus-4": 1_000_000,
        "claude-sonnet-4-6": 1_000_000,
        "claude-sonnet-4": 200_000,
        "claude-haiku-4": 200_000,
        "claude-3-5-sonnet": 200_000,
        "claude-3-5-haiku": 200_000,
        "claude-3-opus": 200_000,
        "claude-3-sonnet": 200_000,
        "claude-3-haiku": 200_000,
    }

    def __init__(self, api_key: str | None = None, model_name: str | None = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY missing.")

        # 1-hour timeout, matching the SDK's own ceiling for non-streaming
        # requests (anthropic._base_client._calculate_nonstreaming_timeout:
        # maximum_time=3600s). Without this, any call requesting close to a
        # model's full max_output_tokens (e.g. 128k for claude-opus-5 and
        # siblings) trips the SDK's built-in guard — "Streaming is required
        # for operations that may take longer than 10 minutes" — which only
        # activates while the client is still on the 10-minute DEFAULT_TIMEOUT.
        self.client = anthropic.AsyncAnthropic(api_key=self.api_key, timeout=3600.0)

        selected_model = model_name or os.getenv("ANTHROPIC_MODEL") or _MODEL_PRIORITIES[0]
        super().__init__("claude", selected_model)

        from .config.limits import get_max_output_tokens

        _ceiling = get_max_output_tokens("claude", self.model_name)
        _env = os.getenv("MAX_LLM_OUTPUT_TOKENS", "").strip()
        self._DEFAULT_MAX_TOKENS = min(int(_env) if _env else _ceiling, _ceiling)

        self._resolved_model: str | None = None

        logger.info(
            f"ClaudeProvider initialized with model: {self.model_name}, max_output_tokens: {self._DEFAULT_MAX_TOKENS}"
        )

    async def list_models(self) -> list[dict]:
        """Return all models available on the Anthropic platform via GET /models."""
        try:
            response = await self.client.models.list()
            return [
                {"id": m.id, "display_name": getattr(m, "display_name", None)}
                for m in response.data
            ]
        except Exception as e:
            logger.warning(f"Claude /models lookup failed: {e}")
            return [{"id": m, "display_name": None} for m in _MODEL_PRIORITIES]

    async def _active_model(self) -> str:
        """Return the validated model name, falling back to the first available if needed.

        Result is cached after the first successful /models call so subsequent
        inference calls pay no extra latency.
        """
        if self._resolved_model is not None:
            return self._resolved_model

        try:
            available = [m["id"] for m in await self.list_models()]
        except Exception as e:
            logger.warning(f"Claude model resolution failed ({e}); using configured model as-is")
            self._resolved_model = self.model_name
            return self._resolved_model

        if self.model_name in available:
            self._resolved_model = self.model_name
        else:
            fallback = available[0] if available else _MODEL_PRIORITIES[0]
            logger.warning(
                f"Model '{self.model_name}' not in live model list; falling back to '{fallback}'"
            )
            self._resolved_model = fallback

        return self._resolved_model

    def _is_reasoning_model(self, model: str) -> bool:
        """Return True for Claude 4+ / 5 models that reject the temperature parameter.

        Claude 3.x models accept temperature; all newer generations (claude-sonnet-4,
        claude-opus-4, claude-sonnet-5, claude-fable-5, etc.) treat it as deprecated.
        """
        m = model.lower()
        # claude-3-* and claude-2-* / claude-instant-* support temperature
        if re.match(r"claude-[123]", m) or re.match(r"claude-instant", m):
            return False
        return True

    def _effort_support(self, model: str) -> str:
        """Return 'ga', 'beta', or 'none' for output_config.effort on *model*.

        GA (no beta header) on Claude 4.6+ and the 5-generation models.
        claude-opus-4-5 supports effort only behind the effort-2025-11-24 beta
        header. Haiku 4.5, Sonnet 4.5, and pre-4.6 models reject the parameter.
        """
        m = model.lower()
        if re.match(r"claude-opus-4-5(-|$)", m):
            return "beta"
        if re.match(r"claude-(haiku-4-5|sonnet-4-5|[123])", m) or re.match(r"claude-instant", m):
            return "none"
        return "ga"

    async def count_tokens(self, prompt: str, system_message: str | None = None) -> int:
        try:
            active = await self._active_model()
            kwargs = {
                "model": active,
                "messages": [{"role": "user", "content": prompt}],
            }
            if system_message:
                kwargs["system"] = system_message

            response = await self.client.messages.count_tokens(**kwargs)
            return response.input_tokens
        except Exception as e:
            logger.warning(f"Claude token count failed, falling back to estimate: {e}")
            return len(prompt) // 4

    @staticmethod
    def _system_param(system_message: str | None, cache: bool) -> Any:
        """Build the ``system`` request value, optionally with a cache breakpoint.

        Anthropic prompt caching is a prefix match over ``tools → system → messages``,
        so a single ``cache_control`` breakpoint on the system block caches the whole
        static prefix. The MCP agent re-sends a large, frozen system prompt on every
        ReAct turn, so caching it turns the per-turn input cost from full price into
        ~0.1× cache reads (the first call pays a ~1.25× write). Prefixes below the
        model's minimum (~4096 tokens on Opus, ~2048 on Sonnet 4.6) silently don't
        cache — no error, no premium — so this is safe to request unconditionally.
        """
        if not system_message:
            return None
        if not cache:
            return system_message
        return [
            {
                "type": "text",
                "text": system_message,
                "cache_control": {"type": "ephemeral"},  # 5-minute TTL
            }
        ]

    async def generate_text(
        self, prompt: str, system_message: str | None = None, **kwargs
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            # Read the cache hint before filtering strips it from kwargs.
            cache_system = bool(kwargs.get("cache_system_prompt", False))

            api_kwargs = {
                "model": await self._active_model(),
                "max_tokens": self._DEFAULT_MAX_TOKENS,
                "messages": [{"role": "user", "content": prompt}],
            }
            system_value = self._system_param(system_message, cache_system)
            if system_value is not None:
                api_kwargs["system"] = system_value

            # Filter out internal metadata from kwargs (incl. cache_system_prompt)
            filtered_kwargs = self._filter_kwargs(kwargs)

            # Claude 4+ / 5 models reject temperature; strip it for those.
            temperature = filtered_kwargs.pop("temperature", None)
            if temperature is not None and not self._is_reasoning_model(api_kwargs["model"]):
                api_kwargs["temperature"] = temperature

            # output_config.effort controls thinking depth / token spend. Default to
            # "medium" on models that support it; strip it where the API rejects it.
            effort_support = self._effort_support(api_kwargs["model"])
            output_config = filtered_kwargs.pop("output_config", None)
            if effort_support == "none":
                output_config = None
            elif output_config is None:
                output_config = {"effort": _DEFAULT_EFFORT}
            if output_config is not None:
                api_kwargs["output_config"] = output_config

            # Merge remaining extra kwargs (allows per-call max_tokens override)
            api_kwargs.update(filtered_kwargs)

            if effort_support == "beta" and output_config is not None:
                response = await self.client.beta.messages.create(
                    betas=[_EFFORT_BETA_HEADER], **api_kwargs
                )
            else:
                response = await self.client.messages.create(**api_kwargs)

            text_content = ""
            for block in response.content:
                if block.type == "text":
                    text_content += block.text

            if response.stop_reason == "max_tokens":
                logger.warning(
                    f"Claude response truncated at max_tokens={api_kwargs['max_tokens']}. "
                    f"Set MAX_LLM_OUTPUT_TOKENS env var to increase the limit."
                )

            # Claude usage contains detailed caching tokens
            usage = response.usage
            input_tokens = usage.input_tokens
            output_tokens = usage.output_tokens

            # Extract cache info if available. usage.cache_creation breaks the
            # write down by TTL (ephemeral_5m/1h_input_tokens) when present —
            # more precise than the flat cache_creation_input_tokens total.
            cache_read = getattr(usage, "cache_read_input_tokens", 0)
            cache_creation = getattr(usage, "cache_creation_input_tokens", 0)
            cache_creation_detail = getattr(usage, "cache_creation", None)
            cache_creation_5m = getattr(cache_creation_detail, "ephemeral_5m_input_tokens", None)
            cache_creation_1h = getattr(cache_creation_detail, "ephemeral_1h_input_tokens", None)

            # Bill at the tier the request was actually served on (e.g. Priority
            # Tier), not an assumed default — mirrors OpenAIProvider's service_tier
            # handling. PriceManager falls back to "standard" if the resolved tier
            # has no pricing data captured for this model.
            tier = getattr(usage, "service_tier", None) or "standard"

            return self.create_response(
                text=text_content,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read,
                cache_creation_tokens=cache_creation,
                cache_creation_5m_tokens=cache_creation_5m,
                cache_creation_1h_tokens=cache_creation_1h,
                inference_geo=getattr(usage, "inference_geo", None),
                tier=tier,
            )
        except Exception as e:
            logger.error(f"Claude text generation failed: {e}")
            self._raise_mapped_error(e)

    async def generate_structured(
        self, prompt: str, schema: Any, system_message: str | None = None, **kwargs
    ) -> LLMResponse:
        raise NotImplementedError(
            "Claude structured generation via Pydantic is not implemented in this version."
        )

    async def generate_vision_structured(
        self,
        image_urls: list[str],
        prompt: str,
        schema: Any,
        system_message: str | None = None,
        **kwargs,
    ) -> Any:
        """
        Pass image URLs + text prompt to Claude and parse the response into *schema*.
        Images are passed as URL-sourced content blocks — no download required.
        Accepted kwargs: temperature, max_tokens.
        """
        from src.intelligence.parsers.markdown_cleaner import OutputParser

        filtered_kwargs = self._filter_kwargs(kwargs)
        max_tokens = filtered_kwargs.pop("max_tokens", self._DEFAULT_MAX_TOKENS)
        temp = filtered_kwargs.pop("temperature", 0.2)

        content: list[dict] = [
            {"type": "image", "source": {"type": "url", "url": url}} for url in image_urls
        ]
        content.append({"type": "text", "text": prompt})

        active = await self._active_model()
        api_kwargs: dict = {
            "model": active,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": content}],
        }
        if not self._is_reasoning_model(active):
            api_kwargs["temperature"] = temp
        if system_message:
            api_kwargs["system"] = system_message

        try:
            response = await self.client.messages.create(**api_kwargs)
            text = "".join(block.text for block in response.content if block.type == "text")
            data = OutputParser.parse_dirty_json(text)
            if not data:
                raise ValueError(
                    f"Vision model returned unparsable JSON (len={len(text)}): {text[:200]!r}"
                )
            return schema(**data)
        except Exception as e:
            logger.error(f"Claude vision generation failed: {e}")
            self._raise_mapped_error(e)

    # ── Batch API ─────────────────────────────────────────────────────────────
    # Limits: 100,000 requests OR 256 MB per batch; expires after 24 h if not
    # completed; individual requests can also expire within a completed batch.

    BATCH_MAX_REQUESTS = 100_000

    def supports_batch(self) -> bool:
        return True

    async def generate_batch(self, requests: list[BatchRequest]) -> BatchJobHandle:
        """Submit a Claude Message Batch. Returns immediately with a handle."""
        if len(requests) > self.BATCH_MAX_REQUESTS:
            raise ValueError(
                f"Claude batch limit is {self.BATCH_MAX_REQUESTS} requests; "
                f"got {len(requests)}. Split into smaller batches."
            )
        self._check_batch_context_limit_sync(requests)
        try:
            active = await self._active_model()
            batch_requests = []
            for req in requests:
                params: dict = {
                    "model": active,
                    "max_tokens": self._DEFAULT_MAX_TOKENS,
                    "messages": [{"role": "user", "content": req.prompt}],
                }
                if req.system_message:
                    params["system"] = req.system_message
                batch_requests.append(
                    {
                        "custom_id": req.custom_id,
                        "params": params,
                    }
                )

            batch = await self.client.messages.batches.create(requests=batch_requests)
            logger.info(f"Claude batch submitted: {batch.id}, {len(requests)} requests")
            return BatchJobHandle(
                job_id=batch.id,
                provider="claude",
                status="pending",
            )
        except Exception as e:
            logger.error(f"Claude batch submission failed: {e}")
            self._raise_mapped_error(e)

    async def poll_batch(self, handle: BatchJobHandle) -> dict[str, LLMResponse] | None:
        """Check batch status. Returns None while pending; dict on completion.

        A completed batch may contain items with result.type:
          succeeded — normal response
          errored   — API / validation error for that request
          canceled  — request was canceled before processing
          expired   — request hit the 24 h processing deadline individually
        Only 'succeeded' items are included in the returned dict; others are
        logged and skipped so BatchPoller's completeness check can warn.
        """
        try:
            batch = await self.client.messages.batches.retrieve(handle.job_id)
            if batch.processing_status != "ended":
                logger.debug(f"Claude batch {handle.job_id} status={batch.processing_status}")
                return None

            results: dict[str, LLMResponse] = {}
            async for result in self.client.messages.batches.results(handle.job_id):
                result_type = result.result.type
                if result_type == "succeeded":
                    msg = result.result.message
                    text = "".join(b.text for b in msg.content if b.type == "text")
                    usage = msg.usage
                    cache_read = getattr(usage, "cache_read_input_tokens", 0)
                    cache_creation = getattr(usage, "cache_creation_input_tokens", 0)
                    cache_creation_detail = getattr(usage, "cache_creation", None)
                    cache_creation_5m = getattr(
                        cache_creation_detail, "ephemeral_5m_input_tokens", None
                    )
                    cache_creation_1h = getattr(
                        cache_creation_detail, "ephemeral_1h_input_tokens", None
                    )
                    tier = getattr(usage, "service_tier", None) or "standard"
                    results[result.custom_id] = self.create_response(
                        text=text,
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        cache_read_tokens=cache_read,
                        cache_creation_tokens=cache_creation,
                        cache_creation_5m_tokens=cache_creation_5m,
                        cache_creation_1h_tokens=cache_creation_1h,
                        inference_geo=getattr(usage, "inference_geo", None),
                        tier=tier,
                        is_batch=True,
                    )
                elif result_type == "expired":
                    logger.error(
                        f"Claude batch item expired custom_id={result.custom_id} "
                        f"batch_id={handle.job_id} — individual request hit 24h deadline"
                    )
                else:
                    logger.warning(
                        f"Claude batch item {result_type} custom_id={result.custom_id} "
                        f"batch_id={handle.job_id}"
                    )

            logger.info(f"Claude batch {handle.job_id} complete: {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Claude batch poll failed: {e}")
            self._raise_mapped_error(e)
