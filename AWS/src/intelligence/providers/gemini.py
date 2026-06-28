from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel

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
        "models/gemini-2.0-flash": 1_048_576,
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
        if system_message:
            fields["system_instruction"] = system_message
        if cached_content:
            fields["cached_content"] = cached_content
        return types.GenerateContentConfig(**fields)

    async def generate_text(
        self, prompt: str, system_message: str | None = None, **kwargs
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            filtered_kwargs = self._filter_kwargs(kwargs)
            temp = filtered_kwargs.pop("temperature", 0.2)
            cached_content = filtered_kwargs.pop("cached_content", None)
            tier = self._effective_service_tier(self._resolve_service_tier(filtered_kwargs))
            config = self._make_config(system_message, temp, tier, cached_content=cached_content)

            response = await self._generate_content_with_retry(
                model=self.model_name,
                contents=prompt,
                config=config,
                **filtered_kwargs,
            )
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

            return self.create_response(
                text=full_text,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens,
            )
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
            tier = self._effective_service_tier(self._resolve_service_tier(filtered_kwargs))

            config_fields = {
                "system_instruction": system_message,
                "response_mime_type": "application/json",
                "response_schema": clean,
                "temperature": temp,
                **self._service_tier_config(tier),
            }
            if cached_content:
                config_fields["cached_content"] = cached_content

            response = await self._generate_content_with_retry(
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(**config_fields),
                **filtered_kwargs,
            )
            self._check_tier_downgrade(tier, response)

            # Since we're asking for a schema, the text should be valid JSON
            text_response = response.text

            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0

            # Extract advanced usage stats for precise billing
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0

            return self.create_response(
                text=text_response,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens,
            )
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
        ttl: str = "3600s",
        display_name: str | None = None,
    ) -> Any:
        """
        Explicitly create a context cache using the google-genai SDK.
        Useful for massive, reusable context like long scraped datasets or standard schemas.
        """
        try:
            config = types.CreateCachedContentConfig(
                contents=contents,
                system_instruction=system_instruction,
                ttl=ttl,
                display_name=display_name,
            )
            cache = self.client.caches.create(
                model=self.model_name,
                config=config,
            )
            logger.info(
                f"Created Gemini context cache: {cache.name} (display_name: {display_name})"
            )
            return cache
        except Exception as e:
            logger.error(f"Failed to create Gemini context cache: {e}")
            raise

    def delete_context_cache(self, cache_name: str) -> None:
        """
        Explicitly delete a context cache.
        """
        try:
            self.client.caches.delete(name=cache_name)
            logger.info(f"Deleted Gemini context cache: {cache_name}")
        except Exception as e:
            logger.error(f"Failed to delete Gemini context cache {cache_name}: {e}")
            raise
