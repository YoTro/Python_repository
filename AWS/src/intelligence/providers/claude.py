from __future__ import annotations
import os
import logging
from typing import Optional, TypeVar, Any, List, Dict
from pydantic import BaseModel
import anthropic
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse, BatchRequest, BatchJobHandle

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Priority order for model selection
_MODEL_PRIORITIES = [
    "claude-3-5-sonnet-20241022",
    "claude-3-opus-20240229",
    "claude-3-haiku-20240307",
]

class ClaudeProvider(BaseLLMProvider):
    """
    Claude (Anthropic) provider with Cost Calculation.
    """

    # Context windows per model family (prefix-matched against self.model_name).
    # All Claude 3+ models share a 200k context window.
    _MODEL_CONTEXT_WINDOWS = {
        "claude-3-5-sonnet": 200_000,
        "claude-3-5-haiku":  200_000,
        "claude-3-opus":     200_000,
        "claude-3-sonnet":   200_000,
        "claude-3-haiku":    200_000,
        "claude-opus-4":     200_000,
        "claude-sonnet-4":   200_000,
        "claude-haiku-4":    200_000,
    }

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None):

        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY missing.")

        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)

        selected_model = model_name or _MODEL_PRIORITIES[0]
        super().__init__("claude", selected_model)

        from .config.limits import get_max_output_tokens
        _ceiling = get_max_output_tokens("claude", self.model_name)
        _env = os.getenv("MAX_LLM_OUTPUT_TOKENS", "").strip()
        self._DEFAULT_MAX_TOKENS = min(int(_env) if _env else _ceiling, _ceiling)

        logger.info(f"ClaudeProvider initialized with model: {self.model_name}, max_output_tokens: {self._DEFAULT_MAX_TOKENS}")

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        try:
            kwargs = dict(
                model=self.model_name,
                messages=[{"role": "user", "content": prompt}],
            )
            if system_message:
                kwargs["system"] = system_message
            
            response = await self.client.messages.count_tokens(**kwargs)
            return response.input_tokens
        except Exception as e:
            logger.warning(f"Claude token count failed, falling back to estimate: {e}")
            return len(prompt) // 4

    async def generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        try:
            api_kwargs = dict(
                model=self.model_name,
                max_tokens=self._DEFAULT_MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
            if system_message:
                api_kwargs["system"] = system_message

            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)

            # Merge extra kwargs (allows per-call max_tokens override)
            api_kwargs.update(filtered_kwargs)

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

            # Extract cache info if available
            cache_read = getattr(usage, "cache_read_input_tokens", 0)
            cache_creation = getattr(usage, "cache_creation_input_tokens", 0)

            return self.create_response(
                text=text_content,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read,
                cache_creation_tokens=cache_creation
            )
        except Exception as e:
            logger.error(f"Claude text generation failed: {e}")
            raise

    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        raise NotImplementedError("Claude structured generation via Pydantic is not implemented in this version.")

    # ── Batch API ─────────────────────────────────────────────────────────────
    # Limits: 100,000 requests OR 256 MB per batch; expires after 24 h if not
    # completed; individual requests can also expire within a completed batch.

    BATCH_MAX_REQUESTS = 100_000

    def supports_batch(self) -> bool:
        return True

    async def generate_batch(self, requests: List[BatchRequest]) -> BatchJobHandle:
        """Submit a Claude Message Batch. Returns immediately with a handle."""
        if len(requests) > self.BATCH_MAX_REQUESTS:
            raise ValueError(
                f"Claude batch limit is {self.BATCH_MAX_REQUESTS} requests; "
                f"got {len(requests)}. Split into smaller batches."
            )
        self._check_batch_context_limit_sync(requests)
        try:
            batch_requests = []
            for req in requests:
                params: dict = {
                    "model": self.model_name,
                    "max_tokens": self._DEFAULT_MAX_TOKENS,
                    "messages": [{"role": "user", "content": req.prompt}],
                }
                if req.system_message:
                    params["system"] = req.system_message
                batch_requests.append({
                    "custom_id": req.custom_id,
                    "params": params,
                })

            batch = await self.client.messages.batches.create(requests=batch_requests)
            logger.info(f"Claude batch submitted: {batch.id}, {len(requests)} requests")
            return BatchJobHandle(
                job_id=batch.id,
                provider="claude",
                status="pending",
            )
        except Exception as e:
            logger.error(f"Claude batch submission failed: {e}")
            raise

    async def poll_batch(self, handle: BatchJobHandle) -> Optional[Dict[str, LLMResponse]]:
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

            results: Dict[str, LLMResponse] = {}
            async for result in self.client.messages.batches.results(handle.job_id):
                result_type = result.result.type
                if result_type == "succeeded":
                    msg = result.result.message
                    text = "".join(b.text for b in msg.content if b.type == "text")
                    usage = msg.usage
                    cache_read = getattr(usage, "cache_read_input_tokens", 0)
                    cache_creation = getattr(usage, "cache_creation_input_tokens", 0)
                    results[result.custom_id] = self.create_response(
                        text=text,
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        cache_read_tokens=cache_read,
                        cache_creation_tokens=cache_creation,
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
            raise
