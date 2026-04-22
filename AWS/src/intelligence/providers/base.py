from __future__ import annotations
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict, TYPE_CHECKING
from src.intelligence.dto import LLMResponse
from .price_manager import PriceManager

if TYPE_CHECKING:
    from src.intelligence.dto import BatchRequest, BatchJobHandle

class BaseLLMProvider(ABC):
    """
    Abstract base class for all LLM providers.
    Uses the Template Method pattern to handle common pre/post-processing logic.
    """

    # Internal metadata keys that should never be sent to LLM APIs
    INTERNAL_METADATA_KEYS = {"session_id", "tenant_id", "user_id", "force_full_log", "metadata"}

    # Subclasses define per-model context windows: {model_name_prefix: token_limit}
    # Prefix matching is used so dated suffixes (e.g. "-20241022") are covered.
    _MODEL_CONTEXT_WINDOWS: Dict[str, int] = {}

    # Tokens reserved for the model's output within the context window
    _OUTPUT_RESERVE: int = 4096

    def __init__(self, provider_name: str, model_name: str):
        self.provider_name = provider_name
        self.model_name = model_name
        self.price_manager = PriceManager(provider=provider_name)

    def _filter_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Filters out internal tracking metadata from API parameters."""
        return {k: v for k, v in kwargs.items() if k not in self.INTERNAL_METADATA_KEYS}

    @abstractmethod
    async def generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        """Subclasses must implement the actual API call logic here."""
        pass

    @abstractmethod
    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        """Subclasses must implement structured generation (native or simulated)."""
        pass

    @abstractmethod
    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        """Subclasses must implement token counting."""
        pass

    # ── Context-limit guards ──────────────────────────────────────────────────

    def _get_context_window(self) -> int:
        """Return the context-window token limit for the current model, or 0 if unknown."""
        for prefix, limit in self._MODEL_CONTEXT_WINDOWS.items():
            if self.model_name == prefix or self.model_name.startswith(prefix):
                return limit
        return 0

    async def _check_context_limit(
        self, prompt: str, system_message: Optional[str] = None
    ) -> None:
        """Accurate pre-flight check for single calls. Raises FatalError if over limit."""
        limit = self._get_context_window()
        if not limit:
            return
        max_input = limit - self._OUTPUT_RESERVE
        tokens = await self.count_tokens(prompt, system_message)
        if tokens > max_input:
            from src.core.errors.exceptions import FatalError
            raise FatalError(
                f"Context limit exceeded: {tokens:,} input tokens > {max_input:,} allowed "
                f"(provider={self.provider_name}, model={self.model_name}, "
                f"window={limit:,}, output_reserve={self._OUTPUT_RESERVE})"
            )

    def _check_batch_context_limit_sync(self, requests: List["BatchRequest"]) -> None:
        """Fast synchronous guard for batch calls using rough char-based estimates.

        Avoids N extra API round trips. Uses a 1.2× safety multiplier to catch
        under-estimates (actual token count can exceed len//4 for CJK / code text).
        Raises FatalError for any request that is clearly over the limit.
        """
        limit = self._get_context_window()
        if not limit:
            return
        max_input = limit - self._OUTPUT_RESERVE
        for req in requests:
            estimated = (len(req.prompt) + len(req.system_message or "")) // 4
            if int(estimated * 1.2) > max_input:
                from src.core.errors.exceptions import FatalError
                raise FatalError(
                    f"Batch request likely exceeds context limit: "
                    f"~{estimated:,} estimated tokens (×1.2 ≈ {int(estimated * 1.2):,}) "
                    f"> {max_input:,} allowed "
                    f"(provider={self.provider_name}, model={self.model_name}, "
                    f"custom_id={req.custom_id})"
                )

    # ── Batch API (optional — providers that support it override these) ───────

    def supports_batch(self) -> bool:
        return False

    async def generate_batch(self, requests: List["BatchRequest"]) -> "BatchJobHandle":
        raise NotImplementedError(f"{self.__class__.__name__} does not support batch generation")

    async def poll_batch(self, handle: "BatchJobHandle") -> Optional[Dict[str, LLMResponse]]:
        """Poll a previously submitted batch.
        Returns None while pending; returns {custom_id: LLMResponse} when complete."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support batch polling")

    # ─────────────────────────────────────────────────────────────────────────

    def create_response(self, text: str, input_tokens: int, output_tokens: int, **kwargs) -> LLMResponse:
        """
        Unified helper to create an LLMResponse with automatic cost calculation.
        """
        # 1. Extract optional counts for specialized billing (e.g. Gemini Thinking/Cache)
        thought_tokens = kwargs.get("thought_tokens", 0)
        cached_tokens = kwargs.get("cached_tokens", 0)
        is_batch = kwargs.pop("is_batch", False)

        # 2. Calculate Cost using the shared price manager
        cost = self.price_manager.calculate_cost(
            model_name=self.model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            thought_token_count=thought_tokens,
            cached_content_token_count=cached_tokens,
            is_batch=is_batch,
            **kwargs
        )

        # 3. Build standard metadata
        metadata = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens
        }
        if thought_tokens: metadata["thoughts_tokens"] = thought_tokens
        if cached_tokens: metadata["cached_tokens"] = cached_tokens

        return LLMResponse(
            text=text,
            provider_name=self.provider_name,
            model_name=self.model_name,
            token_usage=input_tokens + output_tokens + thought_tokens,
            cost=cost,
            currency=self.price_manager.currency,
            metadata=metadata
        )
