from __future__ import annotations
import os
from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict
from src.intelligence.dto import LLMResponse
from .price_manager import PriceManager

class BaseLLMProvider(ABC):
    """
    Abstract base class for all LLM providers.
    Uses the Template Method pattern to handle common pre/post-processing logic.
    """
    
    # Internal metadata keys that should never be sent to LLM APIs
    INTERNAL_METADATA_KEYS = {"session_id", "tenant_id", "user_id", "force_full_log", "metadata"}

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

    def create_response(self, text: str, input_tokens: int, output_tokens: int, **kwargs) -> LLMResponse:
        """
        Unified helper to create an LLMResponse with automatic cost calculation.
        """
        # 1. Extract optional counts for specialized billing (e.g. Gemini Thinking/Cache)
        thought_tokens = kwargs.get("thought_tokens", 0)
        cached_tokens = kwargs.get("cached_tokens", 0)
        
        # 2. Calculate Cost using the shared price manager
        cost = self.price_manager.calculate_cost(
            model_name=self.model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            thought_token_count=thought_tokens,
            cached_content_token_count=cached_tokens,
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
