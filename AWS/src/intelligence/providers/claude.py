from __future__ import annotations
import os
import logging
from typing import Optional, TypeVar, Any
from pydantic import BaseModel
import anthropic
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse

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

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None):

        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY missing.")

        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)
        
        selected_model = model_name or _MODEL_PRIORITIES[0]
        super().__init__("claude", selected_model)
        
        logger.info(f"ClaudeProvider initialized with model: {self.model_name}")

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
        try:
            api_kwargs = dict(
                model=self.model_name,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            if system_message:
                api_kwargs["system"] = system_message
            
            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)
            
            # Merge extra kwargs
            api_kwargs.update(filtered_kwargs)

            response = await self.client.messages.create(**api_kwargs)
            
            text_content = ""
            for block in response.content:
                if block.type == "text":
                    text_content += block.text
            
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
