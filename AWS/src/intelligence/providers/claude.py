from __future__ import annotations
import os
import logging
import asyncio
from typing import Optional, TypeVar, Type
from pydantic import BaseModel
import anthropic
from .base import BaseLLMProvider
from .price_manager import PriceManager
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
        self.model_name = model_name or _MODEL_PRIORITIES[0]
        self.price_manager = PriceManager(provider="claude")
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

    async def generate_text(self, prompt: str, system_message: Optional[str] = None) -> LLMResponse:
        try:
            kwargs = dict(
                model=self.model_name,
                max_tokens=4096,
                messages=[{"role": "user", "content": prompt}],
            )
            if system_message:
                kwargs["system"] = system_message

            response = await self.client.messages.create(**kwargs)
            
            text_content = ""
            for block in response.content:
                if block.type == "text":
                    text_content += block.text
            
            # Claude usage contains detailed caching tokens
            usage = response.usage
            input_tokens = usage.input_tokens
            output_tokens = usage.output_tokens
            
            # Handle prompt caching if present in the SDK version
            cache_read = getattr(usage, "cache_read_input_tokens", 0)
            cache_creation = getattr(usage, "cache_creation_input_tokens", 0)
            
            # In simple billing, we sum them, but PriceManager could be expanded for caching
            total_usage = input_tokens + output_tokens
            
            cost = self.price_manager.calculate_cost(
                model_name=self.model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens # Claude context threshold check
            )

            return LLMResponse(
                text=text_content,
                provider_name="claude",
                model_name=self.model_name,
                token_usage=total_usage,
                cost=cost,
                currency=self.price_manager.currency,
                metadata={
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cache_read_tokens": cache_read,
                    "cache_creation_tokens": cache_creation
                }
            )
        except Exception as e:
            logger.error(f"Claude text generation failed: {e}")
            raise

    async def generate_structured(self, prompt: str, schema: Type[T], system_message: Optional[str] = None) -> LLMResponse:
        raise NotImplementedError("Claude structured generation via Pydantic is not implemented in this version.")

    async def batch_generate_text(self, prompts: list[str], system_message: Optional[str] = None, concurrency: int = 5) -> list[LLMResponse]:
        sem = asyncio.Semaphore(concurrency)
        async def _generate(p):
            async with sem:
                return await self.generate_text(p, system_message)
        
        results = await asyncio.gather(*[_generate(p) for p in prompts], return_exceptions=True)
        return [r for r in results if isinstance(r, LLMResponse)]

    async def batch_generate_structured(self, prompts: list[str], schema: Type[T], system_message: Optional[str] = None, concurrency: int = 5) -> list[LLMResponse]:
        raise NotImplementedError("Claude structured generation via Pydantic is not implemented in this version.")
