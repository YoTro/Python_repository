from __future__ import annotations
import os
import logging
import asyncio
from typing import Optional, TypeVar, Type
from pydantic import BaseModel
import anthropic
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Priority order for model selection
_MODEL_PRIORITIES = [
    "claude-3-opus-20240229",
    "claude-3-sonnet-20240229",
    "claude-3-haiku-20240307",
]

class ClaudeProvider(BaseLLMProvider):
    """
    Claude (Anthropic) provider using the official anthropic SDK.
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None):

        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY missing.")

        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)
        self.model_name = model_name or _MODEL_PRIORITIES[0]
        logger.info(f"ClaudeProvider initialized with model: {self.model_name}")

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        # Anthropic doesn't have a direct token counting API like OpenAI/Gemini.
        # We use a rough estimate.
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
            
            return LLMResponse(
                text=text_content,
                provider_name="claude",
                model_name=self.model_name,
                token_usage=response.usage.input_tokens + response.usage.output_tokens
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
