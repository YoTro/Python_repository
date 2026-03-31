from __future__ import annotations
import os
import logging
import asyncio
import json
import re
from typing import Optional, TypeVar, Type, Any
from pydantic import BaseModel
from google import genai
from google.genai import types
from .base import BaseLLMProvider
from .price_manager import PriceManager
from src.intelligence.dto import LLMResponse

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class GeminiProvider(BaseLLMProvider):
    """
    Ultra-robust Gemini Provider with Auto-Model-Discovery and Cost Calculation.
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None,
                 batch_threshold: int = 50000):

        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY missing.")

        self.client = genai.Client(api_key=self.api_key)
        self.batch_threshold = batch_threshold
        self.price_manager = PriceManager()

        self.model_name = self._discover_best_model(model_name)
        logger.info(f"GeminiProvider initialized with discovered model: {self.model_name}")

    def _discover_best_model(self, preferred: Optional[str]) -> str:
        """Query the API to find the highest-tier available model."""
        try:
            # Try newer attribute first, then fallback to older
            all_models = self.client.models.list()
            available = []
            for m in all_models:
                if hasattr(m, 'supported_generation_methods') and 'generateContent' in m.supported_generation_methods:
                    available.append(m.name)
                elif hasattr(m, 'supported_actions') and "generateContent" in m.supported_actions:
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

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        try:
            full_text = f"{system_message}\n\n{prompt}" if system_message else prompt
            response = await asyncio.to_thread(
                self.client.models.count_tokens,
                model=self.model_name,
                contents=full_text
            )
            return response.total_tokens
        except Exception:
            return len(prompt) // 4

    async def generate_text(self, prompt: str, system_message: Optional[str] = None) -> LLMResponse:
        try:
            config = types.GenerateContentConfig(
                system_instruction=system_message
            ) if system_message else None

            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=config,
            )
            
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else await self.count_tokens(prompt, system_message)
            output_tokens = usage.candidates_token_count if usage else 0
            
            # Extract advanced usage stats for precise billing
            thoughts_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            
            total_tokens = input_tokens + output_tokens + thoughts_tokens

            cost = self.price_manager.calculate_cost(
                model_name=self.model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thoughts_token_count=thoughts_tokens,
                cached_content_token_count=cached_tokens
            )

            return LLMResponse(
                text=response.text,
                provider_name="gemini",
                model_name=self.model_name,
                token_usage=total_tokens,
                cost=cost,
                currency=self.price_manager.currency,
                metadata={
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "thoughts_tokens": thoughts_tokens,
                    "cached_tokens": cached_tokens
                }
            )
        except Exception as e:
            logger.error(f"Gemini text generation failed: {e}")
            raise

    async def batch_generate_text(self, prompts: list[str], system_message: Optional[str] = None, concurrency: int = 5) -> list[LLMResponse]:
        sem = asyncio.Semaphore(concurrency)
        async def _generate(p):
            async with sem:
                return await self.generate_text(p, system_message)
        
        results = await asyncio.gather(*[_generate(p) for p in prompts], return_exceptions=True)
        return [r for r in results if isinstance(r, LLMResponse)]

    async def batch_generate_structured(self, prompts: list[str], schema: Type[T], system_message: Optional[str] = None, concurrency: int = 5) -> list[LLMResponse]:
        sem = asyncio.Semaphore(concurrency)
        async def _generate(p):
            async with sem:
                return await self.generate_structured(p, schema, system_message)
        
        results = await asyncio.gather(*[_generate(p) for p in prompts], return_exceptions=True)
        return [r for r in results if isinstance(r, LLMResponse)]
    
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
                    GeminiProvider._clean_schema(i) if isinstance(i, dict) else i
                    for i in v
                ]
            else:
                result[k] = v
        return result

    async def generate_structured(self, prompt: str, schema: Type[T], system_message: Optional[str] = None) -> LLMResponse:
        try:
            raw_schema = schema.model_json_schema()
            clean = self._clean_schema(raw_schema)

            generation_config = types.GenerationConfig(
                response_mime_type="application/json",
            )
            
            if system_message:
                self.client.system_instruction = system_message

            response = await asyncio.to_thread(
                self.client.generate_content,
                contents=prompt,
                generation_config=generation_config,
                tools=[types.FunctionDeclaration.from_dict(clean)]
            )
            
            # Since we're asking for a schema, the text should be valid JSON
            text_response = response.text
            
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else 0
            output_tokens = usage.candidates_token_count if usage else 0
            
            # Extract advanced usage stats for precise billing
            thoughts_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            
            total_tokens = input_tokens + output_tokens + thoughts_tokens

            cost = self.price_manager.calculate_cost(
                model_name=self.model_name,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thoughts_token_count=thoughts_tokens,
                cached_content_token_count=cached_tokens
            )
            
            return LLMResponse(
                text=text_response,
                provider_name="gemini",
                model_name=self.model_name,
                token_usage=total_tokens,
                cost=cost,
                currency=self.price_manager.currency,
                metadata={
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "thoughts_tokens": thoughts_tokens,
                    "cached_tokens": cached_tokens
                }
            )
        except Exception as e:
            logger.error(f"Structured generation failed on {self.model_name}: {e}")
            raise
