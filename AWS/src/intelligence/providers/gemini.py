from __future__ import annotations
import os
import logging
import asyncio
from typing import Optional, TypeVar, Any
from pydantic import BaseModel
from google import genai
from google.genai import types
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class GeminiProvider(BaseLLMProvider):
    """
    Ultra-robust Gemini Provider with Auto-Model-Discovery and Cost Calculation.
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model_name: Optional[str] = None):

        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY missing.")

        self.client = genai.Client(api_key=self.api_key)
        
        discovered_model = self._discover_best_model(model_name)
        super().__init__("gemini", discovered_model)
        
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

    async def generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        try:
            config = types.GenerateContentConfig(
                system_instruction=system_message
            ) if system_message else None

            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)

            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=config,
                **filtered_kwargs
            )
            
            usage = getattr(response, "usage_metadata", None)
            input_tokens = usage.prompt_token_count if usage else await self.count_tokens(prompt, system_message)
            output_tokens = usage.candidates_token_count if usage else 0
            
            # Extract advanced usage stats for precise billing
            thought_tokens = getattr(usage, "thought_token_count", 0) or 0
            cached_tokens = getattr(usage, "cached_content_token_count", 0) or 0
            
            return self.create_response(
                text=response.text,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                thought_tokens=thought_tokens,
                cached_tokens=cached_tokens
            )
        except Exception as e:
            logger.error(f"Gemini text generation failed: {e}")
            raise

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

    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        try:
            raw_schema = schema.model_json_schema()
            clean = self._clean_schema(raw_schema)

            generation_config = types.GenerationConfig(
                response_mime_type="application/json",
            )
            
            # Filter out internal metadata from kwargs
            filtered_kwargs = self._filter_kwargs(kwargs)

            # Use client.models.generate_content for consistency with generate_text
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=self.model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_message,
                    generation_config=generation_config,
                    tools=[types.Tool(function_declarations=[types.FunctionDeclaration.from_dict(clean)])]
                ),
                **filtered_kwargs
            )
            
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
                cached_tokens=cached_tokens
            )
        except Exception as e:
            logger.error(f"Structured generation failed on {self.model_name}: {e}")
            raise
