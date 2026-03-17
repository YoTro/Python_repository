from __future__ import annotations
import os
import logging
from typing import Optional, Any, TypeVar, Type
from pydantic import BaseModel
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse

try:
    from llama_cpp import Llama
except ImportError:
    Llama = None

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class LlamaCppProvider(BaseLLMProvider):
    """
    Local LLM Provider using llama-cpp-python.
    Runs completely offline using GGUF models.
    """
    
    def __init__(self, 
                 model_path: str, 
                 n_ctx: int = 4096, 
                 n_gpu_layers: int = -1): # -1 means use all GPU layers if available
        
        if Llama is None:
            raise ImportError("llama-cpp-python is not installed. Please add it to your requirements.")
            
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Local model file not found at: {model_path}")
            
        self.model_path = model_path
        self.model_name = os.path.basename(model_path)
        logger.info(f"Loading local model from {model_path}...")
        
        # Initialize the model
        self.llm = Llama(
            model_path=model_path,
            n_ctx=n_ctx,
            n_gpu_layers=n_gpu_layers,
            verbose=False
        )
        logger.info("Local Llama model loaded successfully.")

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        full_text = f"{system_message}\n\n{prompt}" if system_message else prompt
        tokens = self.llm.tokenize(full_text.encode('utf-8'))
        return len(tokens)

    async def generate_text(self, prompt: str, system_message: Optional[str] = None) -> LLMResponse:
        import asyncio
        from src.intelligence.fallback import FallbackHandler, FailureType

        loop = asyncio.get_running_loop()
        logger.info("Local LLM generate_text called, dispatching to executor...")
        try:
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None, self._sync_generate_text, prompt, system_message
                ),
                timeout=120  # 2 minute timeout
            )
            logger.info(f"Local LLM response received ({response.token_usage} tokens used)")
            return response
        except asyncio.TimeoutError:
            logger.error("Local LLM generation timed out after 120s")
            return await FallbackHandler.handle(FailureType.LOCAL_MODEL_TIMEOUT)

    def _sync_generate_text(self, prompt: str, system_message: Optional[str] = None) -> LLMResponse:
        # Use ChatML format (required by Qwen and most modern GGUF models)
        if system_message:
            formatted_prompt = (
                f"<|im_start|>system\n{system_message}<|im_end|>\n"
                f"<|im_start|>user\n{prompt}<|im_end|>\n"
                f"<|im_start|>assistant\n"
            )
        else:
            formatted_prompt = (
                f"<|im_start|>user\n{prompt}<|im_end|>\n"
                f"<|im_start|>assistant\n"
            )

        # Truncate prompt if it exceeds model context to prevent hanging
        tokens = self.llm.tokenize(formatted_prompt.encode('utf-8'))
        max_prompt_tokens = self.llm.n_ctx() - 512  # Reserve tokens for generation
        if len(tokens) > max_prompt_tokens:
            logger.warning(f"Prompt too long ({len(tokens)} tokens), truncating to {max_prompt_tokens}")
            tokens = tokens[:max_prompt_tokens]
            formatted_prompt = self.llm.detokenize(tokens).decode('utf-8', errors='ignore')

        logger.info(f"Local LLM starting inference ({len(tokens)} prompt tokens)...")
        response = self.llm(
            formatted_prompt,
            max_tokens=1024,
            stop=["<|im_end|>", "<|im_start|>"],
            echo=False
        )
        
        text = response['choices'][0]['text'].strip()
        usage = response['usage']
        token_count = usage['total_tokens'] if usage else 0

        return LLMResponse(
            text=text,
            provider_name="local",
            model_name=self.model_name,
            token_usage=token_count
        )

    async def generate_structured(self, prompt: str, schema: Type[T], system_message: Optional[str] = None) -> LLMResponse:
        import json
        import re
        
        schema_json = json.dumps(schema.model_json_schema(), indent=2)
        enriched_prompt = f"{prompt}\n\nReturn ONLY a JSON object matching this schema:\n{schema_json}"
        
        response_obj = await self.generate_text(enriched_prompt, system_message)
        raw_text = response_obj.text
        
        # We need to re-package the LLMResponse because the text has changed (from Pydantic object to string)
        return LLMResponse(
            text=raw_text, # The raw text IS the structured JSON string
            provider_name=response_obj.provider_name,
            model_name=response_obj.model_name,
            token_usage=response_obj.token_usage
        )

    async def batch_generate_text(self, prompts: list[str], system_message: Optional[str] = None, concurrency: int = 2) -> list[LLMResponse]:
        import asyncio
        sem = asyncio.Semaphore(concurrency)
        async def _generate(p):
            async with sem:
                return await self.generate_text(p, system_message)
        
        results = await asyncio.gather(*[_generate(p) for p in prompts], return_exceptions=True)
        return [r for r in results if isinstance(r, LLMResponse)]

    async def batch_generate_structured(self, prompts: list[str], schema: Type[T], system_message: Optional[str] = None, concurrency: int = 2) -> list[LLMResponse]:
        import asyncio
        sem = asyncio.Semaphore(concurrency)
        async def _generate(p):
            async with sem:
                return await self.generate_structured(p, schema, system_message)
        
        results = await asyncio.gather(*[_generate(p) for p in prompts], return_exceptions=True)
        return [r for r in results if isinstance(r, LLMResponse)]
