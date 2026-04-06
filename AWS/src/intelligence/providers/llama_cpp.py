from __future__ import annotations
import os
import logging
import asyncio
import functools
from typing import Optional, Any, TypeVar
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
                 n_ctx: int = 8192, 
                 n_gpu_layers: int = -1): # -1 means use all GPU layers if available
        
        if Llama is None:
            raise ImportError("llama-cpp-python is not installed. Please add it to your requirements.")
            
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Local model file not found at: {model_path}")
            
        self.model_path = model_path
        model_name = os.path.basename(model_path)
        
        super().__init__("local", model_name)
        
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

    async def generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        from src.intelligence.fallback import FallbackHandler, FailureType

        loop = asyncio.get_running_loop()
        logger.info("Local LLM generate_text called, dispatching to executor...")
        
        # Filter out internal metadata from kwargs
        filtered_kwargs = self._filter_kwargs(kwargs)

        try:
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None, functools.partial(self._sync_generate_text, prompt, system_message, **filtered_kwargs)
                ),
                timeout=120  # 2 minute timeout
            )
            logger.info(f"Local LLM response received ({response.token_usage} tokens used)")
            return response
        except asyncio.TimeoutError:
            logger.error("Local LLM generation timed out after 120s")
            return await FallbackHandler.handle(FailureType.LOCAL_MODEL_TIMEOUT)

    def _sync_generate_text(self, prompt: str, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
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
        
        # Prepare default parameters for self.llm
        llm_kwargs = {
            "max_tokens": 1024,
            "stop": ["<|im_end|>", "<|im_start|>"],
            "echo": False
        }
        # Override with any provided kwargs
        llm_kwargs.update(kwargs)
        
        response = self.llm(
            formatted_prompt,
            **llm_kwargs
        )
        
        text = response['choices'][0]['text'].strip()
        usage = response.get('usage', {})
        input_tokens = usage.get('prompt_tokens', 0)
        output_tokens = usage.get('completion_tokens', 0)

        return self.create_response(
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens
        )

    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None, **kwargs) -> LLMResponse:
        import json
        
        schema_json = json.dumps(schema.model_json_schema(), indent=2)
        enriched_prompt = f"{prompt}\n\nReturn ONLY a JSON object matching this schema:\n{schema_json}"
        
        # Filter out internal metadata from kwargs
        filtered_kwargs = self._filter_kwargs(kwargs)

        # generate_text already handles the response creation and filtering
        return await self.generate_text(enriched_prompt, system_message, **filtered_kwargs)
