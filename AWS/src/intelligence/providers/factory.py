from __future__ import annotations
import os
import logging
from typing import Optional
from .base import BaseLLMProvider
from .gemini import GeminiProvider
from .claude import ClaudeProvider
from .llama_cpp import LlamaCppProvider

logger = logging.getLogger(__name__)

class ProviderFactory:
    """
    Factory to create LLM providers based on environment configuration.
    Supports Claude (Anthropic), Gemini (Google), and Llama.cpp (Local).
    Set DEFAULT_LLM_PROVIDER in .env to: claude | gemini | local
    """

    @staticmethod
    def get_provider(provider_type: Optional[str] = None) -> BaseLLMProvider:
        ptype = provider_type or os.getenv("DEFAULT_LLM_PROVIDER", "gemini").lower()

        if ptype == "claude" or ptype == "anthropic":
            model = os.getenv("CLAUDE_MODEL")  # optional override
            return ClaudeProvider(model_name=model)

        elif ptype == "gemini":
            return GeminiProvider()

        elif ptype == "local" or ptype == "llama":
            model_path = os.getenv("LOCAL_MODEL_PATH")
            if not model_path:
                raise ValueError("LOCAL_MODEL_PATH must be set in .env to use local provider.")
            
            # Convert relative path to absolute path based on project root
            if not os.path.isabs(model_path):
                # Assume project root is 3 levels up from this file: src/intelligence/providers -> src -> project_root
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
                model_path = os.path.join(project_root, model_path)
                logger.info(f"Resolved LOCAL_MODEL_PATH to absolute: {model_path}")

            return LlamaCppProvider(model_path=model_path)

        else:
            raise ValueError(f"Unsupported LLM Provider type: {ptype}")

def get_default_provider() -> BaseLLMProvider:
    return ProviderFactory.get_provider()
