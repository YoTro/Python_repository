from __future__ import annotations
from abc import ABC, abstractmethod
from typing import List, Optional, Any
from src.intelligence.dto import LLMResponse

class BaseLLMProvider(ABC):
    """
    Abstract base class for all LLM providers.
    Ensures a consistent interface for the Agents and Processors.
    """
    
    @abstractmethod
    async def generate_text(self, prompt: str, system_message: Optional[str] = None) -> LLMResponse:
        """Simple text generation."""
        pass

    @abstractmethod
    async def generate_structured(self, prompt: str, schema: Any, system_message: Optional[str] = None) -> LLMResponse:
        """
        Generation that returns a structured object (Pydantic model).
        """
        pass

    @abstractmethod
    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        """
        Calculate the total token count for a given prompt and system message.
        Essential for cost control and switching between Online/Batch APIs.
        """
        pass

    @abstractmethod
    async def batch_generate_text(self, prompts: List[str], system_message: Optional[str] = None, concurrency: int = 5) -> List[LLMResponse]:
        """Generate text for a batch of prompts efficiently."""
        pass

    @abstractmethod
    async def batch_generate_structured(self, prompts: List[str], schema: Any, system_message: Optional[str] = None, concurrency: int = 5) -> List[LLMResponse]:
        """Generate structured objects for a batch of prompts."""
        pass
