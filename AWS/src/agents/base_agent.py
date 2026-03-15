from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
from src.intelligence.router import IntelligenceRouter

logger = logging.getLogger(__name__)

class BaseAgent(ABC):
    """
    Abstract base class for all autonomous agents.
    Agents orchestrate extractors and processors to achieve high-level goals.
    """
    
    def __init__(self, router: IntelligenceRouter):
        self.router = router
        logger.info(f"Agent {self.__class__.__name__} initialized with an IntelligenceRouter.")

    @abstractmethod
    async def run(self, *args, **kwargs) -> Any:
        """Execute the agent's primary task."""
        pass
