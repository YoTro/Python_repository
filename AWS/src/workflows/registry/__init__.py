from __future__ import annotations
"""
WorkflowRegistry — maps workflow names to builder functions.

Builder functions receive a merged config dict and return a Workflow instance.
Registration is done via decorator:

    @WorkflowRegistry.register("product_screening")
    def build_screening(config: dict) -> Workflow:
        return Workflow(name="product_screening", steps=[...])
"""

import logging
from typing import Callable, Dict, List

from src.workflows.engine import Workflow

logger = logging.getLogger(__name__)


class WorkflowRegistry:
    """
    Registry of workflow builder functions.
    Each builder takes a config dict and returns a Workflow.
    """
    _builders: Dict[str, Callable[[dict], Workflow]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a workflow builder function."""
        def decorator(fn: Callable[[dict], Workflow]):
            cls._builders[name] = fn
            logger.debug(f"Registered workflow: {name}")
            return fn
        return decorator

    @classmethod
    def build(cls, name: str, config: dict = None) -> Workflow:
        """Build a workflow instance from its registered builder."""
        if name not in cls._builders:
            available = ", ".join(cls._builders.keys())
            raise KeyError(
                f"Workflow '{name}' not found. Available: {available}"
            )
        return cls._builders[name](config or {})

    @classmethod
    def list_workflows(cls) -> List[str]:
        """List all registered workflow names."""
        return list(cls._builders.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        """Check if a workflow is registered."""
        return name in cls._builders
