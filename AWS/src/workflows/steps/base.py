from __future__ import annotations
"""
Step primitives — the atomic building blocks of all Workflows.

ComputeTarget determines where the step executes:
  PURE_PYTHON  — deterministic code, zero cost, <1ms
  LOCAL_LLM    — local inference (llama.cpp / Ollama), zero API cost, <500ms
  CLOUD_LLM    — cloud API (Gemini / Claude), per-token billing, 2-10s
"""

import time
import logging
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class ComputeTarget(Enum):
    PURE_PYTHON = "pure_python"
    LOCAL_LLM = "local_llm"
    CLOUD_LLM = "cloud_llm"


@dataclass
class StepResult:
    """Output of a single Step execution."""
    items: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    # Common metadata keys:
    #   duration_ms, cost_usd, filtered_count, data_source,
    #   input_count, output_count, rejection_reasons


@dataclass
class WorkflowContext:
    """Shared context passed to every Step in a Workflow."""
    job_id: str
    config: Dict[str, Any] = field(default_factory=dict)
    cache: Dict[str, Any] = field(default_factory=dict)
    router: Any = None  # IntelligenceRouter instance (injected)
    mcp: Any = None     # MCPClient instance (injected)
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("workflow"))


class Step(ABC):
    """Abstract base class for all workflow steps."""

    def __init__(
        self,
        name: str,
        compute_target: ComputeTarget = ComputeTarget.PURE_PYTHON,
        enabled: bool = True,
        min_plan: Optional[str] = None,
    ):
        self.name = name
        self.compute_target = compute_target
        self.enabled = enabled
        self.min_plan = min_plan  # Reserved for multi-tenant: "free" | "pro" | "enterprise"

    @abstractmethod
    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        """Execute this step on the given items."""
        ...

    def is_enabled(self, plan_tier: str = "pro", config: dict = None) -> bool:
        """Check if this step should run given the current plan and config."""
        if not self.enabled:
            return False
        if config and not config.get(f"enable_{self.name}", True):
            return False
        # Plan tier check (for future multi-tenant)
        plan_order = {"free": 0, "pro": 1, "enterprise": 2}
        if self.min_plan and plan_order.get(plan_tier, 1) < plan_order.get(self.min_plan, 0):
            return False
        return True

    def _start_timer(self) -> float:
        return time.monotonic()

    def _elapsed_ms(self, start: float) -> int:
        return int((time.monotonic() - start) * 1000)
