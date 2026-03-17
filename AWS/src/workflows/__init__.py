from __future__ import annotations
"""
Workflow Engine — declarative pipeline orchestration.

Core abstractions:
  Workflow        — named sequence of Steps
  WorkflowResult  — execution output with step reports
  WorkflowRegistry — name -> builder function mapping

Step primitives:
  EnrichStep  — fetch data from external sources
  ProcessStep — transform via Python or LLM
  FilterStep  — declarative rule-based filtering

Usage:
    from src.workflows import WorkflowRegistry, Workflow
    workflow = WorkflowRegistry.build("product_screening", config)
    result = await workflow.execute(job_id, params, ctx, callback)
"""

from src.workflows.engine import Workflow, WorkflowResult
from src.workflows.registry import WorkflowRegistry
from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.filter import FilterStep, RangeRule, ThresholdRule, EnumRule, Rule

__all__ = [
    "Workflow",
    "WorkflowResult",
    "WorkflowRegistry",
    "Step",
    "StepResult",
    "WorkflowContext",
    "ComputeTarget",
    "EnrichStep",
    "ProcessStep",
    "FilterStep",
    "RangeRule",
    "ThresholdRule",
    "EnumRule",
    "Rule",
]

# Auto-register workflow definitions
import src.workflows.definitions.product_screening  # noqa: F401
