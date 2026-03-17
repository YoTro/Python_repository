from __future__ import annotations
"""
FilterStep — declarative rule-based filtering of items.

Rules are pure Python, zero external dependencies, deterministic.
Each rule implements check(item) -> bool.
FilterStep tracks rejection reasons for reporting.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from collections import Counter

from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rule primitives
# ---------------------------------------------------------------------------

class Rule(ABC):
    """Abstract rule that checks a single item."""

    def __init__(self, field: str):
        self.field = field

    @abstractmethod
    def check(self, item: dict) -> bool:
        """Return True if item passes the rule."""
        ...

    @abstractmethod
    def describe(self) -> str:
        """Human-readable description of this rule."""
        ...


class RangeRule(Rule):
    """Check that field value is within [min_val, max_val]. None means no bound."""

    def __init__(self, field: str, min_val: Optional[float] = None, max_val: Optional[float] = None):
        super().__init__(field)
        self.min_val = min_val
        self.max_val = max_val

    def check(self, item: dict) -> bool:
        value = item.get(self.field)
        if value is None:
            return False  # Missing data fails the filter
        try:
            value = float(value)
        except (ValueError, TypeError):
            return False
        if self.min_val is not None and value < self.min_val:
            return False
        if self.max_val is not None and value > self.max_val:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.min_val is not None:
            parts.append(f">= {self.min_val}")
        if self.max_val is not None:
            parts.append(f"<= {self.max_val}")
        return f"{self.field} {' and '.join(parts)}"


class ThresholdRule(Rule):
    """Check that field value meets a minimum or maximum threshold."""

    def __init__(self, field: str, min_val: Optional[float] = None, max_val: Optional[float] = None):
        super().__init__(field)
        self.min_val = min_val
        self.max_val = max_val

    def check(self, item: dict) -> bool:
        value = item.get(self.field)
        if value is None:
            return False
        try:
            value = float(value)
        except (ValueError, TypeError):
            return False
        if self.min_val is not None and value < self.min_val:
            return False
        if self.max_val is not None and value > self.max_val:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.min_val is not None:
            parts.append(f"min {self.min_val}")
        if self.max_val is not None:
            parts.append(f"max {self.max_val}")
        return f"{self.field}: {', '.join(parts)}"


class EnumRule(Rule):
    """Check that field value is in the allowed set."""

    def __init__(self, field: str, allowed: List[Any]):
        super().__init__(field)
        self.allowed = set(allowed)

    def check(self, item: dict) -> bool:
        value = item.get(self.field)
        if value is None:
            return False
        return value in self.allowed

    def describe(self) -> str:
        return f"{self.field} in {sorted(self.allowed)}"


class CompositeRule(Rule):
    """Combine multiple rules with AND (all) or OR (any) logic."""

    def __init__(self, field: str, rules: List[Rule], mode: str = "all"):
        super().__init__(field)
        self.rules = rules
        self.mode = mode  # "all" or "any"

    def check(self, item: dict) -> bool:
        if self.mode == "any":
            return any(rule.check(item) for rule in self.rules)
        return all(rule.check(item) for rule in self.rules)

    def describe(self) -> str:
        op = " AND " if self.mode == "all" else " OR "
        return f"({op.join(r.describe() for r in self.rules)})"


# ---------------------------------------------------------------------------
# FilterStep
# ---------------------------------------------------------------------------

class FilterStep(Step):
    """
    Declarative filter step that applies rules to each item.
    Always runs as PURE_PYTHON — no LLM needed.
    Tracks which rules caused rejections for funnel reporting.
    """

    def __init__(self, name: str, rules: List[Rule], **kwargs):
        kwargs.pop("compute_target", None)  # Force PURE_PYTHON
        super().__init__(name=name, compute_target=ComputeTarget.PURE_PYTHON, **kwargs)
        self.rules = rules

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        start = self._start_timer()
        logger.info(f"[{self.name}] Filtering {len(items)} items with {len(self.rules)} rules")

        passed = []
        rejection_reasons: Counter = Counter()

        for item in items:
            item_passed = True
            for rule in self.rules:
                if not rule.check(item):
                    rejection_reasons[rule.describe()] += 1
                    item_passed = False
                    break  # Fail fast on first rule violation

            if item_passed:
                passed.append(item)

        filtered_count = len(items) - len(passed)
        elapsed = self._elapsed_ms(start)

        logger.info(
            f"[{self.name}] {len(passed)}/{len(items)} passed "
            f"({filtered_count} filtered out) in {elapsed}ms"
        )
        if rejection_reasons:
            top_reasons = rejection_reasons.most_common(3)
            for reason, count in top_reasons:
                logger.info(f"  Top rejection: {reason} ({count} items)")

        return StepResult(
            items=passed,
            metadata={
                "duration_ms": elapsed,
                "input_count": len(items),
                "output_count": len(passed),
                "filtered_count": filtered_count,
                "rejection_reasons": dict(rejection_reasons),
            },
        )
