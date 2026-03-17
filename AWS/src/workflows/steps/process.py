from __future__ import annotations
"""
ProcessStep — transforms items via Python functions or LLM inference.

Routes to the appropriate compute target:
  PURE_PYTHON  — calls fn(items) directly
  LOCAL_LLM    — formats prompt, routes to local model via IntelligenceRouter
  CLOUD_LLM    — formats prompt, routes to cloud model via IntelligenceRouter
"""

import logging
from typing import List, Dict, Any, Callable, Optional, Type

from pydantic import BaseModel

from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget

logger = logging.getLogger(__name__)


class ProcessStep(Step):
    """
    Processing step that applies transformations to items.

    For PURE_PYTHON: fn receives the full items list and returns modified list.
    For LOCAL_LLM / CLOUD_LLM: prompt_template is formatted per item,
        sent to IntelligenceRouter, result parsed and merged into item.
    """

    def __init__(
        self,
        name: str,
        fn: Optional[Callable[[List[dict]], List[dict]]] = None,
        prompt_template: Optional[str] = None,
        output_schema: Optional[Type[BaseModel]] = None,
        output_field: str = None,
        compute_target: ComputeTarget = ComputeTarget.PURE_PYTHON,
        **kwargs,
    ):
        super().__init__(name=name, compute_target=compute_target, **kwargs)
        self.fn = fn
        self.prompt_template = prompt_template
        self.output_schema = output_schema
        self.output_field = output_field or name

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        start = self._start_timer()
        logger.info(f"[{self.name}] Processing {len(items)} items via {self.compute_target.value}")

        if self.compute_target == ComputeTarget.PURE_PYTHON:
            processed = await self._run_python(items, ctx)
        else:
            processed = await self._run_llm(items, ctx)

        elapsed = self._elapsed_ms(start)
        logger.info(f"[{self.name}] Completed in {elapsed}ms")

        return StepResult(
            items=processed,
            metadata={
                "duration_ms": elapsed,
                "input_count": len(items),
                "output_count": len(processed),
                "compute_target": self.compute_target.value,
            },
        )

    async def _run_python(self, items: List[dict], ctx: WorkflowContext) -> List[dict]:
        """Execute a pure Python function on all items."""
        if self.fn is None:
            logger.warning(f"[{self.name}] No fn provided for PURE_PYTHON step, passing through")
            return items
        
        import asyncio
        import inspect
        
        if asyncio.iscoroutinefunction(self.fn):
            sig = inspect.signature(self.fn)
            if 'ctx' in sig.parameters:
                return await self.fn(items, ctx)
            return await self.fn(items)
        else:
            sig = inspect.signature(self.fn)
            if 'ctx' in sig.parameters:
                return self.fn(items, ctx)
            return self.fn(items)

    async def _run_llm(self, items: List[dict], ctx: WorkflowContext) -> List[dict]:
        """Execute LLM inference for each item (or batch)."""
        if not self.prompt_template:
            logger.warning(f"[{self.name}] No prompt_template provided for LLM step, passing through")
            return items

        router = ctx.router
        if router is None:
            logger.warning(f"[{self.name}] No IntelligenceRouter in context, skipping LLM step")
            return items

        from src.intelligence.router import TaskCategory

        # Determine routing category
        if self.compute_target == ComputeTarget.LOCAL_LLM:
            category = TaskCategory.SIMPLE_CLEANING
        else:
            category = TaskCategory.DEEP_REASONING

        prompts_to_process = []
        items_to_process = []
        
        for item in items:
            cache_key = f"{ctx.job_id}:{self.name}:{item.get('asin', hash(str(item)))}"
            if cache_key in ctx.cache:
                item[self.output_field] = ctx.cache[cache_key]
                continue
                
            try:
                # Format prompt with item data
                prompt = self.prompt_template.format(
                    count=len(items),
                    **{k: v for k, v in item.items() if isinstance(v, (str, int, float, bool, type(None)))},
                )
                prompts_to_process.append(prompt)
                items_to_process.append((item, cache_key))
            except Exception as e:
                logger.warning(f"[{self.name}] Prompt formatting failed for item: {e}")
                item[self.output_field] = None

        if not prompts_to_process:
            return items

        # Fast token estimation
        estimated_tokens = sum(len(p) // 4 for p in prompts_to_process)
        logger.info(f"[{self.name}] Initiating batch execution for {len(prompts_to_process)} prompts (est. {estimated_tokens} tokens)...")
        
        try:
            if self.output_schema:
                results = await router.batch_route_and_execute(
                    prompts_to_process, category=category, schema=self.output_schema
                )
            else:
                results = await router.batch_route_and_execute(
                    prompts_to_process, category=category
                )
                
            for (item, cache_key), result in zip(items_to_process, results):
                # If result is an Exception string or actual Exception
                if isinstance(result, Exception) or (isinstance(result, str) and result.startswith("Exception:")):
                    logger.warning(f"[{self.name}] LLM processing failed for item: {result}")
                    item[self.output_field] = None
                else:
                    item[self.output_field] = result.model_dump() if hasattr(result, "model_dump") else result
                    ctx.cache[cache_key] = item[self.output_field]

        except Exception as e:
            logger.error(f"[{self.name}] Batch processing failed: {e}")
            for item, _ in items_to_process:
                item[self.output_field] = None

        return items
