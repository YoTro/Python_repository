from __future__ import annotations
"""
EnrichStep — fetches data from external sources and attaches new fields to items.

Wraps an async extractor function with:
  - Parallel execution via asyncio.gather + semaphore
  - Idempotent caching per (job_id, step_name, item_key)
  - Graceful partial failure handling
"""

import asyncio
import logging
from typing import List, Dict, Any, Callable, Awaitable, Optional

from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget

logger = logging.getLogger(__name__)


class EnrichStep(Step):
    """
    Enrichment step that calls an async extractor function for each item.

    Args:
        name: Step name for logging and checkpoints.
        extractor_fn: Async callable (item: dict) -> dict of new fields.
        fields: Optional list of field names to extract (documentation only).
        parallel: If True, fetch items concurrently with semaphore.
        concurrency: Max concurrent extractor calls (default 5).
    """

    def __init__(
        self,
        name: str,
        extractor_fn: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
        fields: Optional[List[str]] = None,
        parallel: bool = True,
        concurrency: int = 5,
        **kwargs,
    ):
        super().__init__(name=name, compute_target=ComputeTarget.PURE_PYTHON, **kwargs)
        self.extractor_fn = extractor_fn
        self.fields = fields
        self.parallel = parallel
        self.concurrency = concurrency

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        start = self._start_timer()
        logger.info(f"[{self.name}] Enriching {len(items)} items (parallel={self.parallel})")

        if self.parallel:
            semaphore = asyncio.Semaphore(self.concurrency)
            results = await asyncio.gather(
                *[self._fetch_one(item, ctx, semaphore) for item in items],
                return_exceptions=True,
            )
        else:
            results = []
            for item in items:
                try:
                    results.append(await self._fetch_one(item, ctx))
                except Exception as e:
                    results.append(e)

        # Separate successes and failures
        enriched = []
        errors = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                errors.append({"index": i, "error": str(result)})
                # Keep the original item even if enrichment failed
                enriched.append(items[i])
            else:
                enriched.append(result)

        if errors:
            logger.warning(f"[{self.name}] {len(errors)}/{len(items)} items failed enrichment")

        elapsed = self._elapsed_ms(start)
        logger.info(f"[{self.name}] Completed in {elapsed}ms, {len(enriched)} items")

        return StepResult(
            items=enriched,
            metadata={
                "duration_ms": elapsed,
                "input_count": len(items),
                "output_count": len(enriched),
                "error_count": len(errors),
                "errors": errors[:5],  # Keep first 5 errors for debugging
                "data_source": self.name,
            },
        )

    async def _fetch_one(
        self,
        item: Dict[str, Any],
        ctx: WorkflowContext,
        semaphore: asyncio.Semaphore = None,
    ) -> Dict[str, Any]:
        """Fetch data for a single item with caching and optional semaphore."""
        item_key = self._get_item_key(item)
        cache_key = f"{ctx.job_id}:{self.name}:{item_key}"

        # Idempotent: return cached result if available
        if cache_key in ctx.cache:
            return ctx.cache[cache_key]

        async def _do_fetch():
            new_fields = await self.extractor_fn(item)
            # Merge new fields into a copy of the item
            merged = {**item, **new_fields}
            ctx.cache[cache_key] = merged
            return merged

        if semaphore:
            async with semaphore:
                return await _do_fetch()
        return await _do_fetch()

    @staticmethod
    def _get_item_key(item: dict) -> str:
        """Generate a stable key for caching."""
        if "asin" in item:
            return item["asin"]
        if "url" in item:
            return item["url"]
        # Fallback: hash of sorted items
        return str(hash(tuple(sorted(item.items()))))
