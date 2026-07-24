from __future__ import annotations

"""
EnrichStep — fetches data from external sources and attaches new fields to items.

Wraps an async extractor function with:
  - Parallel execution via asyncio.gather + semaphore
  - Idempotent caching per (job_id, step_name, item_key)
  - Graceful partial failure handling
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from src.workflows.steps.base import ComputeTarget, Step, StepResult, WorkflowContext

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
        extractor_fn: Callable[[dict[str, Any], WorkflowContext], Awaitable[dict[str, Any]]],
        fields: list[str] | None = None,
        parallel: bool = True,
        concurrency: int = 5,
        **kwargs,
    ):
        super().__init__(name=name, compute_target=ComputeTarget.PURE_PYTHON, **kwargs)
        self.extractor_fn = extractor_fn
        self.fields = fields
        self.parallel = parallel
        self.concurrency = concurrency

    async def run(self, items: list[dict[str, Any]], ctx: WorkflowContext) -> StepResult:
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
            for e in errors[:3]:
                logger.warning(f"[{self.name}] error[{e['index']}]: {e['error']}")

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
        item: dict[str, Any],
        ctx: WorkflowContext,
        semaphore: asyncio.Semaphore = None,
    ) -> dict[str, Any]:
        """Fetch data for a single item with caching and optional semaphore."""
        item_key = self._get_item_key(item)
        cache_key = f"{ctx.job_id}:{self.name}:{item_key}"

        # Idempotent: return cached result if available
        if cache_key in ctx.cache:
            return ctx.cache[cache_key]

        async def _do_fetch():
            new_fields = await self.extractor_fn(item, ctx)
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
        asin = item.get("asin") or item.get("ASIN")
        if asin:
            return str(asin).strip().upper()
        if "url" in item:
            return item["url"]
        # Fallback: stable serialization that tolerates nested dict/list values.
        return str(hash(json.dumps(item, sort_keys=True, default=str)))
