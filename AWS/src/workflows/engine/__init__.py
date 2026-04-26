from __future__ import annotations
"""
Workflow Engine — executes a sequence of Steps with checkpoint support.

Features:
  - Sequential step execution with progress callbacks
  - Checkpoint after each step for resume-on-failure
  - Funnel early termination (stop when no items remain)
  - RetryableError → re-queue, FatalError → abort
"""

import time
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from src.workflows.steps.base import Step, WorkflowContext, StepResult
from src.core.errors.exceptions import RetryableError, FatalError, StepError, BatchPendingError
from src.workflows.engine.activity_runner import ActivityRunner

logger = logging.getLogger(__name__)


@dataclass
class StepReport:
    """Report for a single step execution."""
    step_name: str
    step_index: int
    input_count: int
    output_count: int
    filtered_count: int
    duration_ms: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowResult:
    """Final result of a complete workflow execution."""
    name: str
    final_items: List[Dict[str, Any]] = field(default_factory=list)
    step_reports: List[StepReport] = field(default_factory=list)
    total_duration_ms: int = 0
    completed: bool = False


class Workflow:
    """
    A named sequence of Steps that form a processing pipeline.

    Usage:
        workflow = Workflow(name="product_screening", steps=[...])
        result = await workflow.execute(job_id, params, ctx, callback, checkpoint_mgr)
    """

    def __init__(self, name: str, steps: List[Step]):
        self.name = name
        self.steps = steps

    async def execute(
        self,
        job_id: str,
        params: dict,
        ctx: WorkflowContext,
        callback=None,
        checkpoint_mgr=None,
    ) -> WorkflowResult:
        """
        Execute all steps sequentially.

        Args:
            job_id: Unique job identifier.
            params: Job parameters (already merged with defaults).
            ctx: Shared workflow context.
            callback: Optional JobCallback for progress reporting.
            checkpoint_mgr: Optional CheckpointManager for resume support.
        """
        total_start = time.monotonic()
        result = WorkflowResult(name=self.name)
        activity_runner = ActivityRunner(checkpoint_mgr, job_id)

        # Check for checkpoint to resume from
        start_index = 0
        items = params.get("initial_items") or []
        if not items:
            # Convenience: seed from a bare asin / asins param
            asin = params.get("asin")
            asins = params.get("asins", [asin] if asin else [])
            items = [{"asin": a} for a in asins if a]
        if not items and params.get("keyword"):
            # Keyword-based workflows: seed a single item; the first step expands it
            items = [{"keyword": params["keyword"]}]

        if checkpoint_mgr:
            checkpoint = checkpoint_mgr.load(job_id)
            if checkpoint:
                start_index = checkpoint.step_index + 1
                items = checkpoint.items
                if checkpoint.ctx_cache:
                    ctx.cache.update(checkpoint.ctx_cache)
                logger.info(
                    f"Resuming workflow '{self.name}' from step {start_index} "
                    f"({len(items)} items, {len(checkpoint.ctx_cache)} cache keys restored)"
                )

        # Execute steps
        active_steps = [s for s in self.steps if s.is_enabled(config=ctx.config)]
        total_steps = len(active_steps)

        for i, step in enumerate(active_steps):
            if i < start_index:
                continue  # Skip already-completed steps

            # Progress callback
            if callback:
                try:
                    await callback.on_progress(
                        step_index=i + 1,
                        total_steps=total_steps,
                        step_name=step.name,
                        message=f"Processing {len(items)} items",
                    )
                except Exception as e:
                    logger.warning(f"Callback on_progress failed: {e}")

            # Execute step via ActivityRunner (idempotency + heartbeat + batch suspend)
            try:
                step_result = await activity_runner.run(step, items, ctx, step_index=i)
                items = step_result.items

                # Record step report
                report = StepReport(
                    step_name=step.name,
                    step_index=i,
                    input_count=step_result.metadata.get("input_count", 0),
                    output_count=step_result.metadata.get("output_count", len(items)),
                    filtered_count=step_result.metadata.get("filtered_count", 0),
                    duration_ms=step_result.metadata.get("duration_ms", 0),
                    metadata=step_result.metadata,
                )
                result.step_reports.append(report)

                # Save checkpoint
                if checkpoint_mgr:
                    try:
                        checkpoint_mgr.save(
                            job_id=job_id,
                            step_index=i,
                            step_name=step.name,
                            items=items,
                            metadata=step_result.metadata,
                            workflow_name=self.name,
                            workflow_params=params,
                            ctx_cache=dict(ctx.cache),
                        )
                    except Exception as e:
                        logger.warning(f"Checkpoint save failed: {e}")

                # Funnel termination
                if not items:
                    logger.info(
                        f"Workflow '{self.name}' terminated early at step "
                        f"'{step.name}': no items remaining"
                    )
                    break

                logger.info(
                    f"Step '{step.name}' complete: {report.input_count} → "
                    f"{report.output_count} items"
                )

            except BatchPendingError:
                # Checkpoint already has BATCH_SUBMITTED event (written by ActivityRunner).
                # Propagate to JobManager to transition job → SUSPENDED.
                logger.info(f"Step '{step.name}' suspended: waiting for batch completion")
                raise

            except RetryableError:
                logger.warning(
                    f"RetryableError at step '{step.name}', "
                    f"checkpoint saved at step {i}"
                )
                raise  # Let JobManager handle retry/requeue

            except FatalError as e:
                logger.error(f"FatalError at step '{step.name}': {e}")
                if callback:
                    try:
                        await callback.on_error(e)
                    except Exception:
                        pass
                raise

            except Exception as e:
                logger.error(f"Unexpected error at step '{step.name}': {e}")
                raise StepError(
                    message=str(e),
                    step_name=step.name,
                    step_index=i,
                )

        # Complete
        result.final_items = items
        result.total_duration_ms = int((time.monotonic() - total_start) * 1000)
        result.completed = True

        # Clean up checkpoint on success
        if checkpoint_mgr:
            try:
                checkpoint_mgr.clear(job_id)
            except Exception as e:
                logger.warning(f"Checkpoint clear failed: {e}")

        logger.info(
            f"Workflow '{self.name}' completed in {result.total_duration_ms}ms, "
            f"{len(result.final_items)} final items"
        )

        return result
