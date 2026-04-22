from __future__ import annotations
"""
ActivityRunner — execution isolation layer inspired by Temporal's Activity concept.

Responsibilities:
  1. Idempotency   — if ACTIVITY_COMPLETED already in event log, replay cached result
  2. Batch resume  — if BATCH_SUBMITTED found without BATCH_COMPLETED, re-raise
                     BatchPendingError so WorkflowEngine keeps the job SUSPENDED
  3. Heartbeat     — inject a heartbeat callable into WorkflowContext so long-running
                     Steps can signal liveness to the _reaper_loop
  4. Event logging — append BATCH_SUBMITTED / ACTIVITY_COMPLETED events atomically

WorkflowEngine calls:
    result = await activity_runner.run(step, items, ctx)
instead of:
    result = await step.run(items, ctx)
"""

import time
import logging
from dataclasses import asdict
from typing import Optional, List, Dict, Any

from src.workflows.steps.base import Step, StepResult, WorkflowContext
from src.jobs.checkpoint import CheckpointManager, WorkflowEvent
from src.core.errors.exceptions import BatchPendingError

logger = logging.getLogger(__name__)


class ActivityRunner:

    def __init__(self, checkpoint_mgr: Optional[CheckpointManager], job_id: str) -> None:
        self.checkpoint_mgr = checkpoint_mgr
        self.job_id = job_id

    # ── Private helpers ───────────────────────────────────────────────────

    def _find_latest(self, step_name: str, event_type: str) -> Optional[WorkflowEvent]:
        """Scan event log (newest-first) for the requested event."""
        if not self.checkpoint_mgr:
            return None
        checkpoint = self.checkpoint_mgr.load(self.job_id)
        if not checkpoint:
            return None
        for event in reversed(checkpoint.events):
            if event.step_name == step_name and event.event_type == event_type:
                return event
        return None

    def _append(self, step_name: str, event_type: str, payload: dict) -> None:
        if not self.checkpoint_mgr:
            return
        self.checkpoint_mgr.append_event(self.job_id, WorkflowEvent(
            timestamp=time.time(),
            event_type=event_type,
            step_name=step_name,
            payload=payload,
        ))

    # ── Public interface ──────────────────────────────────────────────────

    async def run(
        self,
        step: Step,
        items: List[Dict[str, Any]],
        ctx: WorkflowContext,
        step_index: int = -1,
    ) -> StepResult:
        """
        Execute *step* with durable-execution guarantees.

        On first run  : executes normally, appends ACTIVITY_COMPLETED on success.
        On replay     : returns cached result from ACTIVITY_COMPLETED — no API call.
        On batch wait : BATCH_SUBMITTED present but no BATCH_COMPLETED → re-raises
                        BatchPendingError to keep the workflow suspended.
        On batch done : BATCH_COMPLETED present → reconstructs StepResult, appends
                        ACTIVITY_COMPLETED, returns result.
        """
        # 1. Idempotency: step already ran and succeeded in a previous attempt
        completed = self._find_latest(step.name, "ACTIVITY_COMPLETED")
        if completed:
            logger.info(f"[ActivityRunner] Replaying cached result for step '{step.name}'")
            r = completed.payload["result"]
            return StepResult(items=r["items"], metadata=r["metadata"])

        # 2. Batch resume path
        batch_submitted = self._find_latest(step.name, "BATCH_SUBMITTED")
        if batch_submitted:
            batch_completed = self._find_latest(step.name, "BATCH_COMPLETED")
            if batch_completed:
                # BatchPoller wrote results into the event log — use them
                r = batch_completed.payload["result"]
                result = StepResult(items=r["items"], metadata=r["metadata"])
                self._append(step.name, "ACTIVITY_COMPLETED", {
                    "result": {"items": result.items, "metadata": result.metadata}
                })
                logger.info(f"[ActivityRunner] Batch result applied for step '{step.name}'")
                return result
            else:
                # Still waiting for BatchPoller signal
                from src.intelligence.dto import BatchJobHandle
                handle = BatchJobHandle(**batch_submitted.payload["handle"])
                raise BatchPendingError(
                    f"Batch '{handle.job_id}' still pending for step '{step.name}'",
                    batch_job_id=handle.job_id,
                    handle=handle,
                )

        # 3. Inject heartbeat so the step can signal liveness during long operations
        def _heartbeat(status: dict) -> None:
            self._append(step.name, "HEARTBEAT", status)
            logger.debug(f"[ActivityRunner] Heartbeat: step='{step.name}' status={status}")

        ctx.heartbeat = _heartbeat

        # 4. Execute the step
        try:
            result = await step.run(items, ctx)
        except BatchPendingError as e:
            self._append(step.name, "BATCH_SUBMITTED", {
                "handle": asdict(e.handle),
                "step_index": step_index,             # WorkflowEngine resume position
                "request_count": len(e.requests),     # for completeness validation
                "requests": e.requests,
                "items_snapshot": e.items_snapshot,
                "output_field": e.output_field,
                "schema_path": e.schema_path,
            })
            logger.info(
                f"[ActivityRunner] Batch submitted for step '{step.name}' "
                f"(index={step_index}): batch_id={e.batch_job_id}, "
                f"requests={len(e.requests)}"
            )
            raise

        # 5. Persist successful result so future replays skip the API call
        self._append(step.name, "ACTIVITY_COMPLETED", {
            "result": {"items": result.items, "metadata": result.metadata}
        })
        return result
