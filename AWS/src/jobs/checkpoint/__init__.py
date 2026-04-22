from __future__ import annotations
"""
CheckpointManager — step-level checkpoint persistence with event log.

Single-user version: stores checkpoints as local JSON files.
Upgrade path: swap to Redis + S3 by implementing the same interface.

Event log follows Temporal's event-sourcing pattern:
  STEP_STARTED, ACTIVITY_COMPLETED, BATCH_SUBMITTED, BATCH_COMPLETED, HEARTBEAT
"""

import os
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime

from src.core.errors.exceptions import CheckpointError

logger = logging.getLogger(__name__)


@dataclass
class WorkflowEvent:
    """A single immutable entry in the workflow event log."""
    timestamp: float
    event_type: str   # STEP_STARTED | ACTIVITY_COMPLETED | BATCH_SUBMITTED | BATCH_COMPLETED | HEARTBEAT
    step_name: str
    payload: dict


@dataclass
class CheckpointData:
    """Snapshot of workflow state at a completed step."""
    job_id: str
    step_index: int
    step_name: str
    items: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    # Resume context — stored so callers don't need to re-supply them
    workflow_name: str = ""
    workflow_params: Dict[str, Any] = field(default_factory=dict)
    # Workflow context cache — populated by earlier steps, needed by later ones
    ctx_cache: Dict[str, Any] = field(default_factory=dict)
    # Append-only event log for durable execution (Temporal-inspired)
    events: List[Dict[str, Any]] = field(default_factory=list)


class CheckpointManager:
    """
    Manages step-level checkpoints for workflow resume-on-failure.

    Storage: local JSON files at data/checkpoints/{job_id}.json
    """

    def __init__(self, checkpoint_dir: str = None):
        self.checkpoint_dir = checkpoint_dir or os.path.abspath(os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "data", "checkpoints"
        ))
        os.makedirs(self.checkpoint_dir, exist_ok=True)

    def _path(self, job_id: str) -> str:
        return os.path.join(self.checkpoint_dir, f"{job_id}.json")

    def save(
        self,
        job_id: str,
        step_index: int,
        step_name: str,
        items: List[Dict[str, Any]],
        metadata: Dict[str, Any] = None,
        workflow_name: str = "",
        workflow_params: Dict[str, Any] = None,
        ctx_cache: Dict[str, Any] = None,
    ) -> None:
        """Save checkpoint after a step completes."""
        checkpoint = CheckpointData(
            job_id=job_id,
            step_index=step_index,
            step_name=step_name,
            items=items,
            metadata=metadata or {},
            workflow_name=workflow_name,
            workflow_params=workflow_params or {},
            ctx_cache=ctx_cache or {},
        )

        path = self._path(job_id)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(asdict(checkpoint), f, ensure_ascii=False, default=str)
            logger.debug(
                f"Checkpoint saved: job={job_id}, step={step_index} ({step_name}), "
                f"{len(items)} items"
            )
        except Exception as e:
            raise CheckpointError(f"Failed to save checkpoint for {job_id}: {e}")

    def load(self, job_id: str) -> Optional[CheckpointData]:
        """Load the latest checkpoint for a job. Returns None if not found."""
        path = self._path(job_id)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Deserialize events list from raw dicts back to WorkflowEvent objects
            raw_events = data.pop("events", [])
            checkpoint = CheckpointData(**data)
            checkpoint.events = [
                WorkflowEvent(**e) if isinstance(e, dict) else e
                for e in raw_events
            ]
            logger.info(
                f"Checkpoint loaded: job={job_id}, resuming from step "
                f"{checkpoint.step_index} ({checkpoint.step_name}), "
                f"{len(checkpoint.events)} events"
            )
            return checkpoint
        except Exception as e:
            logger.warning(f"Failed to load checkpoint for {job_id}: {e}")
            return None

    def append_event(self, job_id: str, event: WorkflowEvent) -> None:
        """
        Append a single event to the checkpoint's event log.
        Read-modify-write — safe for single-process; upgrade to Redis LPUSH for distributed.
        """
        checkpoint = self.load(job_id)
        if checkpoint is None:
            logger.warning(f"append_event: no checkpoint found for job_id={job_id}, skipping")
            return
        checkpoint.events.append(event)
        path = self._path(job_id)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(asdict(checkpoint), f, ensure_ascii=False, default=str)
        except Exception as e:
            raise CheckpointError(f"Failed to append event for {job_id}: {e}")

    def clear(self, job_id: str) -> None:
        """Delete checkpoint after successful workflow completion."""
        path = self._path(job_id)
        try:
            if os.path.exists(path):
                os.remove(path)
                logger.debug(f"Checkpoint cleared: job={job_id}")
        except Exception as e:
            logger.warning(f"Failed to clear checkpoint for {job_id}: {e}")

    def list_checkpoints(self) -> List[str]:
        """List all job IDs with active checkpoints."""
        try:
            return [
                f.replace(".json", "")
                for f in os.listdir(self.checkpoint_dir)
                if f.endswith(".json")
            ]
        except Exception:
            return []
