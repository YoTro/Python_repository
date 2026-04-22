from __future__ import annotations
"""
WorkflowSignalBus — decouples "batch completed" detection from workflow resumption.

Inspired by Temporal's Signal pattern:
  BatchPoller  ──publish()──>  SignalBus  ──event.set()──>  JobManager waiter

Upgrade path: replace asyncio.Event with Redis Pub/Sub for distributed mode.
"""

import asyncio
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class WorkflowSignalBus:
    """
    In-process pub/sub bus keyed by workflow job_id.
    Publishers (BatchPoller) call publish(); consumers (JobManager) await subscribe().
    """

    def __init__(self) -> None:
        self._waiters: Dict[str, asyncio.Event] = {}
        self._payloads: Dict[str, dict] = {}

    def subscribe(self, job_id: str) -> asyncio.Event:
        """Return an asyncio.Event that fires when a signal arrives for job_id."""
        if job_id not in self._waiters:
            self._waiters[job_id] = asyncio.Event()
        return self._waiters[job_id]

    def publish(self, job_id: str, data: dict) -> None:
        """Signal that an external event (e.g. batch completed) has occurred."""
        self._payloads[job_id] = data
        event = self._waiters.get(job_id)
        if event:
            event.set()
            logger.info(f"[SignalBus] Signal published: job_id={job_id}")
        else:
            # Job may not be listening yet; payload is kept until consumed
            logger.debug(f"[SignalBus] No active waiter for job_id={job_id}, payload buffered")

    def consume(self, job_id: str) -> dict:
        """Retrieve and remove the payload for job_id. Returns {} if none."""
        self._waiters.pop(job_id, None)
        return self._payloads.pop(job_id, {})

    def unsubscribe(self, job_id: str) -> None:
        self._waiters.pop(job_id, None)
        self._payloads.pop(job_id, None)


_bus: WorkflowSignalBus | None = None


def get_signal_bus() -> WorkflowSignalBus:
    global _bus
    if _bus is None:
        _bus = WorkflowSignalBus()
    return _bus
