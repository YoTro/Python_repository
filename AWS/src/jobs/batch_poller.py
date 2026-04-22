from __future__ import annotations
"""
BatchPoller — background service that polls provider batch jobs and resumes workflows.

Flow:
  1. Every TICK_INTERVAL seconds, scan all checkpoint files.
  2. Find steps with BATCH_SUBMITTED but no BATCH_COMPLETED event.
  3. Read the latest BATCH_POLLING_HEARTBEAT to decide whether it is time to poll:
       - If time.time() < next_poll_at → skip (not due yet)
       - Otherwise → call provider.poll_batch(handle)
  4. provider.poll_batch returns:
       - None  → still running; write BATCH_POLLING_HEARTBEAT with next interval
       - dict  → complete; reconstruct StepResult, write BATCH_COMPLETED
  5. Publish signal → SignalBus → JobManager.resume(job_id).

Polling strategy — exponential backoff with ±10% jitter:
  initial_interval = 60 s
  multiplier       = 1.5×  per missed poll
  max_interval     = 600 s (10 min)
  The progression naturally forms three phases:
    0–5 min   : ~60 s   (fast probe — catches small / fast batches)
    5–50 min  : 90–454 s (exponentially relaxing)
    50 min+   : capped at 600 s (10 min ceiling — 1 h batch = ~6 polls)

State is stored in BATCH_POLLING_HEARTBEAT checkpoint events so process
restarts resume at the correct backoff interval rather than resetting to 60 s.

On process restart:
  BatchPoller.start() is called by JobManager.__init__, which re-scans
  all checkpoint files, so no in-flight batches are lost.
"""

import asyncio
import importlib
import json
import logging
import math
import random
import time
from typing import Dict, Optional

_GEMINI_BATCH_TTL = 86400   # Gemini batch jobs expire after 24 h
_CLAUDE_BATCH_TTL = 86400   # Claude batch jobs expire after 24 h

_INITIAL_INTERVAL = 60      # seconds before the first poll
_MAX_INTERVAL     = 600     # cap: 10 minutes
_MULTIPLIER       = 1.5     # backoff factor per missed poll
_JITTER           = 0.10    # ±10% random jitter

from src.intelligence.dto import BatchJobHandle, LLMResponse
from src.jobs.checkpoint import CheckpointManager, WorkflowEvent
from src.jobs.signals import WorkflowSignalBus

logger = logging.getLogger(__name__)


def _next_interval(current: float) -> float:
    """Compute the next backoff interval with jitter."""
    raw = min(current * _MULTIPLIER, _MAX_INTERVAL)
    jitter = raw * _JITTER * (2 * random.random() - 1)   # ±10%
    return raw + jitter


class BatchPoller:
    TICK_INTERVAL = 60  # base clock: wake up every 60 s to check due dates

    def __init__(
        self,
        checkpoint_mgr: CheckpointManager,
        signal_bus: WorkflowSignalBus,
        job_manager,          # JobManager — circular import avoided via late binding
    ) -> None:
        self.checkpoint_mgr = checkpoint_mgr
        self.signal_bus = signal_bus
        self.job_manager = job_manager
        self._task: Optional[asyncio.Task] = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("BatchPoller started")

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.TICK_INTERVAL)
                await self._scan_all()
            except asyncio.CancelledError:
                logger.info("BatchPoller stopped")
                break
            except Exception as e:
                logger.error(f"[BatchPoller] Unexpected error: {e}")

    async def _scan_all(self) -> None:
        for job_id in self.checkpoint_mgr.list_checkpoints():
            try:
                await self._check_job(job_id)
            except Exception as e:
                logger.error(f"[BatchPoller] Error checking job {job_id}: {e}")

    # ── Per-job logic ──────────────────────────────────────────────────────────

    async def _check_job(self, job_id: str) -> None:
        checkpoint = self.checkpoint_mgr.load(job_id)
        if not checkpoint:
            return

        submitted: Dict[str, WorkflowEvent] = {}
        completed_steps: set = set()

        for event in checkpoint.events:
            if event.event_type == "BATCH_SUBMITTED":
                submitted[event.step_name] = event
            elif event.event_type == "BATCH_COMPLETED":
                completed_steps.add(event.step_name)

        pending = {
            name: ev for name, ev in submitted.items() if name not in completed_steps
        }

        for step_name, event in pending.items():
            await self._poll_step(job_id, step_name, event, checkpoint.events)

    async def _poll_step(
        self,
        job_id: str,
        step_name: str,
        event: WorkflowEvent,
        all_events: list,
    ) -> None:
        payload = event.payload
        handle = BatchJobHandle(**payload["handle"])

        # ── Expiry check: provider batch jobs have a 24 h TTL ────────────────
        ttl = _GEMINI_BATCH_TTL if handle.provider == "gemini" else _CLAUDE_BATCH_TTL
        age = time.time() - handle.created_at
        if age > ttl:
            logger.error(
                f"[BatchPoller] Batch expired — job={job_id} step={step_name} "
                f"batch_id={handle.job_id} age={age:.0f}s > ttl={ttl}s. "
                f"Marking job FAILED."
            )
            self.checkpoint_mgr.append_event(job_id, WorkflowEvent(
                timestamp=time.time(),
                event_type="BATCH_FAILED",
                step_name=step_name,
                payload={"reason": "expired", "batch_id": handle.job_id, "age_sec": age},
            ))
            self.job_manager.cancel(job_id)
            return

        # ── Backoff gate: check if it is time to poll ─────────────────────────
        # Read latest BATCH_POLLING_HEARTBEAT for this step (newest-first)
        last_heartbeat: Optional[WorkflowEvent] = None
        for ev in reversed(all_events):
            if ev.event_type == "BATCH_POLLING_HEARTBEAT" and ev.step_name == step_name:
                last_heartbeat = ev
                break

        now = time.time()
        if last_heartbeat:
            next_poll_at = last_heartbeat.payload.get("next_poll_at", 0)
            current_interval = last_heartbeat.payload.get("current_interval", _INITIAL_INTERVAL)
            if now < next_poll_at:
                remaining = math.ceil(next_poll_at - now)
                logger.debug(
                    f"[BatchPoller] job={job_id} step={step_name} "
                    f"skipping — next poll in {remaining}s"
                )
                return
        else:
            # First poll: schedule after INITIAL_INTERVAL from submission
            submitted_at = handle.created_at
            if now < submitted_at + _INITIAL_INTERVAL:
                remaining = math.ceil(submitted_at + _INITIAL_INTERVAL - now)
                logger.debug(
                    f"[BatchPoller] job={job_id} step={step_name} "
                    f"waiting for initial interval — {remaining}s remaining"
                )
                return
            current_interval = _INITIAL_INTERVAL

        # ── Call provider ─────────────────────────────────────────────────────
        provider = self._get_provider(handle.provider)
        if provider is None:
            return

        try:
            results = await provider.poll_batch(handle)
        except Exception as e:
            logger.error(f"[BatchPoller] poll_batch failed job={job_id} step={step_name}: {e}")
            # Still write a heartbeat to advance the backoff even on errors
            next_interval = _next_interval(current_interval)
            self._write_heartbeat(job_id, step_name, next_interval, "error")
            return

        if results is None:
            # Still in progress — advance backoff interval
            next_interval = _next_interval(current_interval)
            self._write_heartbeat(job_id, step_name, next_interval, "pending")
            logger.debug(
                f"[BatchPoller] job={job_id} step={step_name} still pending — "
                f"next poll in {next_interval:.0f}s"
            )
            return

        # ── Completeness check ────────────────────────────────────────────────
        expected = payload.get("request_count", len(payload.get("requests", [])))
        if len(results) < expected:
            logger.warning(
                f"[BatchPoller] Incomplete results — job={job_id} step={step_name} "
                f"expected={expected} got={len(results)}. Proceeding with partial results."
            )

        # Reconstruct final items from stored snapshot + LLM results
        final_items = self._reconstruct(payload, results)

        # Append BATCH_COMPLETED event
        self.checkpoint_mgr.append_event(job_id, WorkflowEvent(
            timestamp=time.time(),
            event_type="BATCH_COMPLETED",
            step_name=step_name,
            payload={
                "result": {
                    "items": final_items,
                    "metadata": {
                        "batch_id": handle.job_id,
                        "provider": handle.provider,
                        "batch_size": len(results),
                    },
                }
            },
        ))

        logger.info(
            f"[BatchPoller] Batch complete — job={job_id} step={step_name} "
            f"items={len(final_items)}"
        )
        self.signal_bus.publish(job_id, {"status": "completed", "batch_id": handle.job_id})
        self.job_manager.resume(job_id)

    # ── Heartbeat writer ───────────────────────────────────────────────────────

    def _write_heartbeat(
        self, job_id: str, step_name: str, next_interval: float, status: str
    ) -> None:
        self.checkpoint_mgr.append_event(job_id, WorkflowEvent(
            timestamp=time.time(),
            event_type="BATCH_POLLING_HEARTBEAT",
            step_name=step_name,
            payload={
                "next_poll_at":      time.time() + next_interval,
                "current_interval":  next_interval,
                "status":            status,
            },
        ))

    # ── Reconstruction ─────────────────────────────────────────────────────────

    def _reconstruct(
        self,
        payload: dict,
        results: Dict[str, LLMResponse],
    ) -> list:
        """
        Map LLM responses back onto items_snapshot using the stored request index.
        Applies Pydantic schema parsing when schema_path is provided.
        """
        items = [dict(item) for item in payload.get("items_snapshot", [])]
        output_field = payload.get("output_field") or "result"
        schema_path = payload.get("schema_path")
        requests = payload.get("requests", [])   # [{custom_id, item_idx}]

        schema_cls = self._load_schema(schema_path)

        for req in requests:
            custom_id = req.get("custom_id", "")
            item_idx = req.get("item_idx")
            if item_idx is None or item_idx >= len(items):
                continue

            llm_response = results.get(custom_id)
            if llm_response is None:
                logger.warning(f"[BatchPoller] No result for custom_id={custom_id}")
                continue

            items[item_idx][output_field] = self._parse_response(
                llm_response.text, schema_cls
            )

        return items

    @staticmethod
    def _parse_response(text: str, schema_cls) -> object:
        if schema_cls and text:
            try:
                return schema_cls.model_validate(json.loads(text)).model_dump()
            except Exception as e:
                logger.warning(f"[BatchPoller] Schema parse failed: {e}, falling back to text")
        return text

    @staticmethod
    def _load_schema(schema_path: Optional[str]):
        if not schema_path:
            return None
        try:
            module_path, cls_name = schema_path.rsplit(".", 1)
            mod = importlib.import_module(module_path)
            return getattr(mod, cls_name)
        except Exception as e:
            logger.warning(f"[BatchPoller] Cannot load schema '{schema_path}': {e}")
            return None

    # ── Provider factory ───────────────────────────────────────────────────────

    @staticmethod
    def _get_provider(provider_name: str):
        try:
            from src.intelligence.providers.factory import ProviderFactory
            return ProviderFactory.get_provider(provider_name)
        except Exception as e:
            logger.error(f"[BatchPoller] Cannot create provider '{provider_name}': {e}")
            return None
