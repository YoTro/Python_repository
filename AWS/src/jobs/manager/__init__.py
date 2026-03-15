from __future__ import annotations
"""
JobManager — task lifecycle management.

Single-user version: asyncio.Queue + Worker.
Multi-user extension point: Redis Priority Queue.
"""

import asyncio
import uuid
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Any, Union
from datetime import datetime

from src.core.errors.exceptions import AWSBaseError, RetryableError
from src.core.models.request import UnifiedRequest

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class JobRecord:
    """Tracks the state of a submitted job."""
    job_id: str
    request: UnifiedRequest
    status: JobStatus = JobStatus.PENDING
    callback: Any = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    error: Optional[str] = None
    result: Any = None

class JobManager:
    """
    Manages job submission, tracking, and execution using an asyncio queue.
    """

    def __init__(self, max_workers: int = 2):
        self._jobs: Dict[str, JobRecord] = {}
        
        # In-memory queue instead of launching raw tasks
        self._queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._max_workers = max_workers
        self._start_workers()

    def _start_workers(self):
        """Initialize the background worker pool."""
        for i in range(self._max_workers):
            task = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self._workers.append(task)

    async def _worker_loop(self, name: str):
        """Background loop to process jobs from the queue."""
        logger.debug(f"[{name}] Started")
        while True:
            try:
                job_id = await self._queue.get()
                await self._run_job(job_id)
                self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{name}] Unexpected error processing job: {e}")

    def submit(
        self,
        request_or_workflow: Union[UnifiedRequest, str],
        params: dict = None,
        callback=None,
    ) -> str:
        """
        Submit a new job to the queue.
        Accepts either a UnifiedRequest DTO or backward-compatible arguments.
        """
        if isinstance(request_or_workflow, str):
            request = UnifiedRequest(
                workflow_name=request_or_workflow,
                params=params or {}
            )
        else:
            request = request_or_workflow
            
        job_id = uuid.uuid4().hex[:8]
        
        # Build callback from config if missing
        if not callback and request.callback:
            from src.jobs.callbacks.factory import CallbackFactory
            callback = CallbackFactory.create(request.callback)
            
        record = JobRecord(
            job_id=job_id,
            request=request,
            callback=callback,
        )
        self._jobs[job_id] = record

        # Push to async queue
        self._queue.put_nowait(job_id)
        
        target = request.workflow_name or "Agent Session"
        logger.info(f"Job submitted to queue: {job_id} ({target})")
        return job_id

    async def submit_and_wait(
        self,
        request_or_workflow: Union[UnifiedRequest, str],
        params: dict = None,
        callback=None,
    ) -> Any:
        job_id = self.submit(request_or_workflow, params, callback)
        
        # Wait until the job finishes
        while True:
            record = self._jobs.get(job_id)
            if record.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                break
            await asyncio.sleep(0.5)
            
        if record.status == JobStatus.FAILED:
            raise AWSBaseError(record.error or "Job failed")
        return record.result

    async def _run_job(self, job_id: str) -> None:
        """Execute a job from the queue."""
        record = self._jobs.get(job_id)
        if not record or record.status == JobStatus.CANCELLED:
            return

        record.status = JobStatus.RUNNING

        try:
            if record.request.workflow_name:
                await self._run_workflow_mode(record)
            elif record.request.intent:
                await self._run_agent_mode(record)
            else:
                raise ValueError("Neither workflow_name nor intent provided in UnifiedRequest.")

        except RetryableError as e:
            logger.warning(f"Job {job_id} hit retryable error: {e}")
            record.status = JobStatus.FAILED
            record.error = str(e)
            if record.callback:
                try:
                    await record.callback.on_error(e)
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            record.status = JobStatus.FAILED
            record.error = str(e)
            record.completed_at = datetime.utcnow().isoformat()
            if record.callback:
                try:
                    await record.callback.on_error(e)
                except Exception:
                    pass

    async def _run_workflow_mode(self, record: JobRecord) -> None:
        from src.workflows.registry import WorkflowRegistry
        from src.workflows.config import merge_config
        from src.workflows.steps.base import WorkflowContext
        from src.jobs.checkpoint import CheckpointManager

        job_id = record.job_id
        req = record.request
        config = merge_config(req.workflow_name, req.params)
        workflow = WorkflowRegistry.build(req.workflow_name, config)

        ctx = WorkflowContext(job_id=job_id, config=config)

        try:
            from src.mcp.client import get_mcp_client
            ctx.mcp = get_mcp_client()
        except Exception as e:
            logger.debug(f"MCP Client not available: {e}")

        try:
            from src.intelligence.router import IntelligenceRouter
            ctx.router = IntelligenceRouter()
        except Exception as e:
            logger.debug(f"IntelligenceRouter not available: {e}")

        checkpoint_mgr = CheckpointManager()

        result = await workflow.execute(
            job_id=job_id,
            params=config,
            ctx=ctx,
            callback=record.callback,
            checkpoint_mgr=checkpoint_mgr,
        )

        record.result = result
        record.status = JobStatus.COMPLETED
        record.completed_at = datetime.utcnow().isoformat()

        if record.callback:
            try:
                await record.callback.on_complete(result)
            except Exception as e:
                logger.warning(f"Callback on_complete failed: {e}")
        
        logger.info(f"Workflow Job completed: {job_id}")

    async def _run_agent_mode(self, record: JobRecord) -> None:
        """Execute conversational/exploratory intent."""
        logger.info(f"Agent Job executed for intent: {record.request.intent}")
        try:
            from src.intelligence.router import IntelligenceRouter
            from src.agents.mcp_agent import MCPAgent
            from src.agents.session import AgentSessionManager
            
            router = IntelligenceRouter()
            session_mgr = AgentSessionManager()
            agent = MCPAgent(router, session_mgr)
            
            # The job_id acts as the session_id, ensuring 1-to-1 mapping
            response = await agent.run(query=record.request.intent, session_id=record.job_id, callback=record.callback)
            
            record.result = {"intent": record.request.intent, "message": response}
            record.status = JobStatus.COMPLETED
            record.completed_at = datetime.utcnow().isoformat()
            
            if record.callback:
                try:
                    from src.workflows.engine import WorkflowResult
                    mock_res = WorkflowResult(name="Agent Exploration", final_items=[{"response": response}])
                    await record.callback.on_complete(mock_res)
                except Exception as e:
                    logger.warning(f"Agent callback failed: {e}")
                    
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            raise

    def get_status(self, job_id: str) -> Optional[JobRecord]:
        return self._jobs.get(job_id)

    def resume(self, job_id: str) -> bool:
        """Resume a failed job from checkpoint."""
        record = self._jobs.get(job_id)
        if not record or record.status != JobStatus.FAILED:
            return False
            
        record.status = JobStatus.PENDING
        record.error = None
        record.result = None
        self._queue.put_nowait(job_id)
        logger.info(f"Job resumed and requeued: {job_id}")
        return True

    def cancel(self, job_id: str) -> bool:
        record = self._jobs.get(job_id)
        if not record:
            return False

        record.status = JobStatus.CANCELLED
        logger.info(f"Job cancelled: {job_id}")
        return True

    def list_jobs(self) -> List[JobRecord]:
        return list(self._jobs.values())


# Singleton
_job_manager: Optional[JobManager] = None

def get_job_manager() -> JobManager:
    global _job_manager
    if _job_manager is None:
        _job_manager = JobManager()
    return _job_manager
