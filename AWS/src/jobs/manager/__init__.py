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

from src.core.errors.exceptions import AWSBaseError, RetryableError, JobSuspendedError, BatchPendingError
from src.core.models.request import UnifiedRequest

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SUSPENDED = "suspended"

@dataclass
class JobRecord:
    """Tracks the state of a submitted job."""
    job_id: str
    request: UnifiedRequest
    status: JobStatus = JobStatus.PENDING
    callback: Any = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None
    suspended_at: Optional[float] = None  # Timestamp for timeout tracking
    suspend_timeout_sec: int = 300       # Dynamic timeout per job
    error: Optional[str] = None
    result: Any = None

class JobManager:
    """
    Manages job submission, tracking, and execution using an asyncio queue.
    Includes a reaper task to clean up expired SUSPENDED jobs dynamically.
    """

    def __init__(self, max_workers: int = 2):
        self._jobs: Dict[str, JobRecord] = {}

        self._queue = asyncio.Queue()
        self._workers: List[asyncio.Task] = []
        self._max_workers = max_workers
        self._reaper_task: Optional[asyncio.Task] = None
        self._batch_poller = None
        self._start_workers()

    def _start_workers(self):
        """Initialize background worker pool, reaper, and batch poller."""
        for i in range(self._max_workers):
            task = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self._workers.append(task)

        self._reaper_task = asyncio.create_task(self._reaper_loop())
        self._start_batch_poller()

    def _start_batch_poller(self) -> None:
        try:
            from src.jobs.batch_poller import BatchPoller
            from src.jobs.checkpoint import CheckpointManager
            from src.jobs.signals import get_signal_bus
            self._batch_poller = BatchPoller(
                checkpoint_mgr=CheckpointManager(),
                signal_bus=get_signal_bus(),
                job_manager=self,
            )
            self._batch_poller.start()
        except Exception as e:
            logger.warning(f"BatchPoller could not start: {e}")

    async def _reaper_loop(self):
        """Periodically checks for and cancels expired SUSPENDED jobs."""
        logger.info("JobManager Reaper Task started.")
        while True:
            try:
                await asyncio.sleep(60)  # Check every 60 seconds
                now = datetime.utcnow().timestamp()
                
                expired_jobs = []
                # Use list() to take a snapshot of items to avoid RuntimeError 
                # if another coroutine adds a job during iteration.
                for job_id, record in list(self._jobs.items()):
                    if record.status == JobStatus.SUSPENDED and record.suspended_at:
                        # Use the job's specific timeout setting
                        if now - record.suspended_at > record.suspend_timeout_sec:
                            expired_jobs.append(record)
                
                for record in expired_jobs:
                    logger.warning(f"Job {record.job_id} suspended for {record.suspend_timeout_sec}s. Auto-cancelling.")
                    record.status = JobStatus.CANCELLED
                    record.error = "Job timed out waiting for user interaction."
                    record.completed_at = datetime.utcnow().isoformat()
                    
                    # Notify the user if possible
                    if record.callback:
                        try:
                            error_msg = Exception(f"任务由于长时间未响应 (超过 {record.suspend_timeout_sec} 秒)，已自动取消。")
                            await record.callback.on_error(error_msg)
                        except Exception as e:
                            logger.debug(f"Failed to notify user of job cancellation: {e}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reaper task encountered an error: {e}")

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
        job_id: Optional[str] = None,
    ) -> str:
        """
        Submit a new job to the queue.
        Accepts either a UnifiedRequest DTO or backward-compatible arguments.
        Pass job_id to pin the ID (e.g. for checkpoint-resume in tests).
        """
        if isinstance(request_or_workflow, str):
            request = UnifiedRequest(
                workflow_name=request_or_workflow,
                params=params or {}
            )
        else:
            request = request_or_workflow

        job_id = job_id or uuid.uuid4().hex[:8]
        
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

    def resume_from_checkpoint(
        self,
        job_id: str,
        callback=None,
        workflow_name: str = None,
        params: dict = None,
    ) -> str:
        """
        Re-submit a previously failed job using its existing checkpoint file.

        workflow_name and params are loaded from the checkpoint automatically;
        pass them explicitly only if you need to override the stored values.

        :param job_id:        The original job ID (must match the checkpoint filename).
        :param callback:      Output callback — pass a FeishuCallback / MCPCallback so
                              progress and completion go back to the original channel.
        :param workflow_name: Override the workflow name stored in the checkpoint.
        :param params:        Override the params stored in the checkpoint.
        :returns:             The same job_id, now queued again.
        """
        from src.jobs.checkpoint import CheckpointManager
        checkpoint_mgr = CheckpointManager()
        checkpoint = checkpoint_mgr.load(job_id)
        if not checkpoint:
            raise ValueError(f"No checkpoint found for job_id={job_id}. Cannot resume.")

        resolved_name = workflow_name or checkpoint.workflow_name
        resolved_params = params or checkpoint.workflow_params
        if not resolved_name:
            raise ValueError(
                f"workflow_name not found in checkpoint for job_id={job_id}. "
                "Pass it explicitly via workflow_name=."
            )

        request = UnifiedRequest(workflow_name=resolved_name, params=resolved_params)
        record = JobRecord(job_id=job_id, request=request, callback=callback)
        self._jobs[job_id] = record
        self._queue.put_nowait(job_id)
        logger.info(
            f"Job {job_id} requeued for resume from step "
            f"{checkpoint.step_index} ({checkpoint.step_name})"
        )
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
            # Lazy import breaks the circular dependency:
            # jobs/manager → gateway/rate_limit → gateway/__init__ → gateway/router → jobs/manager
            from src.gateway.rate_limit import RateLimiter  # noqa: PLC0415
            # concurrent_slot acquires on entry and releases in finally —
            # guaranteeing the counter is decremented even if execution crashes.
            async with RateLimiter().concurrent_slot(
                record.request.entry_type, record.request.chat_id
            ):
                if record.request.workflow_name:
                    await self._run_workflow_mode(record)
                elif record.request.intent:
                    await self._run_agent_mode(record)
                else:
                    raise ValueError("Neither workflow_name nor intent provided in UnifiedRequest.")

        except BatchPendingError as e:
            # A Step submitted a provider batch job and needs to wait for results.
            # Suspend the workflow job; BatchPoller will resume it via SignalBus.
            logger.info(
                f"Job {job_id} suspended: waiting for batch '{e.batch_job_id}' "
                f"(timeout={record.suspend_timeout_sec}s)"
            )
            record.status = JobStatus.SUSPENDED
            record.suspended_at = datetime.utcnow().timestamp()
            record.suspend_timeout_sec = 7200  # 2 h — batch jobs can be slow
            if record.callback:
                try:
                    provider_name = getattr(e.handle, "provider", "LLM").upper()
                    await record.callback.on_progress(
                        step_index=0,
                        total_steps=1,
                        step_name="batch_pending",
                        message=(
                            f"📋 数据采集完成，已提交 Batch 分析任务\n"
                            f"🔖 Batch ID: `{e.batch_job_id}`\n"
                            f"⏳ {provider_name} Batch API 通常需要 10–60 分钟处理，完成后自动推送结果。\n"
                            f"💡 如需查看进度，可发送：恢复任务 {job_id}"
                        ),
                    )
                except Exception as cb_err:
                    logger.warning(f"Batch pending callback failed: {cb_err}")

        except RetryableError as e:
            logger.warning(f"Job {job_id} hit retryable error: {e}")
            record.status = JobStatus.FAILED
            record.error = str(e)
            if record.callback:
                try:
                    await record.callback.on_error(e, job_id=job_id)
                except Exception:
                    pass

        except RuntimeError as e:
            err_str = str(e)
            record.status = JobStatus.FAILED
            record.error = err_str
            record.completed_at = datetime.utcnow().isoformat()
            # Concurrent-slot rejections get a user-friendly Feishu message instead
            # of the raw internal error text.
            if "concurrent limit reached" in err_str and record.callback:
                try:
                    await record.callback.notify(
                        "⏳ 上一个任务仍在进行中，请等待完成后再发送新任务。\n"
                        "如任务长时间未响应，请联系管理员检查任务状态。"
                    )
                except Exception:
                    pass
            elif record.callback:
                try:
                    await record.callback.on_error(e, job_id=job_id)
                except Exception:
                    pass
            else:
                logger.error(f"Job {job_id} failed (RuntimeError): {e}")

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}")
            record.status = JobStatus.FAILED
            record.error = str(e)
            record.completed_at = datetime.utcnow().isoformat()
            if record.callback:
                try:
                    await record.callback.on_error(e, job_id=job_id)
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

        ctx = WorkflowContext(
            job_id=job_id, 
            tenant_id=req.tenant_id, 
            user_id=req.user_id, 
            config=config
        )

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
            
            # Prepare runtime context (chat_id, etc.) for task context propagation
            context = {}
            if record.request.callback and record.request.callback.target:
                context["feishu_chat_id"] = record.request.callback.target
                if "bot_name" in record.request.callback.options:
                    context["feishu_bot_name"] = record.request.callback.options["bot_name"]

            # The job_id acts as the session_id, ensuring 1-to-1 mapping
            response = await agent.run(
                query=record.request.intent, 
                session_id=record.job_id, 
                tenant_id=record.request.tenant_id,
                user_id=record.request.user_id,
                callback=record.callback,
                context=context
            )
            
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
                    
        except JobSuspendedError as e:
            logger.info(f"Job {job_id} suspended for interaction: {e}")
            record.status = JobStatus.SUSPENDED
            record.suspended_at = datetime.utcnow().timestamp()
            record.suspend_timeout_sec = e.timeout_sec
            record.error = str(e)
            # Do NOT call on_complete, as the job is still ongoing
            
        except Exception as e:
            logger.error(f"Agent execution failed: {e}")
            raise

    def get_status(self, job_id: str) -> Optional[JobRecord]:
        return self._jobs.get(job_id)

    def resume(self, job_id: str) -> bool:
        """Resume a failed or suspended job."""
        record = self._jobs.get(job_id)
        if not record or record.status not in [JobStatus.FAILED, JobStatus.SUSPENDED]:
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
