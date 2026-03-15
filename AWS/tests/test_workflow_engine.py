import pytest
import asyncio
from src.workflows.engine import Workflow, WorkflowResult, StepReport
from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget
from src.jobs.manager import get_job_manager, JobStatus
from src.core.errors.exceptions import FatalError, RetryableError

class DummyStep(Step):
    def __init__(self, name: str, multiplier: int = 2, fail_type=None):
        super().__init__(name, compute_target=ComputeTarget.PURE_PYTHON)
        self.multiplier = multiplier
        self.fail_type = fail_type

    async def run(self, items, ctx: WorkflowContext) -> StepResult:
        if self.fail_type == "fatal":
            raise FatalError("Simulated Fatal Error")
        elif self.fail_type == "retryable":
            raise RetryableError("Simulated Retryable Error")
            
        processed = [{"value": item.get("value", 0) * self.multiplier} for item in items]
        return StepResult(items=processed, metadata={"input_count": len(items)})


@pytest.mark.asyncio
async def test_workflow_execution():
    steps = [
        DummyStep("step_1", multiplier=2),
        DummyStep("step_2", multiplier=3)
    ]
    workflow = Workflow(name="test_math_workflow", steps=steps)
    ctx = WorkflowContext(job_id="test_job_1")
    params = {"initial_items": [{"value": 1}, {"value": 2}]}
    
    result = await workflow.execute(job_id="test_job_1", params=params, ctx=ctx)
    
    assert result.completed is True
    assert len(result.final_items) == 2
    # 1 * 2 * 3 = 6; 2 * 2 * 3 = 12
    assert result.final_items[0]["value"] == 6
    assert result.final_items[1]["value"] == 12
    assert len(result.step_reports) == 2


@pytest.mark.asyncio
async def test_workflow_fatal_error():
    steps = [
        DummyStep("step_1", multiplier=2),
        DummyStep("step_fail", fail_type="fatal")
    ]
    workflow = Workflow(name="test_error_workflow", steps=steps)
    ctx = WorkflowContext(job_id="test_job_2")
    params = {"initial_items": [{"value": 1}]}
    
    with pytest.raises(FatalError):
        await workflow.execute(job_id="test_job_2", params=params, ctx=ctx)


@pytest.mark.asyncio
async def test_job_manager_submit_and_wait():
    manager = get_job_manager()
    
    # We need to inject a mock workflow into the registry or mock the registry
    # Since we can't easily mock registry here without patching, let's just 
    # test the core manager state tracking by patching the private run method.
    
    # Wait, JobManager uses WorkflowRegistry.build
    # Let's mock the internal _run_job instead to just simulate success.
    
    with pytest.MonkeyPatch.context() as m:
        async def mock_run_job(job_id):
            record = manager._jobs.get(job_id)
            record.status = JobStatus.COMPLETED
            record.result = WorkflowResult(name="mock_wf", final_items=[{"success": True}])
            
        m.setattr(manager, "_run_job", mock_run_job)
        
        result = await manager.submit_and_wait("mock_workflow", params={})
        
        assert result is not None
        assert result.final_items[0]["success"] is True
        
        # Verify status
        jobs = manager.list_jobs()
        assert len(jobs) > 0
        assert jobs[-1].status == JobStatus.COMPLETED
