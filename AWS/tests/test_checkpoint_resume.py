"""
Checkpoint Resume Integration Test
===================================
Simulates a multi-step workflow that fails at step 1 (of 4),
then resumes from the checkpoint and completes successfully.

Run:
    pytest tests/test_checkpoint_resume.py -v
"""

import pytest
import tempfile
import shutil
from typing import List, Dict, Any

from src.jobs.checkpoint import CheckpointManager
from src.workflows.engine import Workflow
from src.workflows.steps.base import Step, StepResult, WorkflowContext, ComputeTarget
from src.core.errors.exceptions import StepError


# ---------------------------------------------------------------------------
# Test Steps
# ---------------------------------------------------------------------------

class AddFieldStep(Step):
    """Adds a field to every item."""

    def __init__(self, field_name: str, field_value: Any):
        super().__init__(name=f"add_{field_name}", compute_target=ComputeTarget.PURE_PYTHON)
        self.field_name = field_name
        self.field_value = field_value

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        out = [{**item, self.field_name: self.field_value} for item in items]
        return StepResult(
            items=out,
            metadata={"input_count": len(items), "output_count": len(out)},
        )


class FailOnceStep(Step):
    """Raises an exception the first time it runs; succeeds on retry."""

    def __init__(self):
        super().__init__(name="fail_once", compute_target=ComputeTarget.PURE_PYTHON)
        self.has_failed = False

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        if not self.has_failed:
            self.has_failed = True
            raise RuntimeError("Simulated transient failure at fail_once step")

        return StepResult(
            items=items,
            metadata={"input_count": len(items), "output_count": len(items)},
        )


class FilterStep(Step):
    """Keeps only items where a given field matches a value."""

    def __init__(self, field_name: str, value: Any):
        super().__init__(name=f"filter_{field_name}", compute_target=ComputeTarget.PURE_PYTHON)
        self.field_name = field_name
        self.value = value

    async def run(self, items: List[Dict[str, Any]], ctx: WorkflowContext) -> StepResult:
        kept = [item for item in items if item.get(self.field_name) == self.value]
        return StepResult(
            items=kept,
            metadata={
                "input_count": len(items),
                "output_count": len(kept),
                "filtered_count": len(items) - len(kept),
            },
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def checkpoint_dir():
    tmp_dir = tempfile.mkdtemp(prefix="ckpt_test_")
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
def checkpoint_mgr(checkpoint_dir):
    return CheckpointManager(checkpoint_dir=checkpoint_dir)


@pytest.fixture
def initial_items():
    return [
        {"id": 1, "name": "Alpha"},
        {"id": 2, "name": "Beta"},
        {"id": 3, "name": "Gamma"},
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_checkpoint_save_and_load(checkpoint_mgr):
    """CheckpointManager can save and load checkpoint data."""
    checkpoint_mgr.save(
        job_id="job-save-load",
        step_index=2,
        step_name="some_step",
        items=[{"a": 1}, {"b": 2}],
        metadata={"key": "value"},
    )

    cp = checkpoint_mgr.load("job-save-load")
    assert cp is not None
    assert cp.job_id == "job-save-load"
    assert cp.step_index == 2
    assert cp.step_name == "some_step"
    assert len(cp.items) == 2
    assert cp.metadata["key"] == "value"


@pytest.mark.asyncio
async def test_checkpoint_clear(checkpoint_mgr):
    """Checkpoint is removed after clear."""
    checkpoint_mgr.save("job-clear", 0, "step_0", [{"x": 1}])
    assert checkpoint_mgr.load("job-clear") is not None

    checkpoint_mgr.clear("job-clear")
    assert checkpoint_mgr.load("job-clear") is None


@pytest.mark.asyncio
async def test_checkpoint_list(checkpoint_mgr):
    """list_checkpoints returns all active job IDs."""
    checkpoint_mgr.save("job-a", 0, "s0", [])
    checkpoint_mgr.save("job-b", 1, "s1", [{"x": 1}])

    jobs = checkpoint_mgr.list_checkpoints()
    assert set(jobs) == {"job-a", "job-b"}


@pytest.mark.asyncio
async def test_load_nonexistent_returns_none(checkpoint_mgr):
    """Loading a checkpoint that doesn't exist returns None."""
    assert checkpoint_mgr.load("no-such-job") is None


@pytest.mark.asyncio
async def test_workflow_resume_after_failure(checkpoint_mgr, initial_items):
    """
    End-to-end resume test:
      1. First run fails at step 1, checkpoint saved at step 0.
      2. Second run resumes from checkpoint, skips step 0, and completes.
    """
    fail_step = FailOnceStep()

    steps = [
        AddFieldStep("status", "active"),   # step 0
        fail_step,                           # step 1 — fails first run
        AddFieldStep("score", 42),           # step 2
        FilterStep("status", "active"),      # step 3
    ]
    workflow = Workflow(name="resume_test", steps=steps)
    job_id = "test-resume-001"
    ctx = WorkflowContext(job_id=job_id)
    params = {"initial_items": initial_items}

    # ── First run: should fail at step 1 ──
    with pytest.raises(StepError):
        await workflow.execute(
            job_id=job_id, params=params, ctx=ctx, checkpoint_mgr=checkpoint_mgr,
        )

    # Verify checkpoint saved at step 0 (last successful step)
    cp = checkpoint_mgr.load(job_id)
    assert cp is not None, "Checkpoint should exist after partial run"
    assert cp.step_index == 0, f"Expected checkpoint at step 0, got {cp.step_index}"
    assert len(cp.items) == 3
    assert all("status" in item for item in cp.items)

    # ── Second run: should resume from step 1 and complete ──
    result = await workflow.execute(
        job_id=job_id, params=params, ctx=ctx, checkpoint_mgr=checkpoint_mgr,
    )

    assert result.completed
    assert len(result.final_items) == 3

    # Verify all fields were added by both runs
    for item in result.final_items:
        assert item["status"] == "active"
        assert item["score"] == 42

    # Checkpoint should be cleared after successful completion
    assert checkpoint_mgr.load(job_id) is None


@pytest.mark.asyncio
async def test_workflow_no_checkpoint_runs_from_start(initial_items):
    """Without a checkpoint manager, workflow runs all steps from scratch."""
    steps = [
        AddFieldStep("status", "active"),
        AddFieldStep("score", 99),
    ]
    workflow = Workflow(name="no_ckpt_test", steps=steps)
    ctx = WorkflowContext(job_id="test-no-ckpt")
    params = {"initial_items": initial_items}

    result = await workflow.execute(job_id="test-no-ckpt", params=params, ctx=ctx)

    assert result.completed
    assert len(result.final_items) == 3
    assert all(item["score"] == 99 for item in result.final_items)
