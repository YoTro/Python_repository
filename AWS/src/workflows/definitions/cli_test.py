from __future__ import annotations
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.base import Step, StepResult, WorkflowContext

class DummyCliStep(Step):
    async def run(self, items, ctx: WorkflowContext) -> StepResult:
        print("\n[Step Execution] Running Dummy CLI Step...")
        return StepResult(items=[{"result": "cli_success", "items_processed": len(items)}])

@WorkflowRegistry.register("cli_test")
def build_cli_test(config: dict) -> Workflow:
    return Workflow(name="cli_test", steps=[DummyCliStep(name="dummy_step")])
