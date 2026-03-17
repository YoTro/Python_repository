from __future__ import annotations
import time
import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

class TimeEstimator:
    """
    Provides static ETA estimations based on heuristics.
    In a production system, this could query a database of historical execution times.
    """
    
    WORKFLOW_AVERAGES = {
        "amazon_bsr": 30.0,  # Baseline 30 seconds
        # Add others as needed
    }
    
    AGENT_BASELINE_PER_ITERATION = 6.0  # Approx 12s per ReAct loop (LLM + Tool)
    
    @classmethod
    def estimate_workflow(cls, workflow_name: str, params: dict = None) -> str:
        base_time = cls.WORKFLOW_AVERAGES.get(workflow_name, 30.0)
        # E.g., if we know pagination params, we could multiply base_time
        return f"~{int(base_time)}秒"

    @classmethod
    def estimate_agent(cls, max_iterations: int = 5) -> str:
        # Agent is highly variable, give a conservative range
        min_time = int(cls.AGENT_BASELINE_PER_ITERATION * 1)
        max_time = int(cls.AGENT_BASELINE_PER_ITERATION * (max_iterations * 0.6)) # Assume it rarely hits max
        return f"{min_time}~{max_time}秒"


class TelemetryTracker:
    """
    Tracks dynamic progress and calculates moving averages for remaining ETA.
    """
    def __init__(self, total_steps: int):
        self.total_steps = total_steps
        self.current_step = 0
        self.start_time = time.monotonic()
        self.step_times: List[float] = []
        self.last_step_time = self.start_time

    def record_step(self):
        """Record the completion of a step and track its duration."""
        now = time.monotonic()
        duration = now - self.last_step_time
        self.step_times.append(duration)
        self.last_step_time = now
        self.current_step += 1

    def get_dynamic_eta(self) -> Optional[int]:
        """Returns the estimated remaining time in seconds."""
        if self.current_step == 0 or self.current_step >= self.total_steps:
            return None
        
        # Simple Average for remaining steps
        avg_time_per_step = sum(self.step_times) / len(self.step_times)
        remaining_steps = self.total_steps - self.current_step
        return int(avg_time_per_step * remaining_steps)
