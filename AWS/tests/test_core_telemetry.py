import pytest
import time
from src.core.telemetry.tracker import TimeEstimator, TelemetryTracker

def test_time_estimator():
    assert "45" in TimeEstimator.estimate_workflow("amazon_bsr")
    assert "30" in TimeEstimator.estimate_workflow("unknown")
    agent_est = TimeEstimator.estimate_agent(max_iterations=5)
    assert "~" in agent_est

def test_telemetry_tracker():
    tracker = TelemetryTracker(total_steps=5)
    assert tracker.current_step == 0
    assert tracker.get_dynamic_eta() is None

    # Simulate steps
    time.sleep(0.01)
    tracker.record_step()
    assert tracker.current_step == 1
    
    eta = tracker.get_dynamic_eta()
    assert eta is not None
    assert isinstance(eta, int)
