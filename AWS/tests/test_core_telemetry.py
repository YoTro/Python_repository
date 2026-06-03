import time

from src.core.telemetry.tracker import TelemetryTracker, TimeEstimator


def test_time_estimator():
    assert "30" in TimeEstimator.estimate_workflow("amazon_bsr")
    assert "30" in TimeEstimator.estimate_workflow("unknown")
    agent_est = TimeEstimator.estimate_agent(max_iterations=5)
    assert "~" in agent_est


def test_telemetry_tracker():
    tracker = TelemetryTracker(total_steps=5)
    assert tracker.current_step == 0
    assert tracker.get_dynamic_eta() is None

    # Simulate steps: first call registers the step name only (no increment),
    # second call records the completed duration and increments current_step.
    time.sleep(0.01)
    tracker.record_step()  # registers pending step name
    tracker.record_step()  # closes prior step, increments current_step
    assert tracker.current_step == 1

    eta = tracker.get_dynamic_eta()
    assert eta is not None
    assert isinstance(eta, str)
