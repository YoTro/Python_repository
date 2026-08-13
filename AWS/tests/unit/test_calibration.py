from __future__ import annotations

import json
import os
import threading

import pytest

from src.intelligence.processors.lp_calibration import (
    _load_pas_log,
    compute_update,
    load_calibration,
    record_pas,
    save_calibration,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_pas(tmp_path):
    """Isolated PAS root: patches _SNAP_ROOT to a temp directory."""
    pas_root = os.path.join(str(tmp_path), "lp_snapshots")
    os.makedirs(pas_root)

    from src.intelligence.processors import lp_calibration

    orig = lp_calibration._SNAP_ROOT
    lp_calibration._SNAP_ROOT = pas_root
    yield pas_root
    lp_calibration._SNAP_ROOT = orig


@pytest.fixture
def isolated_calibration(tmp_path):
    """Isolated PAS root + calibration config directory."""
    pas_root = os.path.join(str(tmp_path), "lp_snapshots")
    cal_dir = os.path.join(str(tmp_path), "config", "lp_calibration")
    os.makedirs(pas_root)
    os.makedirs(cal_dir)

    from src.intelligence.processors import lp_calibration

    orig_snap = lp_calibration._SNAP_ROOT
    orig_cal = lp_calibration._CAL_DIR
    lp_calibration._SNAP_ROOT = pas_root
    lp_calibration._CAL_DIR = cal_dir
    yield pas_root
    lp_calibration._SNAP_ROOT = orig_snap
    lp_calibration._CAL_DIR = orig_cal


# ---------------------------------------------------------------------------
# compute_update — trigger logic
# ---------------------------------------------------------------------------


class TestTriggerLogic:
    def test_mixed_then_consistent_window(self, isolated_pas):
        """Mixed window advances watermark; following consistent window fires trigger."""
        asin = "TESTASIN"
        os.makedirs(os.path.join(isolated_pas, asin))
        log_path = os.path.join(isolated_pas, asin, "pas_history.jsonl")

        history = [
            {"band_result": "over_optimistic"},
            {"band_result": "over_optimistic"},
            {"band_result": "within_band"},
            {"band_result": "over_optimistic"},
            {"band_result": "over_optimistic"},
            {"band_result": "over_optimistic"},
        ]
        with open(log_path, "w") as f:
            for entry in history:
                f.write(json.dumps(entry) + "\n")

        params = {"last_trigger_at": 0, "k_cvr_max": 3.0}

        # Call 1: mixed [0:3] — advance watermark, no trigger
        res1 = compute_update(asin, params)
        assert res1 is not None
        assert res1["last_trigger_at"] == 3
        assert res1["k_cvr_max"] == 3.0

        # Call 2: consistent [3:6] — fire trigger
        res2 = compute_update(asin, res1)
        assert res2 is not None
        assert res2["last_trigger_at"] == 6
        expected_k = round(3.0 * 1.10, 4)
        assert res2["k_cvr_max"] == pytest.approx(expected_k)


# ---------------------------------------------------------------------------
# _load_pas_log — data integrity
# ---------------------------------------------------------------------------


class TestPasLogIntegrity:
    def test_corrupt_lines_are_dropped(self, isolated_pas):
        """Verify corrupt JSON lines are silently skipped."""
        asin = "TESTCORRUPT"
        os.makedirs(os.path.join(isolated_pas, asin))
        log_path = os.path.join(isolated_pas, asin, "pas_history.jsonl")

        with open(log_path, "w") as f:
            f.write(json.dumps({"band_result": "within_band"}) + "\n")
            f.write("CORRUPT_JSON_DATA\n")
            f.write(json.dumps({"band_result": "conservative"}) + "\n")

        history = _load_pas_log(asin)
        assert len(history) == 2
        assert history[0]["band_result"] == "within_band"
        assert history[1]["band_result"] == "conservative"


# ---------------------------------------------------------------------------
# record_pas + load_calibration — concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_no_race_condition_on_trigger(self, isolated_calibration):
        """Concurrent writes must not produce duplicate calibration updates."""
        asin = "TESTCONC"
        os.makedirs(os.path.join(isolated_calibration, asin))
        log_path = os.path.join(isolated_calibration, asin, "pas_history.jsonl")

        with open(log_path, "w") as f:
            f.write(json.dumps({"band_result": "over_optimistic"}) + "\n")
            f.write(json.dumps({"band_result": "over_optimistic"}) + "\n")

        save_calibration(asin, {"k_cvr_max": 3.0, "last_trigger_at": 0})

        def trigger():
            record_pas(asin, "2026-05-29", 0.5, "over_optimistic", 10, 0.5, "ok")

        threads = [threading.Thread(target=trigger) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        params = load_calibration(asin)
        assert params["k_cvr_max"] == 3.3, f"Race condition! k_max={params['k_cvr_max']}"
