import json
import os
import shutil
import tempfile
import threading
import unittest

from src.intelligence.processors.lp_calibration import load_calibration, record_pas


class TestCalibrationConcurrency(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.config_dir = os.path.join(self.test_dir, "config", "lp_calibration")
        self.pas_root = os.path.join(self.test_dir, "data", "intelligence", "lp_snapshots")
        os.makedirs(self.config_dir)
        os.makedirs(self.pas_root)

        from src.intelligence.processors import lp_calibration

        self.orig_cal_dir = lp_calibration._CAL_DIR
        self.orig_snap_root = lp_calibration._SNAP_ROOT
        lp_calibration._CAL_DIR = self.config_dir
        lp_calibration._SNAP_ROOT = self.pas_root

    def tearDown(self):
        from src.intelligence.processors import lp_calibration

        lp_calibration._CAL_DIR = self.orig_cal_dir
        lp_calibration._SNAP_ROOT = self.orig_snap_root
        shutil.rmtree(self.test_dir)

    def test_concurrency_race_condition(self):
        """Simulate concurrent writes to check if multiple triggers cause duplicate updates."""
        asin = "TESTCONC"
        os.makedirs(os.path.join(self.pas_root, asin))

        # Initial state: 2 out-of-band records already exist
        log_path = os.path.join(self.pas_root, asin, "pas_history.jsonl")
        with open(log_path, "w") as f:
            f.write(json.dumps({"band_result": "over_optimistic"}) + "\n")
            f.write(json.dumps({"band_result": "over_optimistic"}) + "\n")

        # Mock initial params
        from src.intelligence.processors.lp_calibration import save_calibration

        save_calibration(asin, {"k_cvr_max": 3.0, "last_trigger_at": 0})

        # Spawn threads to hit the trigger simultaneously.
        # 3 threads: 2 initial records + 1 new = exactly 1 trigger window [0:3].
        # Threads 2 and 3 land with new_obs=1 and 2 after the lock serialises
        # them, so neither can form a second window.  Using 5 threads would
        # create a second full window [3:6] and fire twice even when locked.
        def trigger():
            record_pas(asin, "2026-05-29", 0.5, "over_optimistic", 10, 0.5, "ok")

        threads = [threading.Thread(target=trigger) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Check result
        params = load_calibration(asin)
        # If atomic, k_cvr_max should be 3.0 * 1.1 = 3.3.
        # If race condition, it might be > 3.3 due to multiple updates.
        self.assertEqual(
            params["k_cvr_max"], 3.3, f"Race condition detected! k_max={params['k_cvr_max']}"
        )


if __name__ == "__main__":
    unittest.main()
