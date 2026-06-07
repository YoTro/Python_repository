import json
import os
import shutil
import tempfile
import unittest

from src.intelligence.processors.lp_calibration import compute_update


class TestCalibrationTriggerLogic(unittest.TestCase):
    def setUp(self):
        # Setup temporary directories to mimic the project structure
        self.test_dir = tempfile.mkdtemp()
        self.pas_root = os.path.join(self.test_dir, "data", "intelligence", "lp_snapshots")
        os.makedirs(self.pas_root)

        # Patch the paths used in lp_calibration
        from src.intelligence.processors import lp_calibration

        self.original_snap_root = lp_calibration._SNAP_ROOT
        lp_calibration._SNAP_ROOT = self.pas_root

    def tearDown(self):
        from src.intelligence.processors import lp_calibration

        lp_calibration._SNAP_ROOT = self.original_snap_root
        shutil.rmtree(self.test_dir)

    def test_mixed_window_advances_watermark_then_triggers(self):
        """Mixed window unblocks on next call; following consistent window fires trigger."""
        asin = "TESTASIN"
        os.makedirs(os.path.join(self.pas_root, asin))
        log_path = os.path.join(self.pas_root, asin, "pas_history.jsonl")

        # History: [oo, oo, within_band, oo, oo, oo]
        # Window [0:3] is mixed; window [3:6] is all over_optimistic.
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

        # Call 1: mixed window [0:3] — must advance watermark, not fire trigger.
        res1 = compute_update(asin, params)
        self.assertIsNotNone(res1, "Expected watermark-advance params, got None")
        self.assertEqual(res1["last_trigger_at"], 3, "Watermark must advance past mixed window")
        self.assertAlmostEqual(res1["k_cvr_max"], 3.0, "k_cvr_max must not change on mixed window")

        # Call 2: consistent window [3:6] — must fire trigger and adjust k_cvr_max.
        res2 = compute_update(asin, res1)
        self.assertIsNotNone(
            res2, "Trigger did not fire for the consecutive over_optimistic window"
        )
        self.assertEqual(res2["last_trigger_at"], 6, "Watermark must advance past trigger window")
        expected_k = round(3.0 * (1.0 + 0.10), 4)
        self.assertAlmostEqual(
            res2["k_cvr_max"],
            expected_k,
            msg=f"Expected k_cvr_max={expected_k}, got {res2['k_cvr_max']}",
        )


if __name__ == "__main__":
    unittest.main()
