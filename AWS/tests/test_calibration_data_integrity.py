import json
import os
import shutil
import tempfile
import unittest

from src.intelligence.processors.lp_calibration import _load_pas_log


class TestCalibrationDataIntegrity(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.pas_root = os.path.join(self.test_dir, "data", "intelligence", "lp_snapshots")
        os.makedirs(self.pas_root)

        from src.intelligence.processors import lp_calibration

        self.orig_snap_root = lp_calibration._SNAP_ROOT
        lp_calibration._SNAP_ROOT = self.pas_root

    def tearDown(self):
        from src.intelligence.processors import lp_calibration

        lp_calibration._SNAP_ROOT = self.orig_snap_root
        shutil.rmtree(self.test_dir)

    def test_corrupt_pas_log(self):
        """Verify handling of corrupt JSON lines in PAS log."""
        asin = "TESTCORRUPT"
        os.makedirs(os.path.join(self.pas_root, asin))
        log_path = os.path.join(self.pas_root, asin, "pas_history.jsonl")

        # Write valid, then invalid, then valid
        with open(log_path, "w") as f:
            f.write(json.dumps({"band_result": "within_band"}) + "\n")
            f.write("CORRUPT_JSON_DATA\n")
            f.write(json.dumps({"band_result": "conservative"}) + "\n")

        history = _load_pas_log(asin)

        # The current implementation silently drops corrupt lines
        self.assertEqual(
            len(history), 2, f"Should have dropped the corrupt line, found {len(history)} entries"
        )
        self.assertEqual(history[0]["band_result"], "within_band")
        self.assertEqual(history[1]["band_result"], "conservative")


if __name__ == "__main__":
    unittest.main()
