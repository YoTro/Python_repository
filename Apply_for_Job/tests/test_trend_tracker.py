import unittest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import numpy as np

from src.analysis.trend_tracker import build_snapshot, save_snapshot, load_snapshots


def _make_enriched_df():
    """构造已标准化 + 技能提取后的 DataFrame"""
    return pd.DataFrame([
        {"job_canonical": "Amazon运营", "has_ai_skill": True,  "ai_skill_tier": 2,
         "salary_mid": 18000, "ai_skills_found": ["python", "数据分析"]},
        {"job_canonical": "Amazon运营", "has_ai_skill": False, "ai_skill_tier": 0,
         "salary_mid": 13000, "ai_skills_found": []},
        {"job_canonical": "算法工程师", "has_ai_skill": True,  "ai_skill_tier": 3,
         "salary_mid": 35000, "ai_skills_found": ["大模型", "rag"]},
        {"job_canonical": "算法工程师", "has_ai_skill": True,  "ai_skill_tier": 3,
         "salary_mid": 32000, "ai_skills_found": ["pytorch", "nlp"]},
        {"job_canonical": "数据分析师", "has_ai_skill": False, "ai_skill_tier": 0,
         "salary_mid": 16000, "ai_skills_found": []},
    ])


class TestBuildSnapshot(unittest.TestCase):

    def setUp(self):
        self.df = _make_enriched_df()
        self.snap = build_snapshot(self.df, keyword="amazon运营")

    def test_required_keys(self):
        for key in ["snapshot_time", "keyword", "total_jobs",
                    "ai_jobs", "ai_ratio", "tier_dist",
                    "group_stats", "skill_freq"]:
            self.assertIn(key, self.snap, msg=f"缺少 key: {key}")

    def test_total_jobs(self):
        self.assertEqual(self.snap["total_jobs"], 5)

    def test_ai_jobs_count(self):
        self.assertEqual(self.snap["ai_jobs"], 3)

    def test_ai_ratio(self):
        self.assertAlmostEqual(self.snap["ai_ratio"], 0.6, places=4)

    def test_group_stats_present(self):
        jobs = [g["job"] for g in self.snap["group_stats"]]
        self.assertIn("Amazon运营", jobs)
        self.assertIn("算法工程师", jobs)

    def test_group_ai_ratio(self):
        grp = next(g for g in self.snap["group_stats"] if g["job"] == "Amazon运营")
        # Amazon运营: 1 AI / 2 total = 0.5
        self.assertAlmostEqual(grp["ai_ratio"], 0.5, places=4)

    def test_skill_freq_populated(self):
        # 至少有一个技能词被统计
        self.assertGreater(len(self.snap["skill_freq"]), 0)

    def test_tier_dist(self):
        td = self.snap["tier_dist"]
        # tier 3 有 2 条（算法工程师 ×2），tier 2 有 1 条（Amazon运营 tier2）
        self.assertGreaterEqual(td.get(3, td.get("3", 0)), 2)

    def test_keyword_stored(self):
        self.assertEqual(self.snap["keyword"], "amazon运营")

    def test_empty_df(self):
        snap = build_snapshot(pd.DataFrame(), keyword="test")
        self.assertEqual(snap["total_jobs"], 0)
        self.assertEqual(snap["ai_ratio"], 0)


class TestSaveAndLoadSnapshots(unittest.TestCase):

    def test_save_and_load(self):
        df = _make_enriched_df()
        snap = build_snapshot(df, keyword="test_kw")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "trend_snapshots.csv"

            # patch SNAPSHOT_FILE 到临时路径
            with patch("src.analysis.trend_tracker.SNAPSHOT_FILE", tmp_path):
                save_snapshot(snap)
                self.assertTrue(tmp_path.exists())

                loaded = load_snapshots()
                self.assertEqual(len(loaded), 1)
                self.assertEqual(loaded["keyword"].iloc[0], "test_kw")
                self.assertEqual(loaded["total_jobs"].iloc[0], 5)

    def test_append_multiple(self):
        df = _make_enriched_df()
        snap1 = build_snapshot(df, keyword="kw_a")
        snap2 = build_snapshot(df, keyword="kw_b")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir) / "trend_snapshots.csv"
            with patch("src.analysis.trend_tracker.SNAPSHOT_FILE", tmp_path):
                save_snapshot(snap1)
                save_snapshot(snap2)
                loaded = load_snapshots()
                self.assertEqual(len(loaded), 2)
                self.assertIn("kw_a", loaded["keyword"].values)
                self.assertIn("kw_b", loaded["keyword"].values)

    def test_load_nonexistent(self):
        with patch("src.analysis.trend_tracker.SNAPSHOT_FILE",
                   Path("/tmp/__nonexistent_snapshot__.csv")):
            loaded = load_snapshots()
            self.assertTrue(loaded.empty)


if __name__ == "__main__":
    unittest.main()
