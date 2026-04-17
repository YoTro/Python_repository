import unittest
import tempfile
import json
from pathlib import Path
from io import StringIO
from unittest.mock import patch
import pandas as pd
import numpy as np

from src.analysis.report import (
    _fmt_salary,
    _fmt_pct,
    _fmt_pval,
    save_markdown,
    print_summary,
)


def _make_df():
    return pd.DataFrame([
        {"job_canonical": "Amazon运营", "has_ai_skill": True,
         "salary_mid": 18000, "ai_skill_tier": 2},
        {"job_canonical": "Amazon运营", "has_ai_skill": False,
         "salary_mid": 13000, "ai_skill_tier": 0},
        {"job_canonical": "算法工程师", "has_ai_skill": True,
         "salary_mid": 35000, "ai_skill_tier": 3},
    ])


def _make_premium_df():
    return pd.DataFrame([{
        "job_group":         "Amazon运营",
        "n_total":           60,
        "n_ai":              30,
        "n_no_ai":           30,
        "mean_salary_no_ai": 15000,
        "mean_salary_ai":    20000,
        "raw_premium":       5000,
        "raw_premium_pct":   33.3,
        "ols_premium":       4500,
        "ols_pvalue":        0.003,
        "ols_significant":   True,
        "ols_ci_95":         [2000, 7000],
    }])


def _make_snapshot():
    return {
        "snapshot_time": "2026-04-17 10:00",
        "keyword":       "amazon运营",
        "total_jobs":    3,
        "ai_jobs":       2,
        "ai_ratio":      0.667,
        "tier_dist":     {0: 1, 2: 1, 3: 1},
        "group_stats":   [
            {"job": "Amazon运营", "n_total": 2, "n_ai": 1,
             "ai_ratio": 0.5,
             "salary_all":   {"p25": 14000, "p50": 15500, "p75": 17000, "mean": 15500, "n": 2},
             "salary_ai":    {"p50": 18000, "n": 1},
             "salary_no_ai": {"p50": 13000, "n": 1}},
        ],
        "skill_freq":    {"python": 5, "大模型": 4, "chatgpt": 3},
    }


class TestFormatHelpers(unittest.TestCase):

    def test_fmt_salary_normal(self):
        self.assertEqual(_fmt_salary(15000), "¥15,000/月")

    def test_fmt_salary_none(self):
        self.assertEqual(_fmt_salary(None), "N/A")
        self.assertEqual(_fmt_salary(float("nan")), "N/A")

    def test_fmt_pct_positive(self):
        self.assertEqual(_fmt_pct(33.3), "+33.3%")

    def test_fmt_pct_negative(self):
        self.assertEqual(_fmt_pct(-5.0), "-5.0%")

    def test_fmt_pct_none(self):
        self.assertEqual(_fmt_pct(None), "N/A")

    def test_fmt_pval_significant_levels(self):
        self.assertIn("***", _fmt_pval(0.0001))
        self.assertIn("**",  _fmt_pval(0.005))
        self.assertIn("*",   _fmt_pval(0.03))
        self.assertIn("ns",  _fmt_pval(0.2))

    def test_fmt_pval_none(self):
        self.assertEqual(_fmt_pval(None), "N/A")


class TestSaveMarkdown(unittest.TestCase):

    def setUp(self):
        self.df       = _make_df()
        self.prem_df  = _make_premium_df()
        self.snapshot = _make_snapshot()

    def test_file_created(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, self.prem_df, self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            self.assertTrue(path.exists())

    def test_file_is_markdown(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, self.prem_df, self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            self.assertEqual(path.suffix, ".md")

    def test_content_contains_key_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, self.prem_df, self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            content = path.read_text(encoding="utf-8")
            self.assertIn("AI 技能薪酬溢价", content)
            self.assertIn("高频 AI 技能词", content)
            self.assertIn("Tier", content)

    def test_premium_values_in_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, self.prem_df, self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            content = path.read_text(encoding="utf-8")
            self.assertIn("Amazon运营", content)
            self.assertIn("15,000", content)  # mean_salary_no_ai
            self.assertIn("20,000", content)  # mean_salary_ai

    def test_empty_premium_df(self):
        """空 premium_df 不应抛出异常，应输出提示文字"""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, pd.DataFrame(), self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            content = path.read_text(encoding="utf-8")
            self.assertIn("样本量不足", content)

    def test_filename_contains_keyword(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir)
            path = save_markdown(self.df, self.prem_df, self.snapshot,
                                 keyword="amazon运营", output_dir=out)
            self.assertIn("amazon", path.name)


class TestPrintSummary(unittest.TestCase):
    """print_summary 不崩溃即通过（输出路径测试）"""

    def test_no_exception(self):
        df       = _make_df()
        prem_df  = _make_premium_df()
        snapshot = _make_snapshot()
        with patch("sys.stdout", new_callable=StringIO):
            try:
                print_summary(df, prem_df, snapshot, keyword="amazon运营")
            except Exception as e:
                self.fail(f"print_summary 抛出异常: {e}")

    def test_empty_premium_no_exception(self):
        df       = _make_df()
        snapshot = _make_snapshot()
        with patch("sys.stdout", new_callable=StringIO):
            try:
                print_summary(df, pd.DataFrame(), snapshot, keyword="test")
            except Exception as e:
                self.fail(f"print_summary（空溢价表）抛出异常: {e}")


if __name__ == "__main__":
    unittest.main()
