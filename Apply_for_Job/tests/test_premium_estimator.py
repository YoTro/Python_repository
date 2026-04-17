import unittest
import pandas as pd
import numpy as np
from src.analysis.premium_estimator import estimate_premium, estimate_all_groups


def _make_df(n_ai=30, n_no=30,
             salary_ai=20000, salary_no=15000,
             noise=2000, job="Amazon运营"):
    """
    生成受控测试数据：AI组薪资明显高于非AI组，确保 OLS 能检测到溢价。
    """
    rng = np.random.default_rng(42)
    rows = []

    for _ in range(n_ai):
        rows.append({
            "job_canonical":  job,
            "has_ai_skill":   True,
            "salary_mid":     salary_ai + rng.normal(0, noise),
            "city_tier":      rng.choice([1, 2, 3]),
            "exp_years":      float(rng.integers(1, 8)),
            "company_size_n": rng.choice([100, 500, 1250, 6000]),
        })

    for _ in range(n_no):
        rows.append({
            "job_canonical":  job,
            "has_ai_skill":   False,
            "salary_mid":     salary_no + rng.normal(0, noise),
            "city_tier":      rng.choice([1, 2, 3]),
            "exp_years":      float(rng.integers(1, 8)),
            "company_size_n": rng.choice([100, 500, 1250, 6000]),
        })

    return pd.DataFrame(rows)


class TestEstimatePremium(unittest.TestCase):

    def setUp(self):
        self.df = _make_df(salary_ai=20000, salary_no=15000)

    # ── 基本字段 ────────────────────────────────────────────────────
    def test_required_fields(self):
        r = estimate_premium(self.df, job_group="Amazon运营")
        for key in ["job_group", "n_total", "n_ai", "n_no_ai",
                    "mean_salary_ai", "mean_salary_no_ai",
                    "raw_premium", "raw_premium_pct"]:
            self.assertIn(key, r, msg=f"缺少字段: {key}")

    def test_counts(self):
        r = estimate_premium(self.df, job_group="Amazon运营")
        self.assertEqual(r["n_total"], 60)
        self.assertEqual(r["n_ai"],    30)
        self.assertEqual(r["n_no_ai"], 30)

    # ── 原始溢价方向 ────────────────────────────────────────────────
    def test_raw_premium_positive(self):
        r = estimate_premium(self.df, job_group="Amazon运营")
        self.assertIsNotNone(r["raw_premium"])
        # AI 薪资设计为更高，溢价应为正
        self.assertGreater(r["raw_premium"], 0)

    def test_raw_premium_pct_reasonable(self):
        r = estimate_premium(self.df, job_group="Amazon运营")
        # 15000→20000 理论溢价 33%；加噪声后应在 10%~60% 内
        self.assertGreater(r["raw_premium_pct"], 10)
        self.assertLess(r["raw_premium_pct"], 60)

    # ── OLS 回归 ─────────────────────────────────────────────────────
    def test_ols_available(self):
        try:
            import statsmodels
        except ImportError:
            self.skipTest("statsmodels 未安装")
        r = estimate_premium(self.df, job_group="Amazon运营")
        self.assertIn("ols_premium", r)
        self.assertIn("ols_pvalue", r)
        self.assertIn("ols_significant", r)

    def test_ols_premium_positive(self):
        try:
            import statsmodels
        except ImportError:
            self.skipTest("statsmodels 未安装")
        r = estimate_premium(self.df, job_group="Amazon运营")
        # 信号足够强，OLS 溢价也应为正
        self.assertGreater(r["ols_premium"], 0)

    def test_ols_significant_on_clear_signal(self):
        try:
            import statsmodels
        except ImportError:
            self.skipTest("statsmodels 未安装")
        # 大样本 + 低噪声，应显著
        df = _make_df(n_ai=80, n_no=80, salary_ai=22000,
                      salary_no=15000, noise=1000)
        r = estimate_premium(df, job_group="Amazon运营")
        self.assertTrue(r.get("ols_significant"), "大样本低噪声下 OLS 应显著")

    # ── 无 job_group 过滤（全量） ───────────────────────────────────
    def test_no_group_filter(self):
        r = estimate_premium(self.df)
        self.assertEqual(r["job_group"], "全部岗位")
        self.assertEqual(r["n_total"], 60)

    # ── 样本不足 ────────────────────────────────────────────────────
    def test_insufficient_samples(self):
        tiny = self.df.head(4)
        r = estimate_premium(tiny, job_group="Amazon运营")
        self.assertIn("error", r)

    # ── 缺少 has_ai_skill 列 ────────────────────────────────────────
    def test_missing_has_ai_column(self):
        df_no_ai = self.df.drop(columns=["has_ai_skill"])
        r = estimate_premium(df_no_ai, job_group="Amazon运营")
        self.assertIn("error", r)

    # ── 缺少薪资数据 ────────────────────────────────────────────────
    def test_all_salary_nan(self):
        df_nan = self.df.copy()
        df_nan["salary_mid"] = np.nan
        r = estimate_premium(df_nan, job_group="Amazon运营")
        self.assertIn("error", r)


class TestEstimateAllGroups(unittest.TestCase):

    def _make_multi_job_df(self):
        dfs = [
            _make_df(n_ai=25, n_no=25, salary_ai=20000,
                     salary_no=15000, job="Amazon运营"),
            _make_df(n_ai=25, n_no=25, salary_ai=35000,
                     salary_no=25000, job="算法工程师"),
            _make_df(n_ai=3, n_no=3, job="其他"),   # 样本不足，应被过滤
        ]
        return pd.concat(dfs, ignore_index=True)

    def test_returns_dataframe(self):
        df = self._make_multi_job_df()
        result = estimate_all_groups(df)
        self.assertIsInstance(result, pd.DataFrame)

    def test_filters_small_groups(self):
        df = self._make_multi_job_df()
        result = estimate_all_groups(df)
        # "其他" 组 AI/非AI 各 3 条，低于 MIN_GROUP_SIZE=8，应被过滤
        self.assertNotIn("其他", result["job_group"].values)

    def test_two_valid_groups(self):
        df = self._make_multi_job_df()
        result = estimate_all_groups(df)
        self.assertEqual(len(result), 2)

    def test_sorted_by_premium(self):
        try:
            import statsmodels
        except ImportError:
            self.skipTest("statsmodels 未安装")
        df = self._make_multi_job_df()
        result = estimate_all_groups(df)
        if "ols_premium" in result.columns and result["ols_premium"].notna().all():
            premiums = result["ols_premium"].tolist()
            self.assertEqual(premiums, sorted(premiums, reverse=True))


if __name__ == "__main__":
    unittest.main()
