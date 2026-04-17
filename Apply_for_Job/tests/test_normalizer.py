import unittest
import pandas as pd
from unittest.mock import patch
from src.analysis.normalizer import (
    parse_salary,
    parse_experience,
    canonicalize_job,
    normalize_51job,
    normalize_zhipin,
    _enrich,
    reload_categories,
)


class TestParseSalary(unittest.TestCase):

    def _mid(self, s):
        return parse_salary(s)["salary_mid"]

    def _min(self, s):
        return parse_salary(s)["salary_min"]

    def _max(self, s):
        return parse_salary(s)["salary_max"]

    # ── K/月 ────────────────────────────────────────────────────────
    def test_k_monthly(self):
        r = parse_salary("15k-25k·13薪")
        self.assertEqual(r["salary_min"], 15000)
        self.assertEqual(r["salary_max"], 25000)
        self.assertEqual(r["salary_mid"], 20000)
        self.assertEqual(r["salary_months"], 13)

    def test_k_monthly_uppercase(self):
        self.assertEqual(self._min("10K-20K/月"), 10000)
        self.assertEqual(self._max("10K-20K/月"), 20000)

    def test_k_monthly_dash(self):
        self.assertEqual(self._mid("8k-12k"), 10000)

    # ── 元/月 ───────────────────────────────────────────────────────
    def test_yuan_monthly(self):
        r = parse_salary("8000-12000元/月")
        self.assertEqual(r["salary_min"], 8000)
        self.assertEqual(r["salary_max"], 12000)

    def test_yuan_monthly_no_unit(self):
        r = parse_salary("6000-9000")
        self.assertEqual(r["salary_min"], 6000)
        self.assertEqual(r["salary_max"], 9000)

    # ── 万/年 ───────────────────────────────────────────────────────
    def test_wan_yearly(self):
        r = parse_salary("30-50万/年")
        # 30万/年 ÷ 12 = 25000/月
        self.assertAlmostEqual(r["salary_min"], 25000, delta=10)
        self.assertAlmostEqual(r["salary_max"], 41667, delta=10)

    def test_k_yearly(self):
        r = parse_salary("150K-200K/年")
        self.assertAlmostEqual(r["salary_min"], 12500, delta=10)
        self.assertAlmostEqual(r["salary_max"], 16667, delta=10)

    # ── 异常输入 ────────────────────────────────────────────────────
    def test_mianyi(self):
        r = parse_salary("面议")
        self.assertIsNone(r["salary_mid"])

    def test_empty(self):
        r = parse_salary(None)
        self.assertIsNone(r["salary_mid"])
        r2 = parse_salary("")
        self.assertIsNone(r2["salary_mid"])

    def test_raw_preserved(self):
        s = "15k-25k·13薪"
        self.assertEqual(parse_salary(s)["salary_raw"], s)


class TestParseExperience(unittest.TestCase):

    def test_range(self):
        self.assertEqual(parse_experience("3-5年经验"), 4.0)

    def test_above(self):
        self.assertEqual(parse_experience("10年以上"), 10.0)

    def test_fresh(self):
        self.assertEqual(parse_experience("应届生"), 0.0)
        self.assertEqual(parse_experience("在校生"), 0.0)
        self.assertEqual(parse_experience("经验不限"), 0.0)

    def test_single(self):
        self.assertEqual(parse_experience("2年"), 2.0)

    def test_none(self):
        self.assertIsNone(parse_experience(None))
        self.assertIsNone(parse_experience(""))


class TestCanonicalizeJob(unittest.TestCase):

    # ── yaml 词典命中 ────────────────────────────────────────────────
    def test_amazon_ops(self):
        for title in ["亚马逊运营", "Amazon运营专员", "跨境电商运营-亚马逊"]:
            self.assertEqual(canonicalize_job(title), "Amazon运营", msg=title)

    def test_algo(self):
        for title in ["算法工程师", "Machine Learning Engineer"]:
            result = canonicalize_job(title)
            self.assertEqual(result, "算法工程师", msg=title)

    def test_data(self):
        self.assertEqual(canonicalize_job("数据分析师"), "数据分析师")

    def test_ai_pm(self):
        self.assertEqual(canonicalize_job("AI产品经理"), "AI产品经理")

    # ── 关键词直通（yaml 未收录的岗位）───────────────────────────────
    def test_keyword_passthrough_when_no_match(self):
        # "厨师" 不在 yaml，应 fallback 到 search_keyword
        result = canonicalize_job("厨师", search_keyword="厨师")
        self.assertEqual(result, "厨师")

    def test_keyword_passthrough_novel_job(self):
        # 任意新岗位传入 search_keyword 都能作为分组
        result = canonicalize_job("无人机飞手", search_keyword="无人机飞手")
        self.assertEqual(result, "无人机飞手")

    def test_fallback_to_qita_without_keyword(self):
        # 未命中 + 无 keyword → "其他"
        self.assertEqual(canonicalize_job("厨师"), "其他")
        self.assertEqual(canonicalize_job(None), "其他")

    # ── yaml 词典命中优先于 keyword ─────────────────────────────────
    def test_yaml_match_takes_priority(self):
        # 即使传了 keyword，yaml 命中时应返回 yaml 标准名
        result = canonicalize_job("亚马逊运营专员", search_keyword="某关键词")
        self.assertEqual(result, "Amazon运营")

    # ── yaml 缺失时降级 ──────────────────────────────────────────────
    def test_no_yaml_uses_keyword(self):
        reload_categories()
        with patch("src.analysis.normalizer._CONFIG_PATH") as mock_path:
            mock_path.exists.return_value = False
            reload_categories()
            result = canonicalize_job("UI设计师", search_keyword="UI设计")
            self.assertEqual(result, "UI设计")
        reload_categories()  # 恢复


class TestNormalize51job(unittest.TestCase):

    def _make_df(self, job_title="亚马逊运营"):
        return pd.DataFrame([{
            "Job":        job_title,
            "Salary":     "10k-15k",
            "Company":    "某跨境公司",
            "Location":   "深圳",
            "Education":  "本科",
            "Experience": "3-5年经验",
            "UpdateDate": "2026-04-01",
            "Welfare":    "五险一金|弹性上下班",
            "JobDetail":  "负责亚马逊平台运营，熟悉ChatGPT优先",
            "Href":       "https://we.51job.com/pc/search?jobId=123",
        }])

    def test_columns_present(self):
        df = normalize_51job(self._make_df())
        for col in ["source", "job_title", "salary_mid", "exp_years",
                    "job_canonical", "city_tier"]:
            self.assertIn(col, df.columns, msg=f"缺少列: {col}")

    def test_source_tag(self):
        df = normalize_51job(self._make_df())
        self.assertEqual(df["source"].iloc[0], "51job")

    def test_salary_parsed(self):
        df = normalize_51job(self._make_df())
        self.assertEqual(df["salary_mid"].iloc[0], 12500)

    def test_job_canonical_yaml_hit(self):
        df = normalize_51job(self._make_df())
        self.assertEqual(df["job_canonical"].iloc[0], "Amazon运营")

    def test_job_canonical_keyword_passthrough(self):
        # yaml 未收录的岗位 → 使用 search_keyword
        df = normalize_51job(self._make_df(job_title="无人机飞手"),
                             search_keyword="无人机飞手")
        self.assertEqual(df["job_canonical"].iloc[0], "无人机飞手")

    def test_job_canonical_fallback_qita(self):
        # yaml 未收录 + 无 keyword → "其他"
        df = normalize_51job(self._make_df(job_title="厨师"))
        self.assertEqual(df["job_canonical"].iloc[0], "其他")

    def test_city_tier(self):
        df = normalize_51job(self._make_df())
        self.assertEqual(df["city_tier"].iloc[0], 1)  # 深圳 = tier 1


class TestNormalizeZhipin(unittest.TestCase):

    def _make_df(self, job_name="数据分析师"):
        return pd.DataFrame([{
            "jobName":         job_name,
            "salaryDesc":      "20k-35k",
            "jobDescription":  "使用Python/SQL做数据分析，有大模型经验优先",
            "areaDistrict":    "南山区",
            "businessDistrict":"科技园",
            "jobDegree":       "本科",
            "jobExperience":   "3-5年",
            "brandName":       "某科技公司",
            "brandScaleName":  "500-2000人",
            "brandIndustry":   "互联网",
            "jobLabels":       "五险一金,弹性工作",
            "encryptJobId":    "abc123",
        }])

    def test_columns_present(self):
        df = normalize_zhipin(self._make_df())
        for col in ["source", "job_title", "salary_mid", "exp_years",
                    "job_canonical", "company_size_n"]:
            self.assertIn(col, df.columns, msg=f"缺少列: {col}")

    def test_source_tag(self):
        df = normalize_zhipin(self._make_df())
        self.assertEqual(df["source"].iloc[0], "zhipin")

    def test_company_size_mapped(self):
        df = normalize_zhipin(self._make_df())
        self.assertEqual(df["company_size_n"].iloc[0], 1250)  # 500-2000人 → 1250

    def test_keyword_passthrough(self):
        # 搜索"UI设计师"，yaml 已收录，应返回 yaml 标准名
        df = normalize_zhipin(self._make_df(job_name="UI设计师"),
                              search_keyword="UI设计师")
        self.assertEqual(df["job_canonical"].iloc[0], "UI设计师")


if __name__ == "__main__":
    unittest.main()
