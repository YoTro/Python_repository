import unittest
import pandas as pd
from src.analysis.skill_extractor import extract_skills, enrich_dataframe


class TestExtractSkills(unittest.TestCase):

    # ── Tier 3 核心 AI ──────────────────────────────────────────────
    def test_tier3_llm(self):
        r = extract_skills("熟悉大模型开发，有RAG和微调经验")
        self.assertTrue(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 3)

    def test_tier3_pytorch(self):
        r = extract_skills("深度学习算法工程师，熟练使用PyTorch")
        self.assertTrue(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 3)

    def test_tier3_nlp(self):
        r = extract_skills("NLP工程师，负责文本分类和信息抽取")
        self.assertEqual(r["ai_skill_tier"], 3)

    # ── Tier 2 数据/自动化 ──────────────────────────────────────────
    def test_tier2_python(self):
        r = extract_skills("需要熟悉Python和SQL，有数据分析经验")
        self.assertTrue(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 2)

    def test_tier2_automation(self):
        r = extract_skills("负责自动化运营，使用RPA工具提升效率")
        self.assertEqual(r["ai_skill_tier"], 2)

    def test_tier2_bi(self):
        r = extract_skills("熟悉Power BI或Tableau，能制作数据看板")
        self.assertEqual(r["ai_skill_tier"], 2)

    # ── Tier 1 通用工具 ─────────────────────────────────────────────
    def test_tier1_chatgpt(self):
        r = extract_skills("会使用ChatGPT辅助文案写作")
        self.assertTrue(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 1)

    def test_tier1_aigc(self):
        r = extract_skills("有AIGC相关工作经验者优先")
        self.assertEqual(r["ai_skill_tier"], 1)

    # ── 电商 AI 专项 ────────────────────────────────────────────────
    def test_ecommerce_ai(self):
        r = extract_skills("熟悉AI选品工具，有Helium 10使用经验")
        self.assertTrue(r["is_ecommerce_ai"])
        self.assertTrue(r["has_ai_skill"])

    def test_ecommerce_listing(self):
        r = extract_skills("使用AI优化Listing，提升转化率")
        self.assertTrue(r["is_ecommerce_ai"])

    # ── Tier 优先级：高 tier 覆盖低 tier ──────────────────────────
    def test_tier_priority(self):
        # 同时出现 tier1 和 tier3，应返回 tier3
        r = extract_skills("会用ChatGPT，同时熟悉大模型微调和RAG框架")
        self.assertEqual(r["ai_skill_tier"], 3)

    # ── 无 AI 技能 ──────────────────────────────────────────────────
    def test_no_ai_skill(self):
        r = extract_skills("负责仓储管理，协调供应链，有驾照优先")
        self.assertFalse(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 0)
        self.assertEqual(r["ai_skills_found"], [])

    # ── 异常输入 ────────────────────────────────────────────────────
    def test_none_input(self):
        r = extract_skills(None)
        self.assertFalse(r["has_ai_skill"])
        self.assertEqual(r["ai_skill_tier"], 0)

    def test_empty_string(self):
        r = extract_skills("")
        self.assertFalse(r["has_ai_skill"])

    # ── 技术技能提取 ────────────────────────────────────────────────
    def test_tech_skills_extracted(self):
        r = extract_skills("熟悉Docker、Kubernetes、MySQL和Redis")
        self.assertIn("docker", r["tech_skills"])
        self.assertIn("mysql", r["tech_skills"])

    # ── 去重 ────────────────────────────────────────────────────────
    def test_dedup(self):
        r = extract_skills("ChatGPT ChatGPT ChatGPT 会用ChatGPT写文案")
        # ai_skills_found 不应包含重复项
        self.assertEqual(len(r["ai_skills_found"]),
                         len(set(r["ai_skills_found"])))


class TestEnrichDataframe(unittest.TestCase):

    def _make_df(self):
        return pd.DataFrame([
            {"description": "熟悉大模型开发，RAG框架搭建"},
            {"description": "需要Python和SQL数据分析能力"},
            {"description": "会用ChatGPT辅助文案写作"},   # 纯 Tier 1，不含运营关键词
            {"description": "仓库管理，无特殊技能要求"},
            {"description": None},
        ])

    def test_columns_added(self):
        df = enrich_dataframe(self._make_df())
        for col in ["has_ai_skill", "ai_skill_tier", "ai_skills_found",
                    "is_ecommerce_ai", "tech_skills"]:
            self.assertIn(col, df.columns, msg=f"缺少列: {col}")

    def test_row_count_preserved(self):
        raw = self._make_df()
        enriched = enrich_dataframe(raw)
        self.assertEqual(len(enriched), len(raw))

    def test_tiers_correct(self):
        df = enrich_dataframe(self._make_df())
        self.assertEqual(df["ai_skill_tier"].iloc[0], 3)   # 大模型/RAG
        self.assertEqual(df["ai_skill_tier"].iloc[1], 2)   # Python/SQL
        self.assertEqual(df["ai_skill_tier"].iloc[2], 1)   # ChatGPT
        self.assertEqual(df["ai_skill_tier"].iloc[3], 0)   # 无
        self.assertEqual(df["ai_skill_tier"].iloc[4], 0)   # None

    def test_ai_count(self):
        df = enrich_dataframe(self._make_df())
        self.assertEqual(df["has_ai_skill"].sum(), 3)


if __name__ == "__main__":
    unittest.main()
