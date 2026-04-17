"""
skill_extractor.py - 从职位描述中提取技能信号

分两层：
  1. 通用技术技能词表（算法/开发岗通用）
  2. 非技术岗 AI 工具词表（运营/产品/市场场景）

主要输出：
  has_ai_skill     bool   — JD 是否包含任何 AI 技能要求
  ai_skills_found  list   — 命中的具体 AI 关键词列表
  ai_skill_tier    int    — 0=无 / 1=通用工具(ChatGPT等) / 2=数据能力 / 3=核心AI技能
  tech_skills      list   — 通用技术技能列表（Python/SQL等）
"""
from __future__ import annotations
import re
from typing import Optional

# ══════════════════════════════════════════════════════════════════════
# 词表定义
# ══════════════════════════════════════════════════════════════════════

# Tier 3 — 核心 AI 技能（溢价最高）
AI_TIER3 = [
    "大模型", "llm", r"\bgpt[-\d]", r"gpt模型", "fine.?tun", "微调", "rag",
    "agent", "prompt.*engineer", "提示词工程",
    "langchain", "llamaindex", "向量数据库", "embedding",
    "stable.?diffusion", "midjourney", "comfyui",
    "机器学习", "deep.?learning", "pytorch", "tensorflow",
    "nlp", "cv", "computer.?vision", "目标检测", "语义分割",
]

# Tier 2 — 数据分析 / 自动化能力（运营岗的 AI 入口）
AI_TIER2 = [
    r"\bpython\b", r"\bsql\b", r"\br语言\b",
    "数据分析", "数据挖掘", "bi工具", "power.?bi", "tableau", "metabase",
    "自动化运营", "rpa", "n8n", "zapier", "make\\.com",
    "数据看板", "报表", "excel.*函数", "数据驱动",
]

# Tier 1 — 通用 AI 工具（基础门槛，溢价相对较低）
AI_TIER1 = [
    "chatgpt", "claude", "gemini", "copilot", "kimi", "豆包",
    "文心一言", "通义千问", "讯飞星火", r"ai.{0,8}工具", "aigc",
    r"ai.{0,8}辅助", r"ai.{0,8}写作", r"ai.{0,8}客服", r"ai.{0,8}选品", r"ai.{0,8}作图",
    "midjourney", "sora",
]

# 通用技术技能（不影响 AI 溢价计算，仅供描述统计）
TECH_SKILLS = [
    "java", "golang", "rust", "c\\+\\+", "c#", "php",
    "react", "vue", "angular", "typescript",
    "docker", "kubernetes", "k8s", "terraform",
    "mysql", "postgresql", "mongodb", "redis", "elasticsearch",
    "spark", "hadoop", "kafka", "airflow",
    "aws", "gcp", "azure", "阿里云", "腾讯云",
]

# 电商运营专项 AI 词（Amazon运营场景）
ECOMMERCE_AI = [
    r"ai.{0,12}listing", r"listing.{0,8}ai", r"ai.{0,8}广告", r"广告.{0,8}自动化",
    r"ai.{0,8}选品", r"选品.{0,8}ai", r"ai.{0,8}定价", "动态定价",
    "sellersprite", "卖家精灵", r"helium.?10", r"jungle.?scout",
    r"数据.{0,8}分析.{0,8}运营", r"运营.{0,8}数据.{0,8}分析",
    r"chat.{0,4}gpt.{0,8}运营", r"运营.{0,8}chat.{0,4}gpt",
]


def _compile(patterns: list[str]) -> list[tuple[str, re.Pattern]]:
    """Returns list of (label, compiled_pattern). Label is the raw pattern string."""
    return [(p, re.compile(p, re.IGNORECASE)) for p in patterns]


_T3 = _compile(AI_TIER3)
_T2 = _compile(AI_TIER2)
_T1 = _compile(AI_TIER1)
_EC = _compile(ECOMMERCE_AI)
_TK = _compile(TECH_SKILLS)


def _clean_label(pattern: str) -> str:
    """Convert a regex pattern to a readable skill label."""
    label = pattern
    label = re.sub(r'\\b|\\s[*+]?', '', label)      # remove word boundaries
    label = re.sub(r'\.\{[^}]+\}', '', label)        # remove .{n,m} quantifiers
    label = re.sub(r'\.\*\??', '', label)            # remove .*  or .*?
    label = re.sub(r'\.\?', '', label)               # remove .?
    label = re.sub(r'[\\()\[\]^$+?|*.]', '', label) # remove remaining special chars
    label = re.sub(r'\s+', '', label)
    return label.strip().lower() or pattern


def extract_skills(text: Optional[str]) -> dict:
    """
    从单条 JD 文本中提取技能信号。

    Returns
    -------
    {
        "has_ai_skill":    bool,
        "ai_skill_tier":   int (0-3),
        "ai_skills_found": list[str],
        "is_ecommerce_ai": bool,
        "tech_skills":     list[str],
    }
    """
    if not text or not isinstance(text, str):
        return {
            "has_ai_skill": False,
            "ai_skill_tier": 0,
            "ai_skills_found": [],
            "is_ecommerce_ai": False,
            "tech_skills": [],
        }

    t = text.lower()
    found_ai: list[str] = []
    tier = 0

    for label, pat in _T3:
        if pat.search(t):
            found_ai.append(_clean_label(label))
            tier = max(tier, 3)

    for label, pat in _T2:
        if pat.search(t):
            found_ai.append(_clean_label(label))
            tier = max(tier, 2)

    for label, pat in _T1:
        if pat.search(t):
            found_ai.append(_clean_label(label))
            tier = max(tier, 1)

    is_ec_ai = any(pat.search(t) for _, pat in _EC)
    if is_ec_ai:
        tier = max(tier, 2)
        for label, pat in _EC:
            if pat.search(t):
                found_ai.append(_clean_label(label))

    tech: list[str] = []
    for label, pat in _TK:
        if pat.search(t):
            tech.append(_clean_label(label))

    return {
        "has_ai_skill":    tier > 0,
        "ai_skill_tier":   tier,
        "ai_skills_found": list(dict.fromkeys(found_ai)),  # 去重保序
        "is_ecommerce_ai": is_ec_ai,
        "tech_skills":     list(dict.fromkeys(tech)),
    }


def enrich_dataframe(df) -> object:
    """
    对整个 DataFrame 批量运行 extract_skills，
    将结果列追加到原 df 并返回。
    """
    import pandas as pd

    text_col = "description" if "description" in df.columns else df.columns[0]
    results = df[text_col].apply(extract_skills).apply(pd.Series)
    return pd.concat([df, results], axis=1)
