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
    # Chinese
    "大模型", r"\bgpt[-\d]", r"gpt模型", "fine.?tun", "微调", "rag",
    "prompt.*engineer", "提示词工程",
    "langchain", "llamaindex", "向量数据库",
    "stable.?diffusion", "comfyui",
    "机器学习", "deep.?learning", "pytorch", "tensorflow",
    "nlp", "cv", "目标检测", "语义分割",
    # English
    r"\bllm\b", r"\brag\b", "fine.?tun",
    r"prompt.{0,4}engineer", "vector.{0,8}database", r"\bembedding\b",
    r"machine.?learning", r"deep.?learning",
    r"computer.?vision", "object.?detection",
    r"\bpytorch\b", r"\btensorflow\b", r"\bhugging.?face\b",
    r"generative.?ai", r"\bgenai\b", r"foundation.?model",
]

# Tier 2 — 明确与 AI/自动化工具结合的数据能力
# 注意：通用的"数据分析"、"报表"、"数据驱动"是运营基础技能，不计入 AI 技能
AI_TIER2 = [
    # 编程语言（运营岗出现说明有技术门槛）
    r"\bpython\b", r"\bsql\b", r"\br语言\b",
    # BI / 可视化工具（超出 Excel 的专业工具）
    "bi工具", r"power.?bi", "tableau", "metabase", "looker",
    # 流程自动化 / RPA（明确自动化工具）
    "自动化运营", r"\brpa\b", r"\bn8n\b", "zapier", r"make\.com",
    # AI 驱动的数据分析（必须含 AI 字样）
    r"ai.{0,12}数据", r"数据.{0,12}ai", r"ai.{0,12}analyt",
    r"ai.{0,12}insight", r"智能.{0,8}分析", r"ai.{0,8}报告",
    # English equivalents
    r"\bpython\b", r"\bsql\b",
    r"\bpower.?bi\b", r"\btableau\b", r"\bmetabase\b", r"\blooker\b",
    r"process.?automat", r"\brpa\b", r"\bn8n\b", r"\bzapier\b",
    r"ai.{0,8}data", r"ai.{0,8}analyt", r"ai.{0,8}insight",
    r"automat.{0,8}report", r"data.{0,6}science",
]

# Tier 1 — 通用 AI 工具（基础门槛，溢价相对较低）
AI_TIER1 = [
    # Chinese
    "chatgpt", "claude", "gemini", "copilot", "kimi", "豆包",
    "文心一言", "通义千问", "讯飞星火", r"ai.{0,8}工具", "aigc",
    r"ai.{0,8}辅助", r"ai.{0,8}写作", r"ai.{0,8}客服", r"ai.{0,8}选品", r"ai.{0,8}作图",
    "midjourney", "sora",
    # English
    r"\bchatgpt\b", r"\bclaude\b", r"\bgemini\b", r"\bcopilot\b",
    r"\bmidjourney\b", r"\bsora\b", r"\bdall.?e\b",
    r"ai.{0,8}tool", r"ai.{0,8}generat", r"ai.{0,8}assist",
    r"\baigc\b", "generative.{0,8}content",
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

# 电商运营专项 AI 词（中文 Amazon / 跨境运营）
# 只计入明确带"AI/智能/自动化"字样的电商工具，或专属的 AI 选品工具
ECOMMERCE_AI = [
    # 必须带 AI 字样的电商操作
    r"ai.{0,12}listing", r"listing.{0,8}ai",
    r"ai.{0,8}广告", r"广告.{0,8}自动化",
    r"ai.{0,8}选品", r"选品.{0,8}ai",
    r"ai.{0,8}定价", r"智能.{0,8}定价",
    r"chat.{0,4}gpt.{0,8}运营", r"运营.{0,8}chat.{0,4}gpt",
    # 专属 AI 辅助选品工具（非通用数据分析）
    "sellersprite", "卖家精灵", r"helium.?10", r"jungle.?scout",
    # English
    r"ai.{0,12}listing", r"ai.{0,8}pricing",
    r"ai.{0,8}advertis", r"ppc.{0,8}automat", r"automat.{0,8}ppc",
    r"helium.?10", r"jungle.?scout", r"seller.?sprite",
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
