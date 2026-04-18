"""
questioner.py - Data-goal-driven question generation

Instead of hardcoded question strings, each strategy defines the DATA GOALS
it wants to collect (field name, label, importance). An LLM then reads:
  - the job title / description (to skip what is already disclosed)
  - the existing conversation history (to skip what HR already answered)
  - the data goals (to know what is still needed)

and generates natural, context-aware Chinese questions for only the gaps.

Adding a new job type:
  1. Subclass BaseQuestionStrategy and implement `data_goals()`
  2. Register it in _STRATEGIES at the bottom of this file

The fallback (no LLM / LLM error) renders a sensible default question from
each DataGoal's `default_q` field so the bot can still run offline.
"""
from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from .schemas import JobSnapshot

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# DataGoal — describes one piece of information to collect
# ══════════════════════════════════════════════════════════════════════

@dataclass
class DataGoal:
    """
    One piece of structured information we want HR to reveal.

    field      : matches an HrChatResult attribute (used for dedup checks)
    label      : short Chinese description shown to the LLM
    must       : True → always ask; False → ask only if turns remain
    default_q  : fallback question string used when no LLM is available
    """
    field:     str
    label:     str
    must:      bool = True
    default_q: str = ""


# ══════════════════════════════════════════════════════════════════════
# LLM prompt for question generation
# ══════════════════════════════════════════════════════════════════════

_SYSTEM_QUESTIONER = """\
You are helping a job seeker chat with an HR recruiter on a Chinese recruitment platform.
Write one natural, polite Chinese question per data goal provided.
The questions should sound conversational, not like a formal survey.

Output rules:
- Return ONLY a valid JSON array of question strings, no explanation.
- One question per goal, one sentence each.
- Maximum {n} questions.
"""

_USER_QUESTIONER = """\
Job title : {job_title}
Company   : {company}

Context (already known — use this to make questions sound natural, e.g. reference the company):
{context}

Data goals to ask about (all are genuinely unknown):
{goals_text}

Return ONLY a JSON array, e.g.: ["问题一", "问题二"]
"""


# ── Keyword-based pre-filter ──────────────────────────────────────────

# Keywords that indicate a data goal is already answered.
# If ANY keyword from the list appears in the combined known text, the goal is skipped.
_FIELD_SKIP_KEYWORDS: dict = {
    "category":        ["品类", "类目", "服装", "3c", "家居", "电子", "美妆", "宠物",
                        "户外", "运动", "母婴", "食品", "汽车", "工具"],
    "marketplace":     ["美国站", "欧洲站", "日本站", "澳洲站", "中东站", "全球站",
                        "us站", "eu站", "北美站", "站点", "marketplace"],
    "avg_order_value": ["客单价", "单价", "美元", "usd", "$"],
    "team_size":       ["团队.*?人", r"\d+人", "几人", "人数", "团队规模"],
    "brand_type":      ["自有品牌", "白牌", "分销", "oem", "代工"],
    "work_mode":       ["远程", "居家办公", "弹性", "驻场", "坐班"],
    "monthly_sales":   ["月销", "月营业额", "gmv", "月流水"],
    "tools_used":      ["helium", "精灵", "sp广告", "jungle scout", "卖家精灵",
                        "数据魔方", "生意参谋"],
    # salary is never auto-skipped from description — always confirm directly with HR
}


def _prefilter_goals(
    goals: list[DataGoal],
    job: JobSnapshot,
    existing: list[dict],
) -> list[DataGoal]:
    """
    Remove goals that are already answered based on keyword matching
    against the job description, salary field, and existing conversation.
    Returns only goals that are genuinely still unknown.
    """
    known_parts = [
        (job.description or "").lower(),
        " ".join(m["text"] for m in existing if m.get("role") == "hr").lower(),
    ]
    known_text = " ".join(known_parts)

    remaining = []
    for goal in goals:
        keywords = _FIELD_SKIP_KEYWORDS.get(goal.field, [])
        if keywords and any(re.search(kw, known_text, re.IGNORECASE) for kw in keywords):
            logger.debug("Goal '%s' already covered — skipping", goal.field)
            continue
        remaining.append(goal)
    return remaining


def _build_context_snippet(job: JobSnapshot, existing: list[dict]) -> str:
    """Short context block handed to the LLM so questions sound natural."""
    parts = []
    if job.description:
        parts.append("招聘描述：" + job.description.strip()[:300])
    for m in existing[-4:]:   # last 4 messages for recency
        speaker = "求职者" if m["role"] == "me" else "HR"
        parts.append(f"{speaker}：{m['text'][:150]}")
    return "\n".join(parts) if parts else "（无额外背景信息）"


def generate_questions(
    goals: list[DataGoal],
    job: JobSnapshot,
    existing: list[dict],
    provider,
    max_questions: int = 6,
) -> list[str]:
    """
    Use the LLM to turn DataGoals into context-aware questions.

    Falls back to `goal.default_q` for each goal if the LLM call fails
    or no provider is given.

    Parameters
    ----------
    goals         : ordered list of DataGoal (must goals first)
    job           : current job snapshot
    existing      : messages already in the conversation [{role, text}, ...]
    provider      : LLM provider (from llm.py); None → use defaults only
    max_questions : cap on number of questions returned
    """
    # Step 1: code-level pre-filter — remove goals already answered in description/history
    must_goals = [g for g in goals if g.must]
    nice_goals = [g for g in goals if not g.must]
    all_ordered = must_goals + nice_goals

    remaining = _prefilter_goals(all_ordered, job, existing)
    remaining  = remaining[:max_questions]

    if not remaining:
        logger.info("All data goals already covered for '%s'", job.job_title)
        return []

    if provider is None:
        return _fallback_questions(remaining)

    # Step 2: LLM only writes natural questions for the remaining unknown goals
    context    = _build_context_snippet(job, existing)
    goals_text = "\n".join(f"- {g.label}" for g in remaining)

    try:
        raw = provider.chat(
            system=_SYSTEM_QUESTIONER.format(n=len(remaining)),
            messages=[{
                "role": "user",
                "content": _USER_QUESTIONER.format(
                    job_title  = job.job_title or "未知职位",
                    company    = job.company   or "未知公司",
                    context    = context,
                    goals_text = goals_text,
                    n          = len(remaining),
                ),
            }],
            max_tokens=512,
            temperature=1.3,   # General Conversation — natural question phrasing
        )
        parsed = _parse_json_array(raw)
        if parsed is not None:
            logger.debug(
                "LLM generated %d questions for '%s' (pre-filtered %d covered goals)",
                len(parsed), job.job_title, len(all_ordered) - len(remaining),
            )
            return parsed[:max_questions]
    except Exception as e:
        logger.warning("Question generation LLM call failed (%s); using defaults", e)

    return _fallback_questions(remaining)


def _parse_json_array(raw: str) -> Optional[list[str]]:
    """
    Extract a JSON string array from LLM output (strips markdown fences).
    Returns a list (possibly empty) on success, None if the output is unparseable.
    """
    for prefix in ("```json", "```"):
        if raw.strip().startswith(prefix):
            raw = raw.strip()[len(prefix):]
            break
    if raw.strip().endswith("```"):
        raw = raw.strip()[:-3]
    raw = raw.strip()
    # Find first [ ... ] block
    m = re.search(r'\[.*\]', raw, re.DOTALL)
    if m:
        raw = m.group(0)
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return [str(q).strip() for q in result if str(q).strip()]
    except json.JSONDecodeError:
        pass
    return None  # unparseable — caller should use fallback


def _fallback_questions(goals: list[DataGoal]) -> list[str]:
    return [g.default_q for g in goals if g.default_q]


# ══════════════════════════════════════════════════════════════════════
# Base strategy — defines data goals, not question strings
# ══════════════════════════════════════════════════════════════════════

class BaseQuestionStrategy(ABC):

    @abstractmethod
    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        """Return ordered DataGoals: must-collect first, nice-to-have after."""
        ...

    @staticmethod
    def _salary_unknown(job: JobSnapshot) -> bool:
        return not job.salary_raw or job.salary_raw.strip() in ("", "面议", "薪资面议")


# ══════════════════════════════════════════════════════════════════════
# Concrete strategies
# ══════════════════════════════════════════════════════════════════════

class AmazonOperationsStrategy(BaseQuestionStrategy):
    """Amazon / cross-border e-commerce operations roles."""

    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        goals = [
            DataGoal("category",     "运营的产品品类（如3C、服装、家居等）",
                     default_q="请问这个岗位主要运营的是哪个品类？"),
            DataGoal("marketplace",  "主要运营的亚马逊站点（美国/欧洲/日本/全球等）",
                     default_q="主要运营哪个站点？（美国站/欧洲站/日本站/全球）"),
            DataGoal("avg_order_value", "产品客单价（美元范围）",
                     default_q="产品的客单价大概在什么范围？（美元）"),
            DataGoal("team_size",    "运营团队人数",
                     default_q="团队目前有多少人负责运营？"),
            DataGoal("brand_type",   "品牌模式（自有品牌 / 白牌 / 分销 / OEM）",
                     default_q="品牌是自有品牌还是白牌/分销模式？"),
        ]
        if self._salary_unknown(job):
            goals.append(DataGoal(
                "salary", "薪资范围及底薪+提成结构",
                default_q="请问薪资范围大概是多少？（底薪+提成结构是怎样的？）",
            ))
        goals += [
            DataGoal("monthly_sales", "公司目前月销售额量级", must=False,
                     default_q="公司目前月销售额大概在什么量级？"),
            DataGoal("tools_used",    "日常使用的运营工具（Helium10、卖家精灵、SP等）", must=False,
                     default_q="日常用哪些工具做运营和数据分析？"),
            DataGoal("work_mode",     "是否支持远程或弹性办公", must=False,
                     default_q="这个岗位是否支持远程或弹性办公？"),
        ]
        return goals


class CrossBorderEcommerceStrategy(BaseQuestionStrategy):
    """Generic cross-border e-commerce (non-Amazon-specific)."""

    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        goals = [
            DataGoal("marketplace",     "主要运营的跨境电商平台（Temu、Shopify、速卖通等）",
                     default_q="请问主要是哪个平台的运营？"),
            DataGoal("category",        "运营的产品品类",
                     default_q="运营的产品品类是什么？"),
            DataGoal("avg_order_value", "产品客单价",
                     default_q="产品客单价大概是多少？"),
            DataGoal("brand_type",      "广告投放要求（PPC / 站外引流等）",
                     default_q="是否需要具备广告投放经验？"),
        ]
        if self._salary_unknown(job):
            goals.append(DataGoal(
                "salary", "薪资结构及绩效提成",
                default_q="薪资结构是怎样的？有绩效提成吗？",
            ))
        goals += [
            DataGoal("team_size",    "团队规模及汇报关系", must=False,
                     default_q="团队规模和汇报关系是怎样的？"),
            DataGoal("monthly_sales","公司目标市场及月销售额", must=False,
                     default_q="公司主要目标市场是哪些国家？"),
            DataGoal("work_mode",    "KPI考核方式及办公模式", must=False,
                     default_q="这个岗位的KPI是怎么考核的？"),
        ]
        return goals


class DomesticEcommerceStrategy(BaseQuestionStrategy):
    """Domestic platforms: Taobao, JD, Pinduoduo, Douyin, etc."""

    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        goals = [
            DataGoal("marketplace", "主要运营的国内电商平台（天猫/京东/抖音/拼多多等）",
                     default_q="请问主要运营哪个平台？"),
            DataGoal("category",    "运营品类及客单价",
                     default_q="运营的品类是什么？客单价大概多少？"),
            DataGoal("brand_type",  "是否涉及直播或短视频内容运营",
                     default_q="是否需要负责直播或短视频内容运营？"),
            DataGoal("team_size",   "团队配置（运营/设计/客服各几人）",
                     default_q="团队配置是怎样的？"),
        ]
        if self._salary_unknown(job):
            goals.append(DataGoal(
                "salary", "薪资范围及提成奖金",
                default_q="薪资范围是多少？是否有提成或奖金？",
            ))
        goals += [
            DataGoal("monthly_sales", "店铺目前月销售额量级", must=False,
                     default_q="店铺目前月销售额是多少量级？"),
            DataGoal("tools_used",    "是否使用AI工具辅助运营", must=False,
                     default_q="是否使用AI工具辅助运营？"),
        ]
        return goals


class SupplyChainStrategy(BaseQuestionStrategy):
    """Supply chain / procurement / logistics roles."""

    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        goals = [
            DataGoal("category",   "主要负责的供应链环节（采购/仓储/物流/报关等）",
                     default_q="请问主要负责哪个环节？（采购/仓储/物流/报关/供应商管理）"),
            DataGoal("marketplace","面向国内还是跨境供应链",
                     default_q="主要面向国内供应链还是跨境供应链？"),
            DataGoal("tools_used", "日常使用的ERP或供应链管理系统",
                     default_q="日常使用哪些ERP或供应链管理系统？"),
        ]
        if self._salary_unknown(job):
            goals.append(DataGoal("salary", "薪资范围",
                                  default_q="薪资范围是多少？"))
        goals += [
            DataGoal("avg_order_value", "主要采购品类及年采购额", must=False,
                     default_q="主要采购品类是什么？年采购额大概多少？"),
            DataGoal("team_size",       "团队规模及汇报线", must=False,
                     default_q="团队规模和汇报线是怎样的？"),
            DataGoal("work_mode",       "是否需要出差或驻厂", must=False,
                     default_q="是否需要出差或驻厂？"),
        ]
        return goals


class DefaultStrategy(BaseQuestionStrategy):
    """Fallback strategy for unrecognized job types."""

    def data_goals(self, job: JobSnapshot) -> list[DataGoal]:
        title = job.job_title or "该岗位"
        goals = [
            DataGoal("category",   title + "的核心职责",
                     default_q="请问" + title + "这个岗位的核心职责是什么？"),
            DataGoal("brand_type", "最重要的KPI或考核指标",
                     default_q="日常工作中最重要的KPI或考核指标是什么？"),
            DataGoal("team_size",  "团队规模及汇报关系",
                     default_q="团队规模是多少？直接汇报给谁？"),
        ]
        if self._salary_unknown(job):
            goals.append(DataGoal(
                "salary", "薪资范围及绩效奖金结构",
                default_q="薪资范围大概是多少？绩效或奖金结构是怎样的？",
            ))
        goals += [
            DataGoal("marketplace", "公司发展阶段", must=False,
                     default_q="公司目前处于什么发展阶段？"),
            DataGoal("work_mode",   "是否支持弹性或远程办公", must=False,
                     default_q="这个岗位是否支持弹性或远程办公？"),
        ]
        return goals


# ══════════════════════════════════════════════════════════════════════
# Strategy registry + factory
# ══════════════════════════════════════════════════════════════════════

_STRATEGIES: list[tuple[str, type[BaseQuestionStrategy]]] = [
    (r"amazon|亚马逊",                                    AmazonOperationsStrategy),
    (r"跨境|cross.?border|temu|shopify|速卖通|ebay|wish", CrossBorderEcommerceStrategy),
    (r"天猫|淘宝|京东|拼多多|抖音|快手|直播|国内电商",        DomesticEcommerceStrategy),
    (r"供应链|采购|仓储|物流|报关|sourcing",                 SupplyChainStrategy),
]


def get_strategy(job: JobSnapshot) -> BaseQuestionStrategy:
    """Return the best-matching strategy; falls back to DefaultStrategy."""
    probe = " ".join(filter(None, [job.job_title, job.description or ""])).lower()
    for pattern, cls in _STRATEGIES:
        if re.search(pattern, probe, re.IGNORECASE):
            return cls()
    return DefaultStrategy()
