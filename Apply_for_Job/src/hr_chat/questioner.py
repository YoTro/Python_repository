"""
questioner.py - Strategy pattern for generating HR question sets

Each concrete strategy targets a specific job type.
The factory function `get_strategy()` picks the right one automatically
based on the job title / canonical name in JobSnapshot.

Adding a new job type:
  1. Subclass BaseQuestionStrategy and implement `questions()`
  2. Register it in _STRATEGIES at the bottom of this file
"""
from __future__ import annotations
import re
from abc import ABC, abstractmethod
from typing import Optional

from .schemas import JobSnapshot


# ══════════════════════════════════════════════════════════════════════
# Base strategy
# ══════════════════════════════════════════════════════════════════════

class BaseQuestionStrategy(ABC):
    """
    A strategy defines:
      - must_ask   : questions always asked (high-signal gaps)
      - nice_to_ask: questions asked if time / turns permit
    """

    @abstractmethod
    def must_ask(self, job: JobSnapshot) -> list[str]:
        ...

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:  # noqa: ARG002
        return []

    def all_questions(self, job: JobSnapshot) -> list[str]:
        return self.must_ask(job) + self.nice_to_ask(job)

    # Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _salary_unknown(job: JobSnapshot) -> bool:
        return not job.salary_raw or job.salary_raw.strip() in ("", "面议", "薪资面议")


# ══════════════════════════════════════════════════════════════════════
# Concrete strategies
# ══════════════════════════════════════════════════════════════════════

class AmazonOperationsStrategy(BaseQuestionStrategy):
    """For Amazon e-commerce operations roles."""

    def must_ask(self, job: JobSnapshot) -> list[str]:
        qs = [
            "请问这个岗位主要运营的是哪个品类？（如服装、3C、家居等）",
            "产品的客单价大概在什么范围？（美元）",
            "主要运营哪个站点？（美国站/欧洲站/日本站/全球）",
            "团队目前有多少人负责运营？",
            "品牌是自有品牌还是白牌/分销模式？",
        ]
        if self._salary_unknown(job):
            qs.append("请问薪资范围大概是多少？（底薪+提成结构是怎样的？）")
        return qs

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:
        return [
            "公司目前月销售额大概在什么量级？",
            "日常用哪些工具做运营和数据分析？（如 Helium10、卖家精灵、SP 等）",
            "这个岗位是否支持远程或弹性办公？",
            "旺季一般是几月份？旺季备货压力大吗？",
        ]


class CrossBorderEcommerceStrategy(BaseQuestionStrategy):
    """Generic cross-border e-commerce (not Amazon-specific)."""

    def must_ask(self, job: JobSnapshot) -> list[str]:
        qs = [
            "请问主要是哪个平台的运营？（亚马逊、Temu、Shopify、速卖通等）",
            "运营的产品品类是什么？",
            "产品客单价大概是多少？",
            "是否需要具备广告投放经验？（PPC / 站外引流）",
        ]
        if self._salary_unknown(job):
            qs.append("薪资结构是怎样的？有绩效提成吗？")
        return qs

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:
        return [
            "团队规模和汇报关系是怎样的？",
            "公司主要目标市场是哪些国家？",
            "这个岗位的 KPI 是怎么考核的？",
        ]


class DomesticEcommerceStrategy(BaseQuestionStrategy):
    """Domestic platforms: Taobao, JD, Pinduoduo, Douyin, etc."""

    def must_ask(self, job: JobSnapshot) -> list[str]:
        qs = [
            "请问主要运营哪个平台？（天猫、京东、抖音、拼多多等）",
            "运营的品类是什么？客单价大概多少？",
            "是否需要负责直播或短视频内容运营？",
            "团队配置是怎样的（运营/设计/客服各几人）？",
        ]
        if self._salary_unknown(job):
            qs.append("薪资范围是多少？是否有提成或奖金？")
        return qs

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:
        return [
            "店铺目前月销售额是多少量级？",
            "是否使用 AI 工具辅助运营（选品、文案、客服等）？",
            "旺季备货节奏是怎样的？",
        ]


class SupplyChainStrategy(BaseQuestionStrategy):
    """Supply chain / procurement / logistics roles."""

    def must_ask(self, job: JobSnapshot) -> list[str]:
        qs = [
            "请问主要负责哪个环节？（采购/仓储/物流/报关/供应商管理）",
            "主要面向国内供应链还是跨境供应链？",
            "日常使用哪些 ERP 或供应链管理系统？",
        ]
        if self._salary_unknown(job):
            qs.append("薪资范围是多少？")
        return qs

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:
        return [
            "主要采购品类是什么？年采购额大概多少？",
            "团队规模和汇报线是怎样的？",
            "是否需要出差或驻厂？",
        ]


class DefaultStrategy(BaseQuestionStrategy):
    """Fallback strategy for unrecognized job types."""

    def must_ask(self, job: JobSnapshot) -> list[str]:
        title = job.job_title
        qs = [
            f"请问\u201c{title}\u201d这个岗位的核心职责是什么？",
            "日常工作中最重要的 KPI 或考核指标是什么？",
            "团队规模是多少？直接汇报给谁？",
        ]
        if self._salary_unknown(job):
            qs.append("薪资范围大概是多少？绩效或奖金结构是怎样的？")
        return qs

    def nice_to_ask(self, job: JobSnapshot) -> list[str]:
        return [
            "公司目前处于什么发展阶段？",
            "这个岗位是否支持弹性或远程办公？",
            "最希望候选人在入职前三个月完成哪些目标？",
        ]


# ══════════════════════════════════════════════════════════════════════
# Strategy registry + factory
# ══════════════════════════════════════════════════════════════════════

# Each entry: (regex pattern to match job title/canonical, strategy class)
_STRATEGIES: list[tuple[str, type[BaseQuestionStrategy]]] = [
    (r"amazon|亚马逊",                                    AmazonOperationsStrategy),
    (r"跨境|cross.?border|temu|shopify|速卖通|ebay|wish", CrossBorderEcommerceStrategy),
    (r"天猫|淘宝|京东|拼多多|抖音|快手|直播|国内电商",        DomesticEcommerceStrategy),
    (r"供应链|采购|仓储|物流|报关|sourcing",                 SupplyChainStrategy),
]


def get_strategy(job: JobSnapshot) -> BaseQuestionStrategy:
    """
    Return the best-matching question strategy for the given job.
    Falls back to DefaultStrategy if nothing matches.
    """
    probe = " ".join(filter(None, [job.job_title, job.description or ""])).lower()
    for pattern, cls in _STRATEGIES:
        if re.search(pattern, probe, re.IGNORECASE):
            return cls()
    return DefaultStrategy()
