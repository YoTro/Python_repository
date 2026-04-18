"""
parser.py - Extract structured fields from HR free-text replies

Two layers:
  1. Regex-based fast extraction for common patterns (no LLM cost)
  2. Claude-assisted extraction for complex / ambiguous answers
     (called only when regex yields nothing useful)

The main entry point is `enrich_result()`, which takes an HrChatResult
whose `turns` are already populated and fills in the structured fields.
"""
from __future__ import annotations
import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# Regex extractors
# ══════════════════════════════════════════════════════════════════════

def _extract_category(text: str) -> Optional[str]:
    """
    Amazon / e-commerce product category.
    Tries to find patterns like "品类是服装" / "做3C" / "主营家居".
    """
    # Explicit category mention
    # Exclude interrogative / filler words that aren't real categories
    _QUESTION_WORDS = {"什么", "哪些", "哪个", "哪种", "如何", "怎样", "怎么"}
    m = re.search(
        r'(?:品类|类目|产品|主营|做的是|运营的是)[是为：:]\s*'
        r'([^\s,，。！!？?]{2,10})',
        text,
    )
    if m:
        val = m.group(1).strip()
        if val not in _QUESTION_WORDS:
            return val

    # Named categories without preceding keyword
    cats = ["服装", "3C", "电子", "家居", "家具", "宠物", "户外", "运动", "美妆",
            "个护", "母婴", "玩具", "食品", "工具", "汽配", "珠宝", "书籍", "软件"]
    for cat in cats:
        if cat in text:
            return cat
    return None


def _extract_avg_order_value(text: str) -> Optional[str]:
    """
    Customer average order value.
    e.g. "客单价30美元" / "$20-50" / "20到50刀"
    """
    m = re.search(
        r'(?:客单价|均价|平均.*?价)[约为是在：:\s]*'
        r'([\$￥]?\s*\d+(?:[,，]\d+)*(?:\.\d+)?\s*(?:[-~到至]\s*\d+(?:\.\d+)?)?\s*(?:美?元|美金|刀|usd|rmb|人民币)?)',
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()

    m = re.search(
        r'([\$￥]\s*\d+(?:\.\d+)?\s*[-~]\s*\d+(?:\.\d+)?)',
        text,
    )
    if m:
        return m.group(1).strip()
    return None


def _extract_team_size(text: str) -> Optional[int]:
    """
    Operational team headcount.
    e.g. "团队5人" / "3个运营" / "团队人数10人左右"
    """
    m = re.search(
        r'(?:团队|运营团队|人员|人数|共)[有是约共：:\s]*(\d+)\s*(?:人|名|个)',
        text,
    )
    if m:
        return int(m.group(1))

    m = re.search(
        r'(\d+)\s*(?:人|名)(?:团队|运营|左右|规模)',
        text,
    )
    if m:
        return int(m.group(1))
    return None


def _extract_marketplace(text: str) -> Optional[str]:
    """Amazon marketplace / region."""
    patterns = [
        (r'美国站|北美站|us站', "美国站"),
        (r'欧洲站|欧站|uk站|de站|fr站', "欧洲站"),
        (r'日本站|jp站',         "日本站"),
        (r'澳洲站|au站',         "澳洲站"),
        (r'中东站|ae站',         "中东站"),
        (r'全球|多站|多个站',    "全球"),
    ]
    for pat, label in patterns:
        if re.search(pat, text, re.IGNORECASE):
            return label
    return None


def _extract_brand_type(text: str) -> Optional[str]:
    """Brand model: OBM / white-label / distributor / OEM."""
    if re.search(r'自有品牌|自主品牌|obm', text, re.IGNORECASE):
        return "自有品牌"
    if re.search(r'白牌', text):
        return "白牌"
    if re.search(r'分销|代销|经销', text):
        return "分销"
    if re.search(r'\boem\b|贴牌', text, re.IGNORECASE):
        return "OEM"
    return None


def _extract_work_mode(text: str) -> Optional[str]:
    """Work arrangement: remote / hybrid / onsite."""
    if re.search(r'全远程|完全远程|remote', text, re.IGNORECASE):
        return "remote"
    if re.search(r'混合|弹性|居家.*办公|办公.*居家', text):
        return "hybrid"
    if re.search(r'坐班|全勤|驻场|现场办公|不支持远程', text):
        return "onsite"
    return None


def _extract_tools(text: str) -> list[str]:
    """Named tools / software mentioned by HR."""
    tool_patterns = [
        r'helium\s*10', r'jungle\s*scout', r'卖家精灵', r'sellersprite',
        r'keepa', r'sp\s*(?:广告|工具)',
        r'n8n', r'zapier', r'make\.com',
        r'power\s*bi', r'tableau', r'metabase',
        r'chatgpt|gpt[-\d]', r'claude', r'kimi',
        r'erp', r'sap',
    ]
    found = []
    for pat in tool_patterns:
        if re.search(pat, text, re.IGNORECASE):
            label = re.sub(r'[\\()\[\]^$+?|*.]', '', pat).strip()
            found.append(label)
    return list(dict.fromkeys(found))


# ══════════════════════════════════════════════════════════════════════
# Main enrichment function
# ══════════════════════════════════════════════════════════════════════

def enrich_result(result) -> None:
    """
    Fill HrChatResult structured fields from its `turns` in-place.
    Uses regex; skips fields already set by the agent layer.
    Avoids importing HrChatResult to prevent circular imports.
    """
    combined = " ".join(
        f"{t.question} {t.answer}" for t in result.turns
    )

    if result.category is None:
        result.category = _extract_category(combined)

    if result.avg_order_value is None:
        result.avg_order_value = _extract_avg_order_value(combined)

    if result.team_size is None:
        result.team_size = _extract_team_size(combined)

    if result.marketplace is None:
        result.marketplace = _extract_marketplace(combined)

    if result.brand_type is None:
        result.brand_type = _extract_brand_type(combined)

    if not result.tools_used:
        result.tools_used = _extract_tools(combined)

    if result.work_mode is None:
        result.work_mode = _extract_work_mode(combined)
