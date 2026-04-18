"""
agent.py - Multi-turn HR conversation driver using Claude API

Flow:
  1. Build context from JobSnapshot
  2. Use questioner to generate questions for this job type
  3. Run each question through Claude, simulating an HR interview
  4. After all turns, ask Claude to summarise and extract structured fields
  5. Run parser.enrich_result() to fill in any regex-catchable fields

Environment:
  ANTHROPIC_API_KEY   required
  HRC_MODEL           optional, defaults to claude-haiku-4-5-20251001 (cheap + fast)
  HRC_MAX_TURNS       optional int, max questions to ask (default 6)

Usage:
    from src.hr_chat import run_session
    from src.hr_chat.schemas import JobSnapshot

    row = df.iloc[0]
    result = run_session(JobSnapshot.from_series(row))
    df.loc[row.name, list(result.to_dict())] = result.to_dict()
"""
from __future__ import annotations
import json
import logging
import os
from typing import Optional

from .schemas import ChatTurn, HrChatResult, JobSnapshot
from .questioner import get_strategy
from . import parser

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-haiku-4-5-20251001"
_EXTRACTION_MODEL = "claude-haiku-4-5-20251001"


def _client():
    """Lazy import so the module loads even without anthropic installed."""
    try:
        import anthropic
    except ImportError as e:
        raise ImportError(
            "anthropic package required: pip install anthropic"
        ) from e
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


# ══════════════════════════════════════════════════════════════════════
# System prompts
# ══════════════════════════════════════════════════════════════════════

_SYSTEM_HR = """\
You are an experienced HR recruiter at a Chinese company. The candidate has \
applied for the position described below. Respond naturally in Chinese as an \
HR representative would. Be helpful, concise, and realistic — give specific \
numbers or ranges when you know them, say "这个我需要确认一下" when you don't. \
Do NOT make up details that contradict the job description.

Job posting context:
{job_context}
"""

_SYSTEM_EXTRACTOR = """\
You are a data extraction assistant. Given a conversation between a job seeker \
and an HR representative, extract structured information into JSON.

Return ONLY valid JSON with these keys (use null when information is absent):
{
  "category":        "product category (string or null)",
  "avg_order_value": "customer average order value with unit (string or null)",
  "team_size":       "integer headcount or null",
  "marketplace":     "e.g. 美国站 / 欧洲站 / 全球 (string or null)",
  "monthly_sales":   "monthly revenue target with unit (string or null)",
  "brand_type":      "自有品牌 / 白牌 / 分销 / OEM or null",
  "tools_used":      ["list", "of", "tools"],
  "work_mode":       "remote / hybrid / onsite or null",
  "extra":           {"any_other_key": "value"}
}
"""


# ══════════════════════════════════════════════════════════════════════
# Core session logic
# ══════════════════════════════════════════════════════════════════════

def _build_job_context(job: JobSnapshot) -> str:
    parts = [
        f"职位：{job.job_title}",
        f"公司：{job.company}",
    ]
    if job.location:
        parts.append(f"地点：{job.location}")
    if job.salary_raw:
        parts.append(f"薪资：{job.salary_raw}")
    if job.description:
        # Truncate long JDs to keep token cost low
        desc = job.description[:800] + ("…" if len(job.description) > 800 else "")
        parts.append(f"职位描述：{desc}")
    return "\n".join(parts)


def _simulate_hr_answer(
    client,
    model: str,
    job_context: str,
    history: list[dict],
    question: str,
) -> str:
    """Send one candidate question to Claude acting as HR; return the answer."""
    messages = history + [{"role": "user", "content": question}]
    response = client.messages.create(
        model=model,
        max_tokens=512,
        system=_SYSTEM_HR.format(job_context=job_context),
        messages=messages,
    )
    return response.content[0].text.strip()


def _extract_structured(client, model: str, turns: list[ChatTurn]) -> dict:
    """Ask Claude to parse the full conversation into structured JSON."""
    conversation_text = "\n".join(
        f"求职者：{t.question}\nHR：{t.answer}" for t in turns
    )
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=_SYSTEM_EXTRACTOR,
        messages=[{"role": "user", "content": conversation_text}],
    )
    raw = response.content[0].text.strip()
    # Strip markdown code fences if present
    raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Structured extraction returned non-JSON: %s", raw[:200])
        return {}


def _apply_extraction(result: HrChatResult, data: dict) -> None:
    """Write Claude-extracted fields into result (only if not already set)."""
    if data.get("category") and not result.category:
        result.category = data["category"]
    if data.get("avg_order_value") and not result.avg_order_value:
        result.avg_order_value = data["avg_order_value"]
    if data.get("team_size") and result.team_size is None:
        try:
            result.team_size = int(data["team_size"])
        except (ValueError, TypeError):
            result.team_size = None
    if data.get("marketplace") and not result.marketplace:
        result.marketplace = data["marketplace"]
    if data.get("monthly_sales") and not result.monthly_sales:
        result.monthly_sales = data["monthly_sales"]
    if data.get("brand_type") and not result.brand_type:
        result.brand_type = data["brand_type"]
    if data.get("tools_used") and not result.tools_used:
        result.tools_used = [t for t in data["tools_used"] if t]
    if data.get("work_mode") and not result.work_mode:
        result.work_mode = data["work_mode"]
    if isinstance(data.get("extra"), dict):
        result.extra.update(data["extra"])


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════

def run_session(
    job: JobSnapshot,
    *,
    model: Optional[str] = None,
    max_turns: Optional[int] = None,
    ask_nice_to_ask: bool = True,
) -> HrChatResult:
    """
    Run a full HR chat session for the given job posting.

    Parameters
    ----------
    job            : JobSnapshot built from a normalizer row
    model          : Claude model ID (defaults to HRC_MODEL env var or haiku)
    max_turns      : max questions to ask (defaults to HRC_MAX_TURNS env var or 6)
    ask_nice_to_ask: whether to include lower-priority questions

    Returns
    -------
    HrChatResult with turns filled and structured fields populated
    """
    model = model or os.environ.get("HRC_MODEL", _DEFAULT_MODEL)
    max_turns = max_turns or int(os.environ.get("HRC_MAX_TURNS", 6))

    client = _client()
    strategy = get_strategy(job)

    questions = strategy.must_ask(job)
    if ask_nice_to_ask:
        questions += strategy.nice_to_ask(job)
    questions = questions[:max_turns]

    job_context = _build_job_context(job)
    result = HrChatResult(job=job)
    history: list[dict] = []

    logger.info(
        "Starting HR chat for '%s' @ '%s' (%d questions)",
        job.job_title, job.company, len(questions),
    )

    for q in questions:
        try:
            answer = _simulate_hr_answer(client, model, job_context, history, q)
        except Exception:
            logger.exception("HR answer failed for question: %s", q)
            break

        turn = ChatTurn(question=q, answer=answer)
        result.turns.append(turn)

        # Append to history for follow-up context
        history.append({"role": "user",      "content": q})
        history.append({"role": "assistant", "content": answer})

        logger.debug("Q: %s\nA: %s", q, answer[:120])

    # --- Structured extraction layer ---
    if result.turns:
        try:
            extracted = _extract_structured(client, _EXTRACTION_MODEL, result.turns)
            _apply_extraction(result, extracted)
            result.raw_summary = extracted.get("extra", {}).pop("summary", None)
        except Exception:
            logger.exception("Structured extraction failed; falling back to regex only")

    # --- Regex enrichment layer (fills gaps) ---
    parser.enrich_result(result)

    return result


def batch_run(
    jobs,
    *,
    model: Optional[str] = None,
    max_turns: Optional[int] = None,
    skip_complete: bool = True,
) -> list[HrChatResult]:
    """
    Run HR chat sessions for a list of JobSnapshot objects.

    Parameters
    ----------
    jobs          : iterable of JobSnapshot
    skip_complete : if True, skip jobs where key fields are already known
    """
    results = []
    for job in jobs:
        if skip_complete and _is_sufficiently_known(job):
            logger.info("Skipping '%s' — already has enough info", job.job_title)
            continue
        result = run_session(job, model=model, max_turns=max_turns)
        results.append(result)
    return results


def _is_sufficiently_known(job: JobSnapshot) -> bool:
    """
    Heuristic: skip HR chat if the JD already contains key details.
    Avoids burning API calls on well-documented postings.
    """
    if not job.description:
        return False
    desc = job.description.lower()
    has_category  = any(w in desc for w in ["品类", "类目", "服装", "3c", "家居"])
    has_aov       = bool(__import__("re").search(r'\$\d+|\d+美元|\d+刀', desc))
    has_market    = any(w in desc for w in ["美国站", "欧洲站", "日本站"])
    return sum([has_category, has_aov, has_market]) >= 2
