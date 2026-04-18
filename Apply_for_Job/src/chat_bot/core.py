"""
core.py - Platform-agnostic chat bot engine

ChatBotCore contains all logic that does NOT depend on any specific
recruitment platform: browser connection, Q&A loop, LLM integration,
reply polling, and structured data extraction.

Platform-specific DOM interactions are delegated entirely to a
PlatformAdapter instance (see base.py and adapters/).
"""
from __future__ import annotations

import csv
import json
import logging
import random
import time
from pathlib import Path
from typing import Optional

from .base import PlatformAdapter

logger = logging.getLogger(__name__)

# ── Timing defaults ───────────────────────────────────────────────────
_POLL_INTERVAL = 2       # seconds between DOM polls
_DEFAULT_TIMEOUT = 180   # seconds to wait for HR reply
_SEND_DELAY = (1.5, 3.0) # pause after sending a message
_CHAT_DELAY = (4.0, 8.0) # pause between conversations


# ══════════════════════════════════════════════════════════════════════
# LLM system prompts (platform-independent)
# ══════════════════════════════════════════════════════════════════════

_SYSTEM_CANDIDATE = """\
You are a professional job seeker chatting with an HR recruiter on a recruitment platform.
Respond briefly and professionally in Chinese (1-3 sentences).
Sound genuine, interested, and confident.
Do NOT ask your own questions; only answer what HR asked.

When HR asks about salary expectations, use your salary preferences from the profile below.
If the position violates any constraint listed under "以下条件不可接受", politely decline
that specific point in one sentence before continuing the conversation.

{candidate_profile}

Position context:
{job_context}
"""

_SYSTEM_EXTRACTOR = """\
You are a data extraction assistant. Given a chat transcript between a job seeker and HR,
extract the following into JSON. Use null when information is absent.

{
  "category":        "product category (string or null)",
  "avg_order_value": "customer average order value with unit (string or null)",
  "team_size":       integer headcount or null,
  "marketplace":     "e.g. 美国站 / 欧洲站 / 全球 (string or null)",
  "monthly_sales":   "monthly revenue target with unit (string or null)",
  "brand_type":      "自有品牌 / 白牌 / 分销 / OEM or null",
  "tools_used":      ["list", "of", "tools"],
  "work_mode":       "remote / hybrid / onsite or null",
  "extra":           {"any_other_key": "value"}
}

Return ONLY valid JSON.
"""


# ══════════════════════════════════════════════════════════════════════
# Core engine
# ══════════════════════════════════════════════════════════════════════

class ChatBotCore:
    """
    Platform-agnostic chat bot engine.

    Usage:
        adapter = core.connect(ZhipinAdapter)   # browser attach + navigate
        result  = core.run_session(adapter, 0)  # process sidebar item 0
        core.run_all(adapter, output_path)       # process all conversations
    """

    def __init__(
        self,
        provider,
        max_turns: int = 6,
        reply_timeout: int = _DEFAULT_TIMEOUT,
    ) -> None:
        self.provider      = provider
        self.max_turns     = max_turns
        self.reply_timeout = reply_timeout
        self._page         = None

        from .profile import load_profile, render_profile
        self._candidate_profile_text = render_profile(load_profile())
        if self._candidate_profile_text:
            logger.info("Candidate profile loaded (%d chars)", len(self._candidate_profile_text))

    # ── Browser connection ────────────────────────────────────────────

    def connect(self, adapter_cls: type[PlatformAdapter]) -> PlatformAdapter:
        """
        Attach to an existing Chrome session at localhost:9222,
        open the platform's chat URL, and return an initialised adapter.
        """
        try:
            from DrissionPage import ChromiumPage
        except ImportError as e:
            raise ImportError("pip install DrissionPage") from e

        if self._page is None:
            self._page = ChromiumPage(addr_or_opts='localhost:9222')

        tab = self._page.new_tab(adapter_cls.CHAT_URL)
        time.sleep(3)
        adapter = adapter_cls(tab)
        logger.info("Connected [%s] → %s", adapter.PLATFORM_NAME, adapter_cls.CHAT_URL)
        return adapter

    # ── Reply polling ─────────────────────────────────────────────────

    def wait_for_hr_reply(
        self,
        adapter: PlatformAdapter,
        prev_count: int,
    ) -> Optional[str]:
        """
        Poll until a new message from HR appears.
        Returns the reply text, or None on timeout.
        """
        deadline = time.time() + self.reply_timeout
        while time.time() < deadline:
            time.sleep(_POLL_INTERVAL)
            msgs = adapter.read_messages()
            if len(msgs) > prev_count:
                for msg in reversed(msgs[prev_count:]):
                    if msg.get('role') == 'hr':
                        logger.debug("HR replied: %s", msg['text'][:80])
                        return msg['text']
        logger.warning("Timed out waiting for HR reply after %ds", self.reply_timeout)
        return None

    # ── LLM helpers ───────────────────────────────────────────────────

    def _candidate_reply(
        self,
        hr_message: str,
        history: list[dict],
        job_context: str,
    ) -> str:
        """Generate a brief candidate response to an HR question."""
        messages = history + [{"role": "user", "content": hr_message}]
        return self.provider.chat(
            system=_SYSTEM_CANDIDATE.format(
                candidate_profile=self._candidate_profile_text or "(无个人资料)",
                job_context=job_context,
            ),
            messages=messages,
            max_tokens=256,
            temperature=1.3,   # General Conversation
        )

    def _extract_structured(self, turns: list[dict]) -> dict:
        """Parse the conversation into structured JSON via LLM."""
        transcript = "\n".join(
            f"求职者：{t['question']}\nHR：{t['answer']}"
            for t in turns
        )
        raw = self.provider.chat(
            system=_SYSTEM_EXTRACTOR,
            messages=[{"role": "user", "content": transcript}],
            max_tokens=1024,
            temperature=1.0,   # Data Analysis — structured JSON extraction
        )
        # Strip markdown fences (Python 3.8 compatible)
        for prefix in ("```json", "```"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break
        if raw.endswith("```"):
            raw = raw[:-3]
        raw = raw.strip()
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Non-JSON extraction result: %s", raw[:200])
            return {}

    # ── Single session ────────────────────────────────────────────────

    def run_session(
        self,
        adapter: PlatformAdapter,
        sidebar_index: int,
    ) -> Optional[object]:
        """
        Process one conversation identified by its sidebar position.
        Returns HrChatResult, or None if skipped / no turns recorded.
        """
        from .schemas import JobSnapshot, ChatTurn, HrChatResult
        from .questioner import get_strategy, generate_questions
        from . import parser

        # Open the conversation
        try:
            header = adapter.open_conversation(sidebar_index)
        except Exception as e:
            logger.error("Cannot open conversation %d on %s: %s",
                         sidebar_index, adapter.PLATFORM_NAME, e)
            return None

        job_title = header.get('job_title') or ""
        company   = header.get('company',  '') or ''
        hr_name   = header.get('hr_name',  '')

        # If title is missing or incorrectly parsed as HR name, use generic
        if not job_title or job_title == hr_name:
            job_title = "贵司发布的招聘岗位"
        logger.info("[%s][%d] %s @ %s (HR: %s)",
                    adapter.PLATFORM_NAME, sidebar_index, job_title, company, hr_name)

        job = JobSnapshot(
            job_title=job_title,
            company=company,
            source=adapter.PLATFORM_NAME,
        )
        # Use more natural phrasing if title is generic
        job_context = f"公司：{company}"
        if job_title != "贵司发布的招聘岗位":
            job_context = f"职位：{job_title}\n" + job_context

        # Read existing history
        existing = adapter.read_messages()

        # Build LLM history from existing messages (needed for question generation)
        # Defined early so generate_questions can use the full conversation context.
        llm_history_for_gen: list[dict] = []
        for m in existing:
            llm_history_for_gen.append({
                "role": "assistant" if m['role'] == 'me' else "user",
                "content": m['text'],
            })

        # Generate context-aware questions via LLM, skipping what is already
        # disclosed in the job description or answered in the conversation.
        strategy = get_strategy(job)
        goals    = strategy.data_goals(job)
        pending  = generate_questions(
            goals         = goals,
            job           = job,
            existing      = existing,
            provider      = self.provider,
            max_questions = self.max_turns,
        )

        if not pending:
            logger.info("No questions to ask for '%s', skipping", job_title)
            return None

        # Reuse history already built for question generation
        llm_history: list[dict] = llm_history_for_gen

        # If HR greeted us first, respond before asking questions
        if existing and existing[-1].get('role') == 'hr':
            greeting = existing[-1]['text']
            logger.info("Responding to HR greeting: %s", greeting[:60])
            reply = self._candidate_reply(greeting, llm_history, job_context)
            prev = adapter.message_count()
            if adapter.send_message(reply):
                llm_history.append({"role": "assistant", "content": reply})
            time.sleep(random.uniform(*_SEND_DELAY))

        # Main Q&A loop
        result    = HrChatResult(job=job)
        turns_raw: list[dict] = []

        for question in pending:
            prev = adapter.message_count()
            logger.info("  Q: %s", question[:70])

            if not adapter.send_message(question):
                logger.warning("Failed to send; stopping session")
                break

            llm_history.append({"role": "assistant", "content": question})

            hr_reply = self.wait_for_hr_reply(adapter, prev)
            if hr_reply is None:
                logger.warning("No reply within %ds; stopping", self.reply_timeout)
                break

            result.turns.append(ChatTurn(question=question, answer=hr_reply))
            turns_raw.append({"question": question, "answer": hr_reply})
            llm_history.append({"role": "user", "content": hr_reply})
            logger.debug("  A: %s", hr_reply[:100])

            # Respond if HR asked us something back
            if hr_reply.rstrip().endswith(('？', '?')):
                reply = self._candidate_reply(hr_reply, llm_history, job_context)
                prev = adapter.message_count()
                if adapter.send_message(reply):
                    llm_history.append({"role": "assistant", "content": reply})
                    follow_up = self.wait_for_hr_reply(adapter, prev)
                    if follow_up:
                        llm_history.append({"role": "user", "content": follow_up})

        if not result.turns:
            return None

        # Structured extraction
        try:
            extracted = self._extract_structured(turns_raw)
            _apply_extraction(result, extracted)
        except Exception:
            logger.exception("Structured extraction failed; using regex only")

        parser.enrich_result(result)
        logger.info(
            "Done '%s': %d turns | category=%s | marketplace=%s",
            job_title, len(result.turns), result.category, result.marketplace,
        )
        return result

    # ── Batch runner ──────────────────────────────────────────────────

    def run_all(
        self,
        adapter: PlatformAdapter,
        output_path: str,
        max_chats: int = 50,
        unread_only: bool = False,
    ) -> list:
        """
        Iterate through all sidebar conversations and run sessions.
        Results are written to CSV incrementally.
        """
        conversations = adapter.list_conversations()
        logger.info("[%s] %d conversations found", adapter.PLATFORM_NAME, len(conversations))

        if unread_only:
            conversations = [c for c in conversations if c.get('unread')]
            logger.info("[%s] %d with unread messages", adapter.PLATFORM_NAME, len(conversations))

        conversations = conversations[:max_chats]

        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)

        results    = []
        csv_file   = open(output, 'w', newline='', encoding='utf-8-sig')
        csv_writer = None

        try:
            for conv in conversations:
                idx  = conv['index']
                name = conv.get('name', '?')
                job  = conv.get('job', '')
                print(f"\n[{adapter.PLATFORM_NAME}][{idx}] {name} — {job}")

                result = self.run_session(adapter, idx)

                if result is None:
                    print("  Skipped")
                else:
                    row = _result_to_row(result)
                    if csv_writer is None:
                        csv_writer = csv.DictWriter(csv_file, fieldnames=list(row.keys()))
                        csv_writer.writeheader()
                    csv_writer.writerow(row)
                    csv_file.flush()
                    results.append(result)
                    print(
                        f"  {len(result.turns)} turns | "
                        f"category={result.category} | marketplace={result.marketplace}"
                    )

                if conv is not conversations[-1]:
                    time.sleep(random.uniform(*_CHAT_DELAY))

        finally:
            csv_file.close()

        print(f"\n[{adapter.PLATFORM_NAME}] {len(results)} sessions → {output}")
        return results


# ══════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════

def _apply_extraction(result, data: dict) -> None:
    if data.get("category")        and not result.category:
        result.category = data["category"]
    if data.get("avg_order_value") and not result.avg_order_value:
        result.avg_order_value = data["avg_order_value"]
    if data.get("team_size")       and result.team_size is None:
        try:
            result.team_size = int(data["team_size"])
        except (ValueError, TypeError):
            pass
    if data.get("marketplace")     and not result.marketplace:
        result.marketplace = data["marketplace"]
    if data.get("monthly_sales")   and not result.monthly_sales:
        result.monthly_sales = data["monthly_sales"]
    if data.get("brand_type")      and not result.brand_type:
        result.brand_type = data["brand_type"]
    if data.get("tools_used")      and not result.tools_used:
        result.tools_used = [t for t in data["tools_used"] if t]
    if data.get("work_mode")       and not result.work_mode:
        result.work_mode = data["work_mode"]
    if isinstance(data.get("extra"), dict):
        result.extra.update(data["extra"])


def _result_to_row(result) -> dict:
    row = {
        "platform":    result.job.source,
        "job_title":   result.job.job_title,
        "company":     result.job.company,
        "turns_count": len(result.turns),
        "transcript":  " | ".join(
            f"Q: {t.question[:50]} A: {t.answer[:80]}" for t in result.turns
        ),
    }
    row.update(result.to_dict())
    return row
