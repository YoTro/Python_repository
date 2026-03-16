from __future__ import annotations
"""
FeishuCallback — sends progress to Feishu chat and writes results to Bitable.

Resilience:
  - on_progress is fire-and-forget (asyncio.create_task) so it never blocks the Workflow.
  - A circuit breaker skips Feishu calls after consecutive failures, auto-resets after a cooldown.
  - Exception handling targets network/API errors, not SystemExit or KeyboardInterrupt.
"""

import json
import asyncio
import logging
from datetime import datetime

from src.jobs.callbacks.base import JobCallback
from src.core.telemetry.tracker import TelemetryTracker

logger = logging.getLogger(__name__)

# Errors that indicate a Feishu API / network issue, not a programming bug
_FEISHU_ERRORS = (OSError, TimeoutError, ConnectionError, json.JSONDecodeError, KeyError, ValueError)

_CIRCUIT_OPEN_THRESHOLD = 3     # consecutive failures before opening circuit
_CIRCUIT_COOLDOWN_STEPS = 5     # steps to skip before retrying


class FeishuCallback(JobCallback):
    """
    Callback that pushes progress to Feishu group chat
    and writes final results to a Feishu Bitable.
    """

    def __init__(
        self,
        chat_id: str,
        bot_name: str = "amazon_bot",
        user_token: str = None,
        webhook_url: str = None,
        total_steps: int = 1,
        output_mode: str = "bitable",
    ):
        self.chat_id = chat_id
        self.bot_name = bot_name
        self.user_token = user_token
        self.webhook_url = webhook_url
        self.output_mode = output_mode
        self._feishu = None
        self._tracker = TelemetryTracker(total_steps)
        self._progress_message_id = None

        # Circuit breaker state
        self._consecutive_failures = 0
        self._cooldown_remaining = 0

    @property
    def feishu(self):
        if self._feishu is None:
            from src.entry.feishu.client import FeishuClient
            self._feishu = FeishuClient(bot_name=self.bot_name)
        return self._feishu

    # ── Circuit breaker helpers ──────────────────────────────────────────

    def _record_success(self):
        self._consecutive_failures = 0
        self._cooldown_remaining = 0

    def _record_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= _CIRCUIT_OPEN_THRESHOLD:
            self._cooldown_remaining = _CIRCUIT_COOLDOWN_STEPS
            logger.warning(
                f"Feishu circuit breaker OPEN after {self._consecutive_failures} "
                f"consecutive failures, skipping next {_CIRCUIT_COOLDOWN_STEPS} progress calls"
            )

    def _is_circuit_open(self) -> bool:
        if self._cooldown_remaining > 0:
            self._cooldown_remaining -= 1
            if self._cooldown_remaining == 0:
                logger.info("Feishu circuit breaker HALF-OPEN, will retry on next call")
                self._consecutive_failures = 0
            return True
        return False

    # ── Progress (fire-and-forget, non-blocking) ─────────────────────────

    async def on_progress(
        self, step_index: int, total_steps: int, step_name: str, message: str = ""
    ) -> None:
        if self._tracker.total_steps != total_steps:
            self._tracker.total_steps = total_steps

        self._tracker.record_step()

        if self._is_circuit_open():
            return

        filled = step_index
        empty = total_steps - step_index
        bar = "█" * filled + "░" * empty
        text = f"[{step_index}/{total_steps}] {bar} {step_name}"
        if message:
            text += f" - {message}"

        eta = self._tracker.get_dynamic_eta()
        if eta is not None:
            text += f"\n⏳ 动态预计剩余: {eta}秒"

        # Fire-and-forget: do not block the workflow on Feishu I/O
        asyncio.create_task(self._send_progress(text))

    async def _send_progress(self, text: str) -> None:
        try:
            if not self._progress_message_id:
                response = await asyncio.to_thread(
                    self.feishu.send_card_message, "chat_id", self.chat_id, text
                )
                if response.get("success") and response.get("data"):
                    data = json.loads(response["data"]) if isinstance(response["data"], str) else response["data"]
                    self._progress_message_id = data.get("message_id")
                    self._record_success()
                else:
                    self._record_failure()
            else:
                result = await asyncio.to_thread(
                    self.feishu.update_card_message, self._progress_message_id, text
                )
                if result.get("success"):
                    self._record_success()
                else:
                    self._record_failure()
                    self._progress_message_id = None
        except _FEISHU_ERRORS as e:
            logger.warning(f"Feishu progress notification failed: {e}")
            self._record_failure()

    # ── Completion (blocking — results must be delivered) ────────────────

    async def on_complete(self, result) -> None:
        try:
            items = result.final_items if hasattr(result, "final_items") else []

            if self.output_mode == "card":
                if items and "response" in items[0]:
                    text = items[0]["response"]
                elif items and "message" in items[0]:
                    text = items[0]["message"]
                else:
                    text = "Agent task completed. No specific message returned."
                self.feishu.send_card_message("chat_id", self.chat_id, text)
                return

            workflow_name = result.name if hasattr(result, "name") else "workflow"
            date_str = datetime.now().strftime("%Y-%m-%d")
            bitable_name = f"Report - {workflow_name} - {date_str}"

            # Create Bitable
            create_res = self.feishu.create_bitable(
                bitable_name, user_access_token=self.user_token
            )

            if not create_res.get("success"):
                summary = f"Workflow completed. {len(items)} items found."
                self.feishu.send_text_message("chat_id", self.chat_id, summary)
                return

            create_data = json.loads(create_res["data"]) if isinstance(create_res["data"], str) else create_res["data"]
            app_token = create_data["app"]["app_token"]
            bitable_url = create_data["app"]["url"]

            # Get default table
            tables_res = self.feishu.list_bitable_tables(
                app_token, user_access_token=self.user_token
            )
            tables = json.loads(tables_res["items"]) if isinstance(tables_res["items"], str) else tables_res["items"]
            table_id = tables[0]["table_id"]

            # Clear default empty rows
            self.feishu.delete_all_bitable_records(
                app_token, table_id, user_access_token=self.user_token
            )

            # Create fields from first item
            if items:
                for header in items[0].keys():
                    self.feishu.create_bitable_field(
                        app_token, table_id, header,
                        user_access_token=self.user_token,
                    )

            # Batch add records
            records = []
            for item in items:
                fields = {
                    k: str(v) if v is not None else ""
                    for k, v in item.items()
                }
                records.append(fields)

            if records:
                self.feishu.batch_add_bitable_records(
                    app_token, table_id, records,
                    user_access_token=self.user_token,
                )

            # Send completion notification
            total_ms = result.total_duration_ms if hasattr(result, "total_duration_ms") else 0
            text = (
                f"Workflow completed!\n"
                f"Items: {len(items)}\n"
                f"Duration: {total_ms / 1000:.1f}s\n"
                f"Report: {bitable_url}"
            )
            self.feishu.send_text_message("chat_id", self.chat_id, text)

        except _FEISHU_ERRORS as e:
            logger.error(f"FeishuCallback on_complete failed: {e}")
            try:
                self.feishu.send_text_message(
                    "chat_id", self.chat_id,
                    f"Workflow completed but report creation failed: {e}",
                )
            except _FEISHU_ERRORS:
                pass

    async def on_error(self, error: Exception) -> None:
        text = f"Workflow failed: {error}"
        try:
            self.feishu.send_text_message("chat_id", self.chat_id, text)
        except _FEISHU_ERRORS as e:
            logger.error(f"Feishu error notification failed: {e}")
