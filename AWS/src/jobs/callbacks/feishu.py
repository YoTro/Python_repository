from __future__ import annotations
"""
FeishuCallback — sends progress to Feishu chat and writes results to Bitable.
"""

import json
import logging
from datetime import datetime

from src.jobs.callbacks.base import JobCallback
from src.core.telemetry.tracker import TelemetryTracker

logger = logging.getLogger(__name__)


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
        total_steps: int = 1, # Passed from JobManager if available, defaults to 1
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

    @property
    def feishu(self):
        if self._feishu is None:
            from src.entry.feishu.client import FeishuClient
            self._feishu = FeishuClient(bot_name=self.bot_name)
        return self._feishu

    async def on_progress(
        self, step_index: int, total_steps: int, step_name: str, message: str = ""
    ) -> None:
        # Update tracker's total_steps if the engine passes a more accurate count later
        if self._tracker.total_steps != total_steps:
            self._tracker.total_steps = total_steps
            
        self._tracker.record_step()
        
        filled = step_index
        empty = total_steps - step_index
        bar = "█" * filled + "░" * empty
        text = f"[{step_index}/{total_steps}] {bar} {step_name}"
        if message:
            text += f" - {message}"
            
        eta = self._tracker.get_dynamic_eta()
        if eta is not None:
            text += f"\n⏳ 动态预计剩余: {eta}秒"

        try:
            if not self._progress_message_id:
                # First progress update: send a new card message
                response = self.feishu.send_card_message("chat_id", self.chat_id, text)
                if response.get("success"):
                    data = json.loads(response["data"])
                    self._progress_message_id = data.get("message_id")
            else:
                # Subsequent updates: update the existing card message in-place
                self.feishu.update_card_message(self._progress_message_id, text)
        except Exception as e:
            logger.warning(f"Feishu progress notification failed: {e}")

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
                self.feishu.send_card_message("chat_id", self.chat_id, text) # Send as card, not text
                return

            workflow_name = result.name if hasattr(result, "name") else "workflow"
            date_str = datetime.now().strftime("%Y-%m-%d")
            bitable_name = f"Report - {workflow_name} - {date_str}"

            # Create Bitable
            create_res = self.feishu.create_bitable(
                bitable_name, user_access_token=self.user_token
            )

            if not create_res.get("success"):
                # Fallback: send results as text
                summary = f"Workflow completed. {len(items)} items found."
                self.feishu.send_text_message("chat_id", self.chat_id, summary)
                return

            create_data = json.loads(create_res["data"])
            app_token = create_data["app"]["app_token"]
            bitable_url = create_data["app"]["url"]

            # Get default table
            tables_res = self.feishu.list_bitable_tables(
                app_token, user_access_token=self.user_token
            )
            tables = json.loads(tables_res["items"])
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

        except Exception as e:
            logger.error(f"FeishuCallback on_complete failed: {e}")
            try:
                self.feishu.send_text_message(
                    "chat_id", self.chat_id,
                    f"Workflow completed but report creation failed: {e}",
                )
            except Exception:
                pass

    async def on_error(self, error: Exception) -> None:
        text = f"Workflow failed: {error}"
        try:
            self.feishu.send_text_message("chat_id", self.chat_id, text)
        except Exception as e:
            logger.error(f"Feishu error notification failed: {e}")
