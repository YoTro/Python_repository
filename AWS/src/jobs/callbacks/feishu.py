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
from typing import Set

from src.jobs.callbacks.base import JobCallback, CallbackCapability
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

    @property
    def capabilities(self) -> Set[CallbackCapability]:
        return {
            CallbackCapability.MARKDOWN,
            CallbackCapability.IMAGE_DISPLAY,
            CallbackCapability.INTERACTIVE_BUTTONS
        }

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
            # Check if this is a structural interaction signal
            try:
                if text.strip().startswith("{") and text.strip().endswith("}"):
                    signal = json.loads(text)
                    if signal.get("_type") == "INTERACTION_REQUIRED":
                        logger.info(f"Detected interaction signal: {signal.get('interaction_type')}")
                        logger.info(f"Current capabilities: {self.capabilities}")
                        # If we have the necessary capabilities, send a card
                        if CallbackCapability.IMAGE_DISPLAY in self.capabilities and \
                           CallbackCapability.INTERACTIVE_BUTTONS in self.capabilities:
                            await self._send_interaction_card(signal)
                            self._record_success()
                            return
                        else:
                            # Fallback to text
                            logger.info("Capabilities not sufficient for card, falling back to text.")
                            text = signal.get("fallback_text", text)
            except Exception as e:
                logger.error(f"Error parsing interaction signal: {e}")
                pass # Treat as normal text if JSON fails

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
                    self.feishu.update_card_message,
                    self._progress_message_id,
                    text,
                    receive_id_type="chat_id",
                    receive_id=self.chat_id,
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

    async def _send_interaction_card(self, signal: dict) -> None:
        """Sends a generic rich interactive card driven by the signal's ui_config."""
        logger.info("Starting _send_interaction_card flow...")
        interaction_type = signal.get("interaction_type")
        data = signal.get("data", {})
        context = signal.get("context", {})
        ui_config = signal.get("ui_config", {})
        
        if interaction_type == "AUTH_QR_SCAN":
            qr_url = data.get("url")
            action_name = ui_config.get("action", "UNKNOWN_ACTION")
            logger.info(f"Interaction details: type={interaction_type}, action={action_name}, url={qr_url}")
            
            # Feishu requires an image_key for cards, not a direct URL.
            image_key = None
            if qr_url:
                try:
                    import aiohttp
                    import tempfile
                    import os
                    logger.info(f"Attempting to download QR from: {qr_url}")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(qr_url) as resp:
                            if resp.status == 200:
                                image_data = await resp.read()
                                logger.info(f"Download successful. Image size: {len(image_data)} bytes")
                                
                                with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
                                    temp_file.write(image_data)
                                    temp_path = temp_file.name
                                
                                logger.info(f"Temporary file created at: {temp_path}. Uploading to Feishu...")
                                
                                # Use thread pool for blocking SDK call
                                if hasattr(self.feishu, "upload_image"):
                                    logger.info("Calling feishu.upload_image()...")
                                    upload_res = await asyncio.to_thread(self.feishu.upload_image, temp_path)
                                else:
                                    logger.info("feishu.upload_image not found, falling back to upload_file...")
                                    upload_res = await asyncio.to_thread(
                                        self.feishu.upload_file, 
                                        file_path=temp_path, 
                                        file_type="image", 
                                        file_name="qr_code.jpg"
                                    )
                                
                                # CRITICAL: Log the FULL response from Feishu
                                logger.info(f"Feishu UPLOAD RAW RESPONSE: {upload_res}")
                                
                                if upload_res.get("success") or upload_res.get("code") == 0:
                                    # Try all possible fields for image key
                                    data_obj = upload_res.get("data", upload_res)
                                    if isinstance(data_obj, str):
                                        try: data_obj = json.loads(data_obj)
                                        except: pass
                                        
                                    image_key = (
                                        data_obj.get("image_key") or 
                                        data_obj.get("file_key") or 
                                        (data_obj.get("app", {}) if isinstance(data_obj.get("app"), dict) else {}).get("image_key")
                                    )
                                    logger.info(f"Extracted image_key: '{image_key}'")
                                else:
                                    logger.error(f"Feishu upload failed. Code: {upload_res.get('code')}, Msg: {upload_res.get('msg')}")
                                
                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                            else:
                                logger.error(f"Image download failed with status: {resp.status}")
                except Exception as e:
                    logger.error(f"EXCEPTION during QR image processing: {str(e)}", exc_info=True)

            # Build Generic Feishu Interactive Card JSON
            card = {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "blue",
                    "title": {"tag": "plain_text", "content": ui_config.get("title", "🔐 需要认证")}
                },
                "elements": [
                    {
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": ui_config.get("description", "请扫描下方二维码完成登录。")}
                    }
                ]
            }
            
            if image_key:
                # IMPORTANT: Ensure the key is a string and not empty
                image_key_str = str(image_key).strip()
                if image_key_str and image_key_str != "None":
                    logger.info(f"Adding IMG element to card with key: {image_key_str}")
                    card["elements"].append({
                        "tag": "img",
                        "img_key": image_key_str,
                        "alt": {"tag": "plain_text", "content": "登录二维码"},
                        "mode": "fit_horizontal"
                    })
                else:
                    logger.warning("Extracted image_key is empty or invalid string, using fallback.")
                    card["elements"].append({
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"⚠️ 图片上传成功但未拿到有效 Key，请[点击此处查看二维码]({qr_url})"}
                    })
            else:
                logger.warning("No image_key available, using fallback URL element.")
                card["elements"].append({
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": f"无法显示二维码？[👉 点击此处查看微信登录二维码]({qr_url})"}
                })
                
            card["elements"].extend([
                {
                    "tag": "note",
                    "elements": [{"tag": "plain_text", "content": f"有效期 {data.get('expires_in', 120)} 秒，扫码后请点击下方按钮确认。"}]
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": ui_config.get("button_text", "我已确认扫码")},
                            "type": "primary",
                            "value": {
                                "action": action_name,
                                "tenant_id": context.get("tenant_id"),
                                "job_id": context.get("job_id")
                            }
                        }
                    ]
                }
            ])
            
            logger.info(f"FINAL CARD JSON TO SEND: {json.dumps(card, ensure_ascii=False)}")
            send_res = await asyncio.to_thread(self.feishu.send_raw_card, "chat_id", self.chat_id, card)
            logger.info(f"Feishu send_raw_card response: {send_res}")

    async def on_complete(self, result) -> None:
        try:
            import os
            items = result.final_items if hasattr(result, "final_items") else []

            # --- Artifact Delivery (Attachment) First Priority ---
            file_path = items[0].get("report_file_path") if items else None
            if file_path and os.path.exists(file_path):
                logger.info(f"Detected report artifact for delivery: {file_path}")
                try:
                    # Fix: Use 'stream' for .md files
                    upload_res = self.feishu.upload_file(
                        file_path, 
                        file_type="stream", 
                        file_name=os.path.basename(file_path)
                    )
                    
                    if upload_res.get("success"):
                        file_key = upload_res.get("file_key")
                        self.feishu.send_file_message(
                            receive_id_type="chat_id",
                            receive_id=self.chat_id,
                            file_key=file_key
                        )
                        logger.info("Report attachment sent successfully.")
                except Exception as e:
                    logger.error(f"Failed to send artifact attachment: {e}")

            if self.output_mode == "card":
                # Handle agent exploration or other text-based results
                text = "Workflow completed, but no textual response was provided."
                if items and "response" in items[0]:
                    text = items[0]["response"]
                elif items:
                    # Fallback for generic workflows using card output
                    text = f"Workflow completed with {len(items)} items. First item: {items[0]}"
                
                self.feishu.send_card_message(
                    receive_id_type="chat_id", 
                    receive_id=self.chat_id, 
                    text=text
                )
                logger.info("Final result sent via Feishu card successfully.")
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

            # Fix: Handle both str and dict for data (lark.JSON.marshal returns a JSON string)
            create_data = json.loads(create_res["data"]) if isinstance(create_res["data"], (str, bytes)) else create_res["data"]
            app_token = create_data["app"]["app_token"]
            bitable_url = create_data["app"]["url"]

            # Get default table
            tables_res = self.feishu.list_bitable_tables(
                app_token, user_access_token=self.user_token
            )
            # Fix: Handle items marshaling
            tables_data = json.loads(tables_res["items"]) if isinstance(tables_res["items"], (str, bytes)) else tables_res["items"]
            table_id = tables_data[0]["table_id"]

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

            # --- Artifact Delivery (Attachment) ---
            file_path = items[0].get("report_file_path") if items else None
            if file_path and os.path.exists(file_path):
                logger.info(f"Detected report artifact for delivery: {file_path}")
                try:
                    # 1. Upload to Feishu
                    # Using the client directly for simplicity in the callback
                    upload_res = self.feishu.upload_file(
                        file_path, 
                        file_type="all", 
                        file_name=os.path.basename(file_path)
                    )
                    
                    if upload_res.get("success"):
                        file_key = upload_res.get("file_key")
                        # 2. Send as attachment
                        self.feishu.send_file_message(
                            receive_id_type="chat_id",
                            receive_id=self.chat_id,
                            file_key=file_key
                        )
                        logger.info("Report attachment sent successfully.")
                except Exception as e:
                    logger.error(f"Failed to send artifact attachment: {e}")

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
