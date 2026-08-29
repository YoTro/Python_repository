from __future__ import annotations

"""
SlackCallback — sends progress to a Slack channel/DM and delivers results either as
a Block Kit card (LLM text output) or a CSV file attachment (tabular workflow items).

Slack has no Bitable equivalent, so the "bitable" output_mode from FeishuCallback is
replaced here with: upload a CSV of `final_items` + post a short summary message.

Resilience: same design as FeishuCallback —
  - on_progress is fire-and-forget (asyncio.create_task) so it never blocks the Workflow.
  - A circuit breaker skips Slack calls after consecutive failures, auto-resets after a cooldown.
  - Exception handling targets network/API errors, not SystemExit or KeyboardInterrupt.
"""

import asyncio
import json
import logging
import time
from datetime import datetime

from slack_sdk.errors import SlackApiError

from src.core.telemetry.tracker import TelemetryTracker
from src.jobs.callbacks.base import CallbackCapability, JobCallback

logger = logging.getLogger(__name__)

# Errors that indicate a Slack API / network issue, not a programming bug
_SLACK_ERRORS = (
    OSError,
    TimeoutError,
    ConnectionError,
    SlackApiError,
    json.JSONDecodeError,
    KeyError,
    ValueError,
)

_CIRCUIT_OPEN_THRESHOLD = 3  # consecutive failures before opening circuit
_CIRCUIT_COOLDOWN_STEPS = 5  # steps to skip before retrying

_HEARTBEAT_INTERVAL = 10  # seconds between heartbeat card updates
_SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

# Message text sent as a file attachment above this size, mirroring FeishuCallback's
# 28 KB card-body threshold (Slack's Block Kit section cap is much tighter, 3000 chars).
_CARD_TEXT_LIMIT_CHARS = 2_900


class SlackCallback(JobCallback):
    """
    Callback that pushes progress to a Slack channel/DM and delivers the final
    result as a Block Kit card ("card" mode) or a CSV attachment ("bitable" mode,
    named to match FeishuCallback's config for drop-in CallbackFactory parity).
    """

    @property
    def capabilities(self) -> set[CallbackCapability]:
        return {
            CallbackCapability.MARKDOWN,
            CallbackCapability.IMAGE_DISPLAY,
            CallbackCapability.INTERACTIVE_BUTTONS,
        }

    def __init__(
        self,
        chat_id: str,
        bot_name: str = "toryunbot",
        total_steps: int = 1,
        output_mode: str = "bitable",
        **_ignored_options,
    ):
        self.chat_id = chat_id
        self.bot_name = bot_name
        self.output_mode = output_mode
        self._slack = None
        self._tracker = TelemetryTracker(total_steps)
        self._progress_message_id = None

        # Circuit breaker state
        self._consecutive_failures = 0
        self._cooldown_remaining = 0

        # Heartbeat state
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_base_text: str = ""
        self._heartbeat_start: float = 0.0

    @property
    def slack(self):
        if self._slack is None:
            from src.entry.slack.client import SlackClient

            self._slack = SlackClient(bot_name=self.bot_name)
        return self._slack

    # ── Circuit breaker helpers ──────────────────────────────────────────

    def _record_success(self):
        self._consecutive_failures = 0
        self._cooldown_remaining = 0

    def _record_failure(self):
        self._consecutive_failures += 1
        if self._consecutive_failures >= _CIRCUIT_OPEN_THRESHOLD:
            self._cooldown_remaining = _CIRCUIT_COOLDOWN_STEPS
            logger.warning(
                f"Slack circuit breaker OPEN after {self._consecutive_failures} "
                f"consecutive failures, skipping next {_CIRCUIT_COOLDOWN_STEPS} progress calls"
            )

    def _is_circuit_open(self) -> bool:
        if self._cooldown_remaining > 0:
            self._cooldown_remaining -= 1
            if self._cooldown_remaining == 0:
                logger.info("Slack circuit breaker HALF-OPEN, will retry on next call")
                self._consecutive_failures = 0
            return True
        return False

    # ── Heartbeat ────────────────────────────────────────────────────────

    def _cancel_heartbeat(self) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        """Periodically update the progress card while a step is running."""
        frame_idx = 0
        try:
            while True:
                await asyncio.sleep(_HEARTBEAT_INTERVAL)
                if self._is_circuit_open():
                    continue
                elapsed = int(time.monotonic() - self._heartbeat_start)
                spinner = _SPINNER_FRAMES[frame_idx % len(_SPINNER_FRAMES)]
                frame_idx += 1
                heartbeat_text = (
                    f"{self._heartbeat_base_text}\n{spinner} 正在执行中… 已等待 {elapsed}s"
                )
                asyncio.create_task(self._send_progress(heartbeat_text))
        except asyncio.CancelledError:
            pass

    # ── Progress (fire-and-forget, non-blocking) ─────────────────────────

    async def on_progress(
        self,
        step_index: int,
        total_steps: int,
        step_name: str,
        message: str = "",
        remaining_step_names=None,
        workflow_name: str = "",
    ) -> None:
        if self._tracker.total_steps != total_steps:
            self._tracker.total_steps = total_steps
        if workflow_name and not self._tracker.workflow_name:
            self._tracker.workflow_name = workflow_name

        self._tracker.record_step(step_name)

        if self._is_circuit_open():
            return

        filled = step_index
        empty = total_steps - step_index
        bar = "█" * filled + "░" * empty
        text = f"[{step_index}/{total_steps}] {bar} {step_name}"
        if message:
            text += f" - {message}"

        eta = self._tracker.get_dynamic_eta(remaining_step_names=remaining_step_names)
        if eta is not None:
            text += f"\n⏳ {eta}"

        # Cancel previous heartbeat and start a fresh one for this step
        self._cancel_heartbeat()
        self._heartbeat_base_text = text
        self._heartbeat_start = time.monotonic()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Fire-and-forget: do not block the workflow on Slack I/O
        asyncio.create_task(self._send_progress(text))

    async def _send_progress(self, text: str) -> None:
        try:
            # Check if this is a structural interaction signal
            try:
                if text.strip().startswith("{") and text.strip().endswith("}"):
                    signal = json.loads(text)
                    if signal.get("_type") == "INTERACTION_REQUIRED":
                        logger.info(
                            f"Detected interaction signal: {signal.get('interaction_type')}"
                        )
                        if (
                            CallbackCapability.IMAGE_DISPLAY in self.capabilities
                            and CallbackCapability.INTERACTIVE_BUTTONS in self.capabilities
                        ):
                            await self._send_interaction_card(signal)
                            self._record_success()
                            return
                        else:
                            logger.info(
                                "Capabilities not sufficient for card, falling back to text."
                            )
                            text = signal.get("fallback_text", text)
            except Exception as e:
                logger.error(f"Error parsing interaction signal: {e}")
                pass  # Treat as normal text if JSON fails

            if not self._progress_message_id:
                response = await asyncio.to_thread(
                    self.slack.send_card_message, "channel", self.chat_id, text
                )
                if response.get("success") and response.get("data"):
                    self._progress_message_id = response["data"].get("message_id")
                    self._record_success()
                else:
                    self._record_failure()
            else:
                result = await asyncio.to_thread(
                    self.slack.update_card_message,
                    self._progress_message_id,
                    text,
                    receive_id_type="channel",
                    receive_id=self.chat_id,
                )
                if result.get("success"):
                    self._record_success()
                else:
                    self._record_failure()
                    self._progress_message_id = None
        except _SLACK_ERRORS as e:
            logger.warning(f"Slack progress notification failed: {e}")
            self._record_failure()

    # ── Completion (blocking — results must be delivered) ────────────────

    async def _send_interaction_card(self, signal: dict) -> None:
        """Sends a generic rich interactive Block Kit message driven by the signal's ui_config."""
        logger.info("Starting _send_interaction_card flow...")
        interaction_type = signal.get("interaction_type")
        data = signal.get("data", {})
        context = signal.get("context", {})
        ui_config = signal.get("ui_config", {})

        if interaction_type == "AUTH_QR_SCAN":
            qr_url = data.get("url")
            action_name = ui_config.get("action", "UNKNOWN_ACTION")
            logger.info(
                f"Interaction details: type={interaction_type}, action={action_name}, url={qr_url}"
            )

            blocks = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": ui_config.get("title", "🔐 需要认证")},
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ui_config.get("description", "请扫描下方二维码完成登录。"),
                    },
                },
            ]

            image_sent = False
            if qr_url:
                try:
                    import os
                    import tempfile

                    import aiohttp

                    logger.info(f"Attempting to download QR from: {qr_url}")
                    async with aiohttp.ClientSession() as session:
                        async with session.get(qr_url) as resp:
                            if resp.status == 200:
                                image_data = await resp.read()
                                with tempfile.NamedTemporaryFile(
                                    delete=False, suffix=".jpg"
                                ) as temp_file:
                                    temp_file.write(image_data)
                                    temp_path = temp_file.name

                                # Slack Block Kit can't reference an uploaded file by key —
                                # upload it directly to the channel instead of embedding it.
                                upload_res = await asyncio.to_thread(
                                    self.slack.upload_file,
                                    self.chat_id,
                                    temp_path,
                                    "qr_code.jpg",
                                    "登录二维码",
                                )
                                logger.info(f"Slack QR upload response: {upload_res}")
                                image_sent = bool(upload_res.get("success"))

                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                            else:
                                logger.error(f"Image download failed with status: {resp.status}")
                except Exception as e:
                    logger.error(f"EXCEPTION during QR image processing: {str(e)}", exc_info=True)

            if not image_sent:
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"无法显示二维码？<{qr_url}|👉 点击此处查看登录二维码>",
                        },
                    }
                )

            blocks.extend(
                [
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": f"有效期 {data.get('expires_in', 120)} 秒，扫码后请点击下方按钮确认。",
                            }
                        ],
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": ui_config.get("button_text", "我已确认扫码"),
                                },
                                "style": "primary",
                                "action_id": action_name,
                                "value": json.dumps(
                                    {
                                        "action": action_name,
                                        "tenant_id": context.get("tenant_id"),
                                        "job_id": context.get("job_id"),
                                        "chat_id": self.chat_id,
                                        "bot_name": self.bot_name,
                                        "ticket": context.get("ticket"),
                                    }
                                ),
                            }
                        ],
                    },
                ]
            )

            logger.info(f"FINAL BLOCKS TO SEND: {json.dumps(blocks, ensure_ascii=False)}")
            try:
                await asyncio.to_thread(
                    self.slack.client.chat_postMessage,
                    channel=self.chat_id,
                    text=ui_config.get("title", "🔐 需要认证"),
                    blocks=blocks,
                )
            except SlackApiError as e:
                logger.error(f"Slack send interaction card failed: {e.response.get('error')}")

    async def on_complete(self, result) -> None:
        self._cancel_heartbeat()
        self._tracker.finalize()
        try:
            import os

            items = result.final_items if hasattr(result, "final_items") else []

            # --- Artifact Delivery (Attachment) First Priority ---
            # A workflow may emit multiple report items (e.g. one per category
            # segment), so deliver every distinct report_file_path, not just the
            # first item's.
            artifact_sent = False
            seen_paths: set[str] = set()
            for item in items:
                file_path = item.get("report_file_path")
                if not file_path or file_path in seen_paths:
                    continue
                seen_paths.add(file_path)
                if not os.path.exists(file_path):
                    continue
                logger.info(f"Detected report artifact for delivery: {file_path}")
                try:
                    upload_res = await asyncio.to_thread(
                        self.slack.upload_file, self.chat_id, file_path, os.path.basename(file_path)
                    )
                    if upload_res.get("success"):
                        artifact_sent = True
                        logger.info(f"Report attachment sent successfully: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to send artifact attachment {file_path}: {e}")

            if self.output_mode == "card":
                text = "Workflow completed, but no textual response was provided."
                if items:
                    item = items[0]
                    for key in ("response", "ad_diagnosis_llm", "result", "output"):
                        if key in item and item[key]:
                            text = (
                                item[key]
                                if isinstance(item[key], str)
                                else json.dumps(item[key], ensure_ascii=False, default=str)
                            )
                            logger.info(
                                f"on_complete: using item['{key}'] ({len(text)} chars) for card text"
                            )
                            break
                    else:
                        if artifact_sent:
                            text = "Workflow completed. The full report has been sent as an attachment."
                        else:
                            text = json.dumps(item, ensure_ascii=False, indent=2, default=str)
                            logger.info(
                                f"on_complete: no text key found, using full item JSON ({len(text)} chars)"
                            )

                from src.intelligence.parsers.markdown_cleaner import OutputParser

                text = OutputParser.clean_markdown(text)

                if len(text) > _CARD_TEXT_LIMIT_CHARS:
                    try:
                        import os as _os
                        import tempfile

                        suffix = ".md" if text.strip().startswith("#") else ".txt"
                        with tempfile.NamedTemporaryFile(
                            mode="w", encoding="utf-8", suffix=suffix, delete=False
                        ) as tmp:
                            tmp.write(text)
                            tmp_path = tmp.name
                        upload_res = await asyncio.to_thread(
                            self.slack.upload_file, self.chat_id, tmp_path, f"report{suffix}"
                        )
                        _os.unlink(tmp_path)
                        if upload_res.get("success"):
                            preview = (
                                text[:500].rstrip()
                                + "\n\n…（The content exceeds the message limit and has been sent as an attachment. Please download the attachment to view the full report.）"
                            )
                            await asyncio.to_thread(
                                self.slack.send_card_message, "channel", self.chat_id, preview
                            )
                            logger.info(
                                f"Large result ({len(text)} chars) sent as file attachment."
                            )
                            return
                    except Exception as e:
                        logger.error(f"Failed to send oversized result as attachment: {e}")
                        # Fall through to truncated card

                self.slack.send_card_message(
                    receive_id_type="channel",
                    receive_id=self.chat_id,
                    text=text,
                )
                logger.info("Final result sent via Slack card successfully.")
                return

            # "bitable" mode — Slack has no spreadsheet-app equivalent, so tabular
            # results are delivered as a CSV attachment + a short summary message.
            workflow_name = result.name if hasattr(result, "name") else "workflow"
            date_str = datetime.now().strftime("%Y-%m-%d")
            total_ms = result.total_duration_ms if hasattr(result, "total_duration_ms") else 0

            if not items:
                self.slack.send_text_message(
                    "channel", self.chat_id, "Workflow completed. 0 items found."
                )
                return

            try:
                import tempfile

                from src.core.utils.csv_helper import CSVHelper

                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".csv", delete=False, encoding="utf-8"
                ) as tmp:
                    tmp_path = tmp.name
                CSVHelper.save_to_csv(items, tmp_path)

                csv_name = f"{workflow_name}_{date_str}.csv"
                upload_res = await asyncio.to_thread(
                    self.slack.upload_file, self.chat_id, tmp_path, csv_name
                )
                os.unlink(tmp_path)

                if not upload_res.get("success"):
                    summary = f"Workflow completed. {len(items)} items found."
                    self.slack.send_text_message("channel", self.chat_id, summary)
                    return

                text = (
                    f"Workflow completed!\n"
                    f"Items: {len(items)}\n"
                    f"Duration: {total_ms / 1000:.1f}s\n"
                    f"Report: {csv_name} (attached above)"
                )
                self.slack.send_text_message("channel", self.chat_id, text)
            except Exception as e:
                logger.error(f"Failed to build/upload CSV result: {e}")
                summary = f"Workflow completed. {len(items)} items found (CSV export failed: {e})."
                self.slack.send_text_message("channel", self.chat_id, summary)

        except _SLACK_ERRORS as e:
            logger.error(f"SlackCallback on_complete failed: {e}")
            try:
                self.slack.send_text_message(
                    "channel",
                    self.chat_id,
                    f"Workflow completed but report delivery failed: {e}",
                )
            except _SLACK_ERRORS:
                pass

    async def on_error(self, error: Exception, job_id: str = None) -> None:
        self._cancel_heartbeat()
        lines = [f"❌ Workflow failed: {error}"]
        if job_id:
            lines.append(f"Job ID: `{job_id}`")
            lines.append("A checkpoint was saved. To resume, send:")
            lines.append(f"  `恢复任务 {job_id}`")
        await self.notify("\n".join(lines))

    async def notify(self, message: str) -> None:
        try:
            await asyncio.to_thread(self.slack.send_text_message, "channel", self.chat_id, message)
        except _SLACK_ERRORS as e:
            logger.error(f"Slack notify failed: {e}")
