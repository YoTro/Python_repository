from __future__ import annotations

import logging
import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from src.core.errors import ErrorCode, classify_api_code
from src.core.utils.config_helper import ConfigHelper
from src.core.utils.context import ContextPropagator
from src.entry.slack.const import slack_error_msg

logger = logging.getLogger(__name__)

# Block Kit section.text hard cap is 3000 chars — leave a safety margin, mirroring
# FeishuClient's 28 KB card-body safety margin below Feishu's 30 KB card limit.
_SECTION_TEXT_LIMIT_CHARS = 2_900


class SlackClient:
    """
    Slack Web API client wrapper using slack_sdk.

    Mirrors FeishuClient's public surface (send_text_message, send_card_message,
    update_card_message, upload_file, send_local_file) so entry-point command
    classes and callbacks can be ported with minimal logic changes. Slack has no
    Bitable equivalent, so those methods are intentionally not carried over —
    SlackCallback delivers tabular results as a CSV attachment instead.
    """

    def __init__(self, bot_name: str = None):
        resolved_bot_name = bot_name
        if not resolved_bot_name:
            resolved_bot_name = ContextPropagator.get("slack_bot_name")
        if not resolved_bot_name:
            resolved_bot_name = ConfigHelper.get("integrations.slack.default_bot", "toryunbot")

        bot_config = ConfigHelper.get_slack_bot(resolved_bot_name)

        if not bot_config:
            logger.error(
                f"Slack bot '{resolved_bot_name}' not configured. "
                f"Set SLACK_{resolved_bot_name.upper()}_BOT_TOKEN in .env"
            )
            self.bot_token = ""
        else:
            self.bot_token = bot_config["bot_token"]

        if not self.bot_token:
            logger.warning(f"Slack Bot Token missing for bot '{resolved_bot_name}'.")

        self.client = WebClient(token=self.bot_token)

    def _resolve_receive_params(
        self, receive_id_type: str | None, receive_id: str | None
    ) -> str | None:
        """
        Resolves the target channel id, prioritizing an explicit argument then a
        context variable. receive_id_type is accepted (and ignored) purely for
        call-site parity with FeishuClient — Slack's chat.postMessage takes a
        single `channel` id for both DMs and channels.
        """
        if receive_id:
            return receive_id
        return ContextPropagator.get("slack_channel_id")

    def send_text_message(
        self,
        receive_id_type: str | None = None,
        receive_id: str | None = None,
        text: str = "",
    ):
        """Send a simple text message."""
        channel = self._resolve_receive_params(receive_id_type, receive_id)
        if not channel:
            logger.error("Slack send text failed: No channel provided or resolved from context.")
            return {"success": False, "error": "No channel provided."}

        try:
            response = self.client.chat_postMessage(channel=channel, text=text)
            return {"success": True, "data": {"message_id": response["ts"], "channel": channel}}
        except SlackApiError as e:
            code = e.response.get("error", "unknown")
            logger.error(f"Slack send text failed: {code}")
            return {"success": False, "error": slack_error_msg(code), "code": code}

    @staticmethod
    def _fit_text_to_section(text: str, limit_chars: int = _SECTION_TEXT_LIMIT_CHARS) -> str:
        """Trim text so it fits within a single Block Kit section.text (3000 char cap)."""
        if len(text) <= limit_chars:
            return text
        truncated = text[: limit_chars - 100]
        return truncated.rstrip() + "\n\n…（内容超出卡片限制，已截断。完整内容请下载附件）"

    # Block Kit messages support up to 50 blocks — one section block per chunk lets a
    # single card carry up to ~145,000 chars before a file attachment is needed instead.
    _MAX_BLOCKS = 50

    @staticmethod
    def _split_into_chunks(text: str, limit_chars: int) -> list[str]:
        """Split text into <= limit_chars pieces on line boundaries (never mid-line,
        except for a single line that alone exceeds limit_chars, which is hard-split)."""
        chunks: list[str] = []
        current: list[str] = []
        current_len = 0
        for line in text.split("\n"):
            if len(line) > limit_chars:
                if current:
                    chunks.append("\n".join(current))
                    current, current_len = [], 0
                chunks.extend(line[i : i + limit_chars] for i in range(0, len(line), limit_chars))
                continue
            add_len = len(line) + (1 if current else 0)
            if current and current_len + add_len > limit_chars:
                chunks.append("\n".join(current))
                current, current_len = [], 0
                add_len = len(line)
            current.append(line)
            current_len += add_len
        if current:
            chunks.append("\n".join(current))
        return chunks or [""]

    def send_card_message(
        self,
        receive_id_type: str | None = None,
        receive_id: str | None = None,
        text: str = "",
    ):
        """
        Send a mrkdwn Block Kit message. Long text is split across multiple section
        blocks (Block Kit caps a single section.text at 3000 chars, but a message can
        carry up to 50 blocks) rather than truncated. Only content large enough to
        exceed 50 blocks (~145K chars) falls back to a file attachment.
        """
        channel = self._resolve_receive_params(receive_id_type, receive_id)
        if not channel:
            logger.error("Slack send card failed: No channel provided or resolved from context.")
            return {"success": False, "error": "No channel provided."}

        chunks = self._split_into_chunks(text, _SECTION_TEXT_LIMIT_CHARS)

        if len(chunks) > self._MAX_BLOCKS:
            logger.warning(
                f"Slack card text {len(text)} chars needs {len(chunks)} blocks "
                f"(> {self._MAX_BLOCKS} cap) — sending as a file attachment instead."
            )
            return self._send_text_as_file(channel, text)

        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": c}} for c in chunks]
        # The top-level `text` is just the notification/accessibility fallback, not
        # the rendered content (blocks are) — keep it short regardless of card size.
        fallback_text = text if len(text) <= 300 else text[:300].rstrip() + "…"
        try:
            response = self.client.chat_postMessage(
                channel=channel, text=fallback_text, blocks=blocks
            )
            return {"success": True, "data": {"message_id": response["ts"], "channel": channel}}
        except SlackApiError as e:
            code = e.response.get("error", "unknown")
            logger.error(f"Slack send card failed: {code}")
            return {"success": False, "error": slack_error_msg(code), "code": code}

    def _send_text_as_file(self, channel: str, text: str):
        """Upload oversized text as a .txt/.md attachment with a short preview message."""
        import os
        import tempfile

        suffix = ".md" if text.strip().startswith("#") else ".txt"
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=suffix, delete=False
        ) as tmp:
            tmp.write(text)
            tmp_path = tmp.name
        try:
            upload_res = self.upload_file(channel, tmp_path, f"content{suffix}")
        finally:
            os.unlink(tmp_path)
        return upload_res

    def update_card_message(
        self,
        message_id: str,
        text: str,
        receive_id_type: str | None = None,
        receive_id: str | None = None,
    ):
        """Update an existing Block Kit message in place (chat.update)."""
        channel = self._resolve_receive_params(receive_id_type, receive_id)
        if not channel:
            return {"success": False, "error": "No channel provided."}

        fitted = self._fit_text_to_section(text)
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": fitted}}]
        try:
            self.client.chat_update(channel=channel, ts=message_id, text=fitted, blocks=blocks)
            return {"success": True}
        except SlackApiError as e:
            code = e.response.get("error", "unknown")
            logger.error(f"Slack update card failed: {code}")
            return {"success": False, "error": slack_error_msg(code), "code": code}

    def upload_file(
        self,
        channel: str,
        file_path: str,
        file_name: str = None,
        initial_comment: str = None,
    ):
        """
        Upload a local file and share it in `channel` in one call (files.upload_v2).
        Unlike Feishu, Slack's modern upload API doesn't split upload from send.
        """
        if not file_name:
            file_name = os.path.basename(file_path)

        file_path = os.path.normpath(file_path)
        if not os.path.exists(file_path):
            return {"success": False, "error": "File not found"}

        try:
            response = self.client.files_upload_v2(
                channel=channel,
                file=file_path,
                filename=file_name,
                initial_comment=initial_comment,
            )
            file_info = response.get("file") or {}
            return {
                "success": True,
                "file_id": file_info.get("id"),
                "permalink": file_info.get("permalink"),
            }
        except SlackApiError as e:
            code = e.response.get("error", "unknown")
            error_code = classify_api_code(code, "slack")
            logger.error(f"Slack upload file failed: {code}")
            return {
                "success": False,
                "error": slack_error_msg(code),
                "code": code,
                "error_code": error_code if error_code != ErrorCode.UNKNOWN else None,
            }
        except Exception as e:
            logger.error(f"Slack upload process failed: {e}")
            return {"success": False, "error": str(e)}

    def send_local_file(
        self,
        receive_id_type: str | None = None,
        receive_id: str | None = None,
        file_path: str = "",
        filename: str = None,
    ):
        """Upload a local file and share it — kept for call-site parity with FeishuClient."""
        channel = self._resolve_receive_params(receive_id_type, receive_id)
        if not channel:
            return {"success": False, "error": "No channel provided."}
        return self.upload_file(channel, file_path, filename)
