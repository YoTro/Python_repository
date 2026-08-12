from __future__ import annotations

import asyncio
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.intelligence.dto import LLMResponse
from src.jobs.callbacks.feishu import FeishuCallback
from src.workflows.engine import WorkflowResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_feishu_client():
    """Mock FeishuCallback.feishu property with all required methods."""
    with patch("src.jobs.callbacks.feishu.FeishuCallback.feishu") as mock_property:
        client = MagicMock()
        client.create_bitable.return_value = {
            "success": True,
            "data": '{"app": {"app_token": "mock_token", "url": "http://mock"}}',
        }
        client.list_bitable_tables.return_value = {"items": '[{"table_id": "tbl123"}]'}
        client.delete_all_bitable_records.return_value = True
        client.create_bitable_field.return_value = True
        client.batch_add_bitable_records.return_value = True
        client.send_text_message.return_value = True
        mock_property.__get__ = MagicMock(return_value=client)
        yield client


# ---------------------------------------------------------------------------
# Callback — progress & complete & error
# ---------------------------------------------------------------------------


class TestFeishuCallbackLifecycle:
    @pytest.mark.asyncio
    async def test_progress(self, mock_feishu_client):
        callback = FeishuCallback(chat_id="chat_123", total_steps=2)

        await callback.on_progress(step_index=1, total_steps=2, step_name="Extraction")

        for _ in range(20):
            if mock_feishu_client.send_card_message.called:
                break
            await asyncio.sleep(0.1)

        mock_feishu_client.send_card_message.assert_called()
        call_args = mock_feishu_client.send_card_message.call_args[0]
        assert "chat_123" in call_args
        assert "[1/2]" in call_args[2]
        assert "Extraction" in call_args[2]

    @pytest.mark.asyncio
    async def test_complete(self, mock_feishu_client):
        callback = FeishuCallback(chat_id="chat_123")

        mock_result = WorkflowResult(
            name="test_workflow",
            final_items=[{"ASIN": "B001", "Price": 10.0}, {"ASIN": "B002", "Price": 20.0}],
            total_duration_ms=1500,
        )

        await callback.on_complete(mock_result)

        mock_feishu_client.create_bitable.assert_called()
        mock_feishu_client.list_bitable_tables.assert_called_with(
            "mock_token", user_access_token=None
        )
        mock_feishu_client.batch_add_bitable_records.assert_called()

        mock_feishu_client.send_text_message.assert_called()
        last_call_text = mock_feishu_client.send_text_message.call_args[0][2]
        assert "Workflow completed!" in last_call_text
        assert "Items: 2" in last_call_text

    @pytest.mark.asyncio
    async def test_error(self, mock_feishu_client):
        callback = FeishuCallback(chat_id="chat_123")

        await callback.on_error(Exception("Simulated API failure"))

        mock_feishu_client.send_text_message.assert_called()
        last_call_text = mock_feishu_client.send_text_message.call_args[0][2]
        assert "Workflow failed" in last_call_text
        assert "Simulated API failure" in last_call_text


# ---------------------------------------------------------------------------
# Callback — artifact delivery
# ---------------------------------------------------------------------------


class TestFeishuCallbackArtifact:
    @pytest.mark.asyncio
    async def test_artifact_delivery(self):
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as tmp:
            tmp.write(b"# Mock Analysis Report\nThis is a test.")
            tmp_path = tmp.name

        try:
            mock_feishu = MagicMock()
            mock_feishu.upload_file.return_value = {"success": True, "file_key": "mock_file_123"}
            mock_feishu.send_file_message.return_value = {"success": True}
            mock_feishu.create_bitable.return_value = {"success": False}

            mock_result = MagicMock()
            mock_result.final_items = [{"report_file_path": tmp_path}]
            mock_result.name = "test_workflow"

            callback = FeishuCallback(chat_id="oc_123", bot_name="test_bot")
            with patch.object(FeishuCallback, "feishu", mock_feishu):
                await callback.on_complete(mock_result)

            mock_feishu.upload_file.assert_called_once()
            mock_feishu.send_file_message.assert_called_once_with(
                receive_id_type="chat_id", receive_id="oc_123", file_key="mock_file_123"
            )
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


# ---------------------------------------------------------------------------
# LLMResponse extraction (regression)
# ---------------------------------------------------------------------------


class TestLLMResponseExtraction:
    def test_text_extraction(self):
        """Verify _prepare_report_artifact correctly extracts text from LLMResponse."""
        mock_response = LLMResponse(
            text="Real Insight Content",
            provider_name="gemini",
            model_name="1.5-pro",
            token_usage={},
        )

        report_data = mock_response
        if hasattr(report_data, "text"):
            report_text = report_data.text
        elif isinstance(report_data, dict):
            report_text = report_data.get("text")
        else:
            report_text = str(report_data)

        assert report_text == "Real Insight Content"
        assert "LLMResponse" not in report_text
