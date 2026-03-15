import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from src.jobs.callbacks.feishu import FeishuCallback
from src.workflows.engine import WorkflowResult

@pytest.fixture
def mock_feishu_client():
    with patch("src.jobs.callbacks.feishu.FeishuCallback.feishu") as mock_property:
        client = MagicMock()
        client.create_bitable.return_value = {
            "success": True, 
            "data": '{"app": {"app_token": "mock_token", "url": "http://mock"}}'
        }
        client.list_bitable_tables.return_value = {
            "items": '[{"table_id": "tbl123"}]'
        }
        client.delete_all_bitable_records.return_value = True
        client.create_bitable_field.return_value = True
        client.batch_add_bitable_records.return_value = True
        client.send_text_message.return_value = True
        mock_property.__get__ = MagicMock(return_value=client)
        yield client

@pytest.mark.asyncio
async def test_feishu_callback_progress(mock_feishu_client):
    callback = FeishuCallback(chat_id="chat_123", total_steps=2)
    
    await callback.on_progress(step_index=1, total_steps=2, step_name="Extraction")
    mock_feishu_client.send_card_message.assert_called()
    
    call_args = mock_feishu_client.send_card_message.call_args[0]
    assert "chat_123" in call_args
    assert "[1/2]" in call_args[2]
    assert "Extraction" in call_args[2]

@pytest.mark.asyncio
async def test_feishu_callback_complete(mock_feishu_client):
    callback = FeishuCallback(chat_id="chat_123")
    
    mock_result = WorkflowResult(
        name="test_workflow",
        final_items=[{"ASIN": "B001", "Price": 10.0}, {"ASIN": "B002", "Price": 20.0}],
        total_duration_ms=1500
    )
    
    await callback.on_complete(mock_result)
    
    # Verify Bitable creation was attempted
    mock_feishu_client.create_bitable.assert_called()
    mock_feishu_client.list_bitable_tables.assert_called_with("mock_token", user_access_token=None)
    mock_feishu_client.batch_add_bitable_records.assert_called()
    
    # Verify final summary message
    mock_feishu_client.send_text_message.assert_called()
    last_call_text = mock_feishu_client.send_text_message.call_args[0][2]
    assert "Workflow completed!" in last_call_text
    assert "Items: 2" in last_call_text

@pytest.mark.asyncio
async def test_feishu_callback_error(mock_feishu_client):
    callback = FeishuCallback(chat_id="chat_123")
    
    await callback.on_error(Exception("Simulated API failure"))
    
    mock_feishu_client.send_text_message.assert_called()
    last_call_text = mock_feishu_client.send_text_message.call_args[0][2]
    assert "Workflow failed" in last_call_text
    assert "Simulated API failure" in last_call_text
