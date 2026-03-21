import pytest
import os
import tempfile
from unittest.mock import MagicMock, patch
from src.jobs.callbacks.feishu import FeishuCallback

@pytest.mark.asyncio
async def test_feishu_callback_artifact_delivery():
    """
    Verify that FeishuCallback.on_complete detects report_file_path 
    and calls upload/send methods.
    """
    # 1. Create a dummy report file
    with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as tmp:
        tmp.write(b"# Mock Analysis Report\nThis is a test.")
        tmp_path = tmp.name

    try:
        # 2. Mock FeishuClient and Workflow Result
        mock_feishu = MagicMock()
        mock_feishu.upload_file.return_value = {"success": True, "file_key": "mock_file_123"}
        mock_feishu.send_file_message.return_value = {"success": True}
        # Mock bitable creation to avoid network calls
        mock_feishu.create_bitable.return_value = {"success": False} # Skip bitable logic for simplicity

        mock_result = MagicMock()
        mock_result.final_items = [{"report_file_path": tmp_path}]
        mock_result.name = "test_workflow"

        # 3. Initialize callback with mocked client
        callback = FeishuCallback(chat_id="oc_123", bot_name="test_bot")
        with patch.object(FeishuCallback, 'feishu', mock_feishu):
            await callback.on_complete(mock_result)

        # 4. Assertions
        mock_feishu.upload_file.assert_called_once()
        mock_feishu.send_file_message.assert_called_once_with(
            receive_id_type="chat_id",
            receive_id="oc_123",
            file_key="mock_file_123"
        )
        print("\nFeishuCallback artifact delivery test passed!")

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_feishu_callback_artifact_delivery())
