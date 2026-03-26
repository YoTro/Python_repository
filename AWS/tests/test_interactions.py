import unittest
import json
import asyncio
from unittest.mock import MagicMock, patch

from src.jobs.interactions.registry import InteractionRegistry
from src.jobs.callbacks.feishu import FeishuCallback
from src.jobs.callbacks.base import CallbackCapability

# Import the handler to ensure it registers itself
import src.jobs.interactions.handlers

class TestInteractionsAndCallbacks(unittest.IsolatedAsyncioTestCase):

    def test_registry_has_handler(self):
        """Verify that the Xiyou verification handler is registered."""
        actions = InteractionRegistry.list_actions()
        self.assertIn("VERIFY_XIYOU_LOGIN", actions)

    @patch("src.jobs.interactions.handlers.XiyouZhaociAPI")
    @patch("src.jobs.interactions.handlers.get_job_manager")
    async def test_xiyou_verification_handler(self, mock_get_job_mgr, mock_api_class):
        """Verify the handler correctly processes a SUCCESS status and resumes the job."""
        # Setup mocks
        mock_api_instance = MagicMock()
        mock_api_instance.check_qr_login_status.return_value = {"status": "SUCCESS"}
        mock_api_class.return_value = mock_api_instance
        
        mock_job_mgr = MagicMock()
        mock_job_mgr.resume.return_value = True
        mock_get_job_mgr.return_value = mock_job_mgr

        # Execute handler
        payload = {"tenant_id": "test_tenant", "job_id": "job_123"}
        result = await InteractionRegistry.handle("VERIFY_XIYOU_LOGIN", payload)

        # Assertions
        mock_api_class.assert_called_once_with(tenant_id="test_tenant")
        mock_api_instance.check_qr_login_status.assert_called_once()
        mock_job_mgr.resume.assert_called_once_with("job_123")
        
        self.assertTrue(result.get("success"))
        self.assertIn("✅ 登录成功", result.get("toast", ""))

    @patch("aiohttp.ClientSession.get")
    async def test_feishu_card_rendering(self, mock_aiohttp_get):
        """Verify that FeishuCallback downloads, uploads image and renders a card."""
        from unittest.mock import AsyncMock, PropertyMock
        
        # 1. Setup Mock FeishuClient
        mock_feishu = MagicMock()
        mock_feishu.upload_image.return_value = {"success": True, "image_key": "img_mock_123"}
        mock_feishu.send_raw_card.return_value = {"success": True}
        
        # 2. Setup Mock aiohttp response for image download
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read = AsyncMock(return_value=b"fake_image_data")
        mock_aiohttp_get.return_value.__aenter__.return_value = mock_resp

        callback = FeishuCallback(chat_id="chat_123")
        
        # 3. Patch the 'feishu' property DIRECTLY on the instance or class
        with patch.object(FeishuCallback, "feishu", new_callable=PropertyMock) as mock_feishu_prop:
            mock_feishu_prop.return_value = mock_feishu
            
            # 4. Create a mock INTERACTION_REQUIRED signal
            signal = {
                "_type": "INTERACTION_REQUIRED",
                "interaction_type": "AUTH_QR_SCAN",
                "ui_config": {
                    "title": "Identity Check",
                    "action": "VERIFY_AUTH"
                },
                "data": {"url": "https://fake.url/qr.jpg", "expires_in": 120},
                "context": {"tenant_id": "t1", "job_id": "j1"}
            }

            # 5. Simulate the progress message arriving
            await callback._send_progress(json.dumps(signal))
            
            # 6. Assertions
            # Verify image was "uploaded"
            mock_feishu.upload_image.assert_called_once()
            
            # Verify card was sent with the correct image_key
            mock_feishu.send_raw_card.assert_called_once()
            sent_card = mock_feishu.send_raw_card.call_args[0][2]
            
            self.assertEqual(sent_card["header"]["title"]["content"], "Identity Check")
            # Find the image element and check its key
            img_element = next(el for el in sent_card["elements"] if el["tag"] == "img")
            self.assertEqual(img_element["img_key"], "img_mock_123")


if __name__ == "__main__":
    unittest.main()
