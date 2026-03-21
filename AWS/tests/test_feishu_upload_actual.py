import asyncio
import os
import tempfile
import logging
from src.entry.feishu.client import FeishuClient
from src.core.utils.context import ContextPropagator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_md_upload():
    """
    Actual integration test for Feishu MD upload.
    Requires valid FEISHU_APP_ID/SECRET in .env.
    """
    client = FeishuClient()
    
    # 1. Create a dummy MD file
    content = """# Monopoly Analysis Report
## Summary
This is a test report generated for verification.
- **Risk**: Low
- **Confidence**: 0.98
"""
    with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode='w', encoding='utf-8') as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    logger.info(f"Created temp file: {tmp_path}")

    try:
        # 2. Test Upload
        logger.info("Starting upload with file_type='stream'...")
        upload_res = client.upload_file(
            file_path=tmp_path,
            file_name="Test_Report.md",
            file_type="stream"
        )
        
        if upload_res.get("success"):
            file_key = upload_res.get("file_key")
            logger.info(f"✅ Upload Success! File Key: {file_key}")
            
            # 3. Optional: Try to send if a chat_id is available in env or context
            # You can manually put a chat_id here to test delivery
            test_chat_id = os.getenv("FEISHU_TEST_CHAT_ID")
            if test_chat_id:
                logger.info(f"Attempting to send to chat: {test_chat_id}")
                send_res = client.send_file_message(
                    receive_id_type="chat_id",
                    receive_id=test_chat_id,
                    file_key=file_key
                )
                if send_res.get("success"):
                    logger.info("✅ File Message Sent!")
                else:
                    logger.error(f"❌ Send Failed: {send_res}")
        else:
            logger.error(f"❌ Upload Failed: {upload_res}")

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            logger.info("Cleaned up temp file.")

if __name__ == "__main__":
    asyncio.run(test_md_upload())
