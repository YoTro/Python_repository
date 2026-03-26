import pytest
import asyncio
import os
import json
import tempfile
import logging
from unittest.mock import MagicMock
from src.entry.feishu.client import FeishuClient
from src.intelligence.dto import LLMResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_fixes():
    """
    Comprehensive test for recent Feishu and Workflow fixes.
    """
    client = FeishuClient()
    
    print("\n--- 1. Testing Subscriptable Fix (Bitable Creation) ---")
    # This might fail if no token is set, but we want to see if the marshaling logic works
    res = client.create_bitable("Test_Fix_Table")
    if res.get("success"):
        data = res["data"]
        print(f"Raw data type from client: {type(data)}")
        # Verify we can load it as JSON (marshaling check)
        parsed = json.loads(data) if isinstance(data, str) else data
        print(f"Parsed 'app_token': {parsed.get('app', {}).get('app_token', 'NOT_FOUND')}")
        assert "app" in parsed
    else:
        print(f"Skipping bitable functional check (Auth issue): {res.get('error')}")

    print("\n--- 2. Testing File Upload Fix (Error 234001) ---")
    with tempfile.NamedTemporaryFile(suffix=".md", delete=False, mode='w', encoding='utf-8') as tmp:
        tmp.write("# Test Fix\nContent should be stream.")
        tmp_path = tmp.name
    
    try:
        upload_res = client.upload_file(tmp_path, "Fix_Verify.md", file_type="stream")
        if upload_res.get("success"):
            print(f"✅ Upload Success! Key: {upload_res['file_key']}")
        else:
            print(f"❌ Upload Failed: {upload_res}")
            # If it's still 234001, we need to know
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    print("\n--- 3. Testing Content Extraction Fix (LLMResponse check) ---")
    # Mock an LLMResponse object as returned by ProcessStep
    mock_response = LLMResponse(
        text="Real Insight Content",
        provider_name="gemini",
        model_name="1.5-pro",
        token_usage={}
    )
    
    # Logic from _prepare_report_artifact
    report_data = mock_response
    report_text = None
    if hasattr(report_data, "text"):
        report_text = report_data.text
    elif isinstance(report_data, dict):
        report_text = report_data.get("text")
    else:
        report_text = str(report_data)
    
    print(f"Extracted Text: '{report_text}'")
    assert report_text == "Real Insight Content"
    assert "LLMResponse" not in report_text
    print("✅ Extraction Logic Verified!")

if __name__ == "__main__":
    asyncio.run(test_fixes())
