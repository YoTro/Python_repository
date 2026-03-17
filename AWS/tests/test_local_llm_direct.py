import pytest
import asyncio
import os
from src.intelligence.providers.factory import ProviderFactory

@pytest.mark.asyncio
async def test_local_llm_direct_invocation():
    """
    A direct, minimal test to check if the LlamaCppProvider can be
    initialized and can generate text, bypassing the router and agent layers.
    This helps isolate issues with the local model itself or its provider.
    """
    print("\n--- [Test] Initializing Local LLM Provider directly via Factory ---")
    
    try:
        # This re-uses the absolute path logic from the factory
        local_provider = ProviderFactory.get_provider("local")
    except Exception as e:
        pytest.fail(f"💥 Failed to initialize LlamaCppProvider: {e}", pytrace=True)

    assert local_provider is not None, "ProviderFactory returned None for 'local'"
    print(f"✅ Provider initialized. Model: {local_provider.model_name}")

    print("\n--- [Test] Sending simple prompt: 'Who are you?' ---")
    try:
        response = await local_provider.generate_text("Who are you?")
        
        assert response is not None, "generate_text returned None"
        assert isinstance(response.text, str), f"Expected response.text to be a string, but got {type(response.text)}"
        assert len(response.text) > 5, "Response text is too short, likely an error or empty response."

        print(f"\n✅ Local LLM Raw Response:\n---\n{response.text}\n---")
        
    except Exception as e:
        pytest.fail(f"💥 local_provider.generate_text failed: {e}", pytrace=True)

