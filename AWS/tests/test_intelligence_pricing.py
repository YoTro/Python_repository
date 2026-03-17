from __future__ import annotations
import os
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from src.intelligence.providers.price_manager import PriceManager
from src.intelligence.providers.gemini import GeminiProvider
from src.intelligence.providers.claude import ClaudeProvider
from src.intelligence.dto import LLMResponse

# 1. PriceManager Unit Tests (Gemini)

def test_price_manager_normalization_gemini():
    """Verify model string normalization for Gemini."""
    pm = PriceManager(provider="gemini")
    
    # Prefix removal
    assert pm.normalize_model_name("models/gemini-2.5-pro") == "gemini-2.5-pro"
    # Case-insensitive
    assert pm.normalize_model_name("Models/Gemini-2.5-Flash") == "gemini-2.5-flash"
    # Index mapping
    assert pm.normalize_model_name("gemini-2.0-flash-001") == "gemini-2.0-flash"

def test_price_manager_calculation_gemini():
    pm = PriceManager(provider="gemini")
    # gemini-2.5-flash standard_paid: Input $0.30, Output $2.50
    cost = pm.calculate_cost("gemini-2.5-flash", 1000000, 1000000)
    assert cost == 2.8

# 2. PriceManager Unit Tests (Claude)

def test_price_manager_normalization_claude():
    """Verify model string normalization for Claude."""
    pm = PriceManager(provider="claude")
    # Claude doesn't use models/ prefix
    assert pm.normalize_model_name("claude-3-opus-20240229") == "claude-opus-3"
    assert pm.normalize_model_name("Claude-Sonnet-4-6") == "claude-sonnet-4-6"

def test_price_manager_calculation_claude_standard():
    """Verify standard Claude pricing (no surcharge)."""
    pm = PriceManager(provider="claude")
    # Sonnet 4.6: Input $3.0, Output $15.0
    cost = pm.calculate_cost("claude-sonnet-4-6", 1000000, 1000000)
    assert cost == 18.0

def test_price_manager_calculation_claude_long_context():
    """Verify Claude long context surcharge (>200k) for specific models."""
    pm = PriceManager(provider="claude")
    
    # Case A: Sonnet 4.5 <= 200k (Input $3.0, Output $15.0)
    cost_low = pm.calculate_cost("claude-sonnet-4-5", 100000, 50000, total_tokens=150000)
    assert cost_low == (100000 * 3.0 / 1e6) + (50000 * 15.0 / 1e6) # 0.3 + 0.75 = 1.05
    
    # Case B: Sonnet 4.5 > 200k (Triggers long_context_gt200k: Input $6.0, Output $22.5)
    cost_high = pm.calculate_cost("claude-sonnet-4-5", 210000, 10000, total_tokens=210000)
    expected_high = (210000 * 6.0 / 1e6) + (10000 * 22.5 / 1e6) # 1.26 + 0.225 = 1.485
    assert cost_high == expected_high

# 3. GeminiProvider Integration (Mocked)

@pytest.mark.asyncio
async def test_gemini_provider_cost_population():
    mock_response = MagicMock()
    mock_response.text = "Hello"
    mock_usage = MagicMock()
    mock_usage.prompt_token_count = 1000
    mock_usage.candidates_token_count = 500
    mock_response.usage_metadata = mock_usage
    
    with patch("google.genai.Client"), \
         patch("src.intelligence.providers.gemini.genai.Client"), \
         patch("asyncio.to_thread", return_value=mock_response):
        
        os.environ["GEMINI_API_KEY"] = "fake"
        with patch.object(GeminiProvider, '_discover_best_model', return_value="gemini-2.5-flash"):
            provider = GeminiProvider()
            provider.price_manager.calculate_cost = MagicMock(return_value=0.00123)
            
            response = await provider.generate_text("test")
            assert response.cost == 0.00123
            assert response.currency == "USD"

# 4. ClaudeProvider Integration (Mocked)

@pytest.mark.asyncio
async def test_claude_provider_cost_population():
    """Verify ClaudeProvider populates cost from response usage."""
    
    # Mock Anthropic Response
    mock_message = MagicMock()
    mock_message.content = [MagicMock(type="text", text="Claude response")]
    
    # Mock usage with potential caching fields
    mock_usage = MagicMock()
    mock_usage.input_tokens = 2000
    mock_usage.output_tokens = 1000
    mock_usage.cache_read_input_tokens = 500
    mock_usage.cache_creation_input_tokens = 0
    mock_message.usage = mock_usage
    
    with patch("anthropic.AsyncAnthropic"):
        os.environ["ANTHROPIC_API_KEY"] = "fake-key"
        provider = ClaudeProvider()
        
        # Mock the API call
        provider.client.messages.create = AsyncMock(return_value=mock_message)
        # Mock price manager
        provider.price_manager.calculate_cost = MagicMock(return_value=0.042)
        
        response = await provider.generate_text("Hello Claude")
        
        assert isinstance(response, LLMResponse)
        assert response.cost == 0.042
        assert response.token_usage == 3000
        assert response.metadata["cache_read_tokens"] == 500
        provider.price_manager.calculate_cost.assert_called_once_with(
            model_name=provider.model_name,
            input_tokens=2000,
            output_tokens=1000,
            total_tokens=2000
        )
