import os
import sys
import unittest
import json
from unittest.mock import MagicMock, patch

# Ensure project root is in path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.intelligence.providers.price_manager import PriceManager
from src.intelligence.providers.gemini import GeminiProvider
from src.intelligence.dto import LLMResponse

class TestGeminiAdvancedPricing(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for the new Gemini thoughts-token and cache-token pricing.
    """

    def setUp(self):
        # We need a dummy config to test PriceManager reliably
        self.pm = PriceManager(provider="gemini")
        self.test_model = "models/gemini-2.0-flash"

    def test_thoughts_token_calculation(self):
        """
        Verify that thought_token_count is correctly added to output cost.
        """
        input_tokens = 1000
        output_tokens = 500
        thoughts_tokens = 200 # New field
        
        # 1. Calculation without thoughts (Legacy)
        cost_no_thoughts = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens
        )
        
        # 2. Calculation with thoughts (New)
        cost_with_thoughts = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens,
            thoughts_token_count=thoughts_tokens
        )
        
        # Verification: in Gemini, output and thoughts share the same price.
        # So cost_with_thoughts should equal cost for (output + thoughts) tokens.
        cost_equivalent = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens + thoughts_tokens
        )
        self.assertAlmostEqual(cost_with_thoughts, cost_equivalent, places=10)

    def test_cached_token_calculation(self):
        """
        Verify that cached_content_token_count uses the cache price (cheaper).
        """
        input_tokens = 10000
        output_tokens = 1000
        cached_tokens = 8000
        
        # 1. Calculation without cache (Full input price)
        cost_full = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens
        )
        
        # 2. Calculation with cache (80% of input is cached)
        cost_cached = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens,
            cached_content_token_count=cached_tokens
        )
        
        # Cache reading is cheaper than full input, so cost_cached should be lower
        self.assertLess(cost_cached, cost_full)

    @patch('google.genai.Client')
    async def test_gemini_provider_extraction(self, mock_client):
        """
        Test that GeminiProvider correctly extracts thoughts from SDK response.
        """
        # 1. Setup Mock Provider
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.0-flash")
        # Manually force model_name because discovery might fail in mock environment
        mock_provider.model_name = "models/gemini-2.0-flash"
        
        # 2. Mock SDK Response Metadata
        mock_response = MagicMock()
        mock_response.text = "Thinking complete."
        # Simulate usage metadata from the new GenAI SDK
        mock_response.usage_metadata = MagicMock(
            prompt_token_count=1000,
            candidates_token_count=500,
            thought_token_count=200,      # The key new field
            cached_content_token_count=300
        )
        
        # 3. Patch the generation call
        with patch('asyncio.to_thread', return_value=mock_response):
            llm_res = await mock_provider.generate_text("Tell me a complex story.")
            
            # Verify extracted metadata
            self.assertEqual(llm_res.metadata["thoughts_tokens"], 200)
            self.assertEqual(llm_res.metadata["cached_tokens"], 300)
            self.assertEqual(llm_res.token_usage, 1000 + 500 + 200) # Total usage includes thoughts
            self.assertGreater(llm_res.cost, 0)

if __name__ == '__main__':
    unittest.main()
