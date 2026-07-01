import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

# Ensure project root is in path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.core.data_cache import data_cache
from src.intelligence.providers.gemini import (
    _CACHE_EXPIRY_DOMAIN,
    _CACHE_METRICS_DOMAIN,
    GeminiProvider,
)
from src.intelligence.providers.price_manager import PriceManager


class _DummyStructuredSchema:
    @classmethod
    def model_json_schema(cls):
        return {
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
        }


class TestGeminiAdvancedPricing(unittest.IsolatedAsyncioTestCase):
    """
    Test suite for the new Gemini thoughts-token and cache-token pricing.
    """

    def setUp(self):
        # We need a dummy config to test PriceManager reliably
        self.pm = PriceManager(provider="gemini")
        self.test_model = "models/gemini-2.5-flash"

    def test_thoughts_token_calculation(self):
        """
        Verify that thought_token_count is correctly added to output cost.
        """
        input_tokens = 1000
        output_tokens = 500
        thoughts_tokens = 200  # New field

        # 1. Calculation without thoughts (Legacy)
        self.pm.calculate_cost(self.test_model, input_tokens, output_tokens)

        # 2. Calculation with thoughts (New)
        cost_with_thoughts = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens, thoughts_token_count=thoughts_tokens
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
        cost_full = self.pm.calculate_cost(self.test_model, input_tokens, output_tokens)

        # 2. Calculation with cache (80% of input is cached)
        cost_cached = self.pm.calculate_cost(
            self.test_model, input_tokens, output_tokens, cached_content_token_count=cached_tokens
        )

        # Cache reading is cheaper than full input, so cost_cached should be lower
        self.assertLess(cost_cached, cost_full)

    @patch("google.genai.Client")
    async def test_gemini_provider_extraction(self, mock_client):
        """
        Test that GeminiProvider correctly extracts thoughts from SDK response.
        """
        # 1. Setup Mock Provider
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-flash")
        # Manually force model_name because discovery might fail in mock environment
        mock_provider.model_name = "models/gemini-2.5-flash"

        # 2. Mock SDK Response Metadata
        mock_response = MagicMock()
        mock_response.text = "Thinking complete."
        mock_response.total_tokens = 500  # returned by count_tokens via asyncio.to_thread
        # Simulate usage metadata from the new GenAI SDK
        mock_response.usage_metadata = MagicMock(
            prompt_token_count=1000,
            candidates_token_count=500,
            thoughts_token_count=200,  # current google-genai SDK field name
            cached_content_token_count=300,
        )

        # 3. Patch the generation call
        with patch("asyncio.to_thread", return_value=mock_response):
            llm_res = await mock_provider.generate_text("Tell me a complex story.")

            # Verify extracted metadata
            self.assertEqual(llm_res.metadata["thoughts_tokens"], 200)
            self.assertEqual(llm_res.metadata["cached_tokens"], 300)
            self.assertEqual(llm_res.token_usage, 1000 + 500 + 200)  # Total usage includes thoughts
            self.assertGreater(llm_res.cost, 0)

    @patch("google.genai.Client")
    async def test_gemini_context_cache_creation_and_usage(self, mock_client):
        """
        Verify that create_context_cache and using cached_content in generate_text works correctly.
        """
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-flash")
        mock_provider.model_name = "models/gemini-2.5-flash"

        # Mock cache creation response
        mock_cache = MagicMock()
        mock_cache.name = "projects/fake/locations/global/cachedContents/test_cache_id"
        mock_provider.client.caches.create = MagicMock(return_value=mock_cache)
        mock_provider.client.caches.delete = MagicMock()

        # 1. Test create_context_cache
        cache = await mock_provider.create_context_cache(
            contents=["Some massive competitor reviews dataset"],
            system_instruction="Analyze this reviews dataset carefully.",
            display_name="competitor_reviews_cache",
        )
        self.assertEqual(cache.name, "projects/fake/locations/global/cachedContents/test_cache_id")
        mock_provider.client.caches.create.assert_called_once()

        # 2. Test generate_text with cached_content
        mock_response = MagicMock()
        mock_response.text = "Analysis of cached reviews."
        mock_response.total_tokens = 500
        mock_response.usage_metadata = MagicMock(
            prompt_token_count=1000,
            candidates_token_count=500,
            thoughts_token_count=0,
            cached_content_token_count=800,
        )

        with patch("asyncio.to_thread", return_value=mock_response):
            llm_res = await mock_provider.generate_text(
                "Summarize key issues.",
                system_message="Analyze this reviews dataset carefully.",
                cached_content=cache.name,
            )

            self.assertEqual(llm_res.text, "Analysis of cached reviews.")
            self.assertEqual(llm_res.metadata["cached_tokens"], 800)

        # 3. Test delete_context_cache
        await mock_provider.delete_context_cache(cache.name)
        mock_provider.client.caches.delete.assert_called_once_with(name=cache.name)

    @patch("google.genai.Client")
    async def test_generate_text_cache_pricing_uses_per_call_service_tier(self, mock_client):
        """
        Per-call service_tier overrides must drive both response cost and cache savings.
        """
        mock_provider = GeminiProvider(
            api_key="fake_key",
            model_name="models/gemini-2.5-flash",
            service_tier="standard",
        )
        mock_provider.model_name = "models/gemini-2.5-flash"
        mock_provider._check_context_limit = AsyncMock()
        mock_provider._generate_content_with_retry = AsyncMock(
            return_value=self._mock_cached_response()
        )

        llm_res = await mock_provider.generate_text(
            "Summarize key issues.",
            cached_content="cachedContents/test_cache_id",
            service_tier="flex",
        )

        expected_cost = self.pm.calculate_cost(
            self.test_model,
            1000,
            500,
            tier="flex",
            cached_content_token_count=800,
        )
        expected_saved = self._expected_cache_saved("flex", 800)

        self.assertAlmostEqual(llm_res.cost, expected_cost, places=10)
        self.assertAlmostEqual(llm_res.cache_cost_saved, expected_saved, places=10)

    @patch("google.genai.Client")
    async def test_generate_structured_cache_pricing_uses_per_call_service_tier(self, mock_client):
        """
        Structured generation has the same per-call tier cache-savings requirement.
        """
        mock_provider = GeminiProvider(
            api_key="fake_key",
            model_name="models/gemini-2.5-flash",
            service_tier="standard",
        )
        mock_provider.model_name = "models/gemini-2.5-flash"
        mock_provider._check_context_limit = AsyncMock()
        mock_provider._generate_content_with_retry = AsyncMock(
            return_value=self._mock_cached_response('{"answer":"ok"}')
        )

        llm_res = await mock_provider.generate_structured(
            "Return JSON.",
            _DummyStructuredSchema,
            cached_content="cachedContents/test_cache_id",
            service_tier="flex",
        )

        expected_cost = self.pm.calculate_cost(
            self.test_model,
            1000,
            500,
            tier="flex",
            cached_content_token_count=800,
        )
        expected_saved = self._expected_cache_saved("flex", 800)

        self.assertAlmostEqual(llm_res.cost, expected_cost, places=10)
        self.assertAlmostEqual(llm_res.cache_cost_saved, expected_saved, places=10)

    @patch("google.genai.Client")
    async def test_cache_renewal_uses_interval_before_current_hit(self, mock_client):
        """
        Adaptive renewal should not collapse to the 60s minimum because the current
        hit updates last_hit_at immediately before renewal is computed.
        """
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-flash")
        mock_provider.model_name = "models/gemini-2.5-flash"
        mock_provider._check_context_limit = AsyncMock()
        mock_provider._generate_content_with_retry = AsyncMock(
            return_value=self._mock_cached_response()
        )
        mock_provider.client.caches.update = MagicMock()

        now = time.time()
        cache_name = f"cachedContents/test_renewal_{id(self)}"
        data_cache.set(_CACHE_EXPIRY_DOMAIN, cache_name, now + 1)
        data_cache.set(
            _CACHE_METRICS_DOMAIN,
            cache_name,
            {
                "cache_name": cache_name,
                "content_hash": "test",
                "system_hash": mock_provider._system_hash(None),
                "model": mock_provider.model_name,
                "display_name": None,
                "created_at": now - 360,
                "token_count": 1000,
                "expected_hits": 4,
                "initial_ttl_seconds": 300,
                "hits": 2,
                "misses": 0,
                "renewals": 0,
                "cost_creation": 0.0,
                "cost_storage_accrued": 0.0,
                "cost_saved": 0.0,
                "last_hit_at": now - 120,
                "last_updated_at": now - 120,
            },
        )

        await mock_provider.generate_text("Summarize key issues.", cached_content=cache_name)

        mock_provider.client.caches.update.assert_called_once()
        config = mock_provider.client.caches.update.call_args.kwargs["config"]
        ttl = getattr(config, "ttl", None) if not isinstance(config, dict) else config["ttl"]
        self.assertGreater(int(ttl.rstrip("s")), 60)

    @patch("google.genai.Client")
    async def test_generate_structured_sets_max_output_tokens(self, mock_client):
        """
        Structured responses can be large JSON objects; they should use the same
        provider output-token ceiling as text generation.
        """
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-flash")
        mock_provider.model_name = "models/gemini-2.5-flash"
        mock_provider._check_context_limit = AsyncMock()
        mock_provider._generate_content_with_retry = AsyncMock(
            return_value=self._mock_cached_response('{"answer":"ok"}')
        )

        await mock_provider.generate_structured("Return JSON.", _DummyStructuredSchema)

        config = mock_provider._generate_content_with_retry.call_args.kwargs["config"]
        self.assertEqual(config.max_output_tokens, mock_provider._DEFAULT_MAX_TOKENS)

    @patch("google.genai.Client")
    def test_explicit_model_name_survives_model_discovery_failure(self, mock_client):
        """
        A caller-supplied model_name is a configuration choice and should not be
        silently replaced if model listing is unavailable.
        """
        mock_client.return_value.models.list.side_effect = RuntimeError("list unavailable")

        provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-pro")

        self.assertEqual(provider.model_name, "models/gemini-2.5-pro")

    @patch("google.genai.Client")
    async def test_generate_text_handles_missing_response_text_as_empty_string(self, mock_client):
        """
        Gemini may return no text for blocked or empty candidates; the provider
        should return an empty response object instead of crashing.
        """
        mock_provider = GeminiProvider(api_key="fake_key", model_name="models/gemini-2.5-flash")
        mock_provider.model_name = "models/gemini-2.5-flash"
        mock_provider._check_context_limit = AsyncMock()
        mock_response = self._mock_cached_response(text=None)
        mock_response.usage_metadata.cached_content_token_count = 0
        mock_provider._generate_content_with_retry = AsyncMock(return_value=mock_response)

        llm_res = await mock_provider.generate_text("Return nothing.")

        self.assertEqual(llm_res.text, "")

    @staticmethod
    def _mock_cached_response(text="Analysis of cached reviews."):
        mock_response = MagicMock()
        mock_response.text = text
        mock_response.usage_metadata = MagicMock(
            prompt_token_count=1000,
            candidates_token_count=500,
            thoughts_token_count=0,
            cached_content_token_count=800,
        )
        return mock_response

    def _expected_cache_saved(self, tier: str, cached_tokens: int) -> float:
        canonical_model = self.pm.normalize_model_name(self.test_model)
        price_tier = f"{tier}_paid"
        input_price = self.pm.lookup[f"{canonical_model}#{price_tier}#input#text"]["price"]
        cache_read_price = self.pm.lookup[f"{canonical_model}#{price_tier}#cache_read#text"][
            "price"
        ]
        return (input_price - cache_read_price) * cached_tokens / 1_000_000


if __name__ == "__main__":
    unittest.main()
