
import asyncio
import pytest
import json
from src.mcp.servers.amazon.extractors.profitability_search import ProfitabilitySearchExtractor

@pytest.mark.asyncio
async def test_profitability_search_real_call():
    """Perform a real API call to verify the structure of returned data."""
    extractor = ProfitabilitySearchExtractor()
    results = await extractor.search_products("mouse", page_offset=1)
    
    assert isinstance(results, list)
    if len(results) > 0:
        product = results[0]
        # Verify essential fields exist
        assert "asin" in product
        assert "title" in product
        assert "price" in product
        # Verify rich metadata fields we discussed
        assert "brandName" in product or "brand" in product # The API uses brandName based on curl
        assert "weight" in product
        assert "length" in product
        assert "salesRank" in product
        
        print(f"\nSuccessfully fetched {len(results)} products.")
        print(f"Sample Product ASIN: {product.get('asin')}")
        print(f"Sample Product Title: {product.get('title')[:50]}...")
    else:
        pytest.fail("API returned no results for keyword 'mouse'")

if __name__ == "__main__":
    asyncio.run(test_profitability_search_real_call())
