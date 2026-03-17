import pytest
import json
from src.core.data_cache import DataCache
from src.mcp.servers.amazon.tools import handle_amazon_tool
from src.mcp.servers.finance.tools import handle_finance_tool
from src.registry.tools import tool_registry

@pytest.mark.asyncio
async def test_l1_l2_orchestration_via_cache():
    # 1. Setup local cache for testing
    cache = DataCache()
    # Injected via singleton in tools, but we can verify the mechanism
    
    # 2. L1 Action: Simulate Amazon scraping and cache writing
    # We mock the extractor to avoid real network calls
    from unittest.mock import patch, AsyncMock
    
    mock_products = [{"asin": "B0TEST", "price": 100.0, "title": "Test Product"}]
    
    with patch("src.mcp.servers.amazon.tools.BestSellersExtractor") as mock_ext_class:
        mock_ext = AsyncMock()
        mock_ext.get_bestsellers.return_value = mock_products
        mock_ext_class.return_value = mock_ext
        
        # This should call data_cache.set("amazon", "B0TEST", ...)
        await handle_amazon_tool("get_amazon_bestsellers", {"url": "http://test"})
    
    # 3. Verify L1 wrote to cache
    from src.core.data_cache import data_cache
    assert data_cache.exists("amazon", "B0TEST")
    cached_data = data_cache.get("amazon", "B0TEST")
    assert cached_data["price"] == 100.0

    # 4. L2 Action: Call Finance tool which reads from L1's cache
    # calc_profit(asin="B0TEST", estimated_cost=60)
    # Expected margin: (100 - 60) / 100 = 0.4
    
    result = await handle_finance_tool("calc_profit", {"asin": "B0TEST", "estimated_cost": 60.0})
    
    # 5. Verify L2 output
    assert len(result) == 1
    data = json.loads(result[0].text)
    assert data["margin"] == 0.4
    assert data["source"] == "data_cache"
    assert data["price"] == 100.0
