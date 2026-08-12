from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.core.data_cache import DataCache, data_cache
from src.registry.tools import tool_registry


@pytest.mark.asyncio
async def test_l1_l2_orchestration_via_cache():
    """L1 (scraper tool) writes to DataCache; L2 (finance tool) reads from it.

    Calls go through ToolRegistry.call_tool — the same path production code
    uses — rather than importing handlers directly, so context propagation
    and argument validation are exercised too (see TESTING.md §4.6).
    """
    # Fresh cache instance; data_cache singleton is what the handlers use.
    DataCache()

    mock_products = [{"asin": "B0TEST", "price": 100.0, "title": "Test Product"}]

    # L1: simulate Amazon scraping and cache writing (mock the extractor to
    # avoid real network calls).
    with patch("src.mcp.servers.amazon.tools.BestSellersExtractor") as mock_ext_class:
        mock_ext = AsyncMock()
        mock_ext.get_bestsellers.return_value = mock_products
        mock_ext_class.return_value = mock_ext

        # This should call data_cache.set("amazon", "B0TEST", ...)
        await tool_registry.call_tool("get_amazon_bestsellers", {"url": "http://test"})

    # Verify L1 wrote to cache
    assert data_cache.exists("amazon", "B0TEST")
    cached_data = data_cache.get("amazon", "B0TEST")
    assert cached_data["price"] == 100.0

    # L2: Finance tool reads from L1's cache.
    # calc_profit(asin="B0TEST", estimated_cost=60) — margin depends on fee
    # config (referral, FBA, return fees), so only structure is asserted.
    result = await tool_registry.call_tool(
        "calc_profit", {"asin": "B0TEST", "estimated_cost": 60.0}
    )

    assert len(result) == 1
    data = json.loads(result[0].text)
    assert "profitability" in data
    assert "margin" in data["profitability"]
    assert data["price"] == 100.0
