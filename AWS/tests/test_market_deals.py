from __future__ import annotations
import pytest
import json
from src.mcp.servers.market.tools import handle_market_tool

@pytest.mark.asyncio
async def test_get_deal_history_tool():
    arguments = {"asin": "B0TESTASIN"}
    result = await handle_market_tool("get_deal_history", arguments)
    assert len(result) == 1
    assert result[0].type == "text"
    
    data = json.loads(result[0].text)
    assert isinstance(data, list)
    if data:
        assert "price" in data[0]
        assert "discount_pct" in data[0]

@pytest.mark.asyncio
async def test_analyze_promotions_tool():
    arguments = {
        "current_price": 49.99,
        "deals": [
            {"price": 25.00, "discount_pct": 50},
            {"price": 20.00, "discount_pct": 60},
        ]
    }
    result = await handle_market_tool("analyze_promotions", arguments)
    assert len(result) == 1
    
    data = json.loads(result[0].text)
    assert data["all_time_low"] == 20.00
    assert data["total_deals_found"] == 2
    assert data["median_discount_pct"] == 55.0
