from __future__ import annotations
import pytest
import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from mcp.types import TextContent
from src.mcp.servers.market.tools import handle_market_tool, market_tools

@pytest.mark.asyncio
async def test_xiyou_get_traffic_scores_tool_registration():
    """Verify that xiyou_get_traffic_scores is in the market_tools list."""
    tool = next((t for t in market_tools if t.name == "xiyou_get_traffic_scores"), None)
    assert tool is not None
    assert tool.description.startswith("[Third-party Xiyouzhaoci tool]")
    assert "asins" in tool.inputSchema["properties"]

@pytest.mark.asyncio
async def test_handle_market_tool_traffic_scores():
    """Verify handle_market_tool correctly dispatches xiyou_get_traffic_scores."""
    asins = ["B07T869RNY"]
    mock_response = {"success": True, "data": [{"asin": "B07T869RNY", "advertisingTrafficScoreRatio": 0.5}]}
    
    # Mock XiyouZhaociAPI
    with patch("src.mcp.servers.market.tools._get_xiyou_api") as mock_get_api:
        mock_api_instance = MagicMock()
        mock_api_instance.get_traffic_scores.return_value = mock_response
        mock_get_api.return_value = mock_api_instance
        
        # Call the tool handler
        arguments = {"asins": asins, "country": "US"}
        result = await handle_market_tool("xiyou_get_traffic_scores", arguments)
        
        # Assertions
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        
        # Verify JSON content
        data = json.loads(result[0].text)
        assert data == mock_response
        
        # Verify API was called correctly
        mock_api_instance.get_traffic_scores.assert_called_once_with(country="US", asins=asins)
