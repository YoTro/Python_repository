import pytest
import json
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from src.workflows.engine import WorkflowContext
from src.workflows.registry import WorkflowRegistry
from src.workflows.definitions.product_screening import build_product_screening

@pytest.fixture
def mock_config():
    return {
        "price_min": 20,
        "price_max": 100,
        "rating_min": 4.0,
        "rating_max": 5.0,
        "weight_min": 10,
        "weight_max": 2000,
        "daily_sales_min": 10,
        "profit_margin_min": 0.25,
        "cost_ratio_max": 0.40,
        "compliance_status_allowed": ["pass"],
        "epa_status_allowed": ["exempt", "not_required"],
        "enable_ad_analysis_xiyou": True,
        "enable_social_analysis": False
    }

@pytest.fixture
def mock_mcp():
    mcp = MagicMock()
    mcp.call_tool_json = AsyncMock()
    return mcp

@pytest.mark.asyncio
async def test_product_screening_full_funnel(mock_config, mock_mcp):
    # 1. Setup Mock Data for different products
    # Product A: Perfect (Should Pass)
    # Product B: Too Expensive (Fail basic_filter)
    # Product C: Low Margin (Fail profit_filter)
    # Product D: Compliance Risk (Fail compliance_filter)
    
    initial_items = [
        {"asin": "PASS01", "keyword": "test"},
        {"asin": "FAIL_PRICE", "keyword": "test"},
        {"asin": "FAIL_MARGIN", "keyword": "test"},
        {"asin": "FAIL_COMPLIANCE", "keyword": "test"},
    ]
    
    # Mock Profitability API Enrichment
    async def side_effect_profitability(asin, page_offset=1):
        data = {
            "PASS01": {"title": "Good Product", "price": 30.0, "weight": 0.5, "salesRank": 1000, "customerReviewsRating": 4.5},
            "FAIL_PRICE": {"title": "Expensive", "price": 500.0, "weight": 0.5, "salesRank": 2000, "customerReviewsRating": 4.5},
            "FAIL_MARGIN": {"title": "Thin Profit", "price": 25.0, "weight": 0.5, "salesRank": 3000, "customerReviewsRating": 4.5},
            "FAIL_COMPLIANCE": {"title": "Risky", "price": 35.0, "weight": 0.5, "salesRank": 4000, "customerReviewsRating": 4.5},
        }
        p = data.get(asin)
        return [p] if p else []

    # Mock MCP call for profit calculation
    async def side_effect_mcp(tool_name, arguments):
        from mcp.types import TextContent
        if tool_name == "calc_profit":
            asin = arguments["asin"]
            if asin == "FAIL_MARGIN":
                margin = 0.10 # 10% < 25%
            else:
                margin = 0.35 # 35% > 25%
            return [{"text": json.dumps({
                "profitability": {"net_profit": 10.0, "margin": margin, "roi": 0.5},
                "fees": {"total": 5.0}
            })}]
        elif tool_name == "xiyou_get_traffic_scores":
            return [{"text": json.dumps({
                "success": True,
                "data": [{"advertisingTrafficScoreRatio": 0.20, "totalTrafficScoreGrowthRate": 0.05}]
            })}]
        return []

    mock_mcp.call_tool_json.side_effect = side_effect_mcp

    # Patch all external dependencies
    with patch("src.mcp.servers.amazon.extractors.profitability_search.ProfitabilitySearchExtractor.search_products", side_effect=side_effect_profitability), \
         patch("src.mcp.servers.amazon.extractors.past_month_sales.PastMonthSalesExtractor.get_batch_past_month_sales", return_value={"PASS01": 600, "FAIL_PRICE": 600, "FAIL_MARGIN": 600, "FAIL_COMPLIANCE": 600}), \
         patch("src.mcp.servers.amazon.extractors.fulfillment.FulfillmentExtractor.get_fulfillment_info", return_value={"FulfilledBy": "Amazon"}), \
         patch("src.mcp.servers.market.deals.client.DealHistoryClient.get_deal_history", return_value=[]), \
         patch("src.mcp.servers.compliance.tools.handle_compliance_tool") as mock_compliance:
        
        # Setup compliance mock: FAIL_COMPLIANCE should have a recall
        from mcp.types import TextContent
        def compliance_side_effect(name, args):
            if name == "check_cpsc_recall" and args["keyword"] == "Risky":
                return [TextContent(type="text", text=json.dumps({"status": "recalled", "count": 1, "findings": [{"title": "Exploding Product"}]}))]
            return [TextContent(type="text", text=json.dumps({"status": "pass", "findings": []}))]
        
        mock_compliance.side_effect = compliance_side_effect
        
        # 2. Build and Execute Workflow
        workflow = build_product_screening(mock_config)
        ctx = WorkflowContext(job_id="test_screening_job", mcp=mock_mcp, config=mock_config)
        
        result = await workflow.execute(
            job_id="test_screening_job", 
            params={"initial_items": initial_items}, 
            ctx=ctx
        )
        
        # 3. Assertions
        assert result.completed is True
        
        # We expect only PASS01 to remain
        asins_passed = [item["asin"] for item in result.final_items]
        assert "PASS01" in asins_passed
        assert "FAIL_PRICE" not in asins_passed
        assert "FAIL_MARGIN" not in asins_passed
        assert "FAIL_COMPLIANCE" not in asins_passed
        assert len(result.final_items) == 1
        
        # Verify enriched data on passed item
        passed_item = result.final_items[0]
        assert passed_item["daily_sales"] == 20.0 # 600 / 30
        assert passed_item["profit_margin"] == 0.35
        assert passed_item["compliance_status"] == "pass"

if __name__ == "__main__":
    pytest.main([__file__])
