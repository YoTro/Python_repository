from unittest.mock import MagicMock

import pytest

from src.workflows.definitions.product_screening import (
    _calculate_profit_mcp,
    _enrich_compliance,
    _enrich_social_data,
)
from src.workflows.engine import WorkflowContext


# --- Tests for _enrich_compliance ---
@pytest.mark.asyncio
async def test_enrich_compliance_handles_cpsc_failure():
    # Mock context and MCP tool failure
    ctx = WorkflowContext(job_id="test_job", tenant_id="test", config={})
    ctx.mcp = MagicMock()

    # Simulate CPSC tool failing
    async def mock_handle_compliance_tool(name, params):
        if name == "check_cpsc_recall":
            raise Exception("Network error")
        return []

    import src.workflows.definitions.product_screening

    src.workflows.definitions.product_screening.handle_compliance_tool = mock_handle_compliance_tool

    item = {"title": "Test Product", "brand": "TestBrand"}
    result = await _enrich_compliance(item, ctx)

    # Should not fail, just log warning and proceed
    assert result["compliance_status"] == "pass"
    assert not result["cpsc_recalled"]


# --- Tests for _enrich_social_data ---
def test_enrich_social_data_stub_warning():
    # _enrich_social_data now calls calculate_promotion_strength() which exists.
    # With no videos provided, it returns a zero-strength result.
    ctx = WorkflowContext(job_id="test_job", tenant_id="test", config={})
    item = {"category": "Yoga"}

    import asyncio

    result = asyncio.run(_enrich_social_data(item, ctx))
    assert "social_score" in result
    assert result["social_score"] == 0


# --- Tests for _calculate_profit_mcp ---
@pytest.mark.asyncio
async def test_calculate_profit_mcp_missing_mcp():
    ctx = WorkflowContext(job_id="test_job", tenant_id="test", config={})
    ctx.mcp = None  # Missing MCP

    items = [{"asin": "B012345678", "price": 30.0}]
    result = await _calculate_profit_mcp(items, ctx)

    # Should return original items without profit data
    assert "profit" not in result[0]
    assert result[0]["asin"] == "B012345678"
