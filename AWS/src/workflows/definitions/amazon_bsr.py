"""
Amazon BSR Workflow Definition

Extracts Best Sellers Rank (BSR) products from a given category URL.
"""
from __future__ import annotations
import logging
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import WorkflowContext, ComputeTarget

logger = logging.getLogger(__name__)

@WorkflowRegistry.register("amazon_bsr")
def build_amazon_bsr(config: dict) -> Workflow:
    
    async def _extract_bsr(items: list, ctx: WorkflowContext) -> list:
        url = config.get("amazon_url")
        if not url:
            raise ValueError("amazon_url is required for amazon_bsr workflow")
        
        if ctx.mcp:
            # Use MCP Tool through the client
            results = await ctx.mcp.call_tool_json("get_amazon_bestsellers", {"url": url})
            return results
        else:
            # Fallback to direct call if MCP client is not present
            from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
            extractor = BestSellersExtractor()
            return await extractor.get_bestsellers(url)

    steps = [
        ProcessStep(
            name="extract_bestsellers",
            fn=_extract_bsr,
            compute_target=ComputeTarget.PURE_PYTHON
        )
    ]
    
    category = config.get("category", "Unknown")
    return Workflow(name=f"amazon_bsr_{category}", steps=steps)
