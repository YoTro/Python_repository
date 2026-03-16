import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.core.data_cache import data_cache

logger = logging.getLogger("mcp-finance")

async def handle_finance_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "calc_profit":
        asin = arguments.get("asin")
        cost = arguments.get("estimated_cost", 0)
        
        # L2 Action: Read from Data Cache (L1 data)
        product_data = data_cache.get("amazon", asin)
        price = 0
        if product_data and "price" in product_data:
            price = product_data["price"]
            logger.info(f"Retrieved price from cache for {asin}: {price}")
        
        if price > 0:
            margin = (price - cost) / price
            return [TextContent(type="text", text=json.dumps({
                "asin": asin,
                "price": price,
                "cost": cost,
                "margin": round(margin, 2),
                "source": "data_cache"
            }))]
        
        # Fallback if no price found
        return [TextContent(type="text", text=json.dumps({"margin": 0.25, "status": "calculated_with_defaults"}))]
    elif name == "calc_fba_fee":
        return [TextContent(type="text", text=json.dumps({"fba_fee": 3.50}))]
    elif name == "estimate_cost":
        return [TextContent(type="text", text=json.dumps({"estimated_cost": 10.0}))]
    return [TextContent(type="text", text=f"Unknown tool: {name}")]

finance_tools = [
    Tool(
        name="calc_profit",
        description="Calculate product profit margin.",
        inputSchema={"type": "object", "properties": {"asin": {"type": "string"}, "estimated_cost": {"type": "number"}}, "required": ["asin", "estimated_cost"]}
    ),
    Tool(
        name="calc_fba_fee",
        description="Calculate FBA fee for a product.",
        inputSchema={"type": "object", "properties": {"asin": {"type": "string"}}, "required": ["asin"]}
    ),
    Tool(
        name="estimate_cost",
        description="Estimate the manufacturing cost of a product.",
        inputSchema={"type": "object", "properties": {"asin": {"type": "string"}}, "required": ["asin"]}
    )
]

_FINANCE_META = {
    "calc_profit": ("COMPUTE", "profit margin as decimal"),
    "calc_fba_fee": ("COMPUTE", "FBA fee in USD"),
    "estimate_cost": ("COMPUTE", "estimated manufacturing cost in USD"),
}

for tool in finance_tools:
    cat, ret = _FINANCE_META.get(tool.name, ("COMPUTE", ""))
    tool_registry.register_tool(tool, handle_finance_tool, category=cat, returns=ret)
