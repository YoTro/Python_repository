import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-market")

async def handle_market_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "seller_analysis":
        return [TextContent(type="text", text=json.dumps({"us_seller_percentage": 0.62, "market_concentration": "high"}))]
    elif name == "keyword_data":
        return [TextContent(type="text", text=json.dumps({"search_volume": 150000, "cpc_bid": 1.25}))]
    elif name == "get_ad_traffic":
        return [TextContent(type="text", text=json.dumps({"ad_spend": 5000, "roas": 2.1}))]
    return [TextContent(type="text", text=f"Unknown tool: {name}")]

market_tools = [
    Tool(
        name="seller_analysis",
        description="Analyze seller demographics for a category or keyword.",
        inputSchema={"type": "object", "properties": {"category_url": {"type": "string"}}, "required": ["category_url"]}
    ),
    Tool(
        name="keyword_data",
        description="Get search volume and CPC data for a keyword.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    ),
    Tool(
        name="get_ad_traffic",
        description="Get advertising traffic estimates for an ASIN.",
        inputSchema={"type": "object", "properties": {"asin": {"type": "string"}}, "required": ["asin"]}
    )
]

for tool in market_tools:
    tool_registry.register_tool(tool, handle_market_tool)
