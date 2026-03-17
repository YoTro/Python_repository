import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-social")

async def handle_social_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "tiktok_search":
        return [TextContent(type="text", text=json.dumps({"views": 1000000, "trend": "rising"}))]
    elif name == "meta_ad_search":
        return [TextContent(type="text", text=json.dumps({"active_ads": 15}))]
    elif name == "social_score":
        return [TextContent(type="text", text=json.dumps({"score": 85, "verdict": "high virality"}))]
    return [TextContent(type="text", text=f"Unknown tool: {name}")]

social_tools = [
    Tool(
        name="tiktok_search",
        description="Search TikTok trends for a product keyword.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    ),
    Tool(
        name="meta_ad_search",
        description="Search Meta Ad Library for active ads.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    ),
    Tool(
        name="social_score",
        description="Calculate a composite social virality score.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    )
]

_SOCIAL_META = {
    "tiktok_search": ("DATA", "view count and trend direction"),
    "meta_ad_search": ("DATA", "active ad count"),
    "social_score": ("COMPUTE", "composite virality score 0-100"),
}

for tool in social_tools:
    cat, ret = _SOCIAL_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_social_tool, category=cat, returns=ret)
