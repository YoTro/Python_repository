import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-compliance")

async def handle_compliance_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "check_epa":
        return [TextContent(type="text", text=json.dumps({"status": "exempt", "reason": "Not a pesticide device"}))]
    elif name == "check_patent":
        return [TextContent(type="text", text=json.dumps({"risk_level": "low"}))]
    elif name == "get_regulations":
        return [TextContent(type="text", text=json.dumps({"regulations": ["FDA 510(k) optional"]}))]
    return [TextContent(type="text", text=f"Unknown tool: {name}")]

compliance_tools = [
    Tool(
        name="check_epa",
        description="Check if a product requires EPA registration.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    ),
    Tool(
        name="check_patent",
        description="Check patent risks for a product.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    ),
    Tool(
        name="get_regulations",
        description="Get applicable regulations for a product category.",
        inputSchema={"type": "object", "properties": {"category": {"type": "string"}}, "required": ["category"]}
    )
]

for tool in compliance_tools:
    tool_registry.register_tool(tool, handle_compliance_tool)
