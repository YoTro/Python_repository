import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-lingxing")


async def handle_lingxing_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle Lingxing ERP tool calls."""
    # TODO: wire to LingxingClient for real data
    if name == "lingxing_inventory":
        return [TextContent(type="text", text=json.dumps({"status": "stub", "tool": name}))]
    return [TextContent(type="text", text=f"Unknown tool: {name}")]


lingxing_tools = [
    Tool(
        name="lingxing_inventory",
        description="Query inventory and order data from Lingxing ERP.",
        inputSchema={
            "type": "object",
            "properties": {"sku": {"type": "string"}},
            "required": ["sku"],
        },
    ),
]

for tool in lingxing_tools:
    tool_registry.register_tool(tool, handle_lingxing_tool, category="DATA", returns="inventory and order data")
