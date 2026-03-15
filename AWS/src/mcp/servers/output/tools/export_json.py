from __future__ import annotations
import json
import logging
from mcp.types import Tool, TextContent

logger = logging.getLogger("mcp-output-json")

async def handle_export_json(name: str, arguments: dict) -> list[TextContent]:
    # Placeholder: Perform a POST request to a callback URL
    return [TextContent(type="text", text=json.dumps({"success": True, "message": "JSON POST callback simulated"}, indent=2))]

tools = [
    Tool(
        name="export_json",
        description="Send results as JSON to a POST callback URL.",
        inputSchema={
            "type": "object",
            "properties": {
                "callback_url": {"type": "string"},
                "payload": {"type": "object"}
            },
            "required": ["callback_url", "payload"]
        }
    )
]
