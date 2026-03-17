from __future__ import annotations
import json
import logging
from mcp.types import Tool, TextContent

logger = logging.getLogger("mcp-output-csv")

async def handle_export_csv(name: str, arguments: dict) -> list[TextContent]:
    # Placeholder: In the future, this will upload to S3/OSS and return a link
    return [TextContent(type="text", text=json.dumps({"success": True, "url": "http://mock-storage.com/export.csv", "message": "Stub link returned"}, indent=2))]

tools = [
    Tool(
        name="export_csv",
        description="Export items to a CSV file and upload to object storage.",
        inputSchema={
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "object"}},
                "filename": {"type": "string"}
            },
            "required": ["items"]
        }
    )
]
