from __future__ import annotations
import logging
from mcp.types import TextContent
from src.registry.tools import tool_registry

# Import sub-handlers
from .write_bitable import handle_write_bitable, tools as bitable_tools
from .send_card import handle_send_card, tools as messaging_tools
from .create_doc import handle_create_doc, tools as doc_tools
from .export_csv import handle_export_csv, tools as csv_tools
from .export_json import handle_export_json, tools as json_tools

logger = logging.getLogger("mcp-output-aggregator")

async def handle_output_tool(name: str, arguments: dict) -> list[TextContent]:
    """
    Main aggregator for the Output domain.
    Dispatches tool calls to specific sub-modules.
    """
    try:
        if "bitable" in name:
            return await handle_write_bitable(name, arguments)
        elif "message" in name or "webhook" in name or "card" in name:
            return await handle_send_card(name, arguments)
        elif "doc" in name:
            return await handle_create_doc(name, arguments)
        elif "csv" in name:
            return await handle_export_csv(name, arguments)
        elif "json" in name:
            return await handle_export_json(name, arguments)
        else:
            return [TextContent(type="text", text=f"Output domain could not route tool: {name}")]
    except Exception as e:
        logger.error(f"Error in output domain routing for {name}: {e}")
        import json
        return [TextContent(type="text", text=json.dumps({"success": False, "error": str(e)}))]

# Aggregate and Register all tools
all_output_tools = bitable_tools + messaging_tools + doc_tools + csv_tools + json_tools

_OUTPUT_RETURNS = {
    "list_feishu_bitable_records": "list of Bitable records",
    "add_feishu_bitable_record": "created record ID",
    "update_feishu_bitable_record": "updated record confirmation",
    "get_feishu_bitable_record": "single Bitable record",
    "delete_feishu_bitable_record": "deletion confirmation",
    "list_feishu_bitable_tables": "list of tables in a Bitable app",
    "create_feishu_bitable": "new Bitable app URL and ID",
    "copy_feishu_bitable": "copied Bitable app URL and ID",
    "create_feishu_bitable_field": "created field confirmation",
    "send_feishu_webhook": "webhook delivery confirmation",
    "send_feishu_message": "message delivery confirmation",
    "create_feishu_doc": "new document URL",
    "export_csv": "local CSV file path",
    "export_json": "local JSON file path",
}

for tool in all_output_tools:
    ret = _OUTPUT_RETURNS.get(tool.name, "")
    tool_registry.register_tool(tool, handle_output_tool, category="OUTPUT", returns=ret)
