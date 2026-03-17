from __future__ import annotations
import json
import logging
import asyncio
from mcp.types import Tool, TextContent
from src.entry.feishu.client import FeishuClient

logger = logging.getLogger("mcp-output-bitable")
feishu = FeishuClient()

async def handle_write_bitable(name: str, arguments: dict) -> list[TextContent]:
    user_token = arguments.get("user_access_token")
    
    if name == "list_feishu_bitable_records":
        result = await asyncio.to_thread(
            feishu.list_bitable_records,
            arguments["app_token"], 
            arguments["table_id"], 
            view_id=arguments.get("view_id"), 
            page_size=arguments.get("page_size", 20), 
            user_access_token=user_token
        )
    elif name == "add_feishu_bitable_record":
        result = await asyncio.to_thread(
            feishu.add_bitable_record,
            arguments["app_token"], 
            arguments["table_id"], 
            arguments["fields"], 
            user_access_token=user_token
        )
    elif name == "update_feishu_bitable_record":
        result = await asyncio.to_thread(
            feishu.update_bitable_record,
            arguments["app_token"], 
            arguments["table_id"], 
            arguments["record_id"], 
            arguments["fields"], 
            user_access_token=user_token
        )
    elif name == "get_feishu_bitable_record":
        result = await asyncio.to_thread(
            feishu.get_bitable_record,
            arguments["app_token"], 
            arguments["table_id"], 
            arguments["record_id"], 
            user_access_token=user_token
        )
    elif name == "delete_feishu_bitable_record":
        result = await asyncio.to_thread(
            feishu.delete_bitable_record,
            arguments["app_token"], 
            arguments["table_id"], 
            arguments["record_id"], 
            user_access_token=user_token
        )
    elif name == "list_feishu_bitable_tables":
        result = await asyncio.to_thread(feishu.list_bitable_tables, arguments["app_token"], user_access_token=user_token)
    elif name == "create_feishu_bitable":
        result = await asyncio.to_thread(feishu.create_bitable, arguments["name"], folder_token=arguments.get("folder_token"), user_access_token=user_token)
    elif name == "copy_feishu_bitable":
        result = await asyncio.to_thread(feishu.copy_bitable, arguments["app_token"], arguments["name"], folder_token=arguments.get("folder_token"), user_access_token=user_token)
    elif name == "create_feishu_bitable_field":
        result = await asyncio.to_thread(feishu.create_bitable_field, arguments["app_token"], arguments["table_id"], arguments["field_name"], field_type=arguments.get("field_type", 1), user_access_token=user_token)
    elif name == "batch_update_feishu_bitable_records":
        result = await asyncio.to_thread(
            feishu.batch_update_bitable_records,
            arguments["app_token"],
            arguments["table_id"],
            arguments["updates"],
            user_access_token=user_token
        )
    elif name == "populate_feishu_bitable_records":
        result = await asyncio.to_thread(
            feishu.populate_bitable_records,
            arguments["app_token"],
            arguments["table_id"],
            arguments["records"],
            user_access_token=user_token
        )
    else:
        raise ValueError(f"Unknown tool: {name}")

    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]

tools = [
    Tool(
        name="list_feishu_bitable_records",
        description="List records from a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "view_id": {"type": "string"},
                "page_size": {"type": "integer", "default": 20},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id"]
        }
    ),
    Tool(
        name="add_feishu_bitable_record",
        description="Add a new record to a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "fields": {"type": "object"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "fields"]
        }
    ),
    Tool(
        name="update_feishu_bitable_record",
        description="Update an existing record in a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "record_id": {"type": "string"},
                "fields": {"type": "object"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "record_id", "fields"]
        }
    ),
    Tool(
        name="get_feishu_bitable_record",
        description="Get a single record from a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "record_id": {"type": "string"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "record_id"]
        }
    ),
    Tool(
        name="delete_feishu_bitable_record",
        description="Delete a record from a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "record_id": {"type": "string"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "record_id"]
        }
    ),
    Tool(
        name="list_feishu_bitable_tables",
        description="List all tables in a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token"]
        }
    ),
    Tool(
        name="create_feishu_bitable",
        description="Create a new Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "folder_token": {"type": "string"},
                "user_access_token": {"type": "string"}
            },
            "required": ["name"]
        }
    ),
    Tool(
        name="copy_feishu_bitable",
        description="Create a copy of an existing Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "name": {"type": "string"},
                "folder_token": {"type": "string"},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "name"]
        }
    ),
    Tool(
        name="create_feishu_bitable_field",
        description="Create a new field in a Bitable table.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "field_name": {"type": "string"},
                "field_type": {"type": "integer", "default": 1},
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "field_name"]
        }
    ),
    Tool(
        name="batch_update_feishu_bitable_records",
        description="Batch update multiple records in a Feishu Bitable.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "updates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "record_id": {"type": "string"},
                            "fields": {"type": "object"}
                        },
                        "required": ["record_id", "fields"]
                    }
                },
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "updates"]
        }
    ),
    Tool(
        name="populate_feishu_bitable_records",
        description="Efficiently populate a new table starting from row 1 by reusing default empty rows.",
        inputSchema={
            "type": "object",
            "properties": {
                "app_token": {"type": "string"},
                "table_id": {"type": "string"},
                "records": {
                    "type": "array",
                    "items": {"type": "object", "description": "Fields for each record"}
                },
                "user_access_token": {"type": "string"}
            },
            "required": ["app_token", "table_id", "records"]
        }
    )
]
