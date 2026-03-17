from __future__ import annotations
import json
import logging
import asyncio
from mcp.types import Tool, TextContent
from src.entry.feishu.client import FeishuClient

logger = logging.getLogger("mcp-output-messaging")
feishu = FeishuClient()

async def handle_send_card(name: str, arguments: dict) -> list[TextContent]:
    """
    Handles messaging tools for multiple platforms.
    Currently supports Feishu.
    """
    if name == "send_feishu_webhook":
        result = await asyncio.to_thread(feishu.send_webhook_message, arguments["webhook_url"], arguments["text"])
    
    elif name == "send_feishu_message":
        result = await asyncio.to_thread(feishu.send_text_message, arguments["receive_id_type"], arguments["receive_id"], arguments["text"])
    
    elif name == "send_feishu_data_file":
        result = await asyncio.to_thread(
            feishu.send_data_as_file, 
            arguments["receive_id_type"], 
            arguments["receive_id"], 
            arguments["data"], 
            filename=arguments.get("filename", "export.csv")
        )
    
    elif name == "send_feishu_url_file":
        result = await asyncio.to_thread(
            feishu.send_url_as_file,
            arguments["receive_id_type"],
            arguments["receive_id"],
            arguments["url"],
            filename=arguments.get("filename", "downloaded_file")
        )
    
    elif name == "send_feishu_local_file":
        result = await asyncio.to_thread(
            feishu.send_local_file,
            arguments["receive_id_type"],
            arguments["receive_id"],
            arguments["file_path"],
            filename=arguments.get("filename")
        )
    
    # FUTURE: elif name == "send_dingtalk_card": ...
    
    else:
        raise ValueError(f"Unknown tool: {name}")

    return [TextContent(type="text", text=json.dumps(result, indent=2, ensure_ascii=False))]

tools = [
    Tool(
        name="send_feishu_webhook",
        description="Send a text message via a Feishu Webhook URL (Custom Bot).",
        inputSchema={
            "type": "object",
            "properties": {
                "webhook_url": {"type": "string"},
                "text": {"type": "string"}
            },
            "required": ["webhook_url", "text"]
        }
    ),
    Tool(
        name="send_feishu_message",
        description="Send a text message to a Feishu user or group chat.",
        inputSchema={
            "type": "object",
            "properties": {
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"]},
                "receive_id": {"type": "string"},
                "text": {"type": "string"}
            },
            "required": ["receive_id_type", "receive_id", "text"]
        }
    ),
    Tool(
        name="send_feishu_data_file",
        description="Convert data (list of dicts) into a CSV file and send it directly to a Feishu chat.",
        inputSchema={
            "type": "object",
            "properties": {
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"]},
                "receive_id": {"type": "string"},
                "data": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "List of dictionaries representing the table data"
                },
                "filename": {"type": "string", "default": "export.csv"}
            },
            "required": ["receive_id_type", "receive_id", "data"]
        }
    ),
    Tool(
        name="send_feishu_url_file",
        description="Download a file from a URL and send it as a Feishu file attachment.",
        inputSchema={
            "type": "object",
            "properties": {
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"]},
                "receive_id": {"type": "string"},
                "url": {"type": "string", "description": "The URL of the file to download"},
                "filename": {"type": "string", "description": "Optional name for the file (extension will be guessed if missing)"}
            },
            "required": ["receive_id_type", "receive_id", "url"]
        }
    ),
    Tool(
        name="send_feishu_local_file",
        description="Upload a local file and send it as a Feishu file attachment.",
        inputSchema={
            "type": "object",
            "properties": {
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"]},
                "receive_id": {"type": "string"},
                "file_path": {"type": "string", "description": "Local path to the file"},
                "filename": {"type": "string", "description": "Optional custom name for the attachment"}
            },
            "required": ["receive_id_type", "receive_id", "file_path"]
        }
    )
]
