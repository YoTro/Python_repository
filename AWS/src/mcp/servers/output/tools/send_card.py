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
    
    elif name == "send_feishu_text":
        result = await asyncio.to_thread(feishu.send_text_message, arguments.get("receive_id_type"), arguments.get("receive_id"), arguments["text"])
    
    elif name == "send_feishu_card":
        result = await asyncio.to_thread(feishu.send_card_message, arguments.get("receive_id_type"), arguments.get("receive_id"), arguments["text"])
    
    elif name == "send_feishu_data_file":
        result = await asyncio.to_thread(
            feishu.send_data_as_file, 
            arguments.get("receive_id_type"), 
            arguments.get("receive_id"), 
            arguments["data"], 
            filename=arguments.get("filename", "export.csv")
        )
    
    elif name == "send_feishu_url_file":
        result = await asyncio.to_thread(
            feishu.send_url_as_file,
            arguments.get("receive_id_type"),
            arguments.get("receive_id"),
            arguments["url"],
            filename=arguments.get("filename", "downloaded_file")
        )
    
    elif name == "send_feishu_local_file":
        result = await asyncio.to_thread(
            feishu.send_local_file,
            arguments.get("receive_id_type"),
            arguments.get("receive_id"),
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
        name="send_feishu_text",
        description="Sends a simple text message to a Feishu chat. If receive_id or receive_id_type are not provided, it will attempt to use the chat_id from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Plain text content for the message"},
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"], "description": "Optional. The type of receive ID (e.g., 'open_id', 'chat_id'). If not provided, attempts to resolve from context."},
                "receive_id": {"type": "string", "description": "Optional. The ID of the recipient (user or chat group). If not provided, attempts to resolve from context."}
            },
            "required": ["text"]
        }
    ),
    Tool(
        name="send_feishu_card",
        description="Sends a formatted card message to a Feishu chat. If receive_id or receive_id_type are not provided, it will attempt to use the chat_id from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Markdown formatted content for the card message"},
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"], "description": "Optional. The type of receive ID (e.g., 'open_id', 'chat_id'). If not provided, attempts to resolve from context."},
                "receive_id": {"type": "string", "description": "Optional. The ID of the recipient (user or chat group). If not provided, attempts to resolve from context."}
            },
            "required": ["text"]
        }
    ),
    Tool(
        name="send_feishu_data_file",
        description="Converts a list of dictionaries into a CSV file and sends it to a Feishu chat. If receive_id or receive_id_type are not provided, it will attempt to use the chat_id from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {"type": "object"}, "description": "List of dictionaries to be converted to CSV"},
                "filename": {"type": "string", "description": "Optional custom name for the CSV file (e.g., 'report.csv')"},
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"], "description": "Optional. The type of receive ID (e.g., 'open_id', 'chat_id'). If not provided, attempts to resolve from context."},
                "receive_id": {"type": "string", "description": "Optional. The ID of the recipient (user or chat group). If not provided, attempts to resolve from context."}
            },
            "required": ["data"]
        }
    ),
    Tool(
        name="send_feishu_url_file",
        description="Downloads a file from a given URL and sends it as an attachment to a Feishu chat. If receive_id or receive_id_type are not provided, it will attempt to use the chat_id from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL of the file to download and send"},
                "filename": {"type": "string", "description": "Optional custom name for the attachment"},
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"], "description": "Optional. The type of receive ID (e.g., 'open_id', 'chat_id'). If not provided, attempts to resolve from context."},
                "receive_id": {"type": "string", "description": "Optional. The ID of the recipient (user or chat group). If not provided, attempts to resolve from context."}
            },
            "required": ["url"]
        }
    ),
    Tool(
        name="send_feishu_local_file",
        description="Uploads a local file and sends it as an attachment to a Feishu chat. If receive_id or receive_id_type are not provided, it will attempt to use the chat_id from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Local path to the file to send"},
                "filename": {"type": "string", "description": "Optional custom name for the attachment"},
                "receive_id_type": {"type": "string", "enum": ["open_id", "user_id", "union_id", "email", "chat_id"], "description": "Optional. The type of receive ID (e.g., 'open_id', 'chat_id'). If not provided, attempts to resolve from context."},
                "receive_id": {"type": "string", "description": "Optional. The ID of the recipient (user or chat group). If not provided, attempts to resolve from context."}
            },
            "required": ["file_path"]
        }
    )
]
