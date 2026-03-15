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
    )
]
