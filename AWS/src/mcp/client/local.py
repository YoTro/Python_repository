from __future__ import annotations
import json
import logging
from typing import Any, Dict, List
from mcp.types import Tool, TextContent

from src.mcp.client.base import MCPClient
from src.registry.tools import tool_registry
from src.registry.resources import resource_registry

logger = logging.getLogger(__name__)

class LocalMCPClient(MCPClient):
    """
    In-process MCP Client. 
    Directly calls registered handlers for speed and simplicity in single-user mode.
    """
    
    async def list_tools(self) -> List[Tool]:
        return tool_registry.get_all_tools()

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        logger.debug(f"Local MCP Client calling tool: {name}")
        return await tool_registry.call_tool(name, arguments)

    async def list_resources(self) -> List[Any]:
        return resource_registry.get_all_resources()

    async def read_resource(self, uri: str) -> str:
        return resource_registry.read_resource(uri)

    async def call_tool_json(self, name: str, arguments: Dict[str, Any]) -> Any:
        """Helper to call tool and parse JSON result if text."""
        results = await self.call_tool(name, arguments)
        if not results:
            return None
        
        # Assuming first content is the main result
        content = results[0]
        if hasattr(content, 'text'):
            try:
                return json.loads(content.text)
            except json.JSONDecodeError:
                return content.text
        return content
