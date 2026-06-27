from __future__ import annotations

import logging
from typing import Any

from mcp.types import TextContent, Tool
from src.mcp.client.base import MCPClient
from src.registry.resources import resource_registry
from src.registry.tools import tool_registry

logger = logging.getLogger(__name__)


class LocalMCPClient(MCPClient):
    """
    In-process MCP Client.
    Directly calls registered handlers for speed and simplicity in single-user mode.
    """

    async def list_tools(self) -> list[Tool]:
        return tool_registry.get_all_tools()

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        logger.debug(f"Local MCP Client calling tool: {name}")
        return await tool_registry.call_tool(name, arguments)

    async def list_resources(self) -> list[Any]:
        return resource_registry.get_all_resources()

    async def read_resource(self, uri: str) -> str:
        return resource_registry.read_resource(uri)

    # call_tool_json is inherited from MCPClient (transport-agnostic, built on call_tool).
