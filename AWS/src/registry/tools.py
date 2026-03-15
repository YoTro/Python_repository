from __future__ import annotations
from typing import Callable, Awaitable, List, Dict
from mcp.types import Tool, TextContent
from src.mcp.exceptions import ToolNotFoundError

class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, Tool] = {}
        self._handlers: Dict[str, Callable[[str, dict], Awaitable[List[TextContent]]]] = {}

    def register_tool(self, tool: Tool, handler: Callable[[str, dict], Awaitable[List[TextContent]]]):
        self._tools[tool.name] = tool
        self._handlers[tool.name] = handler

    def get_all_tools(self) -> List[Tool]:
        return list(self._tools.values())

    async def call_tool(self, name: str, arguments: dict) -> List[TextContent]:
        if name not in self._handlers:
            raise ToolNotFoundError(
                message=f"Tool '{name}' is not registered.",
                hint="Use list_tools to see available tools."
            )
        return await self._handlers[name](name, arguments)

# Singleton instance
tool_registry = ToolRegistry()

# Absolute imports for sub-modules to trigger registration
from src.mcp.servers.amazon import tools as amazon
from src.mcp.servers.output import tools as feishu
from src.mcp.servers.market import tools as agents
from src.mcp.servers.finance import tools as finance
from src.mcp.servers.compliance import tools as compliance
from src.mcp.servers.social import tools as social
