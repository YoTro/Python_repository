from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

from mcp.types import TextContent, Tool


class MCPClient(ABC):
    """
    Abstract interface for an MCP Client.
    Workflows use this to call tools without knowing if they are local or remote.
    """

    @abstractmethod
    async def list_tools(self) -> list[Tool]:
        """List all available tools."""
        pass

    @abstractmethod
    async def call_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        """Call a tool by name."""
        pass

    async def call_tool_json(self, name: str, arguments: dict[str, Any]) -> Any:
        """
        Transport-agnostic convenience: call a tool and parse its first
        TextContent as JSON (falls back to raw text on decode error).

        Built on the abstract ``call_tool`` so every client — local or remote —
        inherits it. In-process callers should prefer this over ``call_tool``.
        """
        results = await self.call_tool(name, arguments)
        if not results:
            return None
        content = results[0]
        if hasattr(content, "text"):
            try:
                return json.loads(content.text)
            except json.JSONDecodeError:
                return content.text
        return content

    @abstractmethod
    async def list_resources(self) -> list[Any]:
        """List available resources."""
        pass

    @abstractmethod
    async def read_resource(self, uri: str) -> str:
        """Read a resource by URI."""
        pass
