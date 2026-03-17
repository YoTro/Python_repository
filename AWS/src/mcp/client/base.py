from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from mcp.types import Tool, TextContent

class MCPClient(ABC):
    """
    Abstract interface for an MCP Client.
    Workflows use this to call tools without knowing if they are local or remote.
    """
    
    @abstractmethod
    async def list_tools(self) -> List[Tool]:
        """List all available tools."""
        pass

    @abstractmethod
    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        """Call a tool by name."""
        pass

    @abstractmethod
    async def list_resources(self) -> List[Any]:
        """List available resources."""
        pass

    @abstractmethod
    async def read_resource(self, uri: str) -> str:
        """Read a resource by URI."""
        pass
