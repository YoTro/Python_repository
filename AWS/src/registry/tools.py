from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Callable, Awaitable, List, Dict, Optional
from mcp.types import Tool, TextContent
from src.mcp.exceptions import ToolNotFoundError

logger = logging.getLogger(__name__)


@dataclass
class ToolMeta:
    """Extra metadata for a registered tool (not on the MCP Tool object)."""
    category: str = "DATA"       # DATA | COMPUTE | FILTER | OUTPUT
    returns: str = ""            # short description of what the tool returns


class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, Tool] = {}
        self._handlers: Dict[str, Callable[[str, dict], Awaitable[List[TextContent]]]] = {}
        self._meta: Dict[str, ToolMeta] = {}

    def register_tool(
        self,
        tool: Tool,
        handler: Callable[[str, dict], Awaitable[List[TextContent]]],
        *,
        category: str = "DATA",
        returns: str = "",
    ):
        self._tools[tool.name] = tool
        self._handlers[tool.name] = handler
        self._meta[tool.name] = ToolMeta(category=category, returns=returns)

    def get_all_tools(self) -> List[Tool]:
        return list(self._tools.values())

    def get_tool_meta(self, name: str) -> Optional[ToolMeta]:
        return self._meta.get(name)

    def get_tools_by_category(self, category: str) -> List[Tool]:
        return [t for t in self._tools.values() if self._meta.get(t.name, ToolMeta()).category == category]

    def _validate_arguments(self, name: str, arguments: dict) -> dict:
        """Strip unknown arguments and log a warning. Returns cleaned args."""
        tool = self._tools.get(name)
        if not tool or not tool.inputSchema:
            return arguments

        schema_props = tool.inputSchema.get("properties", {})
        allowed_keys = set(schema_props.keys())
        
        unknown = set(arguments.keys()) - allowed_keys
        if unknown:
            logger.warning(
                f"Tool '{name}' received unknown arguments {unknown}, "
                f"expected one of {allowed_keys}. Stripping unknown args."
            )
            return {k: v for k, v in arguments.items() if k in allowed_keys}
        return arguments

    async def call_tool(self, name: str, arguments: dict) -> List[TextContent]:
        if name not in self._handlers:
            raise ToolNotFoundError(
                message=f"Tool '{name}' is not registered.",
                hint="Use list_tools to see available tools."
            )
            
        # 1. Uniformly handle metadata: Extract and set context
        metadata = arguments.pop("_metadata", {})
        if metadata:
            from src.core.utils.context import ContextPropagator
            # Propagate metadata keys (tenant_id, user_id, job_id, chat_id) to contextvars
            for key, value in metadata.items():
                if value is not None:
                    ContextPropagator.set(key, value)
                    
        # 2. Validate and clean business arguments (now free of _metadata)
        cleaned = self._validate_arguments(name, arguments)
        
        # 3. Call handler with pure business arguments
        return await self._handlers[name](name, cleaned)

# Singleton instance
tool_registry = ToolRegistry()

# Absolute imports for sub-modules to trigger registration
from src.mcp.servers.amazon import tools as amazon
from src.mcp.servers.output import tools as feishu
from src.mcp.servers.market import tools as market
from src.mcp.servers.lingxing import tools as lingxing
from src.mcp.servers.finance import tools as finance
from src.mcp.servers.compliance import tools as compliance
from src.mcp.servers.social import tools as social
