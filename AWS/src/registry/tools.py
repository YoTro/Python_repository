from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from mcp.types import TextContent, Tool

from src.mcp.exceptions import ToolNotFoundError

logger = logging.getLogger(__name__)


@dataclass
class ToolMeta:
    """Extra metadata for a registered tool (not on the MCP Tool object)."""

    category: str = "DATA"  # DATA | COMPUTE | FILTER | OUTPUT
    returns: str = ""  # short description of what the tool returns


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, Tool] = {}
        self._handlers: dict[str, Callable[[str, dict], Awaitable[list[TextContent]]]] = {}
        self._meta: dict[str, ToolMeta] = {}

    def register_tool(
        self,
        tool: Tool,
        handler: Callable[[str, dict], Awaitable[list[TextContent]]],
        *,
        category: str = "DATA",
        returns: str = "",
    ):
        self._tools[tool.name] = tool
        self._handlers[tool.name] = handler
        self._meta[tool.name] = ToolMeta(category=category, returns=returns)

    def get_all_tools(self) -> list[Tool]:
        return list(self._tools.values())

    def get_tool_meta(self, name: str) -> ToolMeta | None:
        return self._meta.get(name)

    def get_tools_by_category(self, category: str) -> list[Tool]:
        return [
            t
            for t in self._tools.values()
            if self._meta.get(t.name, ToolMeta()).category == category
        ]

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

    def get_openai_schemas(self) -> list[dict]:
        """Return all registered tools in OpenAI function-calling schema format."""
        schemas = []
        for tool in self._tools.values():
            schemas.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": (tool.description or "")[:1024],
                        "parameters": tool.inputSchema or {"type": "object", "properties": {}},
                    },
                }
            )
        return schemas

    async def call_tool(self, name: str, arguments: dict) -> list[TextContent]:
        if name not in self._handlers:
            raise ToolNotFoundError(
                message=f"Tool '{name}' is not registered.",
                hint="Use list_tools to see available tools.",
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

# ── Tool discovery ────────────────────────────────────────────────────────────
# Importing each domain's tools module executes its module-level
# `tool_registry.register_tool(...)` calls as an import side effect.  These imports
# MUST stay at the bottom of this file, *after* `tool_registry` is defined: each
# domain module does `from src.registry.tools import tool_registry` at import time,
# so the singleton has to exist before they are imported (otherwise a partial-module
# circular import).  Add a new domain server here to make its tools discoverable to
# both the Agent track (LocalMCPClient.list_tools) and the MCP server.
import src.mcp.servers.amazon.tools  # noqa: E402,F401
import src.mcp.servers.compliance.tools  # noqa: E402,F401
import src.mcp.servers.erp.tools  # noqa: E402,F401
import src.mcp.servers.finance.tools  # noqa: E402,F401
import src.mcp.servers.market.tools  # noqa: E402,F401
import src.mcp.servers.output.tools  # noqa: E402,F401
import src.mcp.servers.social.tools  # noqa: E402,F401
