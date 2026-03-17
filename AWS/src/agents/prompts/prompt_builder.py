"""Build the MCP Agent system prompt from the .md template + live tool catalog."""
from __future__ import annotations

import os
import string
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.registry.tools import ToolRegistry

_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "mcp_agent_system.md")


class PromptBuilder:
    """Loads the .md template once, then renders with runtime variables."""

    def __init__(self, template_path: str | None = None):
        path = template_path or _TEMPLATE_PATH
        with open(path, "r", encoding="utf-8") as f:
            self._template = string.Template(f.read())

    def build(self, registry: "ToolRegistry", *, max_steps: int = 15, token_budget: int = 50000) -> str:
        from src.agents.prompts.tool_catalog_formatter import format_tool_catalog

        catalog = format_tool_catalog(registry)
        return self._template.safe_substitute(
            tool_catalog=catalog,
            max_steps=str(max_steps),
            token_budget=str(token_budget),
        )
