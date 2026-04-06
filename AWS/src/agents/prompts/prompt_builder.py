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
        from src.intelligence.prompts.manager import prompt_manager

        catalog = format_tool_catalog(registry)
        
        # Pull standard components from SSOT
        role_def = prompt_manager.get_role("senior_strategist")
        fws = prompt_manager.get_frameworks(["psi_benchmarking", "sentiment_analysis", "strategic_analysis"])
        std_output = prompt_manager.get_template("standard_report")

        return self._template.safe_substitute(
            role_definition=role_def,
            tool_catalog=catalog,
            max_steps=str(max_steps),
            token_budget=str(token_budget),
            analysis_frameworks=fws,
            output_standard=std_output
        )
