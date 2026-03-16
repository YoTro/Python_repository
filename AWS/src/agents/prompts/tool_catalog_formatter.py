"""Format registered tools into a categorized catalog string for the system prompt."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.registry.tools import ToolRegistry


_CATEGORY_ORDER = ["DATA", "COMPUTE", "FILTER", "OUTPUT"]
_CATEGORY_LABELS = {
    "DATA": "Data Collection",
    "COMPUTE": "Computation / Analysis",
    "FILTER": "Filtering / Compliance",
    "OUTPUT": "Output / Export",
}


def format_tool_catalog(registry: "ToolRegistry") -> str:
    """
    Group all registered tools by category and return a human-readable catalog.

    Example output:
        ## Data Collection
        - **search_products** — Search Amazon by keyword …
          Input: {"keyword": "string", "page": "integer"}
          Returns: list of products with ASIN, title, price
    """
    from src.registry.tools import ToolMeta

    # Bucket tools by category
    buckets: dict[str, list[str]] = {cat: [] for cat in _CATEGORY_ORDER}

    for tool in registry.get_all_tools():
        meta = registry.get_tool_meta(tool.name) or ToolMeta()
        cat = meta.category if meta.category in buckets else "DATA"

        props = tool.inputSchema.get("properties", {}) if tool.inputSchema else {}
        required = set(tool.inputSchema.get("required", [])) if tool.inputSchema else set()

        # Build compact param summary
        param_parts = []
        for pname, pschema in props.items():
            ptype = pschema.get("type", "any")
            marker = " (required)" if pname in required else ""
            param_parts.append(f"{pname}: {ptype}{marker}")
        params_str = ", ".join(param_parts) if param_parts else "none"

        entry = f"- **{tool.name}** — {tool.description}\n"
        entry += f"  Input: {{{params_str}}}\n"
        if meta.returns:
            entry += f"  Returns: {meta.returns}\n"

        buckets[cat].append(entry)

    # Render
    sections = []
    for cat in _CATEGORY_ORDER:
        entries = buckets[cat]
        if not entries:
            continue
        label = _CATEGORY_LABELS.get(cat, cat)
        sections.append(f"## {label}\n\n" + "\n".join(entries))

    return "\n\n".join(sections)
