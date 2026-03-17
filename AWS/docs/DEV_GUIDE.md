# Developer Guide

This guide provides essential information for developers to understand, maintain, and extend the AWS V2 Hybrid Intelligence Platform. It reflects the **Domain-Driven Design (DDD)** and **Dual Orchestration** architecture.

## 1. Environment Setup

1.  **Python Version**: Python 3.11+ is required.
2.  **Virtual Environment**: 
    ```bash
    python3.11 -m venv venv311
    source venv311/bin/activate
    ```
3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    # For local development/testing
    pip install pytest pytest-asyncio
    ```
4.  **Configuration (`.env`)**:
    ```text
    GEMINI_API_KEY=your_key
    ANTHROPIC_API_KEY=your_key
    DEFAULT_LLM_PROVIDER=gemini
    FEISHU_AMAZON_BOT_APP_ID=...
    ```

## 2. Core Coding Standards

*   **Type Safety**: Use `from __future__ import annotations` and strict type hinting.
*   **Domain Isolation**: Implementation logic must stay within its domain server (`src/mcp/servers/<domain>/`).
*   **Async First**: All I/O, including Tool calls and Scrapers, MUST be `async`.
*   **Pydantic Contracts**: Use models in `src/core/models/` for all data exchange.
*   **No Cross-Imports**: Workflow steps and Agents should **never** import extractors directly. Use the `MCPClient` provided in the context.

## 3. Extending capabilities

### A. Adding a New Scraper (Amazon Domain)
1.  Place your script in `src/mcp/servers/amazon/extractors/`.
2.  Inherit from `src.core.scraper.AmazonBaseScraper`.
3.  Expose it as an MCP Tool in `src/mcp/servers/amazon/tools.py`.

### B. Adding a New Workflow Definition
1.  Create a file in `src/workflows/definitions/` (e.g., `market_report.py`).
2.  Use the `@WorkflowRegistry.register("name")` decorator.
3.  Compose your logic using `ProcessStep`, `FilterStep`, or `EnrichStep`.
4.  **Important**: To call tools, use `ctx.mcp.call_tool_json("tool_name", arguments)`.

### C. Adding a New MCP Tool
1.  Locate the relevant domain in `src/mcp/servers/`.
2.  In `tools.py`, define the `Tool` object (Name, Description, InputSchema).
3.  Register with metadata:
    ```python
    tool_registry.register_tool(
        tool, handler,
        category="DATA",      # DATA | COMPUTE | FILTER | OUTPUT
        returns="list of products with ASIN and price",  # shown to LLM
    )
    ```
    The `category` controls grouping in the agent's system prompt. The `returns` description helps the LLM plan which tools to chain.
4.  If it's a new domain (e.g., `advertising`), create the folder and register its import in `src/registry/tools.py`.

### D. Modifying the Agent System Prompt
The agent prompt is a 3-layer system — you should rarely need to touch code:
1.  **Edit the template**: `src/agents/prompts/mcp_agent_system.md` — human-readable Markdown with `$tool_catalog` and `$token_budget` variables.
2.  **Tool catalog is auto-generated**: `tool_catalog_formatter.py` reads `ToolMeta` from the registry and groups tools into DATA/COMPUTE/FILTER/OUTPUT sections.
3.  **Assembly**: `prompt_builder.py` loads the `.md` template and injects the catalog via `string.Template`.

To add a new execution phase or constraint, edit the `.md` file directly — no code changes needed.

### F. Using Telemetry & ETA
When writing a new Feishu command or Workflow step:
*   **Static ETA**: Update `TimeEstimator` in `src/core/telemetry/tracker.py` with your baseline.
*   **Dynamic Progress**: Ensure your workflow step calls `callback.on_progress()` to trigger the `TelemetryTracker` moving-average calculation.

## 4. Testing Protocols

### Import Integrity
Every time you move files or add imports, run:
```bash
venv311/bin/pytest tests/test_imports.py
```

### Full Flow Simulation
To test the bridge between Feishu, the Workflow Engine, and MCP Tools without hitting real APIs:
```bash
venv311/bin/pytest tests/test_feishu_full_flow.py -s
```

## 5. Directory Mapping (Quick Reference)

*   `src/core/`: The "Kernel". Scrapers, Telemetry, Models, Utils (proxy, config, cookies, CSV, parser, account helpers).
*   `src/mcp/servers/`: The "Capabilities". Where the actual work happens.
*   `src/workflows/`: The "Industrial Orchestrator". Deterministic batching.
*   `src/agents/`: The "Intelligent Orchestrator". ReAct-based exploration with cloud token budgeting.
    *   `src/agents/prompts/`: 3-layer system prompt (`.md` template + builder + catalog formatter).
*   `src/entry/`: The "Gates". CLI and Bot listeners.
