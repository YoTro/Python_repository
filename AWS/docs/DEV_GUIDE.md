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
1.  **Create the Logic**: Create a file in `src/workflows/definitions/` (e.g., `market_report.py`).
2.  **Define the Builder**: Use the `@WorkflowRegistry.register("name")` decorator on a function that returns a list of steps.
3.  **Trigger Registration**: **CRITICAL**: Import your new module in `src/workflows/definitions/__init__.py`. Without this, the decorator will not execute, and the workflow will not be registered.
4.  **Compose Steps**: Use `ProcessStep`, `FilterStep`, or `EnrichStep`.
    *   To call tools, use `ctx.mcp.call_tool_json("tool_name", arguments)`.
5.  **Validation**: Run `python main.py --list-workflows` (if supported) or verify the registration via unit tests.

### C. Adding a New MCP Tool
1.  **Locate the Domain**: Decide which server the tool belongs to (e.g., `src/mcp/servers/finance/`). If it's a new domain, create a new folder.
2.  **Define the Logic and Handler**:
    *   Write your core logic within that domain.
    *   Implement an `async` handler function that accepts `name: str` and `arguments: dict` as parameters.
    *   This handler function should execute your core logic and return a `list[mcp.types.TextContent]`.
    *   *Example Signature*: `async def my_tool_handler(name: str, arguments: dict) -> list[mcp.types.TextContent]:`
3.  **Define the MCP Tool Object**: In the domain's `tools.py` (or sub-module), instantiate a `mcp.types.Tool` object with a unique `name`, a clear `description` (crucial for LLMs), and an `inputSchema` (JSON Schema).
4.  **Register the Tool**: Call `tool_registry.register_tool` to link your tool object with its handler function and metadata.
    ```python
    tool_registry.register_tool(
        tool, handler,
        category="DATA",      # DATA | COMPUTE | FILTER | OUTPUT
        returns="list of products with ASIN and price",  # shown to LLM
    )
    ```
    The `category` controls grouping in the agent's system prompt. The `returns` description helps the LLM plan which tools to chain.
5.  **Import the Tools**: If it's a new domain (e.g., `advertising`), create the folder and ensure its `tools.py` is imported in `src/registry/tools.py` to make the tools discoverable.

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

### G. Improving Router Intelligence
The `IntelligenceRouter` uses a hybrid classification system to minimize cloud costs.
1.  **Adding Heuristic Rules**: Edit `_run_heuristics` in `src/intelligence/router/__init__.py`. Add keywords or length constraints in the prioritized order (Complexity -> Intent -> Constraint).
2.  **Managing Logs**: All classification inputs are logged to `data/intelligence/raw_prompts.jsonl`. Use these logs to identify common misclassifications and extract datasets for model distillation.
3.  **Feedback Loop**: Call `record_feedback(session_id, ground_truth)` to record corrections. This helps track misclassification rates for specific heuristic rules.

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

### H. Context Propagation (Feishu Targeting)
The system uses `src.core.utils.context.ContextPropagator` to pass session-level variables across domain boundaries.
*   **Targeting**: When a Feishu bot receives a message, it stores the `chat_id` in the context as `feishu_chat_id`.
*   **Auto-Resolution**: The `FeishuClient` and associated MCP tools (in `src/mcp/servers/output/tools/send_card.py`) automatically check this context if `receive_id` is omitted.
*   **Usage**: This allows the Agent to call `send_feishu_text(text="done")` without needing to know the user's ID, enabling a more natural "reply-to-current-chat" behavior.
