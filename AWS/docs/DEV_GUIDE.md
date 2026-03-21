# Developer Guide: Architectural Flow & Extension

This guide reflects the **Domain-Driven Design (DDD)** and **Dual Orchestration** architecture of the AWS V2 Platform. It is organized according to the data flow of a request.

---

## 1. Environment Setup

1.  **Python Version**: Python 3.11+ required.
2.  **Environment**: 
    ```bash
    python3.11 -m venv venv311
    source venv311/bin/activate
    pip install -r requirements.txt
    ```
3.  **Config**: Populate `.env` with `GEMINI_API_KEY`, `ANTHROPIC_API_KEY`, and `FEISHU_*` credentials.

---

## 2. Request Flow: Development by Layer

### Layer 1: Entry Points (`src/entry/`)
*The "Gates". Where requests first hit the system.*

**How to add a new channel (e.g., Slack/Discord Bot):**
1.  **Adapter**: Create `src/entry/<channel>/` to listen for webhooks or socket events.
2.  **Parse**: Identify if the message is a `workflow` command or a natural language `intent` for the Agent.
3.  **Identify**: Extract the platform-specific `chat_id` and `user_id`.
4.  **Forward**: Call the appropriate `APIGateway` dispatcher.

### Layer 2: API Gateway (`src/gateway/`)
*Identity Resolution, Rate Limiting, and Normalization.*

**How to register a new entry point:**
1.  **Dispatch Method**: Add `dispatch_<channel>_command` to `APIGateway` in `src/gateway/router.py`.
2.  **Normalization**: Map heterogeneous inputs into a `UnifiedRequest` DTO.
3.  **Callback Binding**: Inject a `CallbackConfig` (e.g., `type="slack_message"`) so the system knows where to return results.

### Layer 3: Orchestration (`src/workflows/` & `src/agents/`)
*The "Brains". Deciding HOW to solve the problem.*

**Track A: Deterministic Workflows**
1.  **Define**: Create `src/workflows/definitions/my_flow.py`.
2.  **Register**: Use `@WorkflowRegistry.register("name")` and ensure it's imported in `definitions/__init__.py`.
3.  **Steps**: Compose using `EnrichStep` (fetching), `FilterStep` (logic), or `ProcessStep` (AI reasoning).

**Track B: Exploratory Agents**
1.  **System Prompt**: Edit the human-readable Markdown template in `src/agents/prompts/mcp_agent_system.md`.
2.  **Constraints**: Adjust `token_budget` or `max_steps` in the Agent's session config.

### Layer 4: Capabilities & Tools (`src/mcp/servers/`)
*The "Hands". Where the actual work (scraping, calculating) happens.*

**A. Adding a New Scraper (Amazon Domain)**
1.  Place script in `src/mcp/servers/amazon/extractors/`.
2.  Inherit from `AmazonBaseScraper` for built-in proxy and cookie support.

**B. Adding a New MCP Tool**
1.  **Logic**: Implement an `async` handler in the relevant domain server.
2.  **Definition**: Create a `mcp.types.Tool` object with a precise description (essential for LLM planning).
3.  **Registry**: Call `tool_registry.register_tool(tool, handler, category="DATA", returns="...")` in the domain's `tools.py`.
4.  **Discovery**: Ensure the domain's `tools.py` is imported in `src/registry/tools.py`.

### Layer 5: Intelligence Routing (`src/intelligence/`)
*Cost-aware LLM Dispatching.*

1.  **Heuristics**: Add high-speed rules to `_run_heuristics` in `src/intelligence/router/` to bypass LLM classification for simple tasks.
2.  **Pricing**: Update `PriceManager` JSON configs if model costs change.
3.  **Processors**: Implement complex AI logic (e.g., `MonopolyAnalyzer`) as specialized processors that the orchestrators can call.

### Layer 6: Output & Callbacks (`src/jobs/callbacks/`)
*Delivery of the final value.*

1.  **Implement**: Subclass `BaseCallback` in `src/jobs/callbacks/`.
2.  **Progress**: Implement `on_progress` to send real-time "thinking" cards/messages.
3.  **Factory**: Register your type in `CallbackFactory.create()`.
4.  **Targeting**: Use `ContextPropagator` to automatically resolve `feishu_chat_id` or similar platform IDs without passing them through every function.

---

## 3. Engineering Standards

*   **Async First**: All I/O MUST be `async`.
*   **DDD Isolation**: Domain logic stays in `src/mcp/servers/<domain>/`. No cross-domain imports.
*   **Pydantic Contracts**: Use models in `src/core/models/` for all data exchange.
*   **L1/L2 Split**: L1 (Scrapers) write to `DataCache`; L2 (Calculators/Output) read from `DataCache`.

---

## 4. Testing Protocols

1.  **Import Integrity**: `pytest tests/test_imports.py` (Prevents circular deps).
2.  **Logic Validation**: `pytest tests/test_core_utils.py` etc.
3.  **Full-Flow Simulation**: `pytest tests/test_feishu_full_flow.py -s` (Mocks external APIs but runs full Gateway -> Job -> MCP loop).
4.  **LLM Routing**: `pytest tests/test_gemini_advanced_pricing.py`.

---

## 5. Directory Mapping (Summary)

*   `src/core/`: Kernel, Models, Telemetry, and shared Utils (Proxy, Cookies, Context).
*   `src/entry/`: Entry adapters (CLI, Feishu, etc.).
*   `src/gateway/`: Auth, Rate Limiting, and Unified Dispatching.
*   `src/jobs/`: Job management, Checkpoints, and Callbacks.
*   `src/mcp/servers/`: Microservices providing specific tools.
*   `src/registry/`: The central hub for Tool, Resource, and Prompt discovery.
*   `src/intelligence/`: LLM Providers, Routing, and AI Processors.
*   `src/workflows/`: Sequential, deterministic engine.
*   `src/agents/`: Autonomous, LLM-driven reasoning.
