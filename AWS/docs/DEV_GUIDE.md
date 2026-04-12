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
2.  **Normalization**: Map heterogeneous inputs into a `UnifiedRequest` DTO. Always set `entry_type` (e.g., `"feishu_workflow"`) and `chat_id` so the rate limiter can track per-chat concurrency.
3.  **Callback Binding**: Inject a `CallbackConfig` (e.g., `type="slack_message"`) so the system knows where to return results.
4.  **Rate Limit Config**: Add a matching entry under `entry_limits` in `config/settings.json` (section `rate_limits`) with `concurrent_jobs`, `per_chat_concurrent`, and `cooldown_seconds`.

**Rate Limiting — Three Layers (`src/gateway/rate_limit.py`):**

| Layer | Config Key | Enforced At | Purpose |
|-------|-----------|-------------|---------|
| 1a — Cooldown | `entry_limits.<type>.cooldown_seconds` | `check_limit()` before dispatch | Debounce Feishu double-clicks |
| 1b — Concurrency | `entry_limits.<type>.concurrent_jobs` / `per_chat_concurrent` | `concurrent_slot()` inside `_run_job` | Prevent slot deadlock via `try/finally` |
| 2 — Quota | `tenant_quotas.<tier>.daily_requests` | `check_limit()` before dispatch | Per-tenant daily budget |
| 3 — Token Bucket | `source_limits.<name>.requests_per_minute` / `burst` | `acquire_source()` in each API client | Protect external API accounts |

**How to add a new external API source:**
1.  Add an entry to `source_limits` in `config/settings.json` (section `rate_limits`).
2.  Call `RateLimiter().acquire_source("<name>")` at the top of the client's `_request()` method.
3.  Add 429 exponential backoff using `time.sleep(2 ** attempt + jitter)` in the retry loop.

### Layer 3: Orchestration (`src/workflows/` & `src/agents/`)
*The "Brains". Deciding HOW to solve the problem.*

**Track A: Deterministic Workflows**
1.  **Define**: Create `src/workflows/definitions/my_flow.py`.
2.  **Register**: Use `@WorkflowRegistry.register("name")` and ensure it's imported in `definitions/__init__.py`.
3.  **Steps**: Compose using `EnrichStep` (fetching), `FilterStep` (logic), or `ProcessStep` (AI reasoning).
4.  **Context Access**: The `EnrichStep` passes `ctx: WorkflowContext` to your `extractor_fn`, enabling it to call other MCP tools (e.g., `calc_profit`) securely.

**Track B: Exploratory Agents**
1.  **System Prompt**: Edit the human-readable Markdown template in `src/agents/prompts/mcp_agent_system.md`.
2.  **Constraints**: Adjust `token_budget` or `max_steps` in the Agent's session config.

### Layer 4: Capabilities & Tools (`src/mcp/servers/`)
*The "Hands". Where the actual work (scraping, calculating) happens.*

**A. Adding a New Scraper (Amazon Domain)**
1.  Place script in `src/mcp/servers/amazon/extractors/`.
2.  Inherit from `AmazonBaseScraper` for built-in proxy and cookie support.
3.  **High-Efficiency Alternative**: Use `ProfitabilitySearchExtractor` to fetch `price`, `weight`, `dimensions`, and `bsr_rank` in a single request, bypassing heavy HTML parsing.

**B. Adding a New MCP Tool**
1.  **Logic**: Implement an `async` handler in the relevant domain server.
2.  **Definition**: Create a `mcp.types.Tool` object with a precise description (essential for LLM planning).
3.  **Registry**: Call `tool_registry.register_tool(tool, handler, category="DATA", returns="...")` in the domain's `tools.py`.
4.  **Discovery**: Ensure the domain's `tools.py` is imported in `src/registry/tools.py`.

### Layer 5: Intelligence Routing & Prompt Management (`src/intelligence/`)
*Cost-aware LLM Dispatching & Centralized Knowledge.*

1.  **Heuristics**: Add high-speed rules to `_run_heuristics` in `src/intelligence/router/` to bypass LLM classification for simple tasks.
2.  **Pricing**: Update `PriceManager` JSON configs if model costs change.
3.  **Prompt Management (SSOT)**:
    *   **Roles**: Add new expert personas in `src/intelligence/prompts/config/roles.yaml`.
    *   **Frameworks**: Define analysis models (e.g., PSI, SWOT) in `src/intelligence/prompts/config/frameworks.yaml`. Use `$variable` syntax to inject values from `config/workflow_defaults.yaml`.
    *   **Templates**: Define output structures in `src/intelligence/prompts/config/templates.yaml`.
    *   **Usage**: Access via `PromptManager` singleton to ensure Agent and Workflow consistency.
4.  **Processors**: Implement complex AI logic as specialized processors that the orchestrators can call.
    *   `CategoryMonopolyAnalyzer.analyze()` accepts an optional `historical_data: Dict[str, List[Dict]]` (ASIN → daily records from `XiyouZhaociAPI.get_asin_daily_trends()`). When supplied it enables two additional dimensions:
        *   `market_churn` — detects predatory competition, lemon-market, and rating-attack patterns from BSR/rating time series.
        *   `seasonality` — detrended, log-BSR seasonality score with circular peak-month detection and platform-event (Prime Day / Black Friday) dampening.

### Layer 6: Output & Callbacks (`src/jobs/callbacks/`)
*Delivery of the final value.*

1.  **Implement**: Subclass `BaseCallback` in `src/jobs/callbacks/`.
2.  **Progress**: Implement `on_progress` to send real-time "thinking" cards/messages.
3.  **Error with resume hint**: Implement `on_error(self, error, job_id=None)`. When `job_id` is provided a checkpoint exists — surface it to the user so they can resume (e.g., print a command, send a Feishu message).
4.  **Factory**: Register your type in `CallbackFactory.create()`.
5.  **Targeting**: Use `ContextPropagator` to automatically resolve `feishu_chat_id` or similar platform IDs without passing them through every function.

**Feishu Bot Commands (`src/entry/feishu/commands.py`):**

| Pattern | Command Class | Description |
|---|---|---|
| `更新亚马逊 Cookies` | `RefreshCookieCommand` | Re-launches browser to refresh Amazon session |
| `恢复任务 <job_id>` | `ResumeJobCommand` | Resumes a failed workflow from its last checkpoint |
| `获取 <Category> BSR` | `ExtractBSRCommand` | Kicks off BSR extraction workflow |
| `分析垄断度 <URL>` | `AnalyzeCategoryMonopolyCommand` | Starts category monopoly analysis |
| *(fallback)* | `AgentExploreCommand` | Routes to MCP Agent for open-ended exploration |

**Adding a new bot command:**
1. Subclass `BotCommand`, implement `match(text)` and `execute(text, chat_id)`.
2. Register **before** `AgentExploreCommand` in `CommandDispatcher.__init__` (it is a catch-all fallback and must stay last).

### Layer 7: Interactive Signals (`src/jobs/interactions/`)
*Handling asynchronous human-in-the-loop actions (e.g., QR login, manual approval).*

**How to add a new Interactive Action (e.g., Keepa Login):**
1.  **Signal Output**: In your MCP Tool, return an `INTERACTION_REQUIRED` JSON signal specifying the `interaction_type` and `ui_config` (title, button_text, action_name).
2.  **Capability Negotiation**: Define required capabilities (e.g., `IMAGE_DISPLAY`) in the signal so Callbacks can degrade gracefully (e.g., showing a URL link in CLI instead of rendering a card).
3.  **Register Handler**: Create a handler function in `src/jobs/interactions/handlers.py` and decorate it with `@InteractionRegistry.register("YOUR_ACTION_NAME")`.
4.  **Resume**: Ensure your handler logic calls `get_job_manager().resume(job_id)` upon successful validation.

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
5.  **Rate Limiting** (37 tests, all three layers):
    ```bash
    export PYTHONPATH=$PYTHONPATH:. && .venv311/bin/python3 -m unittest tests/test_rate_limiting_system.py -v
    ```

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
