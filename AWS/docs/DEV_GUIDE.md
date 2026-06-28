# Developer Guide: Architectural Flow & Extension

This guide reflects the **Domain-Driven Design (DDD)** and **Dual Orchestration** architecture of the AWS V2 Platform. It is organized according to the data flow of a request.

---

## 1. Environment Setup

1.  **Python Version**: Python 3.11+ required.
2.  **Environment**:
    ```bash
    python3.11 -m venv venv311
    source venv311/bin/activate
    pip install -e .
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

**CLI Entry Point (`src/entry/cli/main.py`, run via root `main.py`):**

The CLI is the simplest channel — **synchronous and callback-free**. Use it as the reference for the dispatch contract and for local development.

*Argument contract* — argparse, with **exactly one** mode required (mutually exclusive group):

| Argument | Mode | Effect |
|---|---|---|
| `--workflow <name>` | workflow | route to `dispatch_cli_workflow` |
| `--explore "<query>"` | agent | route to `dispatch_cli_explore` |
| `--list-workflows` | utility | print `WorkflowRegistry.list_workflows()` and exit |
| `--refresh-cookies` | utility | open a browser to refresh `config/cookies.json` and exit |
| `--params '<json>'` | modifier | JSON string for the workflow (default `"{}"`); used **only** with `--workflow`. Invalid JSON → log error + `exit(1)` |

*Routing:* `--workflow` → `run_workflow()` → `APIGateway.dispatch_cli_workflow(name, params)` (`entry_type="cli_workflow"`); `--explore` → `run_explore()` → `APIGateway.dispatch_cli_explore(intent)` (`entry_type="cli_explore"`). The two `entry_type`s map to separate rate-limit buckets. Both call `job_mgr.submit_and_wait(request)` — i.e. the CLI **blocks** until the job reaches a terminal state, unlike Feishu which returns a `job_id` immediately.

*Callback preset:* **none.** The CLI dispatchers build a `UnifiedRequest` with `callback=None`, so no `JobCallback` is invoked. Output is consumed directly from the `submit_and_wait` return value: `run_workflow` logs the item count and returns the `WorkflowResult`; `run_explore` prints `result["message"]` to stdout. This is the deliberate contrast with Feishu (`CallbackConfig(type="feishu_bitable"/"feishu_card", target=chat_id)` for async delivery) — a synchronous channel reads the result itself rather than registering a callback. To add structured CLI output, prefer consuming the returned `WorkflowResult` in `main.py` over introducing a callback.

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
4.  **Context Access**: The `EnrichStep` passes `ctx: WorkflowContext` to your `extractor_fn`, enabling it to call MCP tools (e.g., `calc_profit`) via `ctx.mcp.call_tool_json()`. Never import handler functions from `tools.py` directly — `ctx.mcp.call_tool_json()` is the only legitimate call path. See `docs/MCP_PROTOCOL.md` §4 for the full rationale and examples.

**Track B: Exploratory Agents**
1.  **System Prompt**: Edit the human-readable Markdown template in `src/agents/prompts/mcp_agent_system.md`.
2.  **Constraints**: Adjust `token_budget` or `max_steps` in the Agent's session config.

For the agent's internal contracts — session persistence, tool access, cost tracking, the loop's finalization paths, and the long-report attachment policy — see **§15**.

### Layer 4: Capabilities & Tools (`src/mcp/servers/`)
*The "Hands". Where the actual work (scraping, calculating) happens.*

**A. Adding a New Scraper (Amazon Domain)**
1.  Place script in `src/mcp/servers/amazon/extractors/`.
2.  Inherit from `AmazonBaseScraper` for built-in proxy and cookie support.
3.  **High-Efficiency Alternative**: Use `ProfitabilitySearchExtractor` to fetch `price`, `weight`, `dimensions`, and `bsr_rank` in a single request, bypassing heavy HTML parsing.
4.  **Multi-Account Pool Integration**: `AmazonBaseScraper.fetch()` accepts an optional `_session` keyword argument. When `CookieBrowserPool` is active, pass `slot.session` via `_session=slot.session` to route each request through a specific slot's `curl_cffi.AsyncSession` while still applying the shared rate limiter. Scrapers that do not pass `_session` continue to use the default single-account session. This is the consumer side of the identity pool — see **§9** for the full runtime model (slot selection, isolation, circuit breaker, and the `_session` invocation contract in §9.4).

**B. Adding a New MCP Tool**
1.  **Logic**: Implement an `async` handler in the relevant domain server.
2.  **Definition**: Create a `mcp.types.Tool` object with a precise description (essential for LLM planning).
3.  **Registry**: Call `tool_registry.register_tool(tool, handler, category="DATA", returns="...")` in the domain's `tools.py`.
4.  **Discovery**: Add `import src.mcp.servers.<domain>.tools` to the import block at the **bottom** of `src/registry/tools.py` (a *new domain only* — existing domains are already listed). That import side-effect is the single registration trigger; without it the tool is invisible to the Agent track and the MCP server. See §12.

**C. Adding a New ERP Provider (Strategy Pattern)**

The ERP layer (`src/mcp/servers/erp/`) uses a provider registry so new ERP systems can be added without modifying existing code.

1.  **Create a subpackage**: `src/mcp/servers/erp/<name>/` with `__init__.py` and `client.py`.
2.  **Implement `ERPClient`**:
    ```python
    from ..base import ERPClient
    class MyERPClient(ERPClient):
        def get_inventory(self, sku): ...
        def get_purchase_orders(self, sku=None, status=None, **kwargs): ...
        def get_sales_orders(self, sku=None, days=30, **kwargs): ...
    ```
3.  **Register**: In your `__init__.py`:
    ```python
    from .client import MyERPClient
    from ..registry import register_provider
    register_provider("myerp", MyERPClient)
    ```
4.  **Auto-load**: Import your subpackage in `src/mcp/servers/erp/__init__.py`:
    ```python
    from . import myerp  # triggers register_provider
    ```
5.  **Use**: Pass `provider="myerp"` to any `erp_*` MCP tool call. The `get_erp_client("myerp")` registry instantiates `MyERPClient` on demand.

Config keys for the Lingxing provider:

| Key | Default | Description |
|---|---|---|
| `LINGXING_ACCOUNT` | — | Lingxing ERP login account (env var) |
| `LINGXING_PASSWORD` | — | Lingxing ERP login password (env var) |
| Token persisted at | `config/lingxing_token.json` | Auto-refreshed on 401 |

### Layer 5: Intelligence Routing & Prompt Management (`src/intelligence/`)
*Cost-aware LLM Dispatching & Centralized Knowledge.*

1.  **Heuristics**: Add high-speed rules to `_run_heuristics` in `src/intelligence/router/` to bypass LLM classification for simple tasks.
2.  **Pricing**: Update `PriceManager` JSON configs if model costs change.
2b. **Adding a new LLM provider**: subclass `BaseLLMProvider` and register it in `ProviderFactory` — see **§16** for the full interface contract (`LLMResponse`, `create_response` cost population, batch, error mapping, truncation).
3.  **Prompt Management (SSOT)**:
    *   **Roles**: Add new expert personas in `src/intelligence/prompts/config/roles.yaml`.
    *   **Frameworks**: Define analysis models (e.g., PSI, SWOT) in `src/intelligence/prompts/config/frameworks.yaml`. Use `$variable` syntax to inject values from `config/workflow_defaults.yaml`.
    *   **Templates**: Define output structures in `src/intelligence/prompts/config/templates.yaml`.
    *   **Usage**: Access via `PromptManager` singleton to ensure Agent and Workflow consistency.
4.  **Processors**: Implement complex AI logic as specialized processors that the orchestrators can call.
    *   `CategoryMonopolyAnalyzer.analyze()` accepts an optional `historical_data: Dict[str, List[Dict]]` (ASIN → daily records from `XiyouZhaociAPI.get_asin_daily_trends()`). When supplied it enables two additional dimensions:
        *   `market_churn` — detects predatory competition, lemon-market, and rating-attack patterns from BSR/rating time series.
        *   `seasonality` — detrended, log-BSR seasonality score with circular peak-month detection and platform-event (Prime Day / Black Friday) dampening.

### Adding a New Intelligence Processor

Processors live in `src/intelligence/processors/`. There are two distinct kinds — choose based on whether the processor calls an LLM.

#### Pure-Algorithm Processors (no I/O)

Fully deterministic Python computation. No base class, no provider injection.

```python
class MyScorer:
    SOME_THRESHOLD = 0.5   # constants at class level

    def calculate(self, data: list[dict]) -> dict:
        ...
```

Examples: `SocialViralityProcessor`, `SalesEstimator`, `ProductSimilarityProcessor`.

#### AI-Backed Processors (call LLM)

Inject `BaseLLMProvider` via `__init__` — explicit dependency injection, directly mockable in tests.

```python
from src.intelligence.providers.base import BaseLLMProvider

EMPTY_RESULT: dict = { ... }   # module-level fallback — always define one

class MyAnalyzer:
    def __init__(self, provider: BaseLLMProvider) -> None:
        self.provider = provider

    async def analyze(self, data: list[str], ...) -> dict:
        if not data:
            return EMPTY_RESULT.copy()
        try:
            response = await self.provider.generate_text(prompt)   # unstructured → parse JSON
            # OR: await self.provider.generate_structured(prompt, schema=MyModel)  # Pydantic output
            ...
        except Exception as e:
            logger.warning(f"Analysis failed: {e}")
        return EMPTY_RESULT.copy()
```

**`generate_text` vs `generate_structured`:**

| Method | Use when | Returns |
|---|---|---|
| `generate_text(prompt)` | Output is a free-form dict — parse JSON manually | `LLMResponse` → `.text` |
| `generate_structured(prompt, schema=MyModel)` | Output is a fixed Pydantic schema | `LLMResponse` → coerce to model |

Always wrap the LLM call in `try/except` and return `EMPTY_RESULT.copy()` on any error. Never let a provider failure propagate to the caller.

Examples: `CommentAnalyzer`, `ReviewSummarizer`.

#### Registration (required for all processors)

1. Add import to `src/intelligence/processors/__init__.py`
2. Add name to `__all__`

#### Call-site patterns

| Caller | Pattern |
|---|---|
| MCP tool (`tools.py`) | `MyAnalyzer(provider=ProviderFactory.get_provider()).analyze(...)` |
| Workflow step | `MyAnalyzer(provider=provider).analyze(...)` where `provider` is from workflow context |

Import: `from src.intelligence.providers.factory import ProviderFactory`

Never instantiate `IntelligenceRouter` inside a processor or as a substitute for `ProviderFactory` in a tool. `IntelligenceRouter` is for task routing in the agent track; processors receive an already-resolved provider.

### Layer 5.5: Job Lifecycle & Durable Execution (`src/jobs/`)
*The runtime that keeps a job alive across failures, restarts, and minute-to-hour async waits.*

This layer is what Layers 6 (callbacks) and 7 (interactions) plug into — they reference `job_id`, checkpoints, and `resume()`, all of which are defined here. `ARCHITECTURE.md` carries the conceptual diagrams of these components; this section is the practical "how it behaves / how to extend it" companion.

**Components:**

| Component | File | Role |
|---|---|---|
| `JobManager` | `src/jobs/manager/__init__.py` | Queue + worker pool; owns the job state machine and both resume paths. Singleton via `get_job_manager()`. |
| `CheckpointManager` | `src/jobs/checkpoint/__init__.py` | Per-job JSON file holding a step snapshot **and** an append-only event log. |
| `ActivityRunner` | `src/workflows/engine/activity_runner.py` | Per-step execution wrapper: idempotent replay, batch suspend/resume, heartbeats. |
| `BatchPoller` | `src/jobs/batch_poller.py` | 60 s background loop that polls provider batch jobs and resumes suspended workflows. |
| `WorkflowSignalBus` | `src/jobs/signals.py` | In-process pub/sub decoupling "batch done" detection from job resumption. |

#### 1. Job State Machine

`JobStatus` (PENDING · RUNNING · COMPLETED · FAILED · SUSPENDED · CANCELLED). Transitions live in `JobManager._run_job`:

| From | Event | To |
|---|---|---|
| — | `submit()` / `resume_from_checkpoint()` | PENDING (queued) |
| PENDING | worker picks up job, acquires `concurrent_slot` | RUNNING |
| RUNNING | workflow/agent returns normally | COMPLETED |
| RUNNING | `BatchPendingError` (step submitted a provider batch) | SUSPENDED — `suspend_reason="batch"`, far backstop timeout |
| RUNNING | `JobSuspendedError` (agent human-in-the-loop) | SUSPENDED — `suspend_reason="interaction"`, timeout from the exception |
| RUNNING | `RetryableError` / `RuntimeError` / any other `Exception` | FAILED |
| FAILED / SUSPENDED | `resume(job_id)` | PENDING (requeued) |
| SUSPENDED | reaper detects `now - suspended_at > suspend_timeout_sec` | CANCELLED |

The reaper (`_reaper_loop` → `_cancel_expired_suspended`, 60 s) cancels **SUSPENDED** jobs past their per-job timeout and notifies the callback via `on_error`. The cancellation policy is **reason-aware** (`JobRecord.suspend_reason`): an *interaction* wait is the user's responsibility (short timeout, "no response" message), whereas a *batch* wait is the system's — its reaper timeout is only a far backstop (see §3). Concurrency-slot rejection surfaces as a `RuntimeError` containing `"concurrent limit reached"`, which `_run_job` maps to a friendly callback message rather than a raw error.

#### 2. Recovery Semantics — two distinct resume paths

These are **not** interchangeable:

- **`resume(job_id)`** — in-memory requeue (FAILED/SUSPENDED → PENDING). Used by `BatchPoller` and interaction handlers. **Requires the `JobRecord` to still exist in `_jobs`** — i.e. the process never restarted.
- **`resume_from_checkpoint(job_id)`** — rebuilds a fresh `JobRecord` from the on-disk checkpoint. Use this after a process restart or when the in-memory record is gone; `workflow_name` and `params` are loaded from the checkpoint automatically (pass them only to override). This backs the Feishu `恢复任务 <job_id>` command.

Idempotent replay makes resumption safe to repeat:
- `WorkflowEngine` loads the checkpoint and skips every step with index `<= checkpoint.step_index` (resumes at `step_index + 1`), restoring `items` and `ctx_cache`.
- `ActivityRunner` replays an `ACTIVITY_COMPLETED` event's cached result instead of re-calling the API.

#### 3. SUSPENDED / Batch Handling — the full loop

```
Step raises BatchPendingError
  → ActivityRunner writes BATCH_SUBMITTED (handle + reconstruction payload), re-raises
  → WorkflowEngine propagates
  → JobManager: status = SUSPENDED
        (BatchPoller already started in JobManager.__init__)
  → BatchPoller 60 s tick: scans checkpoints, finds steps with
        BATCH_SUBMITTED and no BATCH_COMPLETED
  → backoff gate: BATCH_POLLING_HEARTBEAT carries next_poll_at / current_interval
        (60 s → ×1.5 → cap 600 s, ±10% jitter; survives restarts)
  → provider.poll_batch(handle):
        None → write BATCH_POLLING_HEARTBEAT, keep waiting
        dict → reconstruct items, write BATCH_COMPLETED
  → SignalBus.publish(job_id) → JobManager.resume(job_id)
  → worker re-runs job; ActivityRunner sees BATCH_COMPLETED,
        writes ACTIVITY_COMPLETED, returns result, workflow continues
```

**Timeouts — single owner per wait type, no premature kill:**
- A batch wait is bounded by the **provider 24 h TTL** (`_GEMINI_BATCH_TTL` / `_CLAUDE_BATCH_TTL`): when the handle ages past it, BatchPoller writes `BATCH_FAILED` and cancels the job. This is the *primary* bound.
- The reaper's `suspend_timeout_sec` for a batch job is set to a **far backstop** (`_BATCH_SUSPEND_BACKSTOP_SEC`, ~25 h) that only fires if BatchPoller itself has stopped running — it must never pre-empt a batch the provider would still complete. (Interaction waits keep their own short reaper timeout.)
- **Cancel-aware completion:** if a batch job is *explicitly* cancelled mid-flight, BatchPoller discards the result. But if the in-memory record is gone (process restart), it still writes `BATCH_COMPLETED` so the result remains recoverable via `resume_from_checkpoint`.

#### 4. Checkpoint Event Write Boundaries

One `data/checkpoints/{job_id}.json` file holds **both** a step snapshot (`save()`, overwrites) and an append-only `events` log (`append_event()`, read-modify-write). Writes are split across three components — respect these boundaries:

| Write | Written by | When |
|---|---|---|
| `save()` — step snapshot (`items`, `ctx_cache`, `step_index`, resume context) | `WorkflowEngine` **only** | after each step completes |
| `clear()` | `WorkflowEngine` | on successful workflow completion |
| `append_event` → `ACTIVITY_COMPLETED` / `BATCH_SUBMITTED` / `HEARTBEAT` | `ActivityRunner` | during per-step execution |
| `append_event` → `BATCH_COMPLETED` / `BATCH_FAILED` / `BATCH_POLLING_HEARTBEAT` | `BatchPoller` | during background polling |

Rules:
- Never call `save()` from a Step or tool — only the engine snapshots step boundaries.
- `append_event` is **read-modify-write and single-process-safe only** (Ext Point: swap for Redis `LPUSH` in distributed mode). It is safe here because a SUSPENDED job's worker is idle, so only BatchPoller touches the file during the wait.

#### 5. Extending

**Make a Step participate in batch suspension:** raise `BatchPendingError` from `step.run()` carrying the reconstruction contract so `BatchPoller` can rebuild results without the worker:

```python
raise BatchPendingError(
    f"Batch '{handle.job_id}' submitted for step '{self.name}'",
    batch_job_id=handle.job_id,
    handle=handle,            # BatchJobHandle — provider, job_id, created_at
    requests=requests,        # [{custom_id, item_idx}, ...]
    items_snapshot=items,     # items to merge results back onto
    output_field="analysis",  # where each result is written
    schema_path="src.path.MySchema",  # optional Pydantic parse target
)
```
Idempotency and heartbeats are automatic — `ActivityRunner` wraps every step; no per-step code needed.

**Scaling upgrade points** (single-user → multi-user), all interface-preserving:
- **Ext Point #3** — swap `asyncio.Queue` in `JobManager` for a Redis priority queue.
- **Ext Point #7** — swap `WorkflowSignalBus`'s `asyncio.Event` for Redis Pub/Sub.
- `CheckpointManager` storage — swap local JSON for Redis + S3 behind the same `save`/`load`/`append_event` interface.

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
| `Listing诊断 <ASIN>` | `ListingDiagnosisCommand` | Runs listing quality diagnosis: deterministic module scores (title, bullets, media, social proof, A+) blended with LLM text-semantic and vision-semantic layers, keyword coverage via Xiyouzhaoci, benchmarked against competitors, delivered as a Markdown report attachment |
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
*   **Pydantic Contracts**: Use typed models for data that crosses a boundary — see **§14** for ownership, versioning, the no-ad-hoc-dict rule, and when to add a model.
*   **L1/L2 Split**: L1 (Scrapers) write to `DataCache`; L2 (Calculators/Output) read from `DataCache`.

---

## 4. Logging Guidelines

### 4.1 Module Logger Setup

Every module declares its own logger at module scope, using `__name__` as the identifier. This is the **only** logging statement a domain module ever needs:

```python
import logging
logger = logging.getLogger(__name__)
```

**Rules for domain modules** (`src/core/`, `src/mcp/`, `src/intelligence/`, `src/workflows/`, `src/agents/`, `src/jobs/`):
- Never call `logging.basicConfig()`, `addHandler()`, or `setLevel()`.
- Never configure the root logger. Configuration is the entry point's responsibility.
- The `__name__`-based hierarchy (`src.intelligence.providers.gemini`, etc.) makes namespace filtering trivial without any extra setup.

### 4.2 Entry Point Configuration

Only entry points own root logger configuration. Both existing entry points follow this pattern:

```python
# src/entry/feishu/bot_listener.py  /  src/entry/cli/main.py
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger("feishu-bot")   # named logger, not __name__
```

When adding a new entry point (`src/entry/<channel>/`), call `basicConfig` once at startup with an appropriate named logger for the channel. Do not use `__name__` for entry-point loggers — a stable name like `"feishu-bot"` or `"slack-bot"` makes log filtering easier in production.

### 4.3 Level Semantics

| Level | When to use | Examples from codebase |
|---|---|---|
| `DEBUG` | Internal state useful only during active development | Batch poll cycle state, tool routing decision, token-bucket token count |
| `INFO` | Successful operations worth recording in normal runs | Provider initialized with model + token ceiling; batch job submitted; job complete with result count |
| `WARNING` | Degraded path — operation completed but something was wrong | Response truncated at `max_tokens`; scraper retry attempt N of M; rate-limit token-bucket timeout; cookie data missing |
| `ERROR` | Operation failed; caller may handle or fall back | API fetch failed after all retries; structured generation failed; batch submission failed |
| `exception` | ERROR + full traceback | Only in `except` blocks where the stack trace adds diagnostic value and the exception is swallowed |
| `CRITICAL` | Reserved for process-level failures | Avoid in domain modules; entry points only |

**Correct:**
```python
logger.warning(
    f"[scraper] response truncated at max_tokens={max_tokens}. "
    "Search logs for 'response truncated at max' to find all occurrences."
)
logger.error(f"Failed to fetch content from {url} after {max_retries} attempts.")
```

**Incorrect:**
```python
logger.error("Something went wrong")          # no context
logger.info(f"Error: {e}")                    # wrong level
logger.warning(f"Request to {url}: {html}")  # logs raw content
```

### 4.4 What NOT to Log

Never log the following at any level, even DEBUG:

| Category | Alternatives |
|---|---|
| API keys, tokens, cookies | Log presence/absence: `"cookie loaded: %s"`, `bool(cookie)` |
| Raw HTML / full response body | Log URL, HTTP status code, and byte length |
| Full LLM prompt text | Log model name, token counts, and cost |
| Full LLM response text | Log model name, output tokens, `stop_reason` |
| User PII (names, phone numbers) | Log anonymised identifiers or counts |

### 4.5 Enabling DEBUG for a Specific Namespace

The `__name__` hierarchy means you can enable DEBUG output for one subsystem without noise from others. Do this at the entry point or in a dev script — never commit `setLevel` calls to domain modules:

```python
# Enable DEBUG for all LLM providers only
logging.getLogger("src.intelligence.providers").setLevel(logging.DEBUG)

# Enable DEBUG for a single provider
logging.getLogger("src.intelligence.providers.gemini").setLevel(logging.DEBUG)

# Enable DEBUG for the intelligence router
logging.getLogger("src.intelligence.router").setLevel(logging.DEBUG)
```

Equivalent shell one-liner for the CLI entry point — add a `LOG_LEVEL` branch in `src/entry/cli/main.py`:

```bash
LOG_LEVEL=DEBUG PYTHONPATH=. python main.py --workflow product_screening --params '{...}'
```

### 4.6 Key Log Patterns for Debugging

These are the search strings to grep in production logs for recurring issues:

| Symptom | Search pattern | Source |
|---|---|---|
| Truncated LLM report | `response truncated at max` | `claude.py`, `deepseek.py`, `gemini.py` |
| Rate-limit token bucket hit | `token-bucket timeout` | `scraper.py` |
| Scraper retry loop | `Attempt N/` | `scraper.py` |
| LLM provider init (confirm model + ceiling) | `initialized with model` | `gemini.py`, `claude.py`, `deepseek.py` |
| Batch job lifecycle | `batch submitted` / `batch complete` | `gemini.py` |
| ERP auth failure | `Lingxing` / `401` | `src/mcp/servers/erp/` |
| Cookie missing | `Failed to get cookie data` | `cookie_helper.py` |

---

## 5. Error Handling Standards

Everything error-related lives in `src/core/errors/`: the **exception hierarchy** (`exceptions.py`), the **canonical codes** (`codes.py`), and the helpers that classify and route them. All errors raised in domain modules must carry a canonical `ErrorCode` and use the typed hierarchy, so retry logic, logging, propagation, and user messaging stay consistent without scattering provider-specific strings into callers.

### 5.1 Exception Hierarchy

Every exception derives from `AWSBaseError` (carries `message`, `details`, optional `code`). Always raise the **most specific** class:

| Exception | Raise when | Retry posture |
|---|---|---|
| `ScraperError` | HTTP / TLS / network-level failure in a scraper | wrap as `RetryableError` if transient |
| `ExtractorError` | extraction / parsing failure (missing selector, malformed payload) | non-retryable — usually a site/markup change |
| `ConfigError` | missing or invalid configuration / credentials | fatal — fix config |
| `WorkflowError` | workflow-level execution failure | propagates to JobManager |
| `StepError` (⊂ `WorkflowError`) | individual step failure; carries `step_name`, `step_index` | the engine auto-wraps *unclassified* step exceptions in this |
| `RetryableError` | transient failure (rate limit, timeout, token expiry) | retried; carries `http_status`/`provider`/`retry_after_seconds`, auto-derives `code` |
| `FatalError` | non-recoverable (wrong API key, unsupported op) | never retried |
| `CheckpointError` | checkpoint save/load failure | surfaced; non-fatal to the artifact |
| `BatchPendingError` | a step submitted a provider batch and must suspend | **control-flow, not an error** — see Layer 5.5 §3 |
| `JobSuspendedError` | job needs human interaction (QR scan, approval) | **control-flow, not an error** |

`BatchPendingError` and `JobSuspendedError` are **control-flow signals**, not failures: they drive the SUSPENDED state and must never reach `on_error` (see §5.7).

### 5.2 Raising Errors

Use the exception hierarchy from `src/core/errors/`:

```python
from src.core.errors import RetryableError, FatalError, ErrorCode

# Transient — will be retried (rate limit, timeout, token expiry)
raise RetryableError(
    "Amazon Ads rate limit",
    http_status=429,          # code auto-derived → ErrorCode.RATE_LIMITED
    provider="amazon_ads",
    retry_after_seconds=60,
)

# Permanent — do not retry
raise FatalError(
    "Invalid API key",
    code=ErrorCode.AUTH_FAILED,
)
```

`RetryableError` auto-derives `code` from `http_status` + `provider` via `classify_http()`. Pass `code` explicitly only when you already know the canonical value without needing to inspect the HTTP status.

### 5.3 Classifying HTTP Responses

Apply the three classifiers in order — each refines the result of the previous one:

```python
from src.core.errors import (
    classify_http, classify_api_code, classify_response_message,
    is_retryable, default_retry_after, ErrorCode,
)

# Step 1 — map HTTP status (provider-specific overrides checked first)
code = classify_http(resp.status_code, provider="amazon_ads")

# Step 2 — refine from API-level code in response body (int or str)
api_code = body.get("code")
if api_code is not None:
    refined = classify_api_code(api_code, provider="amazon_ads")
    if refined != ErrorCode.UNKNOWN:
        code = refined

# Step 3 — refine from message text (for 401 sub-variants, overloaded 400, etc.)
if code in (ErrorCode.AUTH_TOKEN_EXPIRED, ErrorCode.INVALID_PARAMS):
    msg_code = classify_response_message(body.get("message", ""), "amazon_ads")
    if msg_code != ErrorCode.UNKNOWN:
        code = refined

# Drive retry and wait logic from the canonical code — no provider-specific checks
if is_retryable(code):
    wait = float(resp.headers.get("Retry-After", 0)) or default_retry_after(code)
    await asyncio.sleep(wait)
```

### 5.4 Adding a New Provider

When adding a new API client, extend `src/core/errors/codes.py` — do not add error-handling logic to the client itself:

| What to add | Where in `codes.py` | When to add |
|---|---|---|
| HTTP status override | `_PROVIDER_HTTP_OVERRIDES["{provider}"]` | Provider reuses standard status codes with non-standard meanings (e.g. 406 = Insufficient Funds) |
| API response code | `_API_CODE_MAP["{provider}"]` | Provider returns a numeric or string code in the response body |
| Message pattern | `_API_MESSAGE_MAP["{provider}"]` | Same HTTP status has multiple meanings distinguishable only from response text |
| New canonical code | `ErrorCode` enum | No existing code covers the failure category |

Always update the `ErrorCode` class docstring's **Sources** block to cite the new provider and its official error reference URL.

### 5.5 Retry Decision Pattern

Never compare raw HTTP status codes or provider strings at the call site:

```python
# Bad — scatters provider knowledge into every caller
if resp.status_code == 429 or body.get("code") == -999:
    retry()

# Good — all provider knowledge lives in codes.py
code = classify_http(resp.status_code, provider)
if is_retryable(code):
    retry()
```

`is_auth_error(code)` drives re-authentication (token refresh, QR login). `is_retryable(code)` drives backoff loops. `default_retry_after(code)` is the fallback floor when no `Retry-After` header is present.

### 5.6 Propagation Policy — raise low, handle high

Errors are raised at the point of failure and handled at exactly **one** terminal boundary. Do not catch-and-swallow in between, and do not build user-facing text in domain code.

| Layer | Responsibility |
|---|---|
| Domain modules (`scraper`, extractors, MCP tools, providers) | **Raise** the most specific typed exception with a `code`. Never log-and-return `None` to hide a failure; never assemble user-facing strings here. |
| Step / processor | Raise `BatchPendingError` to suspend; otherwise let exceptions propagate. |
| `WorkflowEngine` | Pass-through enricher: re-raises `BatchPendingError` and `RetryableError` untouched; calls `callback.on_error` then re-raises `FatalError`; wraps any *other* exception in `StepError` (adds `step_name`/`step_index`) and raises. A checkpoint is saved at the failing step so the job stays resumable. |
| `ActivityRunner` | Catches `BatchPendingError` only — writes `BATCH_SUBMITTED`, then re-raises. Everything else propagates. |
| `JobManager._run_job` | **The single terminal boundary.** Maps the exception to job status + callback: `BatchPendingError`→SUSPENDED (+`on_progress`), `JobSuspendedError`→SUSPENDED, concurrent-limit `RuntimeError`→friendly `notify`, `RetryableError`/any other→FAILED (+`on_error`). |

Rule of thumb: if you are writing `except` in a domain module, you are probably wrong — either let it propagate or re-raise as a typed `AWSBaseError` subclass with a `code`. The only legitimate domain-level catches are (a) translating a third-party exception into a typed one, and (b) best-effort side-effects explicitly allowed to fail (e.g. `storage.delete`, checkpoint cleanup) which log at WARNING and continue.

### 5.7 Converting Errors to Callbacks / User-Facing Messages

User-facing text is produced **only** by the callback layer (`JobCallback`), never from raw exception strings in domain code.

- **`on_error(error, job_id=None)`** is the failure → user conversion point. JobManager passes `job_id` **only when a checkpoint exists**, signalling the callback to surface a resume hint (e.g. "send `恢复任务 <job_id>`"). Translate `error.code` / `error.message` into a channel-appropriate message — never dump a raw traceback to users.
- **`notify(message)`** is for system-level notices not tied to a step or failure (rate-limit rejection, job queued). The default delegates to `on_progress`.
- **Control-flow signals are not errors:** `BatchPendingError` produces an `on_progress` "batch submitted, results will follow" message; `JobSuspendedError` produces an interaction prompt. Neither calls `on_error`.
- **Capability-aware degradation:** check `callback.capabilities` (`MARKDOWN`, `IMAGE_DISPLAY`, `INTERACTIVE_BUTTONS`, `FORM_INPUT`) before emitting rich content; fall back to plain text where unsupported (e.g. a URL instead of a rendered card in CLI).
- **Logging vs. messaging are separate audiences:** log the technical detail (code, provider, status) per §4 at the point of handling; send the human a short, actionable message. Never conflate the two.

---

## 6. Testing Protocols

1.  **Import Integrity**: `pytest tests/test_imports.py` (Prevents circular deps).
2.  **Logic Validation**: `pytest tests/test_core_utils.py` etc.
3.  **Full-Flow Simulation**: `pytest tests/test_feishu_full_flow.py -s` (Mocks external APIs but runs full Gateway -> Job -> MCP loop).
4.  **LLM Routing**: `pytest tests/test_gemini_advanced_pricing.py`.
5.  **Rate Limiting** (37 tests, all three layers):
    ```bash
    export PYTHONPATH=$PYTHONPATH:. && venv311/bin/python3 -m unittest tests/test_rate_limiting_system.py -v
    ```

---

## 7. Directory Mapping (Summary)

*   `src/core/`: Kernel, Models, Telemetry, and shared Utils (Proxy, Cookies, Context).
    *   `src/core/identity/`: **Generic multi-account identity pool** (Strategy Pattern). Manages N (session, browser) slots with circuit breakers, round-robin routing, and WAF warmup.
        *   `strategy.py` — `BaseIdentityStrategy` ABC: `warmup_url()`, `cookie_domain()`, `user_agent()`, `is_hard_block(html)`. No browser or HTTP-client types — domain policy only.
        *   `pool.py` — `IdentityPool`, `IdentitySlot`, `SlotCircuit`, Chrome helpers (`_find_free_port`, `_resolve_chrome_path`, `_resolve_headless`). Inject a strategy at `IdentityPool.init(entries, strategy)`.
        *   Domain implementations live in `src/mcp/servers/<domain>/identity.py` (e.g. `AmazonIdentityStrategy`). The `CookieBrowserPool` shim in `src/mcp/servers/amazon/cookie_pool.py` pre-wires Amazon's strategy and preserves the existing public API.
    *   `src/core/storage/`: **Storage abstraction layer** (Strategy Pattern). Swap backends via `STORAGE_BACKEND` env var — no code changes.
        *   `S3CompatibleBackend` — Cloudflare R2 / AWS S3 / MinIO (same boto3 client, different endpoint)
        *   `LocalHTTPBackend` — VPS local directory served by nginx/caddy
        *   Usage: `from src.core.storage import get_storage_backend; url = storage.upload(key, bytes, mime)`
*   `src/entry/`: Entry adapters (CLI, Feishu, etc.).
*   `src/gateway/`: Auth, Rate Limiting, and Unified Dispatching.
*   `src/jobs/`: Durable execution — `JobManager` (state machine, queue), `CheckpointManager` (snapshot + event log), `BatchPoller`, `SignalBus` (Layer 5.5), plus Callbacks and Interactions (Layers 6–7).
*   `src/mcp/servers/`: Microservices providing specific tools.
    *   `src/mcp/servers/erp/` — ERP integration layer (Strategy Pattern). Providers: `lingxing/`. Add new ERPs as sibling subpackages.
        *   `base.py` — `ERPClient` ABC defining the three required methods.
        *   `registry.py` — `register_provider` / `get_erp_client` registry.
        *   `tools.py` — MCP tools `erp_inventory`, `erp_purchase_orders`, `erp_sales_orders`.
    *   `src/mcp/servers/output/tools/export_html.py` — Converts markdown/HTML to styled HTML file; optionally uploads images via storage backend.
    *   `src/mcp/servers/output/tools/export_csv.py` — Exports records to CSV; uploads via storage backend (falls back to local file if unconfigured).
*   `src/registry/`: Central discovery hub — `tool_registry` (import side-effect), `resource_registry` (filesystem scan), `prompt_registry` (imperative). Also the tool invocation boundary (context propagation + arg validation). See §12.
*   `src/intelligence/`: LLM Providers (`providers/` — `BaseLLMProvider` contract, §16), Routing, AI Processors, and Output Parsers (`parsers/` — dirty-JSON recovery + channel sanitization, §13).
*   `src/workflows/`: Sequential, deterministic engine.
*   `src/agents/`: Autonomous, LLM-driven reasoning — ReAct loop, `AgentSession` persistence, cost tracking, finalization, attachment policy (§15).

## 8. Storage Backend (`src/core/storage/`)

Public-URL artifact storage — charts, exported HTML reports, CSVs. Strategy Pattern: a single `StorageBackend` ABC (`upload`, `upload_file`, `delete`), selected by the `STORAGE_BACKEND` env var with zero code changes. `ARCHITECTURE.md` carries the component diagram; this is the practical contract.

### 8.1 Who may call storage directly

`get_storage_backend()` is resolved by exactly three places today — the **Output domain** and **core rendering utils**:

| Caller | Purpose |
|---|---|
| `src/mcp/servers/output/tools/export_html.py` | upload report images, rewrite `<img>` paths to public URLs |
| `src/mcp/servers/output/tools/export_csv.py` | upload CSV exports |
| `src/core/utils/charts.py` (`chart_upload`) | upload chart PNGs |

**Rule:** storage is an **output / L2 concern**. L1 scrapers and L2 calculators must **not** touch storage — they write structured data to `DataCache`; only the output layer turns artifacts into public URLs. Do not confuse this with **channel delivery**: Feishu file delivery (`src/jobs/callbacks/feishu.py`, `src/entry/feishu/client.py`) uses Feishu's *own* upload API, not this backend. Reach for the storage backend only when you need a **stable public HTTPS URL** (e.g. an `<img src>` embedded in an HTML report).

### 8.2 Object key (path) rules

`key` is the object path: forward-slash separated, **no leading slash**, relative to the bucket / served-directory root. Patterns in use:

| Caller | Key pattern | Collision strategy |
|---|---|---|
| charts (`ad_diagnosis`) | `reports/ad_diagnosis/{ASIN}/{date}/{name}.png` | deterministic → re-runs overwrite (idempotent) |
| export_html images | `reports/{uuid}{ext}` | random uuid |
| export_csv | `exports/{uuid8}_{filename}.csv` | random uuid prefix |

Guidelines for new keys:
- Start with a **top-level namespace** folder (`reports/`, `exports/`, …) so artifacts stay groupable and lifecycle-manageable (see §8.4).
- Use a **deterministic** path when re-runs should overwrite (idempotent workflows); use a **uuid** when every call must be unique.
- Always sanitize a user/filename component with `os.path.basename` — never interpolate raw input into a key.
- Keys are backend-agnostic: the same key must be valid on S3 *and* local-HTTP.

### 8.3 URL rules

Every backend returns the public URL as exactly:

```
{STORAGE_PUBLIC_URL}/{key}        # STORAGE_PUBLIC_URL has its trailing slash stripped
```

This contract is what makes backends swappable: a key uploaded under one backend resolves to the same relative URL under another, provided `STORAGE_PUBLIC_URL` points at the new backend's public root. A new backend **must** preserve this `{public_url}/{key}` shape — do **not** return provider-native or signed URLs (e.g. presigned S3 links), or already-published report links break on switch. Set `content_type` correctly so browsers render inline (`image/png`, `text/csv; charset=utf-8-sig`, …).

### 8.4 Deletion semantics

`delete(key)` is **best-effort and never raises**:
- S3: `delete_object`; swallows errors → `WARNING`.
- local-HTTP: `os.remove`; silent on `FileNotFoundError`, swallows other errors → `WARNING`.
- The ABC permits a pure no-op (e.g. an immutable / CDN-fronted backend).

Implication: callers **cannot rely on deletion** for correctness or security — treat every uploaded artifact as effectively immutable and public. There is currently **no caller of `delete()`**; artifact lifecycle (TTL / cleanup) is expected to be handled out-of-band (bucket lifecycle policy, or an nginx cron for local-HTTP), keyed on the namespace prefixes in §8.2 — not by calling `delete()` on the request path.

### 8.5 Adding a new backend

1. Subclass `StorageBackend` in `src/core/storage/<name>.py` — implement `upload`, `upload_file`, `delete`. `upload` must return `{public_url}/{key}` (§8.3); `delete` must not raise (§8.4).
2. Add a branch in `get_storage_backend()` in `src/core/storage/__init__.py`. **Lazy-import** the class *inside* the branch (matching the existing pattern) so optional deps like `boto3` aren't imported unless the backend is selected.
3. Read config from env in `__init__` (constructor args optional, env fallback). Raise `KeyError` / `ValueError` when a required var is missing — `export_html` and `export_csv` catch `(ValueError, KeyError)` to **degrade gracefully** to local files, so a misconfigured backend must surface as one of those, not a custom exception.
4. Set `STORAGE_BACKEND=<name>` in `.env`. No changes to `export_html`, `export_csv`, `charts`, or any other caller.

---

## 9. Identity & Account Pool (`src/core/identity/`)

A generic multi-account pool that lets scrapers spread load across N web identities (one set of cookies + UA + proxy each), with per-slot isolation, health tracking, and WAF warmup. It is **domain-agnostic**: all site-specific policy is injected via a `BaseIdentityStrategy`, so the same pool serves Amazon, Walmart, etc.

### 9.1 Runtime model — pool, slots, tiers

- **`IdentityPool`** is a named registry: `IdentityPool.init(entries, strategy, name=…, base_port=…)` builds one pool; `get_instance(name)` retrieves it. Multiple pools coexist by `name` (e.g. `amazon_us`, `amazon_jp`) — give each a distinct `base_port` to avoid CDP port collisions on the same host.
- Each entry dict → one **`IdentitySlot`** = one identity: `{"cookies": {...}, "user_agent": "...", "proxy": "..."}` (only `cookies` required; UA falls back to `strategy.user_agent()`).
- A slot carries **two execution tiers**:
  - **HTTP tier** — an independent `curl_cffi.AsyncSession` (`slot.session`), used for lightweight/AJAX requests. Non-blocking.
  - **Browser tier** — a lazily-launched DrissionPage `ChromiumPage` (`slot.get_or_init_browser()`), for JS-heavy / bot-walled pages. Serialized per slot by `slot.browser_lock`.

### 9.2 Isolation strategy

Slots are isolated so one identity's state or failure never leaks into another:
- **Session:** each slot owns its own `AsyncSession` (separate cookie jar, UA, proxy). curl_cffi bakes the jar into the libcurl handle synchronously per request, so concurrent HTTP-tier calls on *different* slots don't interfere.
- **Browser:** each slot gets a unique `--user-data-dir` under `~/.local/share/identity_pool/slot_{id}_p{base_port}` (stable across restarts so WAF tokens survive crashes; keyed by `base_port` so two processes don't share a Chrome profile) and a deterministic CDP port (`base_port + slot_id`, auto-bumped if occupied). The tab is recycled every `_RECYCLE_AFTER=200` browser ops to release V8 heap without killing the process (preserving session cookies).
- **Concurrency:** the HTTP tier needs no lock; the browser tier requires `async with slot.browser_lock` so exactly one browser op runs per slot at a time.

### 9.3 Selection & circuit breaker (health)

- **`pool.next_slot()`** is **round-robin over healthy slots** — it scans from the current cursor and returns the first slot whose circuit is *closed*. It never blocks; if **every** circuit is open it returns the slot whose cooldown expires soonest (degrade, don't stall).
- Each slot has a **`SlotCircuit`** (`threshold=3`, `cooldown=300s`): `record_failure()` trips it open after 3 consecutive failures (skipped by `next_slot`); after cooldown it goes half-open for one trial; `record_success()` resets it. **Trip only on a confirmed identity block** (bot wall / login / `strategy.is_hard_block`), not on an empty/legitimately-missing result — otherwise healthy slots get benched for no reason.

### 9.4 Invocation boundary — the `_session` contract

This is the consumer contract; the pool is **pull-based** — scrapers opt in, the base scraper stays single-account by default.

```python
pool = CookieBrowserPool.get_instance()
slot = pool.next_slot() if pool else None          # 1. pick a healthy slot (or None → default session)

# HTTP tier: route the request through THIS slot's session
_sess = slot.session if slot else None
html = await self.fetch(url, _session=_sess)        # 2. BaseAsyncScraper.fetch(_session=…)

# Browser tier: hold the slot lock for the whole browser session
if slot is not None:
    async with slot.browser_lock:                   # 3. serialize per-slot browser use
        bp = slot.get_or_init_browser()
        ...

# Report health so routing/circuit stays accurate
slot.circuit.record_success()                       # 4a. on a clean fetch
slot.circuit.record_failure()                       # 4b. ONLY on a confirmed block
```

Rules:
- `BaseAsyncScraper.fetch(..., _session=…)` is the **only** seam: pass `slot.session` to pin a request to a slot; omit it and the scraper uses its own default single-account session. A scraper that never passes `_session` is unaffected by the pool.
- Selection (`next_slot`), browser locking, and circuit reporting are the **caller's** responsibility — the pool provides the primitives but does not wrap your scrape. See `src/mcp/servers/amazon/extractors/comments.py` for the canonical 3-tier consumer (AJAX → HTML → browser, with circuit reporting).
- Always tolerate `pool is None` / `slot is None` (pool not initialised) by falling back to the default session — every consumer must work with or without the pool active.

### 9.5 Core types

| Type | File | Role |
|---|---|---|
| `BaseIdentityStrategy` | `strategy.py` | domain policy ABC: `warmup_url()`, `cookie_domain()`, `user_agent()`, `is_hard_block(html)`. **No browser/HTTP types** — policy only. |
| `IdentityPool` | `pool.py` | named pool; `init` / `get_instance` / `next_slot`. |
| `IdentitySlot` | `pool.py` | one identity: `session`, lazy `browser` + `browser_lock`, `circuit`. |
| `SlotCircuit` | `pool.py` | per-slot breaker (closed/open/half-open). |

### 9.6 Adding a new identity domain (e.g. Walmart, Shopify)

`IdentityPool` is domain-agnostic, so no changes to `src/core/identity/` are needed:

1. Create `src/mcp/servers/<domain>/identity.py` and subclass `BaseIdentityStrategy`:
   ```python
   from src.core.identity.strategy import BaseIdentityStrategy

   class WalmartIdentityStrategy(BaseIdentityStrategy):
       def warmup_url(self) -> str: return "https://www.walmart.com/"
       def cookie_domain(self) -> str: return ".walmart.com"
       def user_agent(self) -> str: return "Mozilla/5.0 ..."
       def is_hard_block(self, html: str) -> bool: return "captcha" in html
   ```
2. Create a pool shim (e.g. `src/mcp/servers/walmart/cookie_pool.py`) that subclasses `IdentityPool` and pre-wires the strategy — mirror `src/mcp/servers/amazon/cookie_pool.py` (which adds `from_cookie_files` / `from_cookie_helper` factories so fresh browser cookies are written back to each account's JSON).
3. In your scrapers, follow the §9.4 invocation pattern. No changes to `src/core/identity/`.

---

## 10. DataCache Standards

`DataCache` (`src/core/data_cache.py`) is the shared persistence contract between L1 scrapers and L2 calculators. This section defines the key schema, TTL policy, and ownership rules every contributor must follow.

### 10.1 API

```python
data_cache.set(domain, key, value)                           # write
data_cache.get(domain, key, ttl_seconds=None)                # read; returns None if expired
data_cache.get_model(domain, key, ModelClass, ttl_seconds)   # read + Pydantic validation
data_cache.exists(domain, key)                               # boolean check
```

TTL is enforced **at read time** by comparing the stored `updated_at` timestamp to `datetime.utcnow()`. There is no background eviction — stale entries remain until overwritten.

### 10.2 Redis Key Format

The Redis backend constructs every key as:

```
aws:cache:{domain}:{key}
```

The `{key}` portion for L2 workflows follows:

```
{tenant_id}:{store_id}:{data_type}:{entity_id_or_hash}
```

| Layer | Full Redis key example |
|-------|------------------------|
| L1 (raw product) | `aws:cache:amazon:B01XXXXX` |
| L1 (reviews) | `aws:cache:amazon:reviews:B01XXXXX` |
| L1 (social/TikTok) | `aws:cache:tiktok:yoga mat` |
| L1 (social/TikTok ref) | `aws:cache:tiktok:__ref__yoga mat` |
| L2 (workflow result) | `aws:cache:product_screening:default:US:profitability:B01XXXXX` |
| L2 (ad report) | `aws:cache:ad_diag:tenant123:JP:perf_report:{hash}` |

### 10.3 L1 vs L2 Domain Ownership

| Layer | Who writes | Domain name | Key shape |
|-------|-----------|-------------|-----------|
| **L1** | MCP servers only | Named by data source (`amazon`, `tiktok`, etc.) | `ASIN` \| `{type}:{ASIN}` \| keyword |
| **L2** | Workflow steps only | Named by workflow (`product_screening`, `ad_diag`, `cat_monopoly`) | `{tenant_id}:{store_id}:{data_type}:{entity_id}` |

**Isolation rules:**
- L1 MCP servers **never read L2 domains**.
- Workflow steps **never write to L1 domains**.
- L2 reads L1 as raw input; L2 writes computed results under its own workflow domain.

**L1 key conventions:**
- ASIN keys must be **uppercase** — call `.upper()` before every `set`/`get`.
- Sub-type keys use `{type}:{id}` notation: `reviews:B01XXXXX`.

**L2 key construction** — use the standard helper, never inline the key logic:

```python
_L2_DOMAIN = "product_screening"   # one constant per workflow file

def _l2_key(ctx: WorkflowContext, *parts) -> str:
    tid = ctx.tenant_id or "default"
    sid = ctx.config.get("store_id", "US")
    return ":".join(str(p) for p in (tid, sid) + parts)

def _l2_get(ctx, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)

def _l2_set(ctx, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)
```

### 10.4 TTL Reference

Define TTL constants at module level with a comment. Never pass a magic integer to `get()`.

| Constant | Seconds | Duration | Data type examples |
|----------|---------|----------|--------------------|
| `_TTL_*` | `3_600` | 1 h | BSR scrape, market signals (ABA/SERP/CPC), deal intensity |
| | `7_200` | 2 h | Ad traffic ratios, ad account config (campaigns/keywords) |
| | `14_400` | 4 h | Product reviews |
| | `21_600` | 6 h | Seller/fulfillment info, ad perf reports, change history, LLM keyword extraction, Xiyouzhaoci traffic |
| | `43_200` | 12 h | Fulfillment type, TikTok PSI + YouTube/social signals |
| | `86_400` | 24 h | Product metadata, past-month sales, SellerSprite snapshots, historical timeseries, YoY/ERP data |
| | `604_800` | 7 d | Compliance/regulatory rules (essentially static) |

### 10.5 Adding a New Cached Data Type

1. Decide the layer: **L1** if raw scraped data; **L2** if computed/derived.
2. Use an existing domain if the source matches; create a new one only for a genuinely new workflow or data source.
3. Define a named `_TTL_*` constant using the table above.
4. For L2: key via `_l2_key(ctx, "<data_type>", entity_id)` — never a bare string.
5. For L1: normalize ASIN to uppercase; use `{type}:{ASIN}` for sub-types.
6. Test with `pytest tests/test_l1_l2_cache.py`.

---

## 11. Telemetry & Step History (`src/core/telemetry/`)

**Scope:** today's telemetry is **progress / ETA estimation**, not distributed tracing. The `trace_id` "Gateway → Step → Model" link shown under *Observability* in `ARCHITECTURE.md` is a **roadmap item — not yet implemented** (there is no `trace_id` in the codebase). What exists:

- `TelemetryTracker` (`tracker.py`) — per-job, in-memory step-duration tracker that produces a human-readable ETA.
- `step_history.json` — on-disk rolling history of step durations; the learned baseline for ETA.
- `TimeEstimator` (`tracker.py`) — static heuristic ETA shown *before* a job starts (no history needed).

### 11.1 Write points & file layout

| Artifact | Path | Written by | When |
|---|---|---|---|
| Step history | `src/core/telemetry/step_history.json` | `_append_step()` (via `TelemetryTracker._persist_duration`) | after every completed step |
| Lock sidecar | `src/core/telemetry/step_history.json.lock` | `_append_step()` | held during each write |

The write path is **multi-process safe**: `_append_step` takes an `fcntl.flock` (POSIX advisory lock) on the `.lock` file, re-reads history *under the lock*, appends, then commits via an atomic `os.replace()` of a tempfile — a concurrent reader never sees a half-written file. All failures are swallowed at WARNING (telemetry must never break a job); a corrupt `step_history.json` self-heals by resetting to `{}` on the next read.

> `step_history.json` and its `.lock` are **runtime state, not source** — safe to delete (history rebuilds), and should not be committed (treat like `data/checkpoints/`).

### 11.2 Naming convention (history keys)

Each entry is keyed by:

```
{workflow_name}:{step_name}      # when workflow_name is set
{step_name}                      # agent / no-workflow case, e.g. "Agent Reasoning (Step 1)"
```

The value is a list of step **durations in seconds** (float), capped at a rolling window of **20 samples** (`_HISTORY_MAX_SAMPLES`; oldest dropped first). Keep `step_name` **stable across runs** — renaming a step forks its history under a new key and resets ETA confidence for that step.

### 11.3 Who instruments (responsibility)

Instrumentation lives in the **callback layer**, driven by workflow `on_progress` events — **not** in the engine or domain code:

| Component | Role |
|---|---|
| `FeishuCallback` (`src/jobs/callbacks/feishu.py`) | Owns one `TelemetryTracker(total_steps)`. Calls `record_step(step_name)` in `on_progress` (once per step), `get_dynamic_eta(remaining_step_names=…)` to render the ETA, and `finalize()` on completion to capture the final step. |
| `TimeEstimator` (static) | Used by `src/entry/feishu/commands.py` to show an *initial* ETA at submission time, before any step has run. |
| `WorkflowEngine` | Emits the `on_progress` events that drive `record_step`; it does **not** write telemetry itself. |

To instrument a **new callback / channel**: construct a `TelemetryTracker` with the step count, call `record_step()` on each progress event, and `finalize()` once at the end. Timing detail that matters: `on_progress` fires *before* a step runs, so `record_step` attributes the measured wall-time to the **previously pending** step, not the incoming one (the first call only marks the start). `finalize()` exists precisely because the last step has no following `record_step` to close it.

### 11.4 Fields for troubleshooting

- **"Which step is slow?"** — read `step_history.json`: the `{workflow}:{step}` key with the largest or growing samples is the hotspot. Each list is the last 20 wall-time durations (seconds).
- **ETA confidence tier** in the progress message reflects *data availability*, not accuracy:
  - 🔴 `< 2` completed steps — elapsed-ratio guess only
  - 🟡 `2–4` completed steps — improving
  - 🟢 `≥ 3` samples for **every** remaining step — history-backed (ETA = 40 % elapsed-ratio + 60 % historical)
- A step stuck at 🔴/🟡 for a workflow you've run many times usually means its `step_name` changed (history forked — §11.2) or it has fewer than 3 samples.
- No ETA shown (`get_dynamic_eta` → `None`) before step 1 or after the last step is **expected**, not a bug.

---

## 12. Capability Registry (`src/registry/`)

The central discovery hub shared by both tracks. Three **independent** registries with **different discovery models** — do not assume they behave alike:

| Registry | Singleton | What it holds | Discovery model |
|---|---|---|---|
| `ToolRegistry` (`tools.py`) | `tool_registry` | executable MCP tools (name → `Tool` + handler + `ToolMeta`) | **import side-effect** — domain modules call `register_tool` at import |
| `ResourceRegistry` (`resources.py`) | `resource_registry` | static JSON business knowledge | **filesystem scan** — no registration call needed |
| `PromptRegistry` (`prompts.py`) | `prompt_registry` | reusable SOP prompt templates | **import side-effect** (imperative `register_prompt`); currently unused |

### 12.1 Responsibility boundaries

The registry is a **discovery + invocation hub, not a business-logic layer.** It maps names to capabilities and is the call boundary; the actual work lives in the domain servers (`src/mcp/servers/<domain>/`). Specifically, `ToolRegistry.call_tool` is the one place that, in order: (1) pops `_metadata` and propagates `tenant_id`/`user_id`/`job_id`/`chat_id` to contextvars via `ContextPropagator`, (2) strips unknown arguments not in the tool's `inputSchema` (logged at WARNING), then (3) invokes the handler with clean business args. Keep cross-cutting concerns (context, arg validation, future ACL/versioning — §12.4) here; keep domain logic out.

### 12.2 Tools — import side-effect constraint

Tool registration happens **only** as a side effect of importing a domain's `tools.py` (each calls `tool_registry.register_tool(...)` at module scope). The single trigger point is the **import block at the bottom of `src/registry/tools.py`**:

```python
tool_registry = ToolRegistry()          # singleton FIRST

import src.mcp.servers.amazon.tools     # noqa: E402,F401  — these run register_tool(...)
import src.mcp.servers.market.tools     # noqa: E402,F401
...                                      # one line per domain
```

Hard constraints:
- **Order matters.** The imports must come *after* the `tool_registry` singleton is defined. Each domain module does `from src.registry.tools import tool_registry` at its own import time, so the singleton must already exist — otherwise a partial-module circular import. This is why the block lives at the bottom of the file.
- **This is the only registration trigger.** Neither `src/mcp/server.py` nor `LocalMCPClient` imports domain modules; they only *read* `tool_registry`. If you add a domain server and forget to list it here, its tools register lazily (and unreliably) only if some workflow happens to import that module first — the Agent track (`list_tools`) would otherwise see **zero** of them cold.
- **Idempotent, last-wins.** Re-import is a no-op (Python module cache); duplicate `tool.name` values overwrite (dict keyed by name). Keep tool names unique across domains.

### 12.3 Resources & Prompts

- **Resources** need *no registration call*. `ResourceRegistry.get_all_resources()` walks `src/mcp/servers/**/*.json` and exposes each as `resource://aws-knowledge/{filename}`; `read_resource(uri)` returns its contents. Constraint: the URI is **filename-only, not path-qualified** — two `*.json` files with the same basename in different server dirs collide (last scanned wins). Name knowledge files uniquely. Drop a JSON file in a server dir and it is auto-discovered; nothing to import.
- **Prompts** use imperative `prompt_registry.register_prompt(Prompt(...))`. Discovery is import-side-effect like tools, so a prompt module must likewise be imported to register. There are **no prompt registrations today** (only an example comment) — wire a bottom-of-file import block mirroring §12.2 when the first one is added.

### 12.4 Extension rules — versioning & ACL

Neither exists yet; both belong **in the registry, not in handlers**, so every track inherits them uniformly:
- **Versioning:** add a `version` field to `ToolMeta` (and optionally encode it in `tool.name`, e.g. `amazon_bsr@v2`). `ToolMeta` is the right carrier because it already holds out-of-band metadata the MCP `Tool` object doesn't.
- **ACL / scoping:** enforce in `ToolRegistry.call_tool` — the propagated `tenant_id`/`user_id` (step 1 above) are already available there, so a `min_scope` / `allowed_tenants` field on `ToolMeta` checked before dispatch is the natural hook. Do not scatter permission checks into individual handlers.
- **Categories** (`ToolMeta.category`: `DATA | COMPUTE | FILTER | OUTPUT`) already exist and drive `get_tools_by_category`; reuse them for catalog grouping rather than inventing parallel tags.

---

## 13. Output Parsing & Post-Processing (`src/intelligence/parsers/`)

The fourth piece of the intelligence layer alongside prompt / provider / processor: turning a raw LLM string into something safe to consume or display. All of it lives in `OutputParser` (`markdown_cleaner.py`), a stateless utility class of `@staticmethod`s. Two concerns:

1. **Structured extraction** — `parse_dirty_json` recovers a dict from imperfect model JSON.
2. **Display sanitization** — `clean_markdown` / `clean_for_feishu` / `clean_for_cli` normalize text for a target channel.

### 13.1 Methods

| Method | Input → Output | Use for |
|---|---|---|
| `parse_dirty_json(s, depth=0)` | str → `dict` (**`{}` on failure, never raises**) | extracting JSON / a ReAct tool-call from an LLM reply |
| `clean_markdown(text)` | str → str | generic normalization (JSON blocks, whitespace, HTML entities, chatter) |
| `clean_for_feishu(text)` | Any → str | `clean_markdown` **plus** stripping `![alt](url)` → `alt` |
| `clean_for_cli(text)` | Any → str | currently `clean_markdown` (channel hook for future divergence) |

### 13.2 `parse_dirty_json` — what it repairs

It is deliberately forgiving of real-world LLM output, in order: unwrap a ```` ```json ```` fence (or isolate the root `{…}`); a single state-machine pass that escapes raw newlines/tabs inside strings, drops `//` and `/* */` comments, and tracks brace/bracket balance; **structural repair for truncation** (close an open string, pop the unclosed `{`/`[` stack); strip trailing commas; then `json.loads`. If that fails it runs a targeted regex to escape unescaped inner quotes, and finally a **ReAct fallback** that regex-extracts `action` / `action_input` even from structurally broken text. Recursion is capped at `MAX_RECURSION_DEPTH = 2` (the nested `action_input` parse), so malformed input can't blow the stack.

### 13.3 Graceful degradation — the `{}` contract

`parse_dirty_json` **never raises**; total failure returns `{}`. Every caller branches on that truthiness instead of catching exceptions — this is the platform-wide degradation contract:

| Caller | Failure (`{}`) behavior |
|---|---|
| `ProcessStep._coerce` (`workflows/steps/process.py`) | logs WARNING, returns `None` → the item passes through **unenriched** rather than failing the workflow |
| `MCPAgent` (`agents/mcp_agent.py`) | returns `(None, None)` → no tool call this turn; the ReAct loop continues / re-prompts |
| `review_summarizer`, provider structured calls | fall back to the empty/default result for the feature |

Rules for new callers:
- Treat `{}` as "parse failed" and choose a **safe default** (skip, passthrough, retry) — never assume keys exist.
- Do **not** wrap `parse_dirty_json` in `try/except` for parse errors; it already absorbed them. Reserve `try/except` for the *downstream* step (e.g. `schema(**data)` validation), and on failure log at WARNING and degrade — see the `ProcessStep` pattern (validate, warn, return `None`).
- Define a module-level `EMPTY_RESULT` default for AI-backed processors (see Layer 5) so a parse failure yields a valid-shaped empty object, not a crash.

### 13.4 Channel sanitization — where it belongs

Display cleaning happens at the **output boundary** (router / callback), not inside domain logic:
- The `IntelligenceRouter` applies `clean_for_feishu` on the **local-model route** (raw local output is the noisiest); cloud responses are not cleaned there. The authoritative cleaning happens at the channel boundary — `FeishuCallback` runs `clean_for_feishu` at send time regardless of route. Cleaning is idempotent, so the local route running it twice is harmless.
- `clean_for_feishu` strips Markdown image syntax on purpose: Feishu interactive-card Markdown requires uploaded `image_key`s, not URLs, so a raw `![alt](url)` triggers ErrCode 11310 ("no imagekey is passed in"). Images must be delivered via the storage/upload path (§8), not embedded in card text.
- Add a **new channel** by adding a `clean_for_<channel>` method that composes `clean_markdown` with channel-specific rules, and call it from that channel's callback — keep the per-channel quirks here, not scattered across callers.

---

## 14. Data Models & DTOs (`src/core/models/`)

Beyond "use Pydantic," these are the rules for *where* a model lives, *how* it evolves, and *when* a new one is justified.

### 14.1 Ownership — where a model belongs

Model ownership is **layered**, not "everything in `src/core/models/`":

| Scope | Home | Examples |
|---|---|---|
| Cross-domain business entities & request contracts | `src/core/models/` | `Product`, `Review`, `ReviewSummary`, `MarketAnalysisReport`, `CompetitorEntry`, `UnifiedRequest`, `CallbackConfig` |
| Single-domain DTOs | that domain's module (often `dto.py`) | `LLMResponse`, `BatchJobHandle` (`intelligence/dto.py`); `WorkflowContext` (`workflows/steps/base.py`); `CheckpointData`, `WorkflowEvent` (`jobs/checkpoint`); `ToolMeta` (`registry/tools.py`) |
| Internal to one workflow/processor | module-private, `_`-prefixed | `_VisualSemanticDimensions` in `listing_diagnosis.py` |

Rule: **a model used by ≥2 domains goes in `src/core/models/`; a model internal to one domain stays in that domain.** Promote a private model up only when a second domain genuinely needs it — don't pre-place everything in core (that recreates the cross-domain coupling DDD avoids). `src/core/models/__init__.py` re-exports only the shared entities; import those via `from src.core.models import Product`.

### 14.2 DTO version compatibility

Models are validated against **persisted and in-flight data** that outlives a single deploy — `DataCache.get_model()` re-validates cached JSON, checkpoints store DTO snapshots, and `BatchPoller` reconstructs items from a stored schema path. So a model change can break data written by an earlier version. Rules:

- **Add fields as `Optional` with a default** (`None` or `default_factory`). Almost every field on `Product`/`Review` is `Optional[...] = Field(None, …)` for exactly this reason — old producers, cache entries, and checkpoints still validate.
- **Never remove, rename, or re-type an existing field.** A retyped field fails validation when `get_model()` reads pre-existing cache; a rename silently drops the old data. To replace a field, add the new one and deprecate the old in place.
- Pydantic v2's default `extra="ignore"` means a newer producer adding a field won't break an older consumer (unknown keys are dropped) — lean on this rather than tightening to `extra="forbid"` on shared contracts.
- Keep `Field(description=…)` accurate: descriptions are fed to LLMs for tool planning and structured output, so a stale description is a functional bug, not a cosmetic one.

### 14.3 No ad-hoc dicts across boundaries

"Pass a model, not a bare dict" applies **at boundaries** — cross-domain calls, persisted data, and LLM-facing schemas. A dict with an implicit, undocumented shape that crosses a boundary is the anti-pattern: callers can't see the contract, typos pass silently, and `DataCache` can't validate it.

Two deliberate, *non*-violating uses of dict remain:
- **The workflow item pipeline** (`list[dict]` flowing through steps) is dict by design — items accrete enrichment heterogeneously. But each item's shape should track a real model (`Product`), and `ProcessStep` coerces to `output_schema` at the AI boundary (§Layer 5). Don't invent parallel item shapes.
- **`UnifiedRequest.params: Dict[str, Any]`** is an intentional open extension point for per-workflow parameters, normalized at the gateway.

Everywhere else, if data crosses a domain, gets cached, or is returned to an LLM as structured output → define or extend a model.

### 14.4 When to add a new model

Add one when **any** of these holds: the data crosses a domain boundary; it is persisted (cache/checkpoint); it is an LLM structured-output schema; or the same shape is built at ≥2 call sites. Otherwise:

- **Extend, don't fork.** Add an `Optional` field to the existing model rather than creating a near-duplicate (§14.2 keeps it backward-compatible).
- **Keep it private** (`_`-prefixed, in the module) if it's internal to a single workflow/processor and not persisted — promote to `src/core/models/` only when a second domain needs it.
- **Place by ownership** (§14.1): shared → `src/core/models/`; single-domain → that domain.

---

## 15. MCP Agent — Session, Cost & Finalization (`src/agents/`)

The exploratory track runs a ReAct loop (`MCPAgent.run`) over an `AgentSession`. Beyond editing the prompt/budget (Layer 3, Track B), these are the code boundaries to respect when changing agent behavior.

- `base_agent.py` — `BaseAgent` ABC (`run()` + an injected reasoning dependency).
- `session.py` — `AgentSession` (state DTO) + `AgentSessionManager` (persistence).
- `mcp_agent.py` — the ReAct loop, cost accounting, finalization, attachment policy.

### 15.1 Session persistence boundary

`AgentSession` (Pydantic) is the **single source of agent state**: `history` (`list[AgentMessage]`), `token_usage` / `cloud_token_usage` / `total_cost` / `currency`, `max_steps` / `current_step`, `status` (`active` / `suspended_for_human` / `completed` / `failed`), and `context` (runtime data like `feishu_chat_id`, `report_file_path`). `AgentSessionManager` persists one JSON file per session at `data/sessions/{session_id}.json` (single-user; Redis is the extension point).

Boundaries:
- **`session_id == job_id`** (1:1, set by `JobManager._run_agent_mode`). The agent **owns** persistence — *it* calls `session_mgr.save()`; the JobManager only reads `session.context` afterward (for `report_file_path`).
- **Load-or-create + resume-safe:** `run()` loads an existing session or creates one, and adds the incoming `query` **only if `not session.history`** — so a resumed session continues rather than re-asking. Don't add the user message unconditionally.
- **Save points:** every step (before the LLM call, to persist progress), on every terminal exit, and on suspension. Any new exit path must `save()` before returning/raising.

### 15.2 Allowed tools & invocation boundary

- The agent's tool set is **the whole `tool_registry`** (§12) — there is no per-agent allowlist today. `PromptBuilder` renders the categorized catalog (DATA→COMPUTE→FILTER→OUTPUT) into the system prompt so the LLM can plan.
- Tools are called **only** through `self.mcp.call_tool_json(action, action_input)` (the MCP client, §6) — never by importing handlers.
- **Identity is injected on every call:** the loop sets `action_input["_metadata"] = {tenant_id, user_id, job_id, chat_id}`; the registry propagates these to contextvars (§12.1). Preserve this when adding tool-call logic.
- Tool calls are parsed from the LLM reply via `OutputParser.parse_dirty_json` (§13); an unparsable reply yields `(None, None)` and the loop treats it as a conversational reply, not a crash.

### 15.3 Cost tracking boundary

- `_accum_tokens(session, response_obj)` runs after **every** LLM call. It adds to `token_usage` always, but to **`cloud_token_usage` only when the provider is not local** (`_LOCAL_PROVIDERS` = local/llama runs are free), and adds `response_obj.cost` to `total_cost`.
- **The budget is measured in cloud tokens**, not total tokens or steps. `token_budget` (default 1,000,000 cloud tokens) is the hard ceiling; `max_steps` (default 15) is **progress display only, not a failure limit**.
- Any new code path that calls the LLM must route through `_accum_tokens`, or budget enforcement silently drifts.

### 15.4 Finalization paths (loop exits)

The loop has exactly these terminal/transition outcomes — keep them exhaustive when editing:

| Trigger | Outcome |
|---|---|
| `"Final Answer:"` in reply | extract → attachment policy (§15.5) → `status=completed`, save, return |
| `current_step > max_steps` **and** cloud usage < 80 % budget **and** < 2 extensions | grant **+5 steps** (max 2 grace extensions), inject a "converge" system message, continue |
| step limit hit with low budget / extensions exhausted | `_force_final_answer` → `completed` |
| `cloud_token_usage >= token_budget` | notify "switching to batch", `_force_final_answer` → `completed` |
| tool returns `{"_type": "INTERACTION_REQUIRED"}` | `status=suspended_for_human`, save, forward signal to callback, **raise `JobSuspendedError`** → JobManager suspends the job (§3) |
| reply has no tool call and no Final Answer | treat as conversational reply → `completed` |
| same tool+args ≥ 2× consecutively | inject a hint and `continue` (not terminal) — prevents infinite identical calls |

`_force_final_answer` appends a system message demanding a `Final Answer:`, makes one last `DEEP_REASONING` call, accumulates its tokens, and returns the text. It is the single forced-closure helper — reuse it rather than duplicating closure logic.

### 15.5 Long-report attachment strategy (Attachment-First)

A Final Answer larger than **`_CARD_LIMIT_BYTES = 28_000`** (UTF-8) is not sent inline — chat cards reject oversized payloads. On finalization:

1. If the report exceeds the limit **and** `session.context["report_file_path"]` is unset, the agent calls the `export_md` tool, stores `report_file_path` + `report_filename` in `session.context`, and replaces the returned answer with a **500-char preview + "full report saved as attachment"** note.
2. If the agent **already called `export_md` itself** mid-loop, an interception sets `report_file_path` so the auto-export is **skipped** (no double-write).
3. `JobManager._run_agent_mode` reads `session.context["report_file_path"]` and forwards it to `callback.on_complete` as a `report_file_path` item field; the callback handles channel-specific attachment delivery.

Net rule: the agent decides *what to attach* (size threshold, dedupe vs. explicit export); the callback decides *how to deliver* it. Don't hardcode channel formats in the agent.

---

## 16. LLM Providers (`src/intelligence/providers/`)

Every provider subclasses `BaseLLMProvider` and is constructed by `ProviderFactory`. The base class is a **Template Method**: it owns cost, context-limit, and metadata-filtering logic so subclasses only implement the API call. Respect these contracts when adding one.

### 16.1 The interface (what to implement)

`BaseLLMProvider(provider_name, model_name)` — three **required** abstract methods:

| Method | Returns | Notes |
|---|---|---|
| `generate_text(prompt, system_message=None, **kwargs)` | `LLMResponse` | free-form text |
| `generate_structured(prompt, schema, system_message=None, **kwargs)` | `LLMResponse` | native or simulated structured output; `.text` carries the JSON |
| `count_tokens(prompt, system_message=None)` | `int` | used by the context-limit guard; fall back to `len(prompt)//4` on SDK failure |

Optional overrides (default to NotImplementedError / disabled): `generate_vision_structured(...)`, `supports_batch()`, `generate_batch(...)`, `poll_batch(...)`.

Then register it: add a branch in `ProviderFactory.get_provider()` keyed on `DEFAULT_LLM_PROVIDER` (e.g. `claude` / `gemini` / `openai` / `deepseek` / `local`), reading model/credentials from env. The OpenAI provider also answers to the `gpt` alias.

> **Provider-specific request shaping.** The base contract is the same for everyone, but the SDK call inside `generate_text`/`generate_structured` is yours to adapt to the model family. The OpenAI provider is the live example: the GPT-5.x / reasoning models require `max_completion_tokens` (the legacy `max_tokens` is rejected) and accept only the default `temperature`, so it omits `temperature` unless the caller explicitly passes one. Build params per-family rather than copying another provider's call verbatim.

### 16.2 `LLMResponse` — the universal return type

Every method returns `LLMResponse` (`src/intelligence/dto.py`): `text`, `provider_name`, `model_name`, `token_usage`, `cost`, `currency`, `metadata`. This is the contract the whole platform reads — the agent's cost tracker (§15.3) keys off `token_usage` + `provider_name`, and processors read `.text` (§Layer 5). **Never hand-build an `LLMResponse`.**

### 16.3 Token & cost population — `create_response`

Build the response through the base helper, never manually:

```python
return self.create_response(
    text=text,
    input_tokens=usage.input_tokens,
    output_tokens=usage.output_tokens,
    # optional specialized billing — pass what the SDK reports:
    thought_tokens=…, cached_tokens=…, cache_read_tokens=…, cache_creation_tokens=…,
    is_batch=False,
)
```

`create_response` computes `cost` via the shared `PriceManager.calculate_cost`, sets `token_usage = input + output + thought`, fills `currency`, and assembles `metadata`. This is the **single seam** for cost transparency and budget enforcement — bypassing it makes a provider's usage invisible to the agent budget and the cost ledger. Strip internal keys with `self._filter_kwargs(kwargs)` before forwarding `kwargs` to the SDK (keeps `tenant_id`/`session_id`/etc. out of the API call).

### 16.4 Context-limit guard (provided)

Declare `_MODEL_CONTEXT_WINDOWS = {"model-prefix": token_limit}` (prefix match covers dated suffixes) and rely on the base guards: `await self._check_context_limit(prompt, system_message)` at the top of `generate_text`/`generate_structured` raises `FatalError` when input exceeds `window − _OUTPUT_RESERVE`; `_check_batch_context_limit_sync` does a fast char-estimate check (×1.2 safety) for batches. You don't write the limit logic — just set the map and call the guard.

### 16.5 Batch support (opt-in)

To participate in the async batch pipeline (§3): override `supports_batch() → True`, `generate_batch(requests: list[BatchRequest]) -> BatchJobHandle`, and `poll_batch(handle) -> dict[custom_id, LLMResponse] | None` (**return `None` while pending**, the result map when complete). `BatchPoller` drives polling and reconstruction. Apply the batch discount by passing `is_batch=True` to `create_response` for batch completions. Providers without batch leave the defaults (e.g. DeepSeek `supports_batch()` stays `False`).

### 16.6 Error mapping

SDK failures are mapped onto the framework hierarchy (§5) so retry/abort semantics work upstream. Use the base helper — **`self._raise_mapped_error(e)`** — from every API-call `except` block instead of a bare `raise`:

```python
try:
    resp = await self._client.…(…)
    return self.create_response(…)
except Exception as e:
    logger.error(f"{self.provider_name} … failed: {e}")
    self._raise_mapped_error(e)   # → RetryableError / FatalError, never returns
```

What `_raise_mapped_error` does (in `BaseLLMProvider`):
- **Context overflow** is already a `FatalError` from the pre-flight guard (§16.4) and passes straight through (framework errors are not re-wrapped).
- Reads the HTTP status from the SDK exception (`.status_code` / `.code` / `.response.status_code`), runs `classify_http` + `classify_response_message` (§5.2), then raises `RetryableError(http_status=…, provider=self.provider_name)` for transient codes (429/5xx/401-token-expired) or `FatalError(code=…)` for permanent ones (bad key, 400).
- No HTTP status → exception-type heuristics (timeout/connection → `RetryableError`); otherwise the original exception is re-raised unchanged so unexpected bugs are never masked.
- **Never swallow into a fake `LLMResponse`** — a silent empty response corrupts cost/budget accounting and hides failures (§5.6: raise low, handle high).

> Deliberate fallbacks stay as-is and must **not** be routed through `_raise_mapped_error`: `count_tokens` estimate fallbacks, the local-model timeout (`FallbackHandler`), and image-download failures in vision are intentional degradations, not API errors to classify.

### 16.7 Output truncation detection

A response cut off at the output-token limit must be **detected and surfaced**, not returned as if complete. Each provider checks its SDK's signal and logs the grep-able marker `"response truncated at max"` (§4.6):

| Provider | Truncation signal |
|---|---|
| Claude | `response.stop_reason == "max_tokens"` |
| DeepSeek | `choices[0].finish_reason == "length"` |
| OpenAI | `choices[0].finish_reason == "length"` (warning references `max_completion_tokens`) |
| Gemini | `finish_reason in (MAX_TOKENS, "2")` — **plus** auto-continuation up to `_MAX_CONTINUATIONS=4` rounds before warning |

A new provider must, at minimum, detect its truncation signal and emit that WARNING (it feeds the TROUBLESHOOTING playbook). Continuation (Gemini-style) is optional but preferred for long structured outputs.
