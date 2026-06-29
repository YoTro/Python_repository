# Testing Guide: AWS V2 Hybrid Intelligence Platform

This document outlines the testing strategy, tools, and procedures for the AWS V2 project. Our testing philosophy focuses on **Domain Isolation**, **Protocol Consistency**, and **Full-Flow Simulation** to ensure the reliability of both deterministic workflows and exploratory agents.

## 1. Testing Philosophy

The platform is tested across five primary dimensions:
*   **Unit Reliability**: Ensuring core Pydantic models, utility helpers, `LLMResponse` DTOs, and individual scrapers function correctly in isolation.
*   **Stateful Resilience**: Verifying that Checkpoints (Workflows) and Sessions (Agents) persist and resume correctly.
*   **Data Orchestration**: Validating the L1/L2 separation and the integrity of the mediated `DataCache`.
*   **Intelligence Routing**: Confirming the `IntelligenceRouter` correctly classifies tasks, routes to optimal LLMs (local/cloud), and applies output parsing.
*   **Full-Flow Integration**: Bridging the API Gateway, JobManager, and MCP Tools to ensure end-to-end correctness.

## 2. Environment Setup

To run tests, ensure you have the necessary testing dependencies installed:

```bash
source venv311/bin/activate
pip install pytest pytest-asyncio pytest-mock
```

All tests should be executed from the project root with the `PYTHONPATH` set to the current directory to ensure absolute imports resolve correctly.

## 3. Test Categories & Execution

**Qualifier rules and CI gate order:**

| Cat | Name | Qualifier | Key constraint |
|-----|------|-----------|----------------|
| A | Core Unit | _(none)_ | All external I/O mocked |
| B | Stateful Management | _(none)_ | Verify `cloud_token_usage` stays zero for local-model calls |
| C | Data Orchestration | _(none)_ | `tmp_path`-backed `DataCache`; never real Redis |
| D | LLM Providers | _(none)_ | Mock SDK call; verify `LLMResponse.cost` is populated |
| E | Intelligence Routing | _(none)_ | Mock provider; verify heuristics fire before any LLM call |
| F | Import Integrity | _(none)_ | **Run first** — a circular-import regression makes all downstream categories unreliable |
| G | Rate Limiting | _(none)_ | Reset all `RateLimiter` singleton fields in `setUp` (see §4.5) |
| H | Full-Flow Integration | `_integration` | Mock external HTTP and LLM; run the full internal Gateway → Job → MCP stack |
| I | Ad Diagnosis Live | `_live` | Requires `REDIS_URL`; **never included in the default CI suite without an explicit opt-in flag** |

No qualifier = pure unit test; **all external I/O must be mocked** — no network, no Redis, no disk side-effects outside `tmp_path`.

### A. Core Unit Tests
Test the foundational building blocks and utilities.
*   **Location**: `tests/test_core_models.py`, `tests/test_core_utils.py`, `tests/test_core_telemetry.py`
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_core_models.py tests/test_core_utils.py tests/test_core_telemetry.py
    ```

### B. Stateful Management Tests
Verify session persistence for Agents and execution checkpoints for Workflows.
*   **Location**: `tests/test_agent_session.py`, `tests/test_workflow_engine.py`, `tests/test_checkpoint_resume.py`
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_agent_session.py tests/test_workflow_engine.py tests/test_checkpoint_resume.py
    ```
*   **Note**: `AgentSession` now tracks `cloud_token_usage` separately from `token_usage`. Tests should verify that local model tokens do not increment `cloud_token_usage`.

### C. Data Orchestration (L1/L2) Tests
Validates that L1 scrapers write to the cache and L2 calculators consume from it correctly.
*   **Location**: `tests/test_l1_l2_cache.py`
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_l1_l2_cache.py
    ```

### D. LLM Provider Tests
Verifies individual LLM providers (`GeminiProvider`, `ClaudeProvider`, `LlamaCppProvider`) correctly initialize and return `LLMResponse` objects.
*   **Location**: `tests/test_local_llm_direct.py` (and potentially new cloud provider tests)
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_local_llm_direct.py
    ```

### E. Intelligence Routing & Processors Tests
Confirms the `IntelligenceRouter`'s task classification, model routing, and specialized Processors (like the Monopoly Analyzer) are functioning correctly.
*   **Location**: `tests/test_gemini_advanced_pricing.py` (Price Manager), `tests/test_monopoly_analyzer.py`, `src/intelligence/router/` (Router logic).
*   **Key Coverage**:
    *   **Advanced Pricing**: Verifies thinking tokens, prompt caching, and tiered pricing calculations.
    *   **Heuristics**: Confirms keyword and length-based pre-screening rules trigger correctly.
    *   **Processors**: Verifies `CategoryMonopolyAnalyzer` correctly calculates dynamic CR3, CV pricing, and relative review disparity.
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/python -m pytest tests/test_gemini_advanced_pricing.py tests/test_monopoly_analyzer.py
    ```

### F. Import Integrity Tests
Ensures that the Domain-Driven Design (DDD) structure remains free of circular imports.
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_imports.py
    ```

### G. Rate Limiting System Tests
Validates all three layers of the rate limiting architecture in isolation and combination.
*   **Location**: `tests/test_rate_limiting_system.py`
*   **Coverage** (37 tests):

    | Test Class | Cases | What Is Verified |
    |---|---|---|
    | `TestLayer3TokenBucket` | 5 | Token acquisition, burst depletion, refill over time, source isolation |
    | `TestLayer2TenantQuota` | 7 | Counter increment, free/pro limits, unknown tier unlimited, tenant isolation |
    | `TestLayer1aCooldown` | 7 | First trigger allowed, repeat blocked, different chats independent, blocked call does not reset timer |
    | `TestLayer1bConcurrentSlot` | 9 | Normal release, **release on exception**, **release on `CancelledError`**, per-chat limit, global limit, multi-chat coexistence |
    | `TestCheckLimit` | 5 | Combined gate: cooldown blocked → quota counter must NOT advance |
    | `TestUnifiedRequestMetadata` | 4 | `entry_type` / `chat_id` field propagation and model serialisation |

*   **Command**:
    ```bash
    export PYTHONPATH=$PYTHONPATH:. && venv311/bin/python3 -m unittest tests/test_rate_limiting_system.py -v
    ```
*   **Note**: Tests reset the `RateLimiter` singleton state in `setUp` (`_concurrent`, `_tenant_counters`, `_chat_last`, bucket tokens) to ensure isolation between runs.

### H. Full-Flow Integration Tests
Simulates a complete request starting from the entry points through the API Gateway.
*   **Scenario**: Simulate a Feishu command, track its progress via the Telemetry Tracker, and verify the final Bitable/CSV output.
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_feishu_full_flow.py -s
    ```

### I. Ad Diagnosis: Inventory Gate & Quick Metrics Snapshot Tests

These scripts run against live Redis data and do not require mocking. Set `REDIS_URL` in `.env` before running.

**`tests/test_inventory_gate.py`** — Validates the inventory gate logic end-to-end:
*   Loads campaign, kw_perf, daily_perf, inventory data from Redis.
*   Runs two scenarios: sea freight (`inbound_lead_days=30`, gate triggers) and domestic US (`inbound_lead_days=10`, gate clears).
*   Asserts that spend-up actions (`increase_budget`, `enable_and_increase_budget`, `enable_and_review_bids`) are downgraded to `P2` and carry a `prerequisite` block when effective stock is below `stock_gate_days`.
*   Expected results: sea freight → `effective_stock_days ≈ 12` → actions gated; domestic → `effective_stock_days ≈ 27` → 0 actions gated.

```bash
PYTHONPATH=. python3 tests/test_inventory_gate.py
```

**`tests/test_summary_snapshot.py`** — Validates the Quick Metrics Snapshot output:
*   Loads real Redis data (campaigns, kw_perf, daily_perf, keywords from `aws:cache:ad_diag:*`).
*   Resolves `avg_bid` from the Ads API keywords cache (`aws:cache:ad_diag:default:US:keywords:*`), filtered to campaigns for the test ASIN.
*   Injects synthetic inventory (12-day stock scenario) to exercise the inventory gate display.
*   Calls `_build_item_summary` with a mock `WorkflowContext` and prints the full snapshot JSON.
*   Key assertions: `avg_bid` is non-null (sourced from Ads API, not spSearchTerm report), `keyword_count` matches the filtered keyword set, `match_type_dist` totals to 100%.

```bash
PYTHONPATH=. python3 tests/test_summary_snapshot.py
```

## 4. Test File Naming & Structure Standard

### 4.1 File Naming Convention

```
tests/test_{domain}_{feature}[_{qualifier}].py
```

| Segment | Rule | Examples |
|---|---|---|
| `{domain}` | Matches the `src/` subdirectory or external service | `core`, `workflow`, `agent`, `feishu`, `gemini`, `deepseek`, `xiyou`, `erp`, `rate_limiting` |
| `{feature}` | The specific capability under test | `models`, `engine`, `session`, `pricing`, `client`, `full_flow` |
| `{qualifier}` | Optional suffix that describes test scope | `_live` (requires live infra), `_integration` (mocks nothing), `_snapshot` (golden-file comparison) |

**Rules:**
- No qualifier = unit test; all external I/O is mocked.
- `_live` suffix = requires real Redis (`REDIS_URL`) or a live API; never run in CI without flags.
- `_integration` suffix = hits real sub-systems but no external network.
- One domain/feature concept per file. Split if a file exceeds ~400 lines or covers two unrelated areas.

**Examples:**
```
tests/test_core_models.py          # Unit: Pydantic DTO validation
tests/test_workflow_engine.py      # Unit: WorkflowContext + ActivityRunner
tests/test_gemini_advanced_pricing.py  # Unit: PriceManager for Gemini
tests/test_feishu_full_flow.py     # Integration: Gateway → Job → MCP
tests/test_ad_diagnosis_live.py    # Live: real Redis, real Ads API data
tests/test_summary_snapshot.py     # Live + snapshot: golden JSON comparison
tests/test_listing_diagnosis_workflow.py  # Unit: listing_diagnosis pipeline (mocked extractors, ReviewSummarizer, vision LLM)
tests/test_listing_quality_scorer.py      # Unit: ListingQualityScorer module scores and improvement plan
```

### 4.2 Internal File Structure

Every test file must follow this order:

```python
# 1. Standard library imports
# 2. Third-party imports (pytest, unittest.mock)
# 3. Project imports (use absolute paths, PYTHONPATH=.)

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# ── Fixtures ──────────────────────────────────────────────────────────────────
# Shared setup: tempfiles, mock clients, patched singletons

@pytest.fixture
def mock_data_cache(tmp_path):
    ...

# ── Test class (group by behaviour, not by method) ───────────────────────────
class TestFeatureName:

    def setup_method(self):
        # Reset singletons: RateLimiter._concurrent, AgentSession state, etc.
        ...

    @pytest.mark.asyncio
    async def test_happy_path(self, mock_data_cache):
        ...

    @pytest.mark.asyncio
    async def test_error_case(self, mock_data_cache):
        ...

# ── Standalone script (live tests only) ──────────────────────────────────────
if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

### 4.3 Mocking Rules

| What to mock | How |
|---|---|
| `curl_cffi` HTTP requests | `patch("curl_cffi.requests.AsyncSession.get", new_callable=AsyncMock)` |
| LLM provider responses | `patch.object(GeminiProvider, "generate", return_value=LLMResponse(...))` |
| `DataCache` | Pass a `tmp_path`-backed instance; never use production Redis in unit tests |
| `AgentSession` | Use `tempfile.mkdtemp()` as `session_dir`; delete in teardown |
| Feishu HTTP calls | `patch("src.jobs.callbacks.feishu_callback.send_card_message", new_callable=AsyncMock)` |
| Singleton state | Reset in `setup_method`; document which fields are reset and why |

### 4.4 Writing New Tests

When adding new capabilities, follow these standards:

1.  **Asynchronous by Default**: Use `@pytest.mark.asyncio` for any test involving MCP Tools, Scrapers, or LLMs.
2.  **Mock External I/O**:
    *   Mock `curl_cffi` requests in scrapers to avoid network dependency.
    *   Mock LLM Provider responses to avoid token costs (especially in `IntelligenceRouter` tests).
    *   Use `tempfile` or local directory mocks for `DataCache` and `AgentSession` tests.
3.  **Schema Validation**: When testing new MCP Tools, always validate the `arguments` against the tool's `inputSchema`.
4.  **Gateway Dispatch**: When testing entry points, always use `APIGateway.dispatch_*` methods to ensure you are testing the full production path.

### 4.5 Singleton & State Reset Standards

Several platform components are module-level singletons whose state leaks between tests if not explicitly reset. Reset all relevant singletons in `setup_method` (or the controlling fixture) and add an inline comment naming which fields are cleared and why — future readers must be able to see the isolation contract at a glance.

| Singleton | Fields to reset | Why |
|---|---|---|
| `RateLimiter` | `_concurrent`, `_tenant_counters`, `_chat_last`, per-source bucket `tokens` | All four rate-limit layers persist across calls; any surviving state makes tests order-dependent |
| `JobManager` | `_jobs` dict, the async `_queue`, active worker tasks | Job-status records and queued items carry over into subsequent test cases |
| `ToolRegistry` | Rarely needed — registry is read-only after startup; if a test registers a mock tool, unregister it by name in teardown | Re-registration overwrites silently; a leftover mock tool changes routing for later tests |
| `DataCache` (in-memory) | Instantiate a fresh instance per test; never share across cases | Key collisions between tests produce false-positive passes |
| `AgentSessionManager` | Pass a `tempfile.mkdtemp()` path as `session_dir`; delete the directory in teardown | Session JSON files persist on disk and bleed across runs |

### 4.6 MCP Tool & Protocol Test Standards

1. **Never import handlers directly.** Always call tools through `ToolRegistry.call_tool(name, args)` or `ctx.mcp.call_tool_json(name, args)` — the same path used in production. Direct imports bypass context propagation and argument validation, so tests that pass this way can still fail in production.
2. **Validate `inputSchema` for every new tool.** Include at least one test case that constructs an `arguments` dict and validates it against the tool's `inputSchema` before exercising the handler logic.
3. **Identity pool must be optional.** Every scraper test must pass with `CookieBrowserPool` not initialized (`pool is None`). The single-account fallback must work unmodified — the pool is opt-in, not a dependency.
4. **Test the error → callback mapping.** When a tool raises a typed exception (`RetryableError`, `FatalError`), verify that `JobManager._run_job` maps it to `FAILED` status and that the callback receives `on_error`, not `on_complete`.

### 4.7 Error Handling Test Standards

1. **Assert the specific exception subclass.** Use `pytest.raises(RetryableError)` (or `FatalError`), never the base `AWSBaseError` — a broad catch hides regressions where the wrong subclass is raised.
2. **Assert `code` as well as the exception type.** The `ErrorCode` on the exception is what drives retry, re-auth, and user-message logic; verify it matches the expected canonical value, not just the exception class.
3. **Test provider-specific HTTP overrides.** `classify_http(status, provider=...)` can return a different `ErrorCode` for the same HTTP status depending on the provider. Cover at least one provider-specific override per new API client.
4. **Control-flow signals are not errors.** `BatchPendingError` and `JobSuspendedError` drive the SUSPENDED state — they must never appear in `on_error`. When testing a step that suspends, assert `job.status == SUSPENDED` and that `callback.on_error` was **not** called.

### 4.8 Checkpoint & Resume Test Standards

The two resume paths are not interchangeable and must each be tested independently:

| Path | How to trigger in tests | What to verify |
|---|---|---|
| `JobManager.resume(job_id)` | Submit a job, let it reach SUSPENDED, call `resume()` on the **same** manager instance | Status: SUSPENDED → PENDING → RUNNING → COMPLETED; in-memory `_jobs` record preserved throughout |
| `JobManager.resume_from_checkpoint(job_id)` | Write a checkpoint file, create a **fresh** `JobManager`, call `resume_from_checkpoint` | Fresh manager loads `workflow_name` + `params` from disk; already-completed steps are not re-executed (idempotent replay) |

**Idempotency assertion:** after resuming from a checkpoint, the mock handler for a completed step must have a call count of zero; the mock handler for the first non-completed step must have a call count of one.

**Checkpoint isolation:** each test must use its own `tmp_path`-backed checkpoint directory. A shared directory causes a prior test's on-disk state to satisfy the expected outcome of a later test, producing false-positive passes.

## 5. Troubleshooting Tests

*   **ModuleNotFoundError**: Ensure you are running with `PYTHONPATH=.`.
*   **State Leakage**: If session/checkpoint tests fail, clear the `data/sessions/` and `data/checkpoints/` directories before re-running.
*   **Asyncio Warnings**: Ensure you have `pytest-asyncio` installed and configured correctly.
*   **Local LLM Issues**: If `LlamaCppProvider` fails to load or respond:
    *   Verify `LOCAL_MODEL_PATH` in `.env` points to the *absolute* path of your `.gguf` model file.
    *   Check `llama-cpp-python` installation and GPU support (`n_gpu_layers`).
    *   Use `tests/test_local_llm_direct.py` for isolated troubleshooting.
*   **Cloud LLM SDK Issues**: If `GeminiProvider` or `ClaudeProvider` fail during initialization or generation:
    *   Check for `AttributeError: module 'google.genai' has no attribute 'configure'` or `'Model' object has no attribute 'supported_generation_methods'`. This indicates an `google-generativeai` SDK version mismatch. Consider `pip install google-generativeai --upgrade` or ensure compatibility with older APIs.
    *   Verify `GEMINI_API_KEY` or `ANTHROPIC_API_KEY` in `.env`.
*   **Feishu Output Formatting**: If messages fail to send or update (`invalid message content`, `NOT a card`):
    *   Confirm `IntelligenceRouter` correctly applies `OutputParser.clean_for_feishu` to LLM responses.
    *   Ensure `FeishuCallback` is using `send_card_message` and `update_card_message` for dynamic updates, as Feishu's `patch` API requires interactive card format.
*   **`on_error` called unexpectedly on a batch step**: `BatchPendingError` and `JobSuspendedError` are control-flow signals — they must never reach `on_error`. If they do, the likely cause is that `JobManager._run_job` is catching them inside the general `except Exception` branch instead of the dedicated control-flow branches. Verify the exception-handling order in `_run_job`.
*   **SUSPENDED job never resumes**: If a job stays SUSPENDED after `BatchPoller` should have fired, check (1) the checkpoint file exists and contains a `BATCH_SUBMITTED` event with no following `BATCH_COMPLETED`, (2) `BatchPoller` was started (it starts in `JobManager.__init__`), and (3) the provider's `poll_batch` mock returns `None` while pending and a result dict only when complete — returning `{}` (empty dict) is interpreted as "complete with no results," not "still pending."
*   **Checkpoint file conflict between tests**: If a resume test passes in isolation but fails in a full suite run, a prior test has left a checkpoint file with the same `job_id` in a shared directory. Each test must use a `tmp_path`-backed checkpoint directory — never share across test cases (see §4.8).
