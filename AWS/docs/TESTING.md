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

### E. Intelligence Routing & Fallback Tests
Confirms the `IntelligenceRouter`'s task classification, model routing, and `FallbackHandler` logic are sound.
*   **Location**: Potentially new tests for `src/intelligence/router/` and `src/intelligence/fallback.py`.
*   **Command**:
    ```bash
    # Example (specific tests to be written)
    # PYTHONPATH=. venv311/bin/pytest tests/test_intelligence_router.py
    ```

### F. Import Integrity Tests
Ensures that the Domain-Driven Design (DDD) structure remains free of circular imports.
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_imports.py
    ```

### G. Full-Flow Integration Tests
Simulates a complete request starting from the entry points through the API Gateway.
*   **Scenario**: Simulate a Feishu command, track its progress via the Telemetry Tracker, and verify the final Bitable/CSV output.
*   **Command**:
    ```bash
    PYTHONPATH=. venv311/bin/pytest tests/test_feishu_full_flow.py -s
    ```

## 4. Writing New Tests

When adding new capabilities, follow these standards:

1.  **Asynchronous by Default**: Use `@pytest.mark.asyncio` for any test involving MCP Tools, Scrapers, or LLMs.
2.  **Mock External I/O**: 
    *   Mock `curl_cffi` requests in scrapers to avoid network dependency.
    *   Mock LLM Provider responses to avoid token costs (especially in `IntelligenceRouter` tests).
    *   Use `tempfile` or local directory mocks for `DataCache` and `AgentSession` tests.
3.  **Schema Validation**: When testing new MCP Tools, always validate the `arguments` against the tool's `inputSchema`.
4.  **Gateway Dispatch**: When testing entry points, always use `APIGateway.dispatch_*` methods to ensure you are testing the full production path.

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
