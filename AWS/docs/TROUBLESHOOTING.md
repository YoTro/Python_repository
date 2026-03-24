# Troubleshooting Guide

This guide provides solutions to common issues you might encounter while developing or running the AWS V2 Hybrid Intelligence Platform. It covers problems related to data acquisition, LLM interactions, environment setup, and architecture.

## 1. Data Acquisition (Scraping) Issues

*   **`🚨 LOGIN REQUIRED: Amazon is requesting login...` (in logs)**:
    *   **Cause**: Amazon has detected an unauthenticated session, especially for deeper pages like reviews.
    *   **Solution**: Use the `refresh_amazon_cookies` MCP tool with `headless=false` and `wait_for_login=true`. A browser window will open, allowing you to log in manually. After successful login, the cookies will be saved and reused.

*   **`403 Forbidden`, `503 Service Unavailable`, or Empty Results for Search/Details**:
    *   **Cause**: Amazon's anti-bot system has likely blocked your IP or detected bot-like behavior.
    *   **Solution 1**: Wait for some time (e.g., 30-60 minutes). Your IP might be temporarily blacklisted.
    *   **Solution 2**: Use a VPN or proxy. Configure proxies in `config/settings.json` and enable `use_proxy` in your `AmazonBaseScraper` instance or CLI arguments.
    *   **Solution 3**: Force a cookie refresh (`refresh_amazon_cookies` MCP tool) to get a new session.

*   **`TypeError: 'Product' object is not callable` in Agent code**:
    *   **Cause**: This usually happens when you try to `await asyncio.to_thread(product_object)` instead of `await asyncio.to_thread(extractor.enrich_product, product_object)`.
    *   **Solution**: Ensure you are passing the function reference and its arguments to `asyncio.to_thread`, not the result of a function call.

## 2. LLM Interaction Issues

*   **`404 models/gemini-... is not found...` or `Model Not Found`**:
    *   **Cause**: The LLM provider (e.g., Gemini) cannot find the specified model name, often due to regional availability or incorrect naming conventions for the API version.
    *   **Solution**: The `GeminiProvider` now attempts to auto-discover and rotate through common model names (`gemini-1.5-pro-latest` → `gemini-1.5-flash-latest` → `gemini-1.0-pro` fallback). If this still fails, double-check your API Key and consult the Google AI Studio documentation for available model names.

*   **`AttributeError: module 'google.genai' has no attribute 'configure'` or `'Model' object has no attribute 'supported_generation_methods'`**:
    *   **Cause**: This indicates an `google-generativeai` SDK version mismatch. The codebase uses the `google-genai` SDK (`from google import genai`). Newer API calls were used with an older SDK version.
    *   **Solution**: Upgrade your Google Generative AI SDK: `pip install google-generativeai --upgrade`. Alternatively, ensure `GeminiProvider` uses API calls compatible with your installed SDK version.

*   **LLM Output is Malformed JSON / Raw JSON returned to user**:
    *   **Cause**: The LLM did not escape special characters (newlines, quotes) inside long string parameters (common with `export_md`).
    *   **Solution**: The system now uses `OutputParser.parse_dirty_json` to auto-repair these errors. If failure persists, ensure the system prompt includes the `CRITICAL: JSON ESCAPING` constraint.

*   **High Latency for Chinese Prompts (Local Model loading unnecessarily)**:
    *   **Cause**: Heuristic pre-screening in the `IntelligenceRouter` was missing Chinese keywords, triggering local model classification.
    *   **Solution**: Heuristics now include keywords like "分析", "提取", "清洗". Check `src/intelligence/router/__init__.py` if custom intents are not routing correctly.

*   **Claude API errors (`403`, `429`, model not available)**:
    *   **Cause**: Invalid API key, rate limiting, or model access not enabled for your Anthropic account.
    *   **Solution**: Verify `ANTHROPIC_API_KEY` in `.env`. The `ClaudeProvider` uses model priority fallback (`claude-3-opus-20240229` → `claude-3-sonnet-20240229` → `claude-3-haiku-20240307`). Check your account's model access at console.anthropic.com.

*   **Local LLM (`llama.cpp`) Silent or Slow Response**:
    *   **Cause**: The local model might be taking too long to respond, is stuck, or not running efficiently.
    *   **Solution**: `LlamaCppProvider` now has a 120-second timeout. If it times out, you'll receive a specific fallback message. Check:
        *   `LOCAL_MODEL_PATH` in `.env` points to the *absolute* path of your `.gguf` model file.
        *   `llama-cpp-python` installation and GPU support (`n_gpu_layers=-1` for Metal/CUDA).
        *   Use `tests/test_local_llm_direct.py` for isolated troubleshooting of the local model.

## 3. Agent Behavior Issues

*   **Agent asks user for Bitable ID / table_id instead of creating one**:
    *   **Cause**: The system prompt's Autonomous Output Rules were missing or the agent's LLM didn't follow them.
    *   **Solution**: The `mcp_agent_system.md` template now includes explicit rules: if the user requests Bitable output without providing `app_token`, the agent must call `create_feishu_bitable` autonomously. If this still occurs, check that `PromptBuilder` is being used (not an inline system prompt).

*   **Agent stops at step N with "Agent reached maximum iterations"**:
    *   **Cause**: In the old architecture, `max_steps` was a hard limit. This has been replaced with cloud token budgeting.
    *   **Solution**: `max_steps` is now for progress display only. The real limit is `token_budget` (default 50,000 cloud tokens). When exceeded, the agent forces a final summary instead of failing. If you see this error, you may be running an outdated `mcp_agent.py`.

*   **Agent confuses `xiyou_keyword_analysis` with `search_products`**:
    *   **Cause**: With 48 tools listed flat, the LLM could not distinguish similar tools.
    *   **Solution**: Tools are now grouped by category (DATA/COMPUTE/FILTER/OUTPUT) in the system prompt. Tool descriptions include explicit disambiguation: "[Third-party Xiyouzhaoci tool, NOT Amazon search]".

*   **Agent calls the same tool with identical arguments in a loop**:
    *   **Cause**: LLM reasoning failure, often caused by weak model routing (e.g., local model handling `DEEP_REASONING` tasks).
    *   **Solution**: Three safeguards are in place: (1) `DEEP_REASONING` is forced for all agent calls, (2) duplicate detection after 2 identical calls injects a correction hint, (3) `ToolRegistry._validate_arguments()` strips hallucinated parameters.

*   **`Output domain could not route tool: ...` Error**:
    *   **Cause**: The centralized `handle_output_tool` aggregator lacks a keyword match for the specific tool name.
    *   **Solution**: Update the routing logic in `src/mcp/servers/output/tools/__init__.py` to include the new tool's naming pattern.

*   **`send_feishu_local_file` parameter confusion**:
    *   **Cause**: Distinguishing between `file_path` (source on disk) and `filename` (display name in Feishu).
    *   **Solution**: Ensure `export_md` is called first to generate the local file. The agent should use the absolute path returned by the tool for `file_path`.

*   **`Xiyouzhaoci auth token not found` — agent doesn't auto-authenticate**:
    *   **Cause**: The Xiyouzhaoci token file is missing and the agent didn't trigger the auto-SMS flow.
    *   **Solution**: `_xiyou_auth_required()` in `market/tools.py` auto-sends SMS when `XIYOUZHAOCI_PHONE` env var is set. Ensure the env var is configured.

## 4. Environment & Import Issues

*   **`ImportError: attempted relative import with no known parent package`**:
    *   **Cause**: You're running a submodule directly (e.g., `python src/mcp/server.py`) instead of as part of the package or via an absolute path setup.
    *   **Solution**: This was addressed by modifying `src/mcp/server.py` to use absolute imports and adding `sys.path.insert(0, project_root)`. Ensure you are running with `venv311/bin/python src/mcp/server.py` or similar.

*   **`No module named '...'` after `pip install`**:
    *   **Cause**: The library might not be installed in the currently active virtual environment, or `requirements.txt` is missing an entry.
    *   **Solution**: Re-run `pip install -r requirements.txt` inside your `venv311` after activating it.

*   **`src/core/fallback.py` causes circular import or wrong dependency**:
    *   **Cause**: The `FallbackHandler` was incorrectly placed in the `src/core/` layer, violating dependency rules.
    *   **Solution**: `fallback.py` has been moved to `src/intelligence/fallback.py`, which is its correct domain. Ensure all import paths are updated accordingly.
