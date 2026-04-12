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

*   **`Xiyouzhaoci auth token not found` or `Xiyou Auth required` — agent gets stuck**:
    *   **Cause**: The Xiyouzhaoci token is expired or missing. In the new architecture, this triggers a `JobSuspendedError` and expects human interaction via a QR code.
    *   **Solution**: Ensure you are using an interactive client (like Feishu) that can render the `INTERACTION_REQUIRED` signal as a card. If using CLI, follow the Markdown link printed in the terminal, scan the QR code within 120 seconds, and manually trigger the resume command (or reply 'I have scanned' if the CLI agent supports it). Check `data/sessions/` to ensure the session status is `suspended_for_human`.

*   **Feishu Bot throws `processor not found, type: card.action.trigger`**:
    *   **Cause**: You clicked the "I have scanned" button on the Feishu card, but the bot listener isn't configured to handle interactive card actions.
    *   **Solution**: Ensure `src/entry/feishu/bot_listener.py` has `register_p2_card_action_trigger` enabled and the `InteractionRegistry` is properly imported. Restart the bot listener process.

*   **Agent task hangs indefinitely after scanning QR code**:
    *   **Cause**: The webhook from the Feishu card click didn't reach your server, or the `JobManager` failed to resume the job.
    *   **Solution**: 
        1. Check the bot listener logs for `Received card action trigger: VERIFY_XIYOU_LOGIN`.
        2. Ensure the `JobManager` Reaper task hasn't already cancelled the job (timeout is 120-300 seconds).
        3. Verify that `job_mgr.resume(job_id)` is returning `True`.

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

## 5. Rate Limiting Issues

*   **Feishu command rejected with `"Rate limit exceeded for Feishu workflow"`**:
    *   **Cause**: Either the per-chat cooldown window (default 5 s) has not expired, or the daily tenant quota (`free`: 50 req/day) is exhausted.
    *   **Solution (cooldown)**: Wait for the cooldown window to pass. Adjust `entry_limits.feishu_workflow.cooldown_seconds` in `config/rate_limits.yaml` if the default is too aggressive.
    *   **Solution (quota)**: Check `RateLimiter()._tenant_counters` for today's count. Upgrade `plan_tier` in `AuthMiddleware` or increase `tenant_quotas.free.daily_requests`.

*   **Job fails immediately with `"Per-chat concurrent limit reached"`**:
    *   **Cause**: The same Feishu chat already has `per_chat_concurrent` jobs in `PENDING` or `RUNNING` state (default: 2 for `feishu_workflow`, 1 for `feishu_explore`).
    *   **Solution**: Wait for the previous job to complete. To raise the limit, increase `entry_limits.feishu_workflow.per_chat_concurrent` in `config/rate_limits.yaml`.
    *   **Debugging**: Check `RateLimiter()._concurrent` for the current slot counts (key format: `"feishu_workflow:chat_<id>"`).

*   **Job fails with `"Global concurrent limit reached"`**:
    *   **Cause**: The total number of active jobs of this entry type across all chats has reached `concurrent_jobs` (default: 10 for `feishu_workflow`, 3 for `feishu_explore`).
    *   **Solution**: Increase `entry_limits.<type>.concurrent_jobs` or add more `max_workers` to `JobManager`.

*   **Concurrent slot counter appears stuck (counter never decrements)**:
    *   **Cause**: Should not happen — `concurrent_slot()` uses `try/finally` to guarantee release even on crash or `asyncio.CancelledError`. If observed, a job is likely still running (status `RUNNING`) rather than truly leaked.
    *   **Debugging**: Inspect `get_job_manager()._jobs` for any records stuck in `RUNNING` status. Check worker logs for exceptions that were silently swallowed upstream.

*   **External API client gets `RetryableError: xiyouzhaoci source rate limit timeout`**:
    *   **Cause**: The token bucket for `xiyouzhaoci` has been exhausted and no token refilled within the 30-second timeout (default). This happens when many parallel tasks hit the same source.
    *   **Solution**: Reduce `parallel_concurrency` in `config/workflow_defaults.yaml`, or increase `source_limits.xiyouzhaoci.requests_per_minute` / `burst` in `config/rate_limits.yaml`.

*   **External API returns `429 Too Many Requests` repeatedly**:
    *   **Cause**: The token-bucket rate in `source_limits` is higher than the actual API limit, or a burst of requests depleted the API provider's quota.
    *   **Solution**: The client automatically retries up to 3 times with exponential backoff (reads `Retry-After` header when present). If retries are exhausted, `RetryableError` is raised and the job is marked `FAILED`. Reduce `requests_per_minute` in `config/rate_limits.yaml` for the offending source.

## 6. Workflow Resume & Checkpoint Issues

*   **Job fails mid-workflow — how to resume from the last checkpoint**:
    *   **Cause**: Any unhandled exception in a `ProcessStep` or `EnrichStep` saves the current checkpoint and marks the job `FAILED`. The checkpoint file is kept at `data/checkpoints/<job_id>.json`.
    *   **Via Feishu**: Send `恢复任务 <job_id>` in the same chat. The `ResumeJobCommand` loads the checkpoint, rebuilds `FeishuCallback` for the same `chat_id`, and calls `resume_from_checkpoint()`. The engine skips all completed steps and resumes from the failed one.
    *   **Via code**:
        ```python
        manager.resume_from_checkpoint(job_id="7d480543", callback=your_callback)
        ```
    *   **Note**: `workflow_name` and `workflow_params` are read from the checkpoint automatically (stored there since engine v2). For checkpoints created before this change, patch them manually:
        ```python
        import json
        with open("data/checkpoints/<job_id>.json") as f: d = json.load(f)
        d["workflow_name"] = "category_monopoly_analysis"
        d["workflow_params"] = {"url": "...", "store_id": "US"}
        d["ctx_cache"] = {}
        with open("data/checkpoints/<job_id>.json", "w") as f: json.dump(d, f)
        ```

*   **`unsupported operand type(s) for /: 'NoneType' and 'float'` in `calculate_monopoly_score`**:
    *   **Cause**: Two known patterns:
        1. `dict.get(key, default)` does not apply `default` when the key exists but its value is `None`. This affects `actual_bsr_ad_ratio`, `sales`, and `review_count` fields when the workflow cache is empty (e.g., fresh resume without `ctx_cache`).
        2. `ctx.cache` is empty on resume because old checkpoints did not save it. Later steps that depend on cache values populated by earlier steps receive `None` instead of the expected data.
    *   **Solution**: Both are fixed — `_analyze_ad_competition` and `_analyze_sales_distribution` use `or`-fallback coercion; engine now saves/restores `ctx_cache` in every checkpoint.

*   **`on_error` callback does not include `job_id` — user cannot resume**:
    *   **Cause**: Old `JobCallback.on_error(error)` signature had no `job_id` parameter.
    *   **Solution**: Signature is now `on_error(error, job_id=None)`. `FeishuCallback` prints the job_id and resume command; `MCPCallback.get_result()` returns `{"resumable": True, "job_id": "..."}` on failure.

*   **Feishu file upload fails with `40009 internal server error`**:
    *   **Cause**: Filename contained illegal characters — typically newlines (`\n`) from LLM-generated `main_keyword` strings that were not sanitised before being embedded in the filename.
    *   **Solution**: `_prepare_report_artifact` now sanitises the keyword with `re.sub(r"[^\w]", "_", raw_kw, flags=re.ASCII)[:40]`, stripping all non-ASCII-word characters including `\n`, `\r`, and Windows-reserved characters (`\ / : * ? " < > |`).

## 7. Amazon Advertising API (Ads API) Issues

*   **`404 Method Not Found` for `bidRecommendations`**:
    *   **Cause**: Attempting to use deprecated v2 endpoints.
    *   **Solution**: Ensure the client is using the modern Theme-based endpoint: `/sp/targets/bid/recommendations`.

*   **`429 Too Many Requests`**:
    *   **Cause**: Exceeding the Amazon Advertising API rate limits (TPS).
    *   **Solution**: The `AmazonAdsClient` includes built-in exponential backoff. Increase `max_retries` or ensure you are not making concurrent calls for the same profile in a tight loop.

*   **`415 Unsupported Media Type` or `Cannot consume content type`**:
    *   **Cause**: Amazon Advertising API v5.0 strictly requires specific vendor media types in `Content-Type` and `Accept` headers.
    *   **Solution**: Ensure headers are set to `application/vnd.spthemebasedbidrecommendation.v5+json`. If failure persists, try `application/json` for `Content-Type` while keeping the vendor string for `Accept`.

*   **`400 Bad Request: An unknown scope was requested` during OAuth**:
    *   **Cause**: Your LWA Application has not been granted "Advertising API" access in the Amazon Ads console.
    *   **Solution**: Visit [Amazon Ads API Solutions](https://advertising.amazon.com/api-solutions) and request API access for your Client ID. Simply having an LWA app is not enough.

*   **`401 Unauthorized` or `Invalid Profile ID`**:
    *   **Cause**: The `AMAZON_ADS_PROFILE_ID_{STORE}` in your `.env` does not belong to the account authorized by the Refresh Token.
    *   **Solution**: Run `scripts/setup_amazon_ads.py` to list all valid Profile IDs for your authorized account and verify them against your `.env` settings.
