# LLM Usage Guidelines

This document outlines best practices for interacting with the LLMs integrated into the AWS V2 Hybrid Intelligence Platform. Following these guidelines will help you maximize AI performance, control costs, and achieve reliable structured outputs.

## 1. Prompt Engineering Principles

*   **3-Layer System Prompt Architecture**: The `MCPAgent` system prompt is dynamically assembled from three layers:
    1.  **`mcp_agent_system.md` (Protocol Layer)** — Human-editable Markdown template. Defines the ReAct loop logic, JSON call formats, and autonomous output rules.
    2.  **`src/intelligence/prompts/config/` (Knowledge Layer - SSOT)** — YAML-based repository for Expert Roles, Analysis Frameworks (PSI, SWOT), and Report Templates. This ensures consistent business logic across both Agent and Workflow tracks.
    3.  **`PromptManager` (Assembly Layer)** — Orchestrates the injection of values from `config/workflow_defaults.yaml` into YAML templates using `$variable` syntax, ensuring AI standards are always synchronized with system thresholds.
*   **Execution Phases**: The template guides the LLM through 5 phases: COLLECT → FILTER → ENRICH → ANALYZE → OUTPUT. Not all phases are required for every task.
*   **Autonomous Output Rules**: Organized into **General Principles** (e.g., discover IDs via tools) and **Feishu-Specific Rules** (e.g., Attachment-First Policy for long reports, Bitable automation). This ensures the agent adapts its output strategy based on the platform.
*   **Config-Driven Prompts**: Business "red lines" (e.g., `high_monopoly_score`, `ad_traffic_ratio_max`) are defined once in `config/workflow_defaults.yaml`. Changing a value there automatically updates the reasoning standards in all AI-generated reports.
*   **Ad Dependency Red-lines**: The platform now enforces an **Advertising Dependency Policy**. If an ASIN's `advertisingTrafficScoreRatio` (from XiyouZhaoci) exceeds the threshold (default 35%), the LLM should flag it as high-risk, as it lacks organic "moat".
*   **Tool Disambiguation**: Similar tools are explicitly distinguished in descriptions (e.g., `search_products` = Amazon direct search; `xiyou_keyword_analysis` = third-party Xiyouzhaoci database).
*   **Negative Constraints**: Only use parameters in the tool's schema. One tool call per turn. No hallucinated data.

## 2. Token Management & Cost Control

*   **Cloud-Only Token Budget**: The `MCPAgent` tracks cumulative cloud token usage separately from local tokens. Only cloud API calls (Gemini, Claude, DeepSeek) count toward the budget (default: 50,000 tokens). Local model tokens are free and unlimited.
*   **Gemini Advanced Pricing Support**: The `PriceManager` accurately handles modern Gemini billing features:
    *   **Thinking Tokens**: For models like `gemini-2.0-flash-thinking`, the `thoughts_token_count` is included in the output token billing.
    *   **Prompt Caching**: If a request hits the Gemini cache, the `cached_content_token_count` is automatically deducted from the regular input count and billed at a significantly lower `cache_read` rate.
    *   **Tiered Pricing**: Automatic switching between `lte_200k` and `gt_200k` pricing tiers based on the prompt size.
*   **DeepSeek Pricing Support**: The `PriceManager` handles DeepSeek server-side KV cache billing:
    *   **Cache Hit / Miss split**: `prompt_tokens_details.cached_tokens` is billed at the lower `input_cache_hit` rate; the remainder at the standard `input` rate.
    *   **Reasoning tokens** (`deepseek-v4-flash` thinking mode): already folded into `completion_tokens` by the API — no double-counting.
    *   **V4-Pro promotion**: Standard tier uses the 75%-off price until 2026-05-05T15:59Z, after which `PriceManager` automatically switches to the `undiscounted` tier.
*   **Budget Enforcement**: When cloud tokens exceed the budget, the agent forces a final summary from collected data and notifies the user that remaining work will use batch API. The agent does NOT hard-fail.
*   **Leverage Local Models for Pre-processing**: For large text inputs (e.g., raw HTML, long customer reviews), use the `IntelligenceRouter` to automatically dispatch simple cleaning or summarization tasks to the local Llama.cpp model first.
*   **Independent Routing**: Every item in a batch is independently classified and routed, preventing complex tasks from being misrouted to simpler models.
*   **Token Tracking Fields**:
    *   `session.token_usage` — total tokens across all providers (informational).
    *   `session.cloud_token_usage` — cloud-only tokens (budget-relevant).
*   **GeminiProvider Token Reporting**: Now reads `usage_metadata.total_token_count` from responses, with `count_tokens()` fallback for older API versions.

## 3. Model Selection Strategies

*   **Automatic Routing**: The `IntelligenceRouter` (`src/intelligence/router/`) automatically selects the best model based on task classification.
    *   **Local LLM (`llama.cpp`)**: Ideal for `SIMPLE_CLEANING`, `DATA_EXTRACTION`. Cost-free, low latency for small tasks.
    *   **Cloud LLM (Gemini / Claude / DeepSeek)**: Best for `DEEP_REASONING`, complex `CREATIVE_WRITING`, and structured outputs requiring high accuracy. Set `DEFAULT_LLM_PROVIDER` in `.env` to choose:

| `DEFAULT_LLM_PROVIDER` | Provider | Default model | Batch API |
|---|---|---|---|
| `gemini` (default) | Google Gemini | auto-discovered (2.5-flash preferred) | Yes |
| `claude` / `anthropic` | Anthropic Claude | `CLAUDE_MODEL` env or auto | Yes |
| `deepseek` | DeepSeek | `DEEPSEEK_MODEL` env or `deepseek-v4-flash` | No |
| `local` / `llama` | Llama.cpp (local) | `LOCAL_MODEL_PATH` | No |

*   **DeepSeek Model Guide**:
    *   `deepseek-v4-flash` — general-purpose, very low cost ($0.14/$0.28 per 1M in/out). Non-thinking and thinking modes share the same model name; pass `{"thinking": true}` in request extras to enable chain-of-thought.
    *   `deepseek-v4-pro` — highest quality, reasoning-optimised ($0.435/$0.87 per 1M in/out at 75%-off promotion until 2026-05-05). Use for ad diagnosis synthesis and complex causal narratives.
    *   `deepseek-chat` / `deepseek-reasoner` — deprecated aliases for `deepseek-v4-flash`; billing now uses V4-Flash rates.
*   **Override**: For specific needs, you can temporarily override the router's decision by explicitly passing a `category` to `route_and_execute`.

## 4. Handling Hallucinations & Inaccuracies

*   **Structured Output**: Always aim for structured outputs (Pydantic models) to minimize hallucinations. This forces the LLM to adhere to a schema.
*   **Grounding with Data**: Provide concrete, factual data from extractors and processors to anchor the LLM's responses.
*   **Verification**: Implement post-processing checks where possible to validate LLM output against business rules.

## 5. Debugging LLM Interactions

*   **Detailed Logging**: Enable `DEBUG` level logging for `src.intelligence.providers.*` and `src.intelligence.router` to see raw prompts and responses.
*   **Raw Output Review**: Check the raw JSON output from the LLM when structured parsing fails to identify schema mismatches or formatting issues.
*   **Traceback Analysis**: When an Agent fails, follow the traceback to identify if the error originated from data acquisition, processing, or the LLM itself.
