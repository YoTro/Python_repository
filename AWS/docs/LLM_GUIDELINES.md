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

*   **Cloud-Only Token Budget**: The `MCPAgent` tracks cumulative cloud token usage separately from local tokens. Only cloud API calls (Gemini, Claude) count toward the budget (default: 50,000 tokens). Local model tokens are free and unlimited.
*   **Gemini Advanced Pricing Support**: The `PriceManager` accurately handles modern Gemini billing features:
    *   **Thinking Tokens**: For models like `gemini-2.0-flash-thinking`, the `thoughts_token_count` is included in the output token billing.
    *   **Prompt Caching**: If a request hits the Gemini cache, the `cached_content_token_count` is automatically deducted from the regular input count and billed at a significantly lower `cache_read` rate.
    *   **Tiered Pricing**: Automatic switching between `lte_200k` and `gt_200k` pricing tiers based on the prompt size.
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
    *   **Cloud LLM (Gemini / Claude)**: Best for `DEEP_REASONING`, complex `CREATIVE_WRITING`, and structured outputs requiring high accuracy. The router defaults to Gemini for cloud tasks; Claude is available as an alternative via `DEFAULT_LLM_PROVIDER=claude`.
*   **Override**: For specific needs, you can temporarily override the router's decision by explicitly passing a `category` to `route_and_execute`.

## 4. Handling Hallucinations & Inaccuracies

*   **Structured Output**: Always aim for structured outputs (Pydantic models) to minimize hallucinations. This forces the LLM to adhere to a schema.
*   **Grounding with Data**: Provide concrete, factual data from extractors and processors to anchor the LLM's responses.
*   **Verification**: Implement post-processing checks where possible to validate LLM output against business rules.

## 5. Debugging LLM Interactions

*   **Detailed Logging**: Enable `DEBUG` level logging for `src.intelligence.providers.*` and `src.intelligence.router` to see raw prompts and responses.
*   **Raw Output Review**: Check the raw JSON output from the LLM when structured parsing fails to identify schema mismatches or formatting issues.
*   **Traceback Analysis**: When an Agent fails, follow the traceback to identify if the error originated from data acquisition, processing, or the LLM itself.
