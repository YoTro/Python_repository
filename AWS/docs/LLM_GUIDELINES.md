# LLM Usage Guidelines

This document outlines best practices for interacting with the LLMs integrated into the AWS V2 Hybrid Intelligence Platform. Following these guidelines will help you maximize AI performance, control costs, and achieve reliable structured outputs.

## 1. Prompt Engineering Principles

*   **3-Layer System Prompt**: The `MCPAgent` system prompt is built from three components:
    1.  **`mcp_agent_system.md`** — Human-editable Markdown template with `$tool_catalog` and `$token_budget` variables. Edit this to change agent behavior without touching code.
    2.  **`PromptBuilder`** — Loads the `.md` template and injects runtime values via `string.Template` (no Jinja2 dependency).
    3.  **`ToolCatalogFormatter`** — Reads `ToolMeta(category, returns)` from the registry and groups 48 tools into DATA / COMPUTE / FILTER / OUTPUT sections.
*   **Execution Phases**: The template guides the LLM through 5 phases: COLLECT → FILTER → ENRICH → ANALYZE → OUTPUT. Not all phases are required for every task.
*   **Autonomous Output Rules**: The agent is instructed to never ask the user for IDs or configuration it can discover via tools. For example, if asked to output to Bitable without an `app_token`, the agent must call `create_feishu_bitable` autonomously.
*   **Tool Disambiguation**: Similar tools are explicitly distinguished in descriptions (e.g., `search_products` = Amazon direct search; `xiyou_keyword_analysis` = third-party Xiyouzhaoci database).
*   **Negative Constraints**: Only use parameters in the tool's schema. One tool call per turn. No hallucinated data.

## 2. Token Management & Cost Control

*   **Cloud-Only Token Budget**: The `MCPAgent` tracks cumulative cloud token usage separately from local tokens. Only cloud API calls (Gemini, Claude) count toward the budget (default: 50,000 tokens). Local model tokens are free and unlimited.
*   **Budget Enforcement**: When cloud tokens exceed the budget, the agent forces a final summary from collected data and notifies the user that remaining work will use batch API. The agent does NOT hard-fail.
*   **Leverage Local Models for Pre-processing**: For large text inputs (e.g., raw HTML, long customer reviews), use the `IntelligenceRouter` to automatically dispatch simple cleaning or summarization tasks to the local Llama.cpp model first.
*   **Batch Fallback**: When the agent's cloud token budget is exhausted during a complex research task, `batch_route_and_execute()` can process remaining items asynchronously at lower priority.
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
