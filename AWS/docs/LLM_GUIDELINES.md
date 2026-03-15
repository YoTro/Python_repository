# LLM Usage Guidelines

This document outlines best practices for interacting with the LLMs integrated into the AWS V2 Hybrid Intelligence Platform. Following these guidelines will help you maximize AI performance, control costs, and achieve reliable structured outputs.

## 1. Prompt Engineering Principles

*   **Clear Role & Persona**: Always start your system message by clearly defining the LLM's role (e.g., "You are a Senior Amazon Category Manager.").
*   **Explicit Instructions**: Provide unambiguous instructions for the task. Use bullet points or numbered lists.
*   **Output Format**: Explicitly state the desired output format, especially when expecting JSON. The `IntelligenceRouter` and `Pydantic` schemas handle this, but reinforcing it in the prompt helps.
*   **Context First**: Provide all necessary context (data, previous turns, resources) before asking the question.
*   **Example-Driven (Few-shot)**: For complex or nuanced tasks, provide one or two examples of input/output pairs.
*   **Negative Constraints**: Tell the LLM what *not* to do (e.g., "Do not include conversational filler.", "Return ONLY the JSON.").

## 2. Token Management & Cost Control

*   **Understand Token Limits**: Be aware of the context window limits of both local and cloud models.
*   **Leverage Local Models for Pre-processing**: For large text inputs (e.g., raw HTML, long customer reviews), use the `IntelligenceRouter` to automatically dispatch simple cleaning or summarization tasks to the local Llama.cpp model first.
*   **Batching**: For high-volume, non-time-sensitive tasks, explore batch processing options (currently a future feature for cloud providers).

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
