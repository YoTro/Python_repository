# MCP Protocol Usage and Extension

This document serves as a guide for understanding and extending the Model Context Protocol (MCP) integration within the AWS V2 Hybrid Intelligence Platform. MCP is the foundational bridge that standardizes how our capabilities are exposed to both internal Orchestrators (Workflow Engine & MCP Agent) and external LLM clients (like Claude Desktop).

## 1. Core MCP Concepts in AWS V2

*   **Tools**: Executable functions or scrapers exposed by our Domain Servers. LLMs and Workflows call these tools with structured JSON arguments.
    *   **Examples**: `get_amazon_bestsellers`, `calc_profit`, `check_epa`.
*   **Resources**: Static, structured data (e.g., JSON files) that LLMs can 'read' to get factual context.
    *   **Examples**: `resource://aws-knowledge/fba_fee.json` for FBA fulfillment fees.
*   **Prompts**: Pre-defined Standard Operating Procedures (SOPs) or guided prompts that clients can initiate.

## 2. Architectural Paradigm: Domain-Driven Microservices

Our MCP capabilities are structured to support strict microservice isolation and decoupling:

*   **`src/mcp/servers/` (The Providers)**: Capabilities are grouped into isolated business domains, further categorized into L1 and L2 layers:
    *   **L1 (Raw Data Layer)**: No external dependencies. Scrapes raw data (e.g., `amazon`, `market`, `social`) and writes to the global `DataCache`.
    *   **L2 (Calculation / Output Layer)**: Consumes data from `DataCache` or processes outputs. Does NOT call L1 directly (e.g., `finance`, `compliance`, `output`).
    *   *Example*: The `output` domain is subdivided into discrete handlers (`write_bitable.py`, `send_card.py`, `export_csv.py`) that aggregate via `tools.py` to maintain the Single Responsibility Principle.
*   **`src/mcp/client/` (The Consumers)**: Contains the `LocalMCPClient`. This is the unified interface used by the `WorkflowEngine` and the `MCPAgent` to call tools. By routing internal code through the MCP Client, we ensure 100% consistency between LLM-driven and Code-driven execution.
*   **`src/registry/` (The Capability Hub)**: Moved to the top-level directory (`src/registry/tools.py`, `resources.py`, `prompts.py`). It acts as the central hub that aggregates all capabilities across different servers without tying them to a specific protocol. Each tool carries `ToolMeta` with `category` (DATA/COMPUTE/FILTER/OUTPUT) and `returns` metadata, used by the `ToolCatalogFormatter` to build categorized system prompts for the agent.
*   **`src/mcp/server.py`**: The external-facing stdio server (`AWSHelperServer`) that allows Desktop LLMs (like Claude) to connect to our platform.

## 3. Extending Capabilities (Adding a New Tool)

To add a new capability that an LLM or Workflow can use:

1.  **Locate the Domain**: Decide which server the tool belongs to (e.g., `src/mcp/servers/finance/`). If it's a new domain, create a new folder.
2.  **Define the Logic**: Write your core logic (e.g., a new calculator or scraper) within that domain. For complex domains like `output`, split logic into sub-modules (e.g., `tools/new_feature.py`).
3.  **Define the MCP Tool**: In the domain's `tools.py` (or sub-module):
    *   Instantiate a `mcp.types.Tool` object with a unique `name`, a clear `description` (crucial for LLMs), and an `inputSchema` (JSON Schema).
    *   Implement an `async` handler function that accepts `name: str` and `arguments: dict`.
    *   The handler should execute the core logic (interacting with `DataCache` if crossing L1/L2) and return a `list[mcp.types.TextContent]`.
    *   Call `tool_registry.register_tool(your_tool_instance, your_handler, category="DATA", returns="description of output")`.
        *   `category`: One of `DATA`, `COMPUTE`, `FILTER`, `OUTPUT`. Controls how the tool appears in the agent's system prompt catalog.
        *   `returns`: Short description of what the tool returns (shown to LLM for planning).
4.  **Register the Domain**: Ensure your `tools.py` is imported in **`src/registry/tools.py`** to trigger the registration during startup.

## 4. Using Tools Internally

Do not import Scrapers or Logic directly between domains. Always use the MCP Client or `DataCache`:

**In a Workflow Step (`EnrichStep` / `ProcessStep`):**
```python
async def _my_step(items: list, ctx: WorkflowContext):
    # Call tool securely via the unified client
    results = await ctx.mcp.call_tool_json("calc_profit", {"asin": "B001", "estimated_cost": 10})
    return results
```

**In an Agent:**
The `MCPAgent` uses `PromptBuilder` to assemble a categorized system prompt from `ToolRegistry` metadata. Tools are grouped by category (DATA → COMPUTE → FILTER → OUTPUT) with parameter schemas and return descriptions, enabling the LLM to plan multi-phase execution autonomously.

## 5. External Client Integration (Claude Desktop)

We provide an automated deployment script to connect Claude Desktop to your local AWS V2 tools:

```bash
./scripts/deploy_claude_desktop.sh
```
This script automatically detects your OS and injects the `aws-market-intelligence` configuration into your `claude_desktop_config.json`.

*(Manual Configuration Alternative)*
```json
{
  "mcpServers": {
    "aws-market-intelligence": {
      "command": "/path/to/AWS/venv311/bin/python",
      "args": ["src/mcp/server.py"],
      "cwd": "/path/to/AWS"
    }
  }
}
```

## 7. Featured Tool Capabilities

### Output & Delivery (L2)
*   **`populate_feishu_bitable_records`**: Reuses initial empty rows in a new Bitable to ensure data starts from Row 1. Preferred for new exports.
*   **`send_feishu_local_file`**: Uploads a local file and sends it as an IM attachment.
*   **`send_feishu_url_file`**: Downloads a file from a URL and forwards it as a Feishu attachment.
*   **`send_feishu_data_file`**: Converts raw list-of-dicts data into a CSV and sends it as an attachment.
*   **`send_feishu_text` / `send_feishu_card`**: Sends plain text or markdown cards to Feishu.

> **Note on Feishu Targeting**: All `send_feishu_*` tools (except webhook) now support **Implicit Context Resolution**. If `receive_id` or `receive_id_type` are omitted in the tool call, the system automatically resolves the target `chat_id` from the active conversation context (`feishu_chat_id`). Explicitly provided IDs will always override the context.

### Market Intelligence (L1)
*   **`xiyou_keyword_analysis`**: Requests keyword traffic and competitor data from Xiyouzhaoci, returning a local file path.
*   **`xiyou_asin_lookup`**: Reverse-lookups keywords for an ASIN via Xiyouzhaoci.
*   **`xiyou_asin_compare_keywords`**: Compares multiple ASINs (up to 20) for common keywords and performance trends.
*   **`xiyou_get_aba_top_asins`**: Queries top ASINs and their click/conversion shares for specific search terms based on Amazon Brand Analytics (ABA) ranking data.
*   **`xiyou_get_search_terms_ranking`**: Retrieves search frequency ranks, growth ratios, and trends for variations of a root query string using ABA data.

### Social Media Intelligence (L1/L2 Decoupled)
*   **`tiktok_fetch_data` (L1)**: Scrapes raw TikTok data (tag metadata, trending videos, and comments) for a product. Data is stored in the internal `DataCache`.
*   **`tiktok_calculate_virality` (L2)**: Processes cached TikTok data to compute the Promotional Strength Index (PSI), organic leverage, and purchase intent analysis.

### Compliance核查 (L2)
*   **`check_amazon_restriction`**: Keyword-based lookup in local Amazon restricted products database.
*   **`check_epa`**: Checks if product keywords trigger EPA FIFRA pesticide device regulations.

