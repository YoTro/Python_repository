# Input and Data Schemas: Pydantic-Driven Agents

This project now primarily uses **Pydantic models** (`src/core/models/`) as the standardized data contract across all layers. Inputs to agents and processors are structured Python objects, not raw CSV files. CSV remains a convenient format for file-based I/O.

## 1. Core Data Models (The Universal Language)

All data within the system conforms to the Pydantic models defined in `src/core/models/`:

*   **`Product`**: Represents a single Amazon product with attributes like ASIN, title, price, rank, review count, etc.
*   **`Review`**: Details of a customer review, including content, rating, author, and verification status.
*   **`MarketAnalysisReport`**: Structured output for comprehensive market research, including SWOT analysis, competitor entries, and strategic recommendations.

**Benefit**: These models provide strict validation, type safety, and auto-generated JSON schemas, which are crucial for LLMs to understand and manipulate data correctly via the MCP Protocol.

## 1.1 Tool Registration Metadata

Each tool registered in `ToolRegistry` carries additional `ToolMeta`:

| Field | Type | Values | Purpose |
|---|---|---|---|
| `category` | `str` | `DATA`, `COMPUTE`, `FILTER`, `OUTPUT` | Groups tools in the agent system prompt |
| `returns` | `str` | Free text | Describes output to help LLM plan tool chains |

Example registration:
```python
tool_registry.register_tool(
    tool, handler,
    category="DATA",
    returns="list of products with ASIN, title, price",
)
```

The `ToolCatalogFormatter` reads this metadata to generate a categorized tool catalog (48 tools across 4 categories) injected into the agent's system prompt via `$tool_catalog`.

## 2. Agent Inputs and Task Arguments

Agents and high-level processors primarily receive inputs as either:

*   **Direct Python Objects**: Instances of Pydantic models (e.g., `List[Product]`).
*   **Function Arguments**: Simple parameters like `keyword: str`, `depth: str`, as defined in the `Agent`'s `run` method or `Processor`'s methods.

### A. `MarketResearcher` Agent (Planned)

> **Status**: This agent is planned but not yet implemented in the codebase.

**Goal**: Perform a comprehensive market analysis for a given keyword.

**Input Arguments (via MCP Tool Call or direct Python call):**
*   `keyword`: `str` (Required) - The primary search term (e.g., "massage gun", "yoga mat").
*   `depth`: `str` (Optional, `"quick"` or `"full"`, default `"quick"`) - Controls the intensity of the research (number of competitors analyzed).

**Output**: `MarketAnalysisReport` Pydantic model (JSON format if via MCP).

### B. `ReviewSummarizer` Processor

**Goal**: Generate a structured summary from raw product reviews.

**Input Arguments (via internal Agent call):**
*   `reviews`: `List[Review]` (Required) - A list of Pydantic `Review` objects.

**Output**: `ReviewSummary` Pydantic model.

### C. `SalesEstimator` Processor

**Goal**: Predict monthly sales for a product based on its Best Sellers Rank.

**Input Arguments (via internal Agent call):**
*   `product`: `Product` (Required) - A Pydantic `Product` object containing `sales_rank`.
*   `training_data_path`: `str` (Optional) - Path to a CSV file for training the regression model.

**Output**: `int` (Estimated monthly sales).

### D. `ProductSimilarityProcessor` Processor

**Goal**: Analyze product similarity and perform clustering based on textual data.

**Input Arguments (via internal Agent call):**
*   `products`: `List[Product]` (Required) - A list of Pydantic `Product` objects.

**Output**: `List[Dict]` (Products with cluster IDs or similarity scores).

## 3. CLI-Driven Tasks - Legacy (Migrated)

> **Status**: The `src/tasks/` module has been removed. These CLI tasks are legacy and have been migrated to the MCP server architecture (`src/mcp/servers/`). The input schemas below are retained as reference for the equivalent MCP tools.

This section details the input requirements for tasks originally designed for command-line interface (CLI) execution via `main.py`. These tasks utilized CSV files for bulk input/output operations.

### A. General ASIN-Based Extraction Tasks

Most tasks requiring product-specific data will expect a CSV file containing a list of ASINs.

**Affected CLI Commands**: `details`, `stock`, `review_count`, `past_month_sales`, `fulfillment`, `dimensions`, `ranks`, `images`, `videos`, `feedback`.

| Column Header      | Required / Optional | Description                                  |
| :----------------- | :------------------ | :------------------------------------------- |
| `ASIN`             | Required            | Amazon Standard Identification Number.       |
| `URL`              | Optional            | Direct product URL (used as fallback if ASIN fails). |

**Example Input (`input.csv`):**
```csv
ASIN,URL
B083L8RNJR,
B09JBCSC7H,https://www.amazon.com/dp/B09JBCSC7H
```

### B. URL-Based Seller Tasks

These tasks require a seller storefront URL.

**Affected CLI Commands**: `product_num`.

| CLI Argument       | Required / Optional | Description                                  |
| :----------------- | :------------------ | :------------------------------------------- |
| `--url`            | Required            | Seller storefront URL on Amazon.             |

### C. Keyword Search Tasks

These tasks initiate a search on Amazon and generate an initial list of products or ASINs. They typically do not require an input CSV but are driven by CLI arguments.

**Affected CLI Commands**: `sales`, `keywords_rank`.

| CLI Argument       | Required / Optional | Description                                  |
| :----------------- | :------------------ | :------------------------------------------- |
| `--keyword`        | Required            | The search term (e.g., "bluetooth speaker"). |
| `--pages`          | Optional            | Number of search result pages to scrape (default: 1). |

**Example CLI Usage:**
```bash
python3.11 main.py sales --keyword "outdoor rug" --pages 3 --output data/rug_asins.csv
```

### D. Bestsellers Extraction Task

This task scrapes products from a specific Amazon Best Sellers category URL.

**Affected CLI Commands**: `bestsellers`.

| CLI Argument       | Required / Optional | Description                                  |
| :----------------- | :------------------ | :------------------------------------------- |
| `--url`            | Required            | The full URL of the Amazon Best Sellers category page. |

**Example CLI Usage:**
```bash
python3.11 main.py bestsellers --url "https://www.amazon.com/Best-Sellers-Electronics/zgbs/electronics/" --output data/electronics_bsr.csv
```

### E. Analysis Capabilities (Intelligence Layer)

The following analysis functionalities have been migrated from CLI tasks to the Intelligence Layer (`src/intelligence/processors/`) and are now accessed through Agent workflows or direct Python calls:

*   **Product Similarity** (`ProductSimilarityProcessor`): TF-IDF vectorization + KMeans/DBSCAN clustering. Accepts `List[Product]` Pydantic models.
*   **Sales Estimation** (`SalesEstimator`): UCLA Sales-Rank Regression. Accepts `Product` Pydantic models with `sales_rank` data.
*   **Review Summarization** (`ReviewSummarizer`): LLM-powered sentiment analysis. Accepts `List[Review]` Pydantic models.
*   **Category Monopoly Analysis** (`CategoryMonopolyAnalyzer`): Multi-dimensional market scoring (CR3, CV pricing, etc.). Accepts lists of dicts containing BSR product data and keyword metrics.

These processors can be called directly via the Intelligence Router or through Agent workflows.

---

**Note**: CLI tasks are designed for deterministic data acquisition only. For analysis and AI-driven workflows, use the Agent layer (`src/agents/`) or Intelligence processors (`src/intelligence/processors/`) which leverage Pydantic models for type-safe, validated data contracts.
