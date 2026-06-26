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

## 3.5 What Belongs in `tools.py` — Boundary Rules

`tools.py` is the **orchestration layer**: it wires L1/L2 calls together, manages `DataCache`, and returns `TextContent`. It is not the place for business logic, prompt engineering, or LLM inference.

### Belongs in `tools.py`

| What | Why |
|---|---|
| `async` handler dispatching on `name` | Core MCP protocol requirement |
| `data_cache.get` / `data_cache.set` | L1→L2 handoff contract |
| `mcp.types.Tool` definition + `tool_registry.register_tool()` | Discovery and registration |
| Private helper functions tightly coupled to a single tool's domain data | See exception rule below |

### Does NOT belong in `tools.py`

| What | Where instead |
|---|---|
| Reusable LLM analysis (sentiment, summarization, classification) | `src/intelligence/processors/` as an AI-backed processor |
| Business scoring logic | Domain-specific calculator class or pure-algorithm processor |
| Prompt construction for non-trivial analysis | Processor class or `intelligence/prompts/` template |

### LLM calls in `tools.py` — the exception rule

Avoid inline LLM calls in `tools.py`. The one acceptable exception is a **thin, private helper** that meets all three conditions:

1. Called by exactly one MCP tool (not shared across tools or domains)
2. Tightly coupled to domain data defined in the same file (e.g., a category taxonomy dict)
3. Not useful to workflows — a workflow would never call it directly

When these conditions are met, a module-level `async def _my_helper(...)` function is acceptable. When any condition is broken, extract to `intelligence/processors/`.

**Correct — delegating to a processor:**
```python
from src.intelligence.processors.comment_analyzer import CommentAnalyzer
from src.intelligence.providers.factory import ProviderFactory

result = await CommentAnalyzer(provider=ProviderFactory.get_provider()).analyze(
    comments, brand, product_name
)
```

**Incorrect — inline LLM logic in handler:**
```python
router = IntelligenceRouter()
response = await router.route_and_execute(long_prompt_with_schema...)
# parsing, fallback logic, schema constants all inline in the handler
```

`IntelligenceRouter` is for task routing in the agent track. In `tools.py`, use `ProviderFactory.get_provider()` to obtain the default cloud provider.

## 4. Using Tools Internally

Do not import scrapers, logic, or handler functions directly between domains or from `tools.py` into workflows. Always use the MCP Client or `DataCache`:

**In a Workflow Step (`EnrichStep` / `ProcessStep`):**
```python
async def _my_step(item: dict, ctx: WorkflowContext):
    # Correct — call through the unified MCP client
    results = await ctx.mcp.call_tool_json("calc_profit", {"asin": "B001", "estimated_cost": 10})
    return results
```

**Why not import from `tools.py` directly?**

Handler functions in `tools.py` have an internal interface (raw `dict` arguments, `DataCache` side effects, `TextContent` return type) that is not designed for direct callers. Importing them into a workflow creates tight coupling to implementation details that should be opaque — if the handler signature or cache key changes, the workflow silently breaks. The MCP Tool JSON schema is the stable public contract; `ctx.mcp.call_tool_json()` is the only legitimate caller.

```python
# Incorrect — never do this in a workflow
from src.mcp.servers.social.tools import _handle_tiktok_calculate_virality
result = await _handle_tiktok_calculate_virality({"keyword": kw}, data_cache)

# Correct — call through ctx.mcp
result = await ctx.mcp.call_tool_json("tiktok_calculate_virality", {"keyword": kw})
```

The one exception is **`intelligence/processors/`** — processor classes (`CommentAnalyzer`, `ReviewSummarizer`, etc.) are designed for direct import by both workflows and tools. They are not MCP handlers; they are reusable Python objects with stable interfaces.

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

### Amazon Data Extraction (L1)
*   **`search_profitability_products`**: Fast, ad-free product search using the internal Profitability Calculator API. Returns rich organic metadata (ASIN, title, brand, dimensions, weight, price, category rank, reviews). Ideal for bulk, precise data extraction without HTML parsing overhead.
*   **`search_products`**: Standard Amazon keyword search returning basic product lists, reflecting the actual customer search page (including sponsored positions).
*   **`get_product_details`**: Deep-dive extraction for a specific ASIN, fetching high-fidelity data like feature bullets, full descriptions, fulfillment status (FBA/FBM), A+ content flag (`has_a_plus_content`), and A+ premium background image URLs (`aplus_images`).
*   **`get_amazon_bestsellers`**: Scrapes Best Sellers lists (up to 100 items) with pagination support, returning ranked products.
*   **`get_stock_estimate`**: Estimates remaining inventory for an ASIN using the sophisticated 999 Add-to-Cart method to bypass limits.
*   **`get_batch_past_month_sales`**: Fetches the "X bought in past month" badge for one or more ASINs via Amazon search (`/s/?k=ASIN1|ASIN2...`). Accepts `asins: array`. Returns `{ASIN: int|null}`. Batches up to 20 ASINs per request; hit rate ~98% on BSR products.
*   **`get_review_count`**: Fetches `GlobalRatings` (all star ratings) and `WrittenReviews` (ratings with text) for a product, plus their `Ratio`. Natural ratio ≈ 0.10 (1:10); `Ratio > 0.50` is a strong fake-review signal. Uses the dedicated `/product-reviews/{asin}` page.
*   **`get_keyword_rank`**: Scans multiple search pages to determine the exact organic ranking position of target ASINs for a given keyword.
*   **`get_reviews`**: Fetches paginated customer reviews using a three-tier fallback: AJAX POST (fast internal endpoint) → HTML GET (page scraping) → Chromium browser (WAF-bypass). Each tier is selected automatically based on WAF state; the browser tier is only invoked when upper tiers are blocked. Supports `filter_by_star`, `reviewer_type`, `sort_by`, `format_type`, `media_type`, and `filter_by_keyword` parameters. See the Cookie Pool section below for concurrent multi-account architecture.
*   **BSR Navigation**: `get_top_bsr_categories` and `get_bsr_subcategories` allow dynamic exploration of the Best Sellers Rank category tree.
*   **Seller Intelligence**: `get_seller_product_count` and `get_seller_feedback` provide insights into a merchant's storefront size and recent performance.
*   **`refresh_amazon_cookies`**: Launches a headless (or manual) browser to capture fresh `session-id` cookies to bypass CAPTCHAs and WAF restrictions for strictly protected endpoints.
*   **`get_amazon_keyword_bid_recommendations`**: Fetches suggested bids and bidding ranges for Sponsored Products using the high-fidelity **v5.0 Theme-based API**. Requires a valid Advertising API Refresh Token and Profile ID. Supports various bidding strategies (`AUTO_FOR_SALES`, `LEGACY_FOR_SALES`) and optional advanced impact analysis.

#### Multi-Account Cookie Pool (`CookieBrowserPool`)

`src/mcp/servers/amazon/cookie_pool.py` is a thin Amazon-specific shim over the generic `src/core/identity/IdentityPool`. It pre-wires `AmazonIdentityStrategy` (warmup URL, cookie domain, UA, WAF block detection) and exposes the original `CookieBrowserPool` API unchanged. All mechanism (Chrome launch, port probing, circuit breakers, round-robin routing) lives in `src/core/identity/pool.py`; domain policy lives in `src/mcp/servers/amazon/identity.py`.

**Architecture**

Each *slot* (`IdentitySlot`, aliased as `CookieSlot` for backward compat) encapsulates one Amazon identity:

| Field | Type | Purpose |
|---|---|---|
| `session` | `curl_cffi.AsyncSession` | Tier 1/2 HTTP requests — independent per slot, no locking needed |
| `browser` | `ChromiumPage` | Tier 3 browser, lazily launched on first use |
| `browser_lock` | `asyncio.Lock` | Serialises Tier-3 operations on the same slot (one browser call at a time) |
| `cache_file` | `str` | Per-account cookie JSON path — isolated to prevent cross-slot contamination |
| `circuit` | `SlotCircuit` | Circuit breaker tracking consecutive WAF failures |

**Circuit Breaker (`SlotCircuit`)**

Three-state machine:

- **Closed** (healthy): failures below threshold; slot receives traffic normally
- **Open** (tripped): `failures >= threshold` (default 3); slot skipped for `cooldown` seconds (default 300 s)
- **Half-open** (cooldown elapsed): slot gets one trial request — success resets the counter, failure re-opens

`next_slot()` performs round-robin across closed slots. If every circuit is open (all accounts throttled simultaneously), it returns the slot with the earliest `open_until` timestamp rather than blocking or raising.

**Failure vs. Empty-ASIN Discrimination**

`get_reviews` returns `(None, None)` on a definitive WAF block (bot detection or login wall) and `([], None)` for a genuinely empty ASIN. Only the `(None, None)` path increments the circuit failure counter, preventing healthy slots from being penalised for products that simply have no reviews.

**Tab Recycling**

After `_RECYCLE_AFTER = 200` Tier-3 invocations, `get_or_init_browser()` calls `_recycle_tab()`:

1. Snapshot `stale_tabs = bp.get_tabs()`
2. Open a fresh blank tab via `bp.new_tab()` (it becomes the active tab on the browser object)
3. Close each stale tab

The Chrome *process* (and its WAF session cookie store in `--user-data-dir`) is preserved; only the renderer context is destroyed, releasing accumulated V8 old-generation heap. Each browser is also launched with memory caps:

```
--js-flags=--max-old-space-size=256   # V8 old-gen heap cap per renderer (MB)
--disk-cache-size=1                   # disable on-disk HTTP cache growth
--media-cache-size=1
--disable-application-cache
```

Each slot gets a deterministic CDP port (`19300 + slot_id`) and a unique `--user-data-dir` under `/tmp`, so slot cookie stores never bleed into each other.

**Domain Strategy (`AmazonIdentityStrategy`)**

`src/mcp/servers/amazon/identity.py` implements `BaseIdentityStrategy` with Amazon-specific policy:

| Method | Value |
|---|---|
| `warmup_url()` | `https://www.amazon.com/` — seeds CloudFront WAF cookies on browser init |
| `cookie_domain()` | `.amazon.com` — applies injected cookies to all Amazon sub-domains |
| `user_agent()` | Chrome 130 UA matching the curl_cffi impersonation profile |
| `is_hard_block(html)` | `True` when `validateCaptcha` or `auth-page-heading` appears |

To add a second domain (e.g. Walmart), create `src/mcp/servers/walmart/identity.py` with a `WalmartIdentityStrategy` subclass and pass it to `IdentityPool.init(entries, WalmartIdentityStrategy())`. See `docs/DEV_GUIDE.md` §9 for the full extension guide.

**Initialisation**

```python
# From persistent per-account cookie JSON files
CookieBrowserPool.from_cookie_files([
    "config/cookies_a.json",
    "config/cookies_b.json",
])

# From AmazonCookieHelper instances (preserves per-account cache_file paths)
CookieBrowserPool.from_cookie_helper(
    AmazonCookieHelper("config/cookies_a.json"),
    AmazonCookieHelper("config/cookies_b.json"),
)

# CommentsExtractor picks up the pool automatically via CookieBrowserPool.get_instance()
# — no call-site changes are needed in callers.
```

When no pool is configured, `CommentsExtractor` falls back to the single-account `AmazonCookieHelper` session transparently.

---

### Finance & Profitability (L2)

Two tools exposed by `src/mcp/servers/finance/tools.py`. Both are L2 — they consume `DataCache` and static JSON configs; they do not call L1 servers directly.

*   **`calc_profit`**: Full profit analysis — referral fee, FBA fee, refund admin fee, high-return-rate penalty, net margin, ROI. Automatically injects category benchmarks (avg return rate, avg search-to-buy ratio) from `us_category_metrics.json` when the product category can be resolved. If `return_rate` is omitted by the caller, the category average from `us_category_metrics.json` is used as the default.
    *   **Response** includes a `category_benchmarks` block: `{matched_category, avg_return_rate_pct, avg_search_to_buy_pm, return_rate_source}`.

*   **`calc_fba_fee`**: Estimates FBA fulfillment fee from product weight.

**Static data files** (`src/mcp/servers/finance/`):

| File | Purpose |
|---|---|
| `fba_fee.json` | FBA fee tiers by size/weight and high-return-rate penalty schedule |
| `referral_fee_rates.json` | Amazon US referral fee schedule (37 categories). Each entry has a `node_id` field (Sellersprite top-level node) for cross-referencing with `us_category_metrics.json`. Shared-node entries (e.g. multiple Electronics sub-categories) retain a `subcategory` field with the original Amazon billing name. |
| `us_category_metrics.json` | 25 US top-level categories × all subcategories (14,829 rows). Each item: `{node_id, category_name, return_rate_pct (%), avg_return_rate_pct (%), search_to_buy_ratio_pm (‰)}`. Generated by `scripts/generate_sellersprite_category_fallback.py` (checkpoint/resume, RPM=40). Loaded at startup; queried via `get_category_metrics(node_id, category)` with 3-tier resolution: node_id → referral fee `node_id` → partial label match. |

**Key design**: `referral_fee_rates.json` and `us_category_metrics.json` use the same `node_id` values (Sellersprite top-level node IDs) as the shared key, enabling `calc_profit` to join referral rates with category return/conversion benchmarks without an API call.

### Output & Delivery (L2)
*   **`populate_feishu_bitable_records`**: Reuses initial empty rows in a new Bitable to ensure data starts from Row 1. Preferred for new exports.
*   **`send_feishu_local_file`**: Uploads a local file and sends it as an IM attachment.
*   **`send_feishu_url_file`**: Downloads a file from a URL and forwards it as a Feishu attachment.
*   **`send_feishu_data_file`**: Converts raw list-of-dicts data into a CSV and sends it as an attachment.
*   **`send_feishu_text` / `send_feishu_card`**: Sends plain text or markdown cards to Feishu.

> **Note on Feishu Targeting**: All `send_feishu_*` tools (except webhook) now support **Implicit Context Resolution**. If `receive_id` or `receive_id_type` are omitted in the tool call, the system automatically resolves the target `chat_id` from the active conversation context (`feishu_chat_id`). Explicitly provided IDs will always override the context.

### Market Intelligence (L1)

#### Xiyouzhaoci (西柚找词)
*   **`xiyou_get_login_qr`**: Initiates WeChat QR code login for Xiyouzhaoci. Returns a cross-platform interaction signal that renders an interactive card with a scan verification button in Feishu, or falls back to a Markdown image URL.
*   **`xiyou_check_login_status`**: Checks the status of a pending QR code login. This is typically invoked automatically via webhook callbacks from interactive UI elements (like the Feishu 'I have scanned' button).
*   **`xiyou_keyword_analysis`**: Requests keyword traffic and competitor data from Xiyouzhaoci, returning a local file path.
*   **`xiyou_asin_lookup`**: Reverse-lookups keywords for an ASIN via Xiyouzhaoci.
*   **`xiyou_asin_compare_keywords`**: Compares multiple ASINs (up to 20) for common keywords and performance trends.
*   **`xiyou_get_aba_top_asins`**: Queries top ASINs and their click/conversion shares for specific search terms based on Amazon Brand Analytics (ABA) ranking data.
*   **`xiyou_get_search_terms_ranking`**: Retrieves search frequency ranks, growth ratios, and trends for variations of a root query string using ABA data.
*   **`xiyou_get_traffic_scores`**: Fetches 7-day traffic metrics for ASINs, including `advertisingTrafficScoreRatio` (real ad dependency) and growth trends.
*   **`xiyou_get_asin_daily_trends`**: Fetches daily historical trends (price, ratings, BSR) for a single ASIN within a date range. Used by `CategoryMonopolyAnalyzer` for rating-collapse detection.
*   **`xiyou_get_search_term_trends`** *(internal API)*: Fetches weekly ABA search volume history for a root keyword (`searchTerms[0].trends.weekSearch` array, oldest → newest). Used by `CategoryMonopolyAnalyzer._analyze_seasonality_from_keyword_trends()` to classify category seasonality. The method applies `log(weekSearch)` → OLS detrend → platform dampening (July/Nov → 0.3) → residuals; high residuals = demand peak. Labels: `peak_season`, `off_season`, `shoulder`, `year_round`.

#### Sellersprite (卖家精灵)

Auth lifecycle mirrors Xiyouzhaoci: credentials are resolved per-tenant (`SELLERSPRITE_EMAIL_{TENANT_ID}` → `SELLERSPRITE_EMAIL` fallback) and the token is persisted at `config/auth/sellersprite_{tenant_id}_token.json`. On 401, the client reloads the file and re-logs in automatically. The API version string is centralised in `SellerspriteAuth.__init__` (`self.VERSION = "5.0.2"`) and referenced by all methods, so version bumps only require one edit.

*   **`sellersprite_resolve_node_path`**: Searches BSR category nodes by label using the `nodeLabelPath` parameter.
    *   Pass a **bare numeric node ID** (e.g. `"8297518011"` extracted from `.../gp/bestsellers/industrial/8297518011/`) → returns a single exact match.
    *   Pass a **keyword** (e.g. `"Traps"`) → returns all nodes whose label contains the keyword, ordered by product count. Present the list to the user for selection.
    *   Returns: list of `{id, label, nodeLabelLocale, nodeLabelPathLocale, products}` dicts. The `id` field is the full colon-joined `nodeIdPath` (e.g. `"16310091:8297370011:8297381011:8297518011"`) to pass to `sellersprite_competing_lookup`.
    *   **Note**: This uses the `nodeLabelPath` query param and is distinct from `sellersprite_category_nodes`, which uses `nodeIdPath` for child-tree navigation.

*   **`sellersprite_category_nodes`**: Fetches **child** category nodes for a given ancestor path, using the `nodeIdPath` param for tree navigation. Use this to drill down into subcategories once you already have a `nodeIdPath`.

*   **`sellersprite_market_research`**: Fetches subcategory market research data for a top-level category node — the primary signal source for category entry evaluation.
    *   **`node_id_path`** (required): top-level category node ID (e.g. `"1055398"` for Home & Kitchen). Use the node IDs defined in `src/mcp/servers/finance/us_category_metrics.json`.
    *   **`month_name`** (optional): defaults to `"bsr_sales_nearly"` (latest rolling snapshot).
    *   **`page`** (optional): 1-based pagination — server returns ~10 rows per page regardless of `size`.
    *   **Response**: `{total_products, items}`. Each item: `{node_id, category_name, return_rate_pct (%), avg_return_rate_pct (%), search_to_buy_ratio_pm (‰)}`.
    *   **Offline fallback**: `src/mcp/servers/finance/us_category_metrics.json` pre-caches all 25 US top-level categories (14,829 subcategories) for zero-latency lookups via `get_category_metrics()` in `finance/tools.py`.

*   **`sellersprite_competing_lookup`**: Fetches a paginated BSR-ranked competitor product list for one or more category node paths in a given monthly snapshot.
    *   **`amazon_url`**: Accepts an Amazon BSR URL directly (e.g. `https://www.amazon.com/gp/bestsellers/industrial/8297518011/`). The tool automatically extracts the node ID, calls `resolve_node_path` internally, and resolves the full `nodeIdPath`.
    *   **`month_name`** (optional): Accepts flexible formats — omitted (defaults to 2 months prior), `"June 2025"`, `"2025-06"`, `"202506"`, or the canonical `"bsr_sales_monthly_202506"`. Rejects snapshots newer than the latest available (2 months prior to today) with a clear error.
    *   **Response (slim)**: Returns `{snapshot, today, latest_available_snapshot, total, returned, items}`. Each item contains only: `asin, rank, price, brand, reviewCount, rating, bsr`. Bulky fields (trends, images, seller details) are stripped to keep LLM context manageable.
    *   **Note**: Full per-product monthly sales `trends` data is **not** exposed via this MCP tool. Workflows that need it (e.g. for BSR churn analysis) call the `SellerspriteAPI` client directly.

**Typical call sequence from an Amazon BSR URL (LLM-driven):**
```
# Option A: single-step — pass URL directly (tool resolves node path internally)
sellersprite_competing_lookup(amazon_url="https://www.amazon.com/gp/bestsellers/industrial/8297518011/", month_name="2026-01")
  → {snapshot: "bsr_sales_monthly_202601", total: 270, returned: 100, items: [...]}

# Option B: two-step — explicit node resolution then lookup
sellersprite_resolve_node_path(query="8297518011", table="bsr_sales_monthly_202601")
  → [{id: "16310091:8297370011:8297381011:8297518011", label: "...", products: 270}]

sellersprite_competing_lookup(node_id_paths=["16310091:8297370011:8297381011:8297518011"], month_name="bsr_sales_monthly_202601")
  → {snapshot: "bsr_sales_monthly_202601", total: 270, returned: 100, items: [...]}
```

**BSR榜单代谢率 (List Churn) — workflow-level analysis:**

The `CategoryMonopolyAnalyzer._analyze_bsr_churn()` method computes churn by comparing ASIN sets across 4 monthly snapshots fetched directly via the API (T, T-3, T-6, T-12 months):

```
churn_Nm = |T_set − T-N_set| / |T_set|
```

Each snapshot is slimmed to `{asin, rank, brand}` only. The churn labels are:
- **fomo_spike_die**: `churn_3m > 0.40` AND `churn_12m > 0.65` — fast turnover, trend product
- **high_churn**: `churn_12m > 0.55` — competitive, crowded market
- **mature_stable**: `churn_12m < 0.30` — dominated by incumbents, hard to enter
- **blue_ocean**: `0.30 ≤ churn_12m ≤ 0.55` AND `churn_3m < 0.25` — stable recent top, room for new entrants

#### Deal History
*   **`get_deal_history`**: Fetches off-Amazon deal history for a given ASIN or keyword from multiple top-tier deal sites (currently Slickdeals and DealNews), supporting multi-page scraping. Returns a structured list of historical promotions (price, discount, date, site, type).

### Standard: Implicit Context Resolution
To keep tool calls concise for LLMs, domain servers (especially `finance` and `output`) should implement **Implicit Context Resolution**. If a primary parameter like `asin` or `receive_id` is missing from the `arguments` dict, the handler should attempt to resolve it via `ContextPropagator.get("field_name")`.

Example (Finance):
```python
asin = arguments.get("asin") or ContextPropagator.get("asin")
```
This allows the Agent to simply say "calculate profit" without re-stating the ASIN every time.

### Social Media Intelligence (L1/L2 Decoupled)

Four tools form a pipeline. Call them in order; each caches its output for the next.

| Tool | Layer | Purpose |
|---|---|---|
| `tiktok_fetch_data` | L1 | Scrape tag metadata + trending videos. Adaptive sample: `max(50, video_count // 10, 300)`. Cache key: `tiktok:{keyword}` |
| `tiktok_fetch_reference_data` | L1 | Fetch competitor/category hashtag videos for peer benchmarks. Uses LLM to generate competitor hashtags + hardcoded category seeds. Cache key: `tiktok:__ref__{keyword}` |
| `tiktok_fetch_comments` | L1 | Fetch comments for the target tag's top videos using **tier-stratified sampling**: comment budget allocated proportionally to KOL/KOC tier share (nano/micro/mid/macro/mega), highest-view videos first within each tier. Applies same `window_days` filter as `tiktok_calculate_virality` for temporal consistency. Cache key: `tiktok:__comments__{keyword}__w{window_days}` |
| `tiktok_calculate_virality` | L2 | Compute PSI (0–100). If `__comments__` cache exists, delegates to `CommentAnalyzer` (an `intelligence/processors/` AI-backed processor) for LLM deep analysis: sentiment, purchase signals, competitor mentions, language distribution. Falls back to keyword-based analysis if comments not fetched. |

**Recommended call sequence:**
```
tiktok_fetch_data(keyword, window_days=30)
tiktok_fetch_reference_data(brand, product_name, keyword)   # optional — improves benchmarks
tiktok_fetch_comments(keyword, window_days=30)              # optional — enables LLM comment analysis
tiktok_calculate_virality(keyword, window_days=30)
```

`window_days` must be consistent across all four calls. The cache key for comments encodes `window_days` to prevent cross-window data pollution.

**PSI output fields:** `strength_score` (0–100), `kol_koc_matrix`, `hhi_concentration`, `promo_tag_ratio`, `benchmarks` (peer_median or default), `comment_analysis` (LLM schema or keyword fallback), `penalties`, `metrics` (per-component contributions), `verdict`.

### ERP Integration (L1.5 — Strategy Pattern)

Tools in `src/mcp/servers/erp/` provide real-time inventory and order data from external ERP systems. All three tools accept an optional `provider` argument (default: `"lingxing"`) to select the active ERP backend.

*   **`erp_inventory`**: Query real-time inventory for a SKU from the configured ERP system.
    *   **Arguments**: `sku` (required), `provider` (optional, default `lingxing`)
    *   **Returns**: `{sku, available_qty, total_qty, pending_orders, warehouse_location, last_updated}`

*   **`erp_purchase_orders`**: Query inbound purchase orders (replenishment shipments) from the ERP.
    *   **Arguments**: `sku` (optional filter), `status` (optional filter), `provider` (optional)
    *   **Returns**: List of orders with `{status, qty, eta}`

*   **`erp_sales_orders`**: Query recent sales orders for a SKU from the ERP.
    *   **Arguments**: `sku` (optional filter), `days` (lookback window, default 30), `provider` (optional)
    *   **Returns**: List of orders with `{qty, date}`

**Adding a new ERP provider** (zero changes to existing code):
1. Subclass `ERPClient` (ABC) in a new subpackage, e.g. `src/mcp/servers/erp/eccang/`.
2. Call `register_provider("eccang", EccangClient)` in the subpackage `__init__.py`.
3. Import the subpackage in `src/mcp/servers/erp/__init__.py`.
4. Pass `provider="eccang"` in any `erp_*` tool call.

Currently registered providers: `lingxing` (领星ERP).

### Compliance核查 (L2)
*   **`check_amazon_restriction`**: Keyword-based lookup in local Amazon restricted products database.
*   **`check_epa`**: Checks if product keywords trigger EPA FIFRA pesticide device regulations.
