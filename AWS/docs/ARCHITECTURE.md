# System Architecture: Hybrid Intelligence Agentic Platform

The AWS (Amazon Web Scraper) V2 project is a **Hybrid Intelligence Agentic Platform** featuring a robust **Agentic Architecture** and **Model Context Protocol (MCP)** integration. It enables LLMs (Claude, Gemini, etc.) and deterministic code-based Workflow Engines to autonomously perform market research, competitor analysis, and listing optimization.

```
================================================================================
                     DUAL-TRACK AGENT ARCHITECTURE
              Single-User · MCP-Native · Multi-User Ready
================================================================================

+==============================================================================+
|                          ENTRY POINTS                                        |
|                                                                              |
|   +---------------+  +---------------+  +------------+  +----------------+  |
|   |  Feishu Bot   |  | Claude Desktop|  |    CLI     |  |      Cron      |  |
|   |  (Chat Cmd)   |  | (Native MCP)  |  | --workflow |  | (w/ callback   |  |
|   |               |  |               |  | --explore  |  |   preset)      |  |
|   +-------+-------+  +-------+-------+  +-----+------+  +-------+--------+  |
|           |                  |                |                  |           |
|           | Context Propag.  |                |                  |           |
|           | (feishu_chat_id) |                |                  |           |
|           +------------------+----------------+------------------+           |
|                              | Unified Request                               |
|                 (Entry points forward; no routing logic)                     |
+==============================|===============================================+
                               |
                               v
+==============================================================================+
|                           API GATEWAY                                        |
|                                                                              |
|   1. Auth          token/key --> tenant_id + user_id + plan_tier             |
|                    [Single-User: Hardcoded "default"] [Ext Point #1]         |
|   2. Rate Limit    Three-layer enforcement (config/rate_limits.yaml)         |
|      Layer 1  entry_limits   cooldown debounce (check_limit at dispatch)     |
|                              concurrent slot   (concurrent_slot in _run_job) |
|      Layer 2  tenant_quotas  daily request budget per plan tier              |
|      Layer 3  source_limits  token-bucket per external API (acquire_source)  |
|                    [Ext Point #2] Swap in-memory counters for Redis          |
|   3. Normalize     Heterogeneous msg --> UnifiedRequest DTO                  |
|      UnifiedRequest = {                                                      |
|        tenant_id, user_id, plan_tier,                                        |
|        workflow_name?, intent?,                                              |
|        params: { filters_override, ... },                                    |
|        callback: { type, target },                                           |
|        entry_type,   ← set by Gateway; consumed by RateLimiter.concurrent_slot|
|        chat_id       ← Feishu chat identifier for per-chat concurrency       |
|      }                                                                       |
|   4. Mode Selection  (Centralized routing logic)                             |
|      +-------------------------------------------------------------+        |
|      |  workflow_name known ──────────────────► Workflow Track     |        |
|      |  Exploration / Chat  ──────────────────► Agent Track        |        |
|      |  Native MCP Client   ──────────────────► Agent Track        |        |
|      |  Cron triggered  ──► Inject preset callback ► Workflow Track|        |
|      +-------------------------------------------------------------+        |
+==============================|===============================================+
                               |
                               v
+==============================================================================+
|                      JOB MANAGER  (Shared by both tracks)                    |
|                                                                              |
|   submit(request)   --> job_id                                               |
|   resume(job_id)    --> Resume from checkpoint                               |
|   cancel(job_id)    --> bool                                                 |
|   get_status(job_id)--> JobStatus                                            |
|                                                                              |
|   Queue Implementation:                                                      |
|   [Single-User] asyncio.Queue (Memory)  [Ext Point #3] Redis Priority Queue  |
|   +----------------+   +-------------------------------------------------+  |
|   |  Workflow Job  |   |  Enterprise: Dedicated Worker pool, no queuing   |  |
|   |  Agent Session |   |  Pro:        High priority, N concurrency        |  |
|   |  (Unified MGMT)|   |  Free:       Low priority, 1 concurrency, limit  |  |
|   +----------------+   +-------------------------------------------------+  |
+=======================================|======================================+
                                        |
               +------------------------+------------------------+
               |                                                 |
               v                                                 v
+==============================+             +==================================+
|    WORKFLOW ENGINE           |             |          MCP AGENT               |
|    (Deterministic Pipeline)  |             |    (LLM-driven, Exploratory)     |
|                              |             |                                  |
|  WorkflowRegistry:           |             |  Constraint Boundaries:          |
|    "product_screening"       |             |  +---------------------------+     |
|    "competitor_monitor"      |             |  | allowed_tools[ ]          |     |
|    "review_analysis"         |             |  | max_steps: N (display)    |     |
|    "category_monopoly"       |             |  | token_budget: N (cloud)   |     |
|    ...register(name, fn)     |             |  | cumulative_cost: $ (cloud)|     |
|                              |             |  | dynamic_step_extension    |     |
|  Execution Engine:           |             |  | convergence_hints: true   |     |
|  for step in steps:          |             |  +---------------------------+     |
|    checkpoint.save()  ───────┼─ Resume ───┼── session.save() (inc. cost)     |
|    result = step.run() ──────┼────────────┼── tool = agent.decide()          |
|    if not items: break       |             |                                  |
|                              |             |  Step Logic:                     |
|  Step Tri-primitives:        |             |   1. Check steps vs budget/cost  |
|  +----------------------+    |             |   2. If steps > max:             |
|  | EnrichStep  ─────────┼────┼── MCP Client|      - Budget > 20%: +5 steps    |
|  | ProcessStep ─────────┼────┼── Intel Router      - Budget < 20%: Force Final |
|  | FilterStep  (Python) |    |             |                                  |
|  +----------------------+    |             |  System Prompt Architecture:     |
|                              |             |   · .md template (editable)      |
|                              |             |   · PromptBuilder (assembly)     |
|                              |             |   · ToolCatalogFormatter         |
|                              |             |  Autonomous output rules:        |
|                              |             |  · Auto-populate Bitable (robust)|
|                              |             |  · Direct File Attachments       |
|                              |             |                                  |
+==============|===============+             +=================|================+
               |                                               |
               |      Both discover tools via Tool Registry    |
               +---------------------+--------------------------+
                                     |
                                     v
+==============================================================================+
|                         TOOL REGISTRY                                        |
|                                                                              |
|   Unified Reg / Discovery / Versioning / ACL                                 |
|   ToolMeta per tool: category (DATA/COMPUTE/FILTER/OUTPUT) + returns desc    |
|                                                                              |
|   tool_name       category   server            returns                       |
|   ─────────────── ────────── ───────────────── ──────────────────────────── |
|   search_products DATA       amazon-server     list of products w/ ASIN      |
|   xiyou_analysis  DATA       market-server     local xlsx file path          |
|   calc_profit     COMPUTE    finance-server    profit margin as decimal      |
|   check_epa       FILTER     compliance-server EPA requirement status        |
|   populate_bitable OUTPUT    output-server     created record ID (robust)    |
|   send_local_file  OUTPUT    output-server     Feishu attachment confirmation|
|   ...  (52 tools total across 7 domain servers)                              |
|                                                                              |
|   [Single-User] Memory dict, no filtering                                    |
|   [Ext Point #4] Per-tenant tool visibility control                          |
+==============================|===============================================+
                               |
                               v
+==============================================================================+
|                    MCP TOOL SERVERS  (L1 / L2 Layering)                      |
|                                                                              |
|   Rule: No direct Server-to-Server calls; data flows through Data Cache      |
|                                                                              |
|   ── L1: Raw Data Layer (No external dependencies) ────────────────────────  |
|                                                                              |
|   +--------------------+  +--------------------+  +--------------------+    |
|   |   amazon-server    |  |   market-server    |  |   social-server    |    |
|   |                    |  | (inc. Xiyouzhaoci) |  |                    |    |
|   | search_products    |  | keyword_analysis   |  | tiktok_trend       |    |
|   | get_details        |  | asin_reverse_look  |  | meta_ad_density    |    |
|   | get_bsr            |  | seller_origin      |  | pinterest_interest |    |
|   | get_past_sales     |  |                    |  |                    |    |
|   +--------------------+  +--------------------+  +--------------------+    |
|           |                        |                        |                |
|           +------------------------+------------------------+                |
|                                    |                                         |
|                         +──────────────────+                                 |
|                         |   Data Cache     |  (L1 Write / L2 Read)           |
|                         |   Redis / File   |  Decouples L1-L2 deps           |
|                         +──────────────────+                                 |
|                                    |                                         |
|   ── L2: Calculation / Compliance (Consume Cache, No L1 calls) ──────────── |
|                                                                              |
|   +--------------------+  +--------------------+  +--------------------+    |
|   |   finance-server   |  | compliance-server  |  |   output-server    |    |
|   |                    |  | (Local JSON lookup)|  | (Direct IM attach) |    |
|   | calc_profit        |  | restriction_check  |  | populate_bitable   |    |
|   | calc_margin        |  | epa_check          |  | send_local_file    |    |
|   | calc_cost_ratio    |  | patent_risk_calc   |  | send_url_file      |    |
|   | estimate_ads       |  |                    |  | send_data_file     |    |
|   +--------------------+  +--------------------+  +--------------------+    |
                               |
                               v
+==============================================================================+
|                       INTELLIGENCE ROUTER                                    |
|                                                                              |
|   Unified LLM call entry for ProcessStep and MCP Agent                       |
|   route_and_execute(prompt, category?, **kwargs) --> LLMResponse             |
|                                                                              |
|   ── Cost & Billing ──────────────────────────────────────────────────────   |
|   +──────────────────────────────────────────────────────────────────────+   |
|   |  PriceManager provides universal cost calculation for all providers. |   |
|   |  - Reads from provider-specific JSON configs (e.g., gemini_pricing)  |   |
|   |  - Handles complex tiered pricing (Gemini) and surcharges (Claude).  |   |
|   |  - Cost is populated into every LLMResponse.                           |   |
|   +──────────────────────────────────────────────────────────────────────+   |
|                                                                              |
|   ── Compute Targets ──────────────────────────────────────────────────────  |
|                                                                              |
|   +──────────────+──────────────────────────────────────────────────────+   |
|   | PURE_PYTHON  | Profit calc / Rule filtering / Stats  Zero cost <1ms |   |
|   +──────────────+──────────────────────────────────────────────────────+   |
|   | LOCAL_LLM    | Data cleaning / Classification / Origin  Local <500ms|   |
|   +──────────────+──────────────────────────────────────────────────────+   |
|   | CLOUD_LLM    | Patent risk / Synthesis / Market trends  Claude/Gemini|   |
|   +──────────────+──────────────────────────────────────────────────────+   |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Task Auto-Classification (when category not specified) ──────────────   |
|                                                                              |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | SIMPLE_CLEANING      | Whitespace / format normalization → LOCAL    |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | DATA_EXTRACTION      | Structured field extraction       → LOCAL    |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | SIMPLE_CHAT          | FAQ-style short answers            → LOCAL    |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | DEEP_REASONING       | Multi-step analysis / Synthesis    → CLOUD   |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | CREATIVE_WRITING     | Report generation / Copywriting    → CLOUD   |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|                                                                              |
|   Fallback: LOCAL unavailable or classification fails → DEEP_REASONING       |
|             Send first 300 chars of prompt to LOCAL for classification        |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Routing Decision Flow ────────────────────────────────────────────────  |
|                                                                              |
|   route_and_execute(prompt, category?, **kwargs)                             |
|          |                                                                   |
|          v                                                                   |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  category provided?                                                  |  |
|   |       YES                              NO                            |  |
|   |        |                                |                            |  |
|   |        |                                v                            |  |
|   |        |                 _classify_task(prompt[:300]) via LOCAL      |  |
|   |        |                                |                            |  |
|   |        +────────────────────────────────+                            |  |
|   |                          |                                           |  |
|   |                          v                                           |  |
|   |              resolved category                                       |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|          |                                                                   |
|          v                                                                   |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  LOCAL provider available?                                           |  |
|   |       YES                              NO                            |  |
|   |        |                                |                            |  |
|   |        v                                v                            |  |
|   | category in                       fall through                       |  |
|   | [SIMPLE_*, EXTRACTION, CHAT]?     to CLOUD                          |  |
|   |   YES            NO               (see Fallback section)            |  |
|   |    |              |                                                  |  |
|   |    v              v                                                  |  |
|   | LOCAL call   fall through                                            |  |
|   | timeout 120s  to CLOUD                                               |  |
|   |    |                                                                 |  |
|   |    v                                                                 |  |
|   | OutputParser                                                         |  |
|   | .clean_for_feishu()                                                  |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|          |                                                                   |
|          v                                                                   |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  CLOUD provider (Gemini / Claude)                                    |  |
|   |                                                                      |  |
|   |  Text prompt   ──► generate_text()                                   |  |
|   |  With schema   ──► generate_structured() (JSON response_mime_type)   |  |
|   |  On error      ──► FallbackHandler.handle(failure_type, context)     |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Provider System (ProviderFactory + Strategy Pattern) ────────────────   |
|                                                                              |
|   BaseLLMProvider (ABC):                                                     |
|     generate_text(prompt, system_message)     --> LLMResponse                |
|     generate_structured(prompt, schema)       --> LLMResponse                |
|     count_tokens(prompt)                      --> int                        |
|                                                                              |
|   ProviderFactory.get_provider(type) --> BaseLLMProvider                     |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "local" / "llama"    | LlamaCppProvider  (llama-cpp-python, GPU)    |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "gemini"             | GeminiProvider    (google-generativeai)      |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "claude"/"anthropic" | ClaudeProvider    (anthropic SDK)            |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Fallback Strategy (Strategy Pattern) ─────────────────────────────────  |
|                                                                              |
|   FallbackHandler maps FailureType --> async handler                         |
|                                                                              |
|   +─────────────────────────+────────────────────────────────────────────+  |
|   | FailureType             | Handler                                    |  |
|   +─────────────────────────+────────────────────────────────────────────+  |
|   | LOCAL_MODEL_TIMEOUT     | Return user-friendly msg; no retry         |  |
|   +─────────────────────────+────────────────────────────────────────────+  |
|   | CLOUD_API_UNAVAILABLE   | Enqueue to asyncio.Queue retry queue       |  |
|   |                         | Spawn background consumer (10s backoff)    |  |
|   +─────────────────────────+────────────────────────────────────────────+  |
|   | CLOUD_API_RATE_LIMIT    | Queue wait + Feishu Alert                  |  |
|   +─────────────────────────+────────────────────────────────────────────+  |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Response DTO ──────────────────────────────────────────────────────────  |
|                                                                              |
|   LLMResponse = {                                                            |
|     text, provider_name, model_name, token_usage, cost, currency, metadata   |
|   }                                                                          |
|   Standardized across all providers for unified downstream handling.          |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Integration Points ────────────────────────────────────────────────────  |
|                                                                              |
|   +──────────────────+──────────────────────────────────────────────────+   |
|   | WorkflowEngine   | ProcessStep.run()                                |   |
|   |                  | --> asyncio.gather(router.route_and_execute)      |   |
|   |                  | compute_target maps to TaskCategory automatically |   |
|   +──────────────────+──────────────────────────────────────────────────+   |
|   | MCP Agent        | BaseAgent.__init__(router)                       |   |
|   |                  | --> router.route_and_execute()                    |   |
|   |                  | Used in ReAct loop for each reasoning step        |   |
|   +──────────────────+──────────────────────────────────────────────────+   |
|   | JobManager       | Instantiates IntelligenceRouter for both tracks   |   |
|   +──────────────────+──────────────────────────────────────────────────+   |
|                                                                              |
|   [Single-User] No limits, direct call                                       |
|   [Ext Point] Deduct tenant quota before call; queue if exceeded             |
|                                                                              |
+==============================================================================+
                               |
                               v
+==============================================================================+
|                      CALLBACK  (inside job_manager/)                         |
|                                                                              |
|   Unified Interface:                                                         |
|     on_progress(step, total, msg)   ── Real-time progress notifications      |
|     on_complete(workflow_result)    ── Route to target output (Bitable/IM)   |
|                                                                              |
|   ── Artifact Delivery Mechanism ──────────────────────────────────────────  |
|   Workflows can generate local artifacts (e.g., .md, .csv, .pdf). If a       |
|   result item contains a `report_file_path`, the Callback system (Feishu,    |
|   Slack) automatically uploads and sends it as an IM attachment. This        |
|   bypasses card character limits and provides high-fidelity reports.         |
|                                                                              |
|   CallbackFactory.create(request.callback) --> Instance                      |
|   Callbacks invoke output-server tools via MCP Client; no direct SDK calls   |
|                                                                              |
|   ── Stateful Interaction Signals (e.g., QR Login) ────────────────────────  |
|   1. Tool returns an `INTERACTION_REQUIRED` JSON signal w/ `tenant_id`.      |
|   2. Callback checks its capabilities (`IMAGE_DISPLAY`, `BUTTONS`).          |
|   3. Feishu renders an interactive card; CLI gracefully downgrades to Text.  |
|   4. User click hits Webhook -> `InteractionRegistry.handle(action_name)`.   |
|   5. Handler validates status (e.g., Xiyou QR) and calls `job_mgr.resume()`. |
|                                                                              |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | feishu_bitable   | Candidates / Funnel stats / Lineage  (3 tabs)     | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | feishu_card      | Summary card + metrics, real-time push            | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | feishu_doc       | Full selection report                             | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | json             | POST callback_url, standard JSON                  | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | csv              | Write to Object Store, return download link       | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | mcp              | Structured JSON returned to LLM Client            | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|   | composite        | Parallel execution of multiple callbacks          | |
|   +──────────────────+────────────────────────────────────────────────────+ |
|                                                                              |
+==============================================================================+


================================================================================
                    CROSS-CUTTING CONCERNS
================================================================================

+────────────────────────────────────+  +──────────────────────────────────────+
|  STATE STORE                       |  |  OBSERVABILITY                       |
|                                    |  |                                      |
|  [Single-User]                     |  |  Tracing:                            |
|    Local files: checkpoint/history |  |    trace_id full link                |
|    SQLite:      Config / Usage logs|  |    Gateway -> Step -> Model          |
|                                    |  |    Duration / cost per step per job  |
|  [Ext Point #6]                    |  |                                      |
|    Redis:      Queue / Checkpoint  |  |  Metrics:                            |
|                Rate limit / Session|  |    Funnel conversion / Token usage   |
|    PostgreSQL: Tenant / Quota      |  |    Worker utilization / Queue depth  |
|                Job history / Audit |  |    Model cost (per tenant)           |
|    Vault:      API Key encryption  |  |                                      |
|                Auto-rotation/Audit |  |  Alerting:                           |
|                                    |  |    Backlog > threshold               |
|                                    |  |    Cloud API Error rate > 5%         |
|                                    |  |    Scraper blocked / anti-bot        |
+────────────────────────────────────+  +──────────────────────────────────────+


================================================================================
                    EXTENSION RULES  (5-Dimensional Orthogonal)
================================================================================

  New Entry Point    EntryPoint adapter  +  Gateway register     Zero change elsewhere
  New Workflow       WorkflowRegistry.register(name, build_fn)   Zero change elsewhere
  New Data Source    New MCP Server  +  Tool Registry register   Zero business change
  New Output Format  Callback subclass  +  CallbackFactory reg   Zero change elsewhere
  Switch Model       Intelligence Router providers register       Zero business change

  Result: Five extension dimensions are orthogonal; changing one leaves others intact


================================================================================
                    MULTI-USER MIGRATION CHECKLIST
================================================================================

  # Extension Point       Single-User                Multi-User Replacement
  ─── ─────────────────── ─────────────────────────── ──────────────────────────────
  1   Auth middleware       Hardcoded "default"         JWT / API Key + user table
  2   Rate Limit            3-layer (in-memory)         Swap counters/buckets → Redis
  3   Task queue            asyncio.Queue               Redis Priority Queue (Celery)
  4   Tool ACL              No filtering                Per-tenant visibility control
  5   API credentials       .env single key set         Vault per tenant_id lookup
  6   Persistent storage    Local files + SQLite        Redis + PostgreSQL
  ─── ─────────────────── ─────────────────────────── ──────────────────────────────
  Unchanged core (never needs modification):
    JobRequest DTO structure  ·  Workflow Engine  ·  Step tri-primitives
    MCP Server business logic ·  Intelligence Router routing rules
    Callback interface        ·  All WorkflowRegistry Workflow definitions


================================================================================
                    PRODUCT SCREENING WORKFLOW EXAMPLE
================================================================================

  Step  Type          Tool / Target        Input --> Output
  ────  ────────────  ───────────────────  ────────────────────────────────────
   1    EnrichStep    profitability_api    keyword  -->  ASIN + Dims + BSR + Price
   2    EnrichStep    past_sales_api       ASIN     -->  Monthly Sales Volume
   3    FilterStep    price/rating         Filter   -->  Basic candidates remain
   4    EnrichStep    fulfillment_api      ASIN     -->  FBA/FBM Status
   5    EnrichStep    deal_history         ASIN     -->  Promo history appended
   6    ProcessStep   promo_analysis       PURE_PYTHON -> Risk score
   7    FilterStep    promo_risk           Filter   -->  Stable prices remain
   8    ProcessStep   calc_profit (MCP)    FINANCE  -->  Exact Margin & ROI
   9    FilterStep    profitability        Filter   -->  Profitable ASINs remain
  10    ProcessStep   epa_check            LOCAL_LLM
  11    FilterStep    compliance           Filter   -->  Low-risk ASINs remain
  12    EnrichStep    xiyou_traffic (MCP)  MARKET   -->  Actual Ad Dependency %
  13    FilterStep    ad_dependency        Filter   -->  Natural-traffic winners
  14    ProcessStep   final_synthesis      CLOUD_LLM --> Selection Report
  ────  ────────────  ───────────────────  ────────────────────────────────────
  Key: Efficiency optimized via Profitability API; Logic unified via Finance MCP.
