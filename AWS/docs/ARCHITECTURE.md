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
|   2. Rate Limit    Single-User: No limit (Stub)  [Ext Point #2]              |
|   3. Normalize     Heterogeneous msg --> UnifiedRequest DTO                  |
|      UnifiedRequest = {                                                      |
|        tenant_id, user_id, plan_tier,                                        |
|        workflow_name?, intent?,                                              |
|        params: { filters_override, ... },                                    |
|        callback: { type, target }                                            |
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
|    "replenish_alert"         |             |  | token_budget: N (cloud)   |     |
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
|   route_and_execute(prompt, category?) --> LLMResponse                       |
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
|   route_and_execute(prompt, category?)                                       |
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
|     batch_generate_text(prompts, concurrency) --> list[LLMResponse]          |
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
|   ── Batch Processing ─────────────────────────────────────────────────────  |
|                                                                              |
|   batch_route_and_execute(prompts[], category?)                              |
|                                                                              |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  Step 1  Classify once (based on first prompt)                       |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  Step 2  Route entire batch to same provider                         |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  Step 3  Concurrent execution via asyncio.Semaphore                  |  |
|   |          LOCAL → concurrency = 2  (CPU-bound)                        |  |
|   |          CLOUD → concurrency = 5  (I/O-bound)                        |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  Step 4  asyncio.gather() with exception filtering                   |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   |  Step 5  Return list[LLMResponse]                                    |  |
|   +──────────────────────────────────────────────────────────────────────+  |
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
|   |                  | --> ctx.router.batch_route_and_execute()          |   |
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
|     on_progress(step, total, msg)   ── Real-time Feishu progress cards       |
|     on_complete(workflow_result)    ── Route to target output                |
|     on_error(exception)             ── Preserve checkpoint, retryable        |
|                                                                              |
|   CallbackFactory.create(request.callback) --> Instance                      |
|   Callbacks invoke output-server tools via MCP Client; no direct SDK calls   |
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
  New Data Source    New MCP Server  +  Tool Registry register   Zero change elsewhere
  New Output Format  Callback subclass  +  CallbackFactory reg   Zero change elsewhere
  Switch Model       Intelligence Router providers register       Zero business change

  Result: Five extension dimensions are orthogonal; changing one leaves others intact


================================================================================
                    MULTI-USER MIGRATION CHECKLIST
================================================================================

  # Extension Point       Single-User                Multi-User Replacement
  ─── ─────────────────── ─────────────────────────── ──────────────────────────────
  1   Auth middleware       Hardcoded "default"         JWT / API Key + user table
  2   Rate Limit            No limit (Stub)             Per-user token bucket (Redis)
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
   1    EnrichStep    amazon_search        keyword  -->  200 ASIN
   2    EnrichStep    amazon_details       200 ASIN -->  details (parallel)
   3    FilterStep    price/weight/rating  200      -->  58 remain
   4    EnrichStep    bsr_competitors      58 ASIN  -->  competition data
   5    ProcessStep   seller_origin        LOCAL_LLM
   6    FilterStep    us_seller_ratio      58       -->  22 remain
   7    EnrichStep    fba_fees             22 ASIN  -->  fee data appended
   8    ProcessStep   profit_calc          PURE_PYTHON
   9    FilterStep    margin/cost_ratio    22       -->  10 remain
  10    ProcessStep   epa_check            LOCAL_LLM
  11    ProcessStep   patent_check         CLOUD_LLM
  12    FilterStep    compliance           10       -->   7 remain
  13    EnrichStep    ad_traffic           market-server
  14    FilterStep    ad_ratio             7        -->   5 remain
  15    EnrichStep    tiktok + meta        social-server
  16    ProcessStep   final_synthesis      CLOUD_LLM  --> selection report
  ────  ────────────  ───────────────────  ────────────────────────────────────
  Each step writes checkpoint on completion; failures resume from last checkpoint
```
---

## 2. Directory Structure (Domain-Driven Design)

The project follows a strict **Layered Architecture** within the `src/` directory, adhering to Domain-Driven Design (DDD) principles:

```text
AWS/
├── src/
│   ├── agents/             # AGENT TRACK: Exploratory reasoning loops
│   │   ├── mcp_agent.py    # ReAct loop with cloud token budget tracking
│   │   ├── session.py      # AgentSession (cloud_token_usage + token_usage)
│   │   ├── base_agent.py   # Abstract base for all intelligent agents
│   │   └── prompts/        # 3-layer system prompt architecture
│   │       ├── mcp_agent_system.md      # Human-editable template
│   │       ├── prompt_builder.py        # Runtime assembly (string.Template)
│   │       └── tool_catalog_formatter.py # Groups tools by category
│   │
│   ├── workflows/          # WORKFLOW TRACK: Deterministic batch pipelines
│   │   ├── engine/         # Sequential execution engine with checkpoint support
│   │   ├── steps/          # Tri-primitives: EnrichStep, FilterStep, ProcessStep
│   │   ├── definitions/    # Registered business pipelines (e.g., product_screening)
│   │   └── config/         # Workflow-specific parameter merging
│   │
│   ├── gateway/            # API GATEWAY: Entry normalization & Protection
│   │   ├── router.py       # APIGateway: Identity resolution & Track selection
│   │   ├── auth.py         # Authentication Middleware (Ext Point #1)
│   │   └── rate_limit.py   # Rate Limiting Middleware (Ext Point #2)
│   │
│   ├── jobs/               # JOB CONTROL: Lifecycle & Async Scheduling
│   │   ├── manager/        # JobManager with internal Worker Pool
│   │   ├── checkpoint/     # Step-level persistence for Workflow resume
│   │   └── callbacks/      # Output strategies (Feishu, CSV, MCP, Factory)
│   │
│   ├── registry/           # CAPABILITY HUB: Unified Discovery
│   │   ├── tools.py        # ToolRegistry + ToolMeta(category, returns) metadata
│   │   ├── resources.py    # Registry for static JSON/Markdown context
│   │   └── prompts.py      # Registry for Standard Operating Procedures (SOPs)
│   │
│   ├── mcp/                # PROTOCOL LAYER: Low-level MCP implementation
│   │   ├── client/         # LocalMCPClient (Unified bridge for both tracks)
│   │   ├── server.py       # External Stdio Server for desktop LLM clients
│   │   └── servers/        # DOMAIN MICROSERVICES (L1/L2 Tools)
│   │       ├── amazon/     # L1: 17 Amazon scrapers (curl_cffi)
│   │       ├── market/     # L1: Xiyouzhaoci + SellerSprite (keyword/ASIN)
│   │       ├── lingxing/   # L1: Lingxing ERP inventory (AES-ECB auth)
│   │       ├── social/     # L1: TikTok/Meta trend analysis
│   │       ├── finance/    # L2: Profit & Fee calculation (Consumes Cache)
│   │       ├── compliance/ # L2: EPA/Patent/Trademark checking
│   │       └── output/     # L2: 14 tools (Bitable CRUD, Card, Doc, CSV, JSON)
│   │
│   ├── intelligence/       # AI ORCHESTRATION: Provider routing
│   │   ├── dto.py          # LLMResponse DTOs (Zero dependencies)
│   │   ├── fallback.py     # FallbackHandler (for LLM failures)
│   │   ├── parsers/        # Output cleaning/formatting (e.g., MarkdownCleaner)
│   │   ├── providers/      # Multi-provider support (Gemini, Claude, Llama.cpp)
│   │   └── router/         # IntelligenceRouter (Task classification & Batching)
│   │
│   ├── core/               # KERNEL: Shared infrastructure
│   │   ├── models/         # Unified Pydantic DTOs (Product, Review, Request)
│   │   ├── telemetry/      # TimeEstimator & TelemetryTracker (Dynamic ETA)
│   │   ├── utils/          # ConfigHelper, CookieHelper, ProxyManager
│   │   └── data_cache.py   # Mediated Persistence Singleton (L1 -> L2 sync)
│   │
│   └── entry/              # ENTRY POINTS: Protocol-specific adapters
│       ├── cli/            # CommandLineInterface main entry
│       └── feishu/         # WebSocket Bot Listener & Command Dispatchers
│
├── data/                   # PERSISTENCE STORE
│   ├── cache/              # L1/L2 Mediated Data Cache files
│   ├── checkpoints/        # Workflow step-level state
│   ├── sessions/           # Agent conversation history files
│   └── cookies/            # Cached browser session cookies
│
├── scripts/                # DEPLOYMENT: Automation & Runners
├── tests/                  # VALIDATION: Unit, Integrity, and Full-Flow tests
└── main.py                 # Root CLI Wrapper
```

### Layer Responsibilities

#### A. Core Layer (`src/core/`)
Provides the foundational DTOs and shared utilities. The `DataCache` here is critical for the L1/L2 decoupling principle.

#### B. Gateway Layer (`src/gateway/`)
Acts as the security and normalization barrier. Every request is converted into a `UnifiedRequest` before reaching the Job Manager.

#### C. Registry Layer (`src/registry/`)
The system's single source of truth for "What can this system do?". It decouples the orchestrators from the underlying tool implementations.

#### D. Protocol & Services Layer (`src/mcp/`)
Implements the Model Context Protocol. Business logic is isolated into Domain Servers (Microservices).

#### E. Intelligence Layer (`src/intelligence/`)
A stateless routing layer that decides which LLM (Local or Cloud) is best suited for a specific ProcessStep or Agent thought.

#### F. Orchestration Layer (`src/workflows/` & `src/agents/`)
The "Brains". It contains the logic for chaining tools into complex business value, either through deterministic code (Workflow) or LLM reasoning (Agent).

---

## 3. Data Flow Examples

### Workflow Execution (e.g., Feishu BSR Command)
1. **Trigger**: User types `获取 Electronics BSR`.
2. **Gateway**: `APIGateway` normalizes request to `UnifiedRequest`, resolves identity, and submits to `JobManager`.
3. **Queue**: `JobManager` pushes task to `asyncio.Queue`. A worker picks it up.
4. **Execution**: `WorkflowEngine` runs `amazon_bsr` pipeline.
5. **Caching**: `EnrichStep` fetches BSR data via MCP, then writes raw data to **`DataCache`**.
6. **L2 Interaction**: Subsequent steps (e.g., profit calculation) read from **`DataCache`** instead of re-scraping.
7. **Output**: Result written to Feishu Bitable via `CallbackFactory` instantiated instance.

---

## 4. Key Architectural Principles

| Principle | Implementation |
|---|---|
| **Domain-Driven Design** | Capabilities are isolated into `mcp/servers/` microservices. |
| **Dual-Track Orchestration**| Workflows for batching; MCPAgent for exploration. |
| **Decoupled Discovery** | `Tool Registry` is a top-level hub for all orchestrators. |
| **Identity Normalization** | `API Gateway` transforms all inputs into a `UnifiedRequest`. |
| **Stateful Resilience** | `CheckpointManager` (Workflows) and `SessionManager` (Agents) ensure robustness. |
| **Mediated Persistence** | `DataCache` decouples raw data acquisition (L1) from reasoning/calculation (L2). |

---

## 5. Intelligence Router Deep Dive

The Intelligence Router (`src/intelligence/`) is the system's **cost-aware AI dispatch layer**. It solves a core problem: not every AI task needs a cloud API call. Simple data cleaning can run locally for free, while patent analysis demands cloud-grade reasoning. The router makes this decision automatically, keeping costs low and latency tight.

### 5.1 Design Patterns

- **Strategy Pattern**: `BaseLLMProvider` (ABC) defines a uniform interface (`generate_text`, `generate_structured`, `batch_generate_text`, `count_tokens`). Each provider (LlamaCpp, Gemini, Claude) is a concrete strategy, swappable at runtime.
- **Factory Pattern**: `ProviderFactory.get_provider(type)` instantiates the correct provider based on configuration, with graceful degradation if a provider fails to load.
- **Chain of Responsibility**: The routing flow cascades through classification → local check → cloud fallback, with each stage deciding whether to handle or pass.

### 5.2 Task Auto-Classification

When a caller does not specify a `compute_target`, the router classifies the task using a hybrid approach:

1.  **Heuristic Pre-screening (<1ms)**: Executes high-speed keyword and length-based rules.
    *   **Complexity**: Prompts > 4000 chars → `DEEP_REASONING`.
    *   **Intent**: Keywords like `analyze`, `compare` → `DEEP_REASONING`.
    *   **Extraction**: Keywords like `extract`, `find` (if < 2000 chars) → `DATA_EXTRACTION`.
    *   **Cleaning**: Keywords like `clean`, `format` (if < 1000 chars) → `SIMPLE_CLEANING`.
2.  **Model Classification**: If heuristics don't match, the first 400 characters are sent to the LOCAL model.
3.  **Data Logging**: All classification decisions (including prompt previews, rules triggered, and confidence) are logged to `data/intelligence/raw_prompts.jsonl` for future model distillation and fine-tuning.

### 5.3 End-to-End Dispatch Flow

```
┌─ Caller: ProcessStep.run() / MCPAgent.think() ──────────────────────────┐
│                                                                          │
│  router.route_and_execute(prompt, category=None)                        │
│    │                                                                     │
│    ├─ 1. CLASSIFY & LOG ────────────────────────────────────────────┐   │
│    │   category provided?                                            │   │
│    │   ├─ YES → use as-is                                           │   │
│    │   └─ NO  → 1. Run _run_heuristics() (Zero cost)                │   │
│    │            2. Fallback: _classify_task() via LOCAL model       │   │
│    │            3. Log result to raw_prompts.jsonl                  │   │
│    │                                                                 │   │
│    ├─ 2. ROUTE ─────────────────────────────────────────────────────┤   │
...
│    │   Auth error        → Return critical error LLMResponse        │   │
│    │                                                                 │   │
│    ├─ 4. COST CALCULATION (PriceManager) ───────────────────────────┤   │
│    │   - Support for Gemini "Thinking Tokens" (billed as output)     │   │
│    │   - Support for "Prompt Caching" (cheaper cache_read price)     │   │
│    │   - Precise tiered pricing (>200k context)                      │   │
│    │                                                                 │   │
│    └─ 5. RETURN ────────────────────────────────────────────────────┘   │
│       LLMResponse { text, provider_name, model_name,                     │
│                     token_usage, cost, metadata }                        │
└──────────────────────────────────────────────────────────────────────────┘
```

### 5.4 Batch Processing

`batch_route_and_execute(prompts[], category?)` processes multiple items efficiently:

1. **Single classification** — classify once using the first prompt, apply to entire batch.
2. **Unified routing** — the whole batch goes to the same provider (no per-item switching).
3. **Concurrency control** — `asyncio.Semaphore` limits parallel calls:
   - LOCAL: `concurrency = 2` (CPU-bound, avoids starving the model)
   - CLOUD: `concurrency = 5` (I/O-bound, maximizes throughput)
4. **Exception isolation** — `asyncio.gather()` with exception filtering; failed items are dropped, successful results returned.

This is used by `ProcessStep` in workflows. For example, classifying 58 products' seller origin runs as a single batch routed to LOCAL with concurrency 2.

### 5.5 Fallback & Resilience

The `FallbackHandler` uses the **Strategy Pattern** with a dictionary mapping `FailureType` → async handler:

| FailureType | Handler Behavior |
|---|---|
| `LOCAL_MODEL_TIMEOUT` | Return user-friendly message. No retry (task is simple; local model is stuck). |
| `CLOUD_API_UNAVAILABLE` | Enqueue to in-memory `asyncio.Queue`. Background consumer retries with 10s backoff. |
| `CLOUD_API_RATE_LIMIT` | Queue wait + alert. Same retry mechanism as unavailable. |

**Extension point**: Replace `asyncio.Queue` with Redis + Celery/RQ for multi-user deployments.

### 5.6 Provider Details

| Provider | Model Priority | Timeout | Concurrency | Key Features |
|---|---|---|---|---|
| `LlamaCppProvider` | Configured GGUF model | 120s | 2 | ChatML format, GPU acceleration (`n_gpu_layers=-1`), auto context truncation (reserves 512 tokens) |
| `GeminiProvider` | `1.5-pro` > `1.5-flash` > `1.0-pro` | API default | 5 | Auto-discovers best available model, structured JSON via `response_mime_type` |
| `ClaudeProvider` | `opus` > `sonnet` > `haiku` | API default | 5 | Max 4096 output tokens, rough token estimation (`len // 4`) |

### 5.7 Integration with Orchestration Tracks

The router serves both orchestration tracks through a single `IntelligenceRouter` instance created by the `JobManager`:

- **Workflow Track**: `ProcessStep` declares a `compute_target` (PURE_PYTHON / LOCAL_LLM / CLOUD_LLM). The step maps this to a `TaskCategory` and calls `ctx.router.batch_route_and_execute()`. Results are cached per `(job_id, step_name, item_id)` to avoid redundant LLM calls on workflow resume.
- **Agent Track**: `BaseAgent` receives the router in its constructor. The `MCPAgent` calls `router.route_and_execute()` in each iteration of its ReAct loop, forced to `DEEP_REASONING` since agent tasks are exploratory by nature.

### 5.8 Agent Budget Strategy (Tokens & Cost)

The `MCPAgent` tracks cumulative token usage and monetary cost across all LLM calls, but only **cloud** usage counts toward the budget:

| Metric | Tracked In | Budget-Relevant |
|---|---|---|
| `session.token_usage` | All providers (total) | No (informational) |
| `session.cloud_token_usage` | Cloud providers only | Yes (triggers batch switch) |
| `session.total_cost` | Cloud providers only | Yes (for budget alerts) |

**Local model tokens are free** — they consume local compute, not API credits. When the local model is strong enough to handle agent reasoning, the agent loop runs without any budget constraint.

When `cloud_token_usage >= token_budget` (default 1,000,000):
1. Progress callback notifies user: "Switching to batch mode"
2. Agent injects a forced summarization prompt
3. LLM produces Final Answer from all data collected so far
4. Session status → `completed` (not `failed`)

`max_steps` remains as a **progress display counter** — it does NOT terminate the agent. The `total_cost` is displayed in real-time in logs and callbacks.

### 5.9 Agent System Prompt Architecture

The agent system prompt is built from three layers:

```
mcp_agent_system.md          Human-editable template with $variables
        │
        v
PromptBuilder.build()        Loads .md, injects runtime values via string.Template
        │
        v
ToolCatalogFormatter          Groups 48 tools into 4 categories from ToolMeta
                              DATA → COMPUTE → FILTER → OUTPUT
```

The template includes:
- **Execution Phases**: COLLECT → FILTER → ENRICH → ANALYZE → OUTPUT
- **Autonomous Output Rules**: Agent must never ask for IDs it can discover via tools (e.g., auto-create Bitable)
- **Tool Disambiguation**: Explicit warnings about similar tools (e.g., `search_products` vs `xiyou_keyword_analysis`)
