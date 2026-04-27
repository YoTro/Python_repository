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
|   2. Rate Limit    Three-layer enforcement (config/settings.json → rate_limits)|
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
|   submit(request)                    --> job_id                              |
|   resume_from_checkpoint(job_id,     --> job_id (requeued)                   |
|     callback, workflow_name?, params?)    Loads checkpoint; engine skips     |
|                                           completed steps automatically.      |
|   cancel(job_id)                     --> bool                                |
|   get_status(job_id)                 --> JobStatus                           |
|                                                                              |
|   Job Statuses: PENDING · RUNNING · COMPLETED · FAILED · CANCELLED          |
|                 SUSPENDED  ← batch job submitted; awaiting provider results  |
|                             (timeout 7200s; auto-cancelled by reaper)        |
|                                                                              |
|   Queue Implementation:                                                      |
|   [Single-User] asyncio.Queue (Memory)  [Ext Point #3] Redis Priority Queue  |
|   +----------------+   +-------------------------------------------------+  |
|   |  Workflow Job  |   |  Enterprise: Dedicated Worker pool, no queuing   |  |
|   |  Agent Session |   |  Pro:        High priority, N concurrency        |  |
|   |  (Unified MGMT)|   |  Free:       Low priority, 1 concurrency, limit  |  |
|   +----------------+   +-------------------------------------------------+  |
|                                                                              |
|   Background Services:                                                       |
|   +──────────────────────────────────────────────────────────────────────+  |
|   | BatchPoller   60s scan loop; polls provider batch jobs; on complete: |  |
|   |               appends BATCH_COMPLETED event → signals SignalBus →    |  |
|   |               calls job_manager.resume(job_id)                       |  |
|   |               24h TTL expiry: appends BATCH_FAILED, cancels job      |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   | SignalBus     In-process asyncio.Event pub/sub; decouples BatchPoller|  |
|   |               from JobManager.  [Ext Point #7] Redis Pub/Sub         |  |
|   +──────────────────────────────────────────────────────────────────────+  |
|   | Reaper        60s loop; auto-cancels SUSPENDED jobs past timeout     |  |
|   +──────────────────────────────────────────────────────────────────────+  |
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
|  activity_runner =           |             |  +---------------------------+     |
|    ActivityRunner(ckpt, id)  |             |                                  |
|  for step in active_steps:   |             |  Step Logic:                     |
|    checkpoint.save()  ───────┼─ Resume ───┼── session.save() (inc. cost)     |
|    result = activity_runner  |             |                                  |
|      .run(step,items,ctx)    |             |                                  |
|       ├ idempotency: replay  |             |                                  |
|       │  ACTIVITY_COMPLETED  |             |                                  |
|       ├ batch wait: re-raise |             |                                  |
|       │  BatchPendingError   |             |                                  |
|       ├ heartbeat: inject fn |             |                                  |
|       └─► step.run() ────────┼────────────┼── tool = agent.decide()          |
|    if not items: break       |             |                                  |
|                              |             |  Step Logic:                     |
|  Step Tri-primitives:        |             |   1. Check steps vs budget/cost  |
|  +----------------------+    |             |   2. If steps > max:             |
|  | EnrichStep  ─────────┼────┼── MCP Client|      - Budget > 20%: +5 steps    |
|  | ProcessStep ─────────┼────┼── Intel Router      - Budget < 20%: Force Final |
|  |  batch_threshold=N   |    |             |                                  |
|  |  N items→ Batch API  |    |             |                                  |
|  | FilterStep  (Python) |    |             |                                  |
|  +----------------------+    |             |  System Prompt Architecture:     |
|                              |             |   · .md template (editable)      |
|                              |             |   · PromptBuilder (assembly)     |
|                              |             |   · ToolCatalogFormatter         |
|                              |             |  Autonomous output rules:        |
|                              |             |  · Auto-populate Bitable (robust)|
|                              |             |  · Attachment-First Policy:      |
|                              |             |    Answer > 8000 chars →         |
|                              |             |    export_md → session.context   |
|                              |             |    ["report_file_path"] →        |
|                              |             |    final_items → on_complete     |
|                              |             |    (channel-agnostic delivery)   |
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
|   ...  (54 tools total across 7 domain servers)                              |
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
|   |                    |  | (Sellersprite +    |  |                    |    |
|   | search_products    |  |  Xiyouzhaoci)      |  | tiktok_trend       |    |
|   | get_details        |  | competing_lookup   |  | meta_ad_density    |    |
|   | get_bsr            |  | market_research    |  | pinterest_interest |    |
|   | get_past_sales     |  | keyword_analysis   |  |                    |    |
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
|   | calc_fba_fee       |  | epa_check          |  | send_local_file    |    |
|   | (+ category bench) |  | patent_risk_calc   |  | send_url_file      |    |
|   |                    |  |                    |  | send_data_file     |    |
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
|   |  CLOUD provider (Gemini / Claude / DeepSeek)                        |  |
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
|     supports_batch()                          --> bool  (default False)      |
|     generate_batch(requests: List[BatchRequest]) --> BatchJobHandle          |
|     poll_batch(handle: BatchJobHandle)        --> Dict[str, LLMResponse]|None|
|       None = still running; dict = complete (keyed by custom_id)            |
|                                                                              |
|   Batch pricing: create_response(..., is_batch=True) → PriceManager applies |
|   50% discount on Gemini and Claude batch completions.                       |
|   DeepSeek: cache hit/miss split billing; V4-Pro auto tier-switch post promo.|
|                                                                              |
|   ProviderFactory.get_provider(type) --> BaseLLMProvider                     |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "local" / "llama"    | LlamaCppProvider  (llama-cpp-python, GPU)    |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "gemini"             | GeminiProvider    supports_batch=True        |   |
|   |                      |   generate_batch: client.batches.create()    |   |
|   |                      |   poll_batch: JOB_STATE_SUCCEEDED check      |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "claude"/"anthropic" | ClaudeProvider    supports_batch=True        |   |
|   |                      |   generate_batch: messages.batches.create()  |   |
|   |                      |   poll_batch: processing_status=="ended"     |   |
|   +──────────────────────+──────────────────────────────────────────────+   |
|   | "deepseek"           | DeepSeekProvider  supports_batch=False       |   |
|   |                      |   OpenAI-compatible REST API                 |   |
|   |                      |   models: deepseek-v4-flash / deepseek-v4-pro|   |
|   |                      |   KV cache: cached_tokens split billing      |   |
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
|   BatchRequest = {                                                           |
|     custom_id, prompt, system_message?, schema?                              |
|   }  — submitted to provider.generate_batch()                               |
|                                                                              |
|   BatchJobHandle = {                                                         |
|     job_id, provider, status, created_at, metadata                          |
|   }  — stored in BATCH_SUBMITTED event; used by BatchPoller for TTL check   |
|                                                                              |
|   WorkflowEvent = {                                                          |
|     timestamp, event_type, step_name, payload                               |
|   }                                                                          |
|   event_type values:                                                         |
|     ACTIVITY_COMPLETED  — step ran successfully; payload has cached result  |
|     BATCH_SUBMITTED     — batch job dispatched to provider; includes handle,|
|                           requests, items_snapshot, output_field, schema_path|
|     BATCH_COMPLETED     — BatchPoller wrote results; payload has final items |
|     BATCH_FAILED        — batch expired (>24h TTL) or provider error        |
|     HEARTBEAT           — liveness ping from long-running step               |
|                                                                              |
+==============================================================================+
|                                                                              |
|   ── Integration Points ────────────────────────────────────────────────────  |
|                                                                              |
|   +──────────────────+──────────────────────────────────────────────────+   |
|   | WorkflowEngine   | ProcessStep.run()                                |   |
|   |                  | Sync path: asyncio.gather(router.route_and_execute)|  |
|   |                  | Batch path (n >= batch_threshold):               |   |
|   |                  |   provider.generate_batch(requests)              |   |
|   |                  |   → raise BatchPendingError                      |   |
|   |                  |   → ActivityRunner writes BATCH_SUBMITTED        |   |
|   |                  |   → JobManager suspends job (7200s timeout)      |   |
|   |                  |   → BatchPoller polls; writes BATCH_COMPLETED    |   |
|   |                  |   → job_manager.resume() re-runs from checkpoint |   |
|   |                  | compute_target maps to TaskCategory automatically|   |
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
|     on_progress(step, total, msg)        ── Real-time progress notifications |
|     on_complete(workflow_result)         ── Route to target output           |
|     on_error(error, job_id=None)         ── job_id present when checkpoint   |
|                                             exists; surface to user for      |
|                                             manual resume via Feishu command |
|     notify(message)                     ── Channel-agnostic lightweight msg  |
|                                             (default delegates to on_progress)|
|                                             Override per channel for cheaper |
|                                             delivery (e.g. Feishu text msg)  |
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

+──────────────────────────────────────────────────────────────────────────────+
|  DURABLE EXECUTION  (Temporal-inspired, no Temporal dependency)              |
|                                                                              |
|  Goal: survive process restarts and async provider delays without losing     |
|  workflow state or resubmitting already-completed LLM calls.                 |
|                                                                              |
|  Key components:                                                             |
|  +──────────────────+──────────────────────────────────────────────────────+|
|  | ActivityRunner   | Wraps every step execution. On each run:             ||
|  |                  |  1. Check ACTIVITY_COMPLETED → replay cached result  ||
|  |                  |  2. Check BATCH_SUBMITTED+COMPLETED → use results    ||
|  |                  |  3. Check BATCH_SUBMITTED only → re-raise pending    ||
|  |                  |  4. Execute step; inject heartbeat callable          ||
|  |                  |  5. Persist result as ACTIVITY_COMPLETED             ||
|  +──────────────────+──────────────────────────────────────────────────────+|
|  | Event Log        | Append-only List[WorkflowEvent] in CheckpointData.   ||
|  |                  | Source of truth for idempotency and batch handoff.   ||
|  +──────────────────+──────────────────────────────────────────────────────+|
|  | BatchPoller      | Background service that polls provider APIs every 60s||
|  |                  | Reconstructs items via stored items_snapshot and      ||
|  |                  | schema_path. Survives process restarts.              ||
|  +──────────────────+──────────────────────────────────────────────────────+|
|  | SignalBus        | Decouples poller (writer) from JobManager (reader).  ||
|  |                  | asyncio.Event per job_id.                            ||
|  +──────────────────+──────────────────────────────────────────────────────+|
|                                                                              |
|  Batch API cost benefit: 50% discount applied when is_batch=True.           |
|  Use batch_threshold=1 to force a step to always use batch mode             |
|  (recommended when prior steps already take 30+ min, e.g. ad_diagnosis).   |
+──────────────────────────────────────────────────────────────────────────────+

+────────────────────────────────────+  +──────────────────────────────────────+
|  STATE STORE                       |  |  OBSERVABILITY                       |
|                                    |  |                                      |
|  [Single-User]                     |  |  Tracing:                            |
|    Local files: checkpoint/history |  |    trace_id full link                |
|    SQLite:      Config / Usage logs|  |    Gateway -> Step -> Model          |
|                                    |  |    Duration / cost per step per job  |
|  CheckpointData schema:            |  |                                      |
|    job_id, step_index, step_name   |  |                                      |
|    items          pipeline payload |  |                                      |
|    ctx_cache      WorkflowContext  |  |                                      |
|                   .cache snapshot  |  |                                      |
|                   (restored on     |  |                                      |
|                    resume so later |  |                                      |
|                    steps get data  |  |                                      |
|                    from earlier    |  |                                      |
|                    steps)          |  |                                      |
|    workflow_name, workflow_params  |  |                                      |
|    metadata, created_at            |  |                                      |
|    events  List[WorkflowEvent]     |  |                                      |
|      append-only durable event log |  |                                      |
|      enables idempotent replay and |  |                                      |
|      batch state recovery on       |  |                                      |
|      process restart               |  |                                      |
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
  7   SignalBus             asyncio.Event (in-process)  Redis Pub/Sub (cross-process)
  ─── ─────────────────── ─────────────────────────── ──────────────────────────────
  Unchanged core (never needs modification):
    JobRequest DTO structure  ·  Workflow Engine  ·  Step tri-primitives
    MCP Server business logic ·  Intelligence Router routing rules
    Callback interface        ·  All WorkflowRegistry Workflow definitions
    ActivityRunner / BatchPoller / event log schema


================================================================================
                    PRODUCT SCREENING WORKFLOW EXAMPLE
================================================================================

  Step  Type          Tool / Target        Input --> Output
  ────  ────────────  ───────────────────  ────────────────────────────────────
   0    ProcessStep   search_and_expand    keyword  -->  []{asin, title, price}
                      (SearchExtractor,               parallel pages 1..N,
                       pages=search_pages)            deduped by ASIN
   1    EnrichStep    profitability_api    ASIN     -->  Dims + BSR + Price + Weight
   2    EnrichStep    past_sales_api       ASIN     -->  Monthly Sales Volume
   3    FilterStep    price/rating/weight  Filter   -->  Basic candidates remain
   4    EnrichStep    fulfillment_api      ASIN     -->  FBA/FBM Status
   5    EnrichStep    deal_history         ASIN     -->  Promo history appended
   6    ProcessStep   promo_analysis       PURE_PYTHON -> Risk score
   7    FilterStep    promo_risk           Filter   -->  Stable prices remain
   8    ProcessStep   calc_profit (MCP)    FINANCE  -->  Exact Margin & ROI
   9    FilterStep    profitability        Filter   -->  Profitable ASINs remain
  10    EnrichStep    reviews              ASIN     -->  Raw review list
  11    ProcessStep   summarize_reviews    CLOUD_LLM --> Manipulation risk score
  12    FilterStep    review_manipulation  Filter   -->  Clean-review ASINs remain
  13    EnrichStep    compliance           ASIN     -->  CPSC/EPA/restriction flags
  14    FilterStep    compliance_filter    Filter   -->  Low-risk ASINs remain
  15    EnrichStep    xiyou_traffic (MCP)  MARKET   -->  Actual Ad Dependency %
  16    FilterStep    ad_dependency        Filter   -->  Natural-traffic winners
  17    ProcessStep   final_synthesis      CLOUD_LLM --> Selection Report
  ────  ────────────  ───────────────────  ────────────────────────────────────
  Key: Step 0 seeds ASIN list from keyword; engine seeds items=[{keyword}] when
       no asin/initial_items param is present. Finance MCP unifies profit calc.
