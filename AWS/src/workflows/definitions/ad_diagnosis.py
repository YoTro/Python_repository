from __future__ import annotations
"""
Ad Diagnosis Workflow

Collects advertising and inventory data for one or more ASINs, then runs
an LLM-powered diagnostic to identify issues and produce prioritised recommendations.

Data sources (all run in parallel within each EnrichStep):
  - Amazon Ads API v3  : campaigns, ad groups, keywords, performance report
  - SP-API             : FBA inventory
  - SP-API Catalog     : product metadata
  - Xiyouzhaoci        : organic keyword rankings (ad traffic ratio)

Diagnostic dimensions:
  - Budget adequacy        (daily budget vs actual spend)
  - Bid competitiveness    (keyword bids vs recommended)
  - Bidding strategy       (AUTO vs MANUAL, placement adjustments)
  - Keyword health         (high-ACOS, low-impression, missing keywords)
  - Organic keyword rank   (xiyou positions vs ad coverage)
  - Inventory risk         (available days < threshold)
  - ACOS & profitability   (per-campaign and account-level)

Input items shape:
  {"asin": "B0FXFGMD7Z"}          # minimum
  {"asin": "B0FXFGMD7Z", "sku": "SKU-001", "cogs": 8.5, "price": 24.99}

Config keys (with defaults):
  store_id                str   "US"
  region                  str   "NA"
  days                    int   30       report lookback days
  inventory_risk_days     int   30       flag if available < this many days
  acos_warn_threshold     float 0.30     warn if campaign ACOS > 30%
  acos_crit_threshold     float 0.50     critical if campaign ACOS > 50%
  budget_exhaustion_pct   float 0.90     flag if spend/budget > 90%
  enable_xiyou            bool  True     fetch organic rankings from xiyouzhaoci
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow, WorkflowContext
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import ComputeTarget

logger = logging.getLogger(__name__)

# ── Cache keys (shared across per-item enrichers to avoid duplicate API calls) ──
_KEY_CAMPAIGNS      = "ad_diag:campaigns"
_KEY_PERFORMANCE    = "ad_diag:performance"
_KEY_KEYWORDS       = "ad_diag:keywords"
_KEY_KW_PERFORMANCE = "ad_diag:kw_performance"


# ---------------------------------------------------------------------------
# Shared account-level fetchers (fetch once, cached in ctx.cache)
# ---------------------------------------------------------------------------

async def _ensure_campaigns(ctx: WorkflowContext) -> List[Dict]:
    if _KEY_CAMPAIGNS not in ctx.cache:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient
        store_id = ctx.config.get("store_id")
        region   = ctx.config.get("region", "NA")
        client   = AmazonAdsClient(store_id=store_id, region=region)
        campaigns = await client.list_campaigns(states=["ENABLED", "PAUSED"], max_results=2000)
        ctx.cache[_KEY_CAMPAIGNS] = campaigns
        logger.info(f"Fetched {len(campaigns)} campaigns from Ads API")
    return ctx.cache[_KEY_CAMPAIGNS]


async def _ensure_performance(ctx: WorkflowContext) -> List[Dict]:
    if _KEY_PERFORMANCE not in ctx.cache:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient
        store_id = ctx.config.get("store_id")
        region   = ctx.config.get("region", "NA")
        days     = ctx.config.get("days", 30)
        client   = AmazonAdsClient(store_id=store_id, region=region)
        records  = await client.get_performance_report(
            report_type="spCampaigns", days=days
        )
        ctx.cache[_KEY_PERFORMANCE] = records
        logger.info(f"Fetched {len(records)} campaign performance records")
    return ctx.cache[_KEY_PERFORMANCE]


async def _ensure_keyword_performance(ctx: WorkflowContext) -> List[Dict]:
    """Fetch account-wide spKeywords performance report, cached for the run."""
    if _KEY_KW_PERFORMANCE not in ctx.cache:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient
        store_id = ctx.config.get("store_id")
        region   = ctx.config.get("region", "NA")
        days     = ctx.config.get("days", 30)
        client   = AmazonAdsClient(store_id=store_id, region=region)
        records  = await client.get_performance_report(
            report_type="spKeywords", days=days
        )
        ctx.cache[_KEY_KW_PERFORMANCE] = records
        logger.info(f"Fetched {len(records)} keyword performance records")
    return ctx.cache[_KEY_KW_PERFORMANCE]


async def _ensure_keywords(ctx: WorkflowContext, campaign_ids: List[str]) -> List[Dict]:
    """Fetch keywords for a set of campaign_ids, cached by sorted id-tuple."""
    cache_key = f"{_KEY_KEYWORDS}:{','.join(sorted(campaign_ids))}"
    if cache_key not in ctx.cache:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient
        store_id = ctx.config.get("store_id")
        region   = ctx.config.get("region", "NA")
        client   = AmazonAdsClient(store_id=store_id, region=region)
        keywords = await client.list_keywords(
            campaign_ids=campaign_ids, states=["ENABLED", "PAUSED"]
        )
        ctx.cache[cache_key] = keywords
    return ctx.cache[cache_key]


# ---------------------------------------------------------------------------
# Per-ASIN enrichers
# ---------------------------------------------------------------------------

async def _enrich_catalog(item: Dict, ctx: WorkflowContext) -> Dict:
    """Fetch product title, brand, size from SP-API Catalog."""
    from src.mcp.servers.amazon.sp_api.client import SPAPIClient
    asin = item.get("asin")
    if not asin:
        return {}
    try:
        client = SPAPIClient(store_id=ctx.config.get("store_id"))
        data   = await client.get_catalog_item(asin)
        return {
            "title":              data.get("title"),
            "brand":              data.get("brand"),
            "size":               data.get("size"),
            "bullet_point_count": data.get("bullet_point_count"),
        }
    except Exception as e:
        logger.warning(f"Catalog fetch failed for {asin}: {e}")
        return {}


async def _enrich_inventory(item: Dict, ctx: WorkflowContext) -> Dict:
    """Fetch FBA inventory for the item's SKU(s) from SP-API."""
    from src.mcp.servers.amazon.sp_api.client import SPAPIClient
    asin = item.get("asin")
    sku  = item.get("sku")
    try:
        client  = SPAPIClient(store_id=ctx.config.get("store_id"))
        records = await client.get_inventory(seller_skus=[sku] if sku else None)
        # Match by ASIN if no SKU filter was applied
        matched = [r for r in records if r.get("asin") == asin] if not sku else records
        if not matched:
            return {"inventory_records": [], "total_available": 0}
        total_available = sum(r.get("available_quantity", 0) for r in matched)
        total_inbound   = sum(r.get("inbound_quantity", 0)   for r in matched)
        # Estimate can-sell days using item daily sales if provided
        daily_sales = item.get("daily_sales") or 0
        can_sell_days = (
            round(total_available / daily_sales) if daily_sales > 0 else None
        )
        return {
            "inventory_records":  matched,
            "total_available":    total_available,
            "total_inbound":      total_inbound,
            "can_sell_days":      can_sell_days,
            "inventory_risk":     (
                can_sell_days is not None
                and can_sell_days < ctx.config.get("inventory_risk_days", 30)
            ),
        }
    except Exception as e:
        logger.warning(f"Inventory fetch failed for {asin}: {e}")
        return {"inventory_records": [], "total_available": 0, "inventory_risk": False}


async def _enrich_campaigns(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Match account campaigns to this ASIN by name convention (ASIN substring match).
    Falls back to all campaigns if no match found — analyst can triage manually.
    """
    asin = item.get("asin", "").upper()
    all_campaigns = await _ensure_campaigns(ctx)

    matched = [c for c in all_campaigns if asin in c.get("name", "").upper()]
    if not matched:
        logger.debug(f"No campaigns matched by name for {asin}; returning all enabled.")
        matched = [c for c in all_campaigns if c.get("state") == "ENABLED"]

    campaign_ids = [str(c["campaign_id"]) for c in matched]
    total_daily_budget = sum(
        c.get("daily_budget") or 0 for c in matched if c.get("state") == "ENABLED"
    )
    strategies = list({c.get("bidding_strategy") for c in matched if c.get("bidding_strategy")})

    return {
        "campaigns":          matched,
        "campaign_ids":       campaign_ids,
        "total_daily_budget": total_daily_budget,
        "bidding_strategies": strategies,
    }


async def _enrich_performance(item: Dict, ctx: WorkflowContext) -> Dict:
    """Filter performance report records to this ASIN's campaigns."""
    campaign_ids = set(item.get("campaign_ids", []))
    all_perf     = await _ensure_performance(ctx)

    matched = [
        r for r in all_perf
        if str(r.get("campaign_id")) in campaign_ids
    ] if campaign_ids else all_perf

    if not matched:
        return {"performance_records": [], "total_spend": 0, "account_acos": None}

    total_spend  = sum(r.get("spend",  0) or 0 for r in matched)
    total_sales  = sum(r.get("sales",  0) or 0 for r in matched)
    total_orders = sum(r.get("orders", 0) or 0 for r in matched)
    total_clicks = sum(r.get("clicks", 0) or 0 for r in matched)
    account_acos = round(total_spend / total_sales * 100, 2) if total_sales > 0 else None

    # Flag campaigns exceeding ACOS thresholds
    warn_thresh = ctx.config.get("acos_warn_threshold", 0.30) * 100
    crit_thresh = ctx.config.get("acos_crit_threshold", 0.50) * 100
    high_acos_campaigns = [
        r for r in matched
        if r.get("acos") and r["acos"] > warn_thresh
    ]

    # Budget exhaustion: spend / (daily_budget * days) > threshold
    days = ctx.config.get("days", 30)
    budget_pct_threshold = ctx.config.get("budget_exhaustion_pct", 0.90)
    total_budget_capacity = item.get("total_daily_budget", 0) * days
    budget_exhaustion_pct = (
        round(total_spend / total_budget_capacity, 4)
        if total_budget_capacity > 0 else None
    )

    return {
        "performance_records":    matched,
        "total_spend":            total_spend,
        "total_sales":            total_sales,
        "total_orders":           total_orders,
        "total_clicks":           total_clicks,
        "account_acos":           account_acos,
        "high_acos_campaigns":    high_acos_campaigns,
        "budget_exhaustion_pct":  budget_exhaustion_pct,
        "budget_likely_exhausted": (
            budget_exhaustion_pct is not None
            and budget_exhaustion_pct > budget_pct_threshold
        ),
    }


async def _enrich_keywords(item: Dict, ctx: WorkflowContext) -> Dict:
    """Fetch manual keywords for this ASIN's campaigns."""
    campaign_ids = item.get("campaign_ids", [])
    if not campaign_ids:
        return {"keywords": [], "keyword_count": 0}

    keywords = await _ensure_keywords(ctx, campaign_ids)

    # Summarise bid distribution
    bids = [k["bid"] for k in keywords if k.get("bid") is not None]
    avg_bid = round(sum(bids) / len(bids), 4) if bids else None
    match_type_dist = {}
    for k in keywords:
        mt = k.get("match_type", "UNKNOWN")
        match_type_dist[mt] = match_type_dist.get(mt, 0) + 1

    return {
        "keywords":         keywords,
        "keyword_count":    len(keywords),
        "avg_bid":          avg_bid,
        "min_bid":          min(bids, default=None),
        "max_bid":          max(bids, default=None),
        "match_type_dist":  match_type_dist,
    }


async def _enrich_keyword_performance(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch keyword-level performance (spKeywords report) and aggregate per
    (keyword_text, match_type): avg_cpc, cvr, daily_clicks, impressions.

    Only keywords with >= min_clicks_for_cvr clicks are included to ensure
    reliable CVR estimates for the LP optimizer.
    """
    campaign_ids = set(item.get("campaign_ids", []))
    days         = ctx.config.get("days", 30)
    min_clicks   = ctx.config.get("min_clicks_for_cvr", 5)

    all_kw_perf = await _ensure_keyword_performance(ctx)

    # Filter to this ASIN's campaigns
    relevant = [
        r for r in all_kw_perf
        if str(r.get("campaign_id")) in campaign_ids
    ] if campaign_ids else all_kw_perf

    # Aggregate by (keyword_text, match_type)
    agg: Dict[tuple, Dict] = {}
    for r in relevant:
        key = (r.get("keyword_text", ""), r.get("match_type", ""))
        if key not in agg:
            agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0}
        agg[key]["spend"]       += r.get("spend", 0) or 0
        agg[key]["clicks"]      += r.get("clicks", 0) or 0
        agg[key]["orders"]      += r.get("orders", 0) or 0
        agg[key]["impressions"] += r.get("impressions", 0) or 0

    kw_performance = []
    for (kw_text, match_type), v in agg.items():
        clicks = v["clicks"]
        if clicks < min_clicks:
            continue
        avg_cpc      = round(v["spend"] / clicks, 4)
        cvr          = round(v["orders"] / clicks, 4)
        daily_clicks = round(clicks / days, 2)
        kw_performance.append({
            "keyword_text":  kw_text,
            "match_type":    match_type,
            "total_spend":   round(v["spend"], 2),
            "total_clicks":  clicks,
            "total_orders":  v["orders"],
            "impressions":   v["impressions"],
            "avg_cpc":       avg_cpc,
            "cvr":           cvr,
            "daily_clicks":  daily_clicks,
            "acos":          round(v["spend"] / (v["orders"] * avg_cpc / cvr) * 100, 2)
                             if v["orders"] > 0 and cvr > 0 else None,
        })

    # Sort by spend descending (most important keywords first)
    kw_performance.sort(key=lambda x: x["total_spend"], reverse=True)

    return {"keyword_performance": kw_performance}


def _optimize_budget(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    ProcessStep (pure Python): run LP budget optimisation for each item.

    Inputs per item (set by previous steps):
      keyword_performance  list of per-keyword aggregated metrics
      total_daily_budget   float — campaign budget cap

    LP formulation (via AdBudgetOptimizer / OR-Tools GLOP):
      Maximise  Σ clicks_i × cvr_i          (= expected orders)
      Subject to Σ clicks_i × avg_cpc_i ≤ daily_budget
                 0 ≤ clicks_i ≤ max_daily_clicks_i

    max_daily_clicks_i = daily_clicks × headroom_factor (default 3×)
    — allows the solver to explore expanding high-CVR keywords.

    Adds to each item:
      lp_summary          overall budget efficiency metrics
      lp_top_allocations  top keywords by LP-assigned clicks (increase budget here)
      lp_zero_keywords    keywords LP dropped to 0 (pause candidates)
      lp_maxed_keywords   keywords hitting the ceiling (raise bid / budget)
      lp_actual_orders    estimated orders at current spend (for comparison)
    """
    from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer

    headroom = ctx.config.get("lp_headroom_factor", 3.0)
    optimizer = AdBudgetOptimizer()

    for item in items:
        kw_perf        = item.get("keyword_performance", [])
        daily_budget   = item.get("total_daily_budget", 0) or 0

        if not kw_perf or daily_budget <= 0:
            item["lp_summary"] = {"skipped": True, "reason": "no keyword data or zero budget"}
            continue

        # Build LP input — one row per (keyword, match_type) with enough data
        lp_input = []
        actual_daily_orders = 0.0
        for kw in kw_perf:
            if not kw.get("avg_cpc") or not kw.get("cvr"):
                continue
            max_daily = max(round(kw["daily_clicks"] * headroom, 1), 1.0)
            lp_input.append({
                "name":              f"{kw['keyword_text']}|{kw['match_type']}",
                "avg_cpc":           kw["avg_cpc"],
                "estimated_cvr":     kw["cvr"],
                "max_daily_clicks":  max_daily,
            })
            actual_daily_orders += kw["daily_clicks"] * kw["cvr"]

        if not lp_input:
            item["lp_summary"] = {"skipped": True, "reason": "all keywords filtered (insufficient clicks)"}
            continue

        result = optimizer.optimize(lp_input, total_budget=daily_budget)

        if result.get("status") != "OPTIMAL":
            item["lp_summary"] = {"skipped": True, "reason": result.get("message")}
            continue

        summary  = result["summary"]
        alloc    = result["allocation"]
        alloc_names = {a["keyword"] for a in alloc}

        # Keywords LP assigned 0 clicks → pause candidates
        zero_kws = [
            kw["keyword_text"] for kw in kw_perf
            if f"{kw['keyword_text']}|{kw['match_type']}" not in alloc_names
            and kw.get("avg_cpc")
        ]

        # Keywords at LP ceiling → raise bid / increase budget candidates
        maxed_kws = []
        kw_map = {lp["name"]: lp for lp in lp_input}
        for a in alloc:
            cap = kw_map.get(a["keyword"], {}).get("max_daily_clicks", 0)
            if cap and a["optimized_clicks"] >= cap * 0.95:
                maxed_kws.append(a["keyword"].split("|")[0])

        optimal_orders = summary["total_expected_orders"]
        item["lp_summary"] = {
            "daily_budget":           daily_budget,
            "lp_optimal_spend":       summary["actual_spend"],
            "lp_optimal_orders":      optimal_orders,
            "actual_daily_orders":    round(actual_daily_orders, 2),
            "order_gap":              round(optimal_orders - actual_daily_orders, 2),
            "avg_effective_cpc":      summary["avg_effective_cpc"],
            "keywords_in_lp":         len(lp_input),
            "keywords_allocated":     len(alloc),
            "keywords_zeroed":        len(zero_kws),
            "keywords_maxed":         len(maxed_kws),
        }
        item["lp_top_allocations"] = alloc[:10]    # top 10 by clicks
        item["lp_zero_keywords"]   = zero_kws[:20] # pause candidates
        item["lp_maxed_keywords"]  = maxed_kws[:10]# raise-bid candidates

    return items


async def _enrich_xiyou_rankings(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch organic keyword traffic scores and ABA ranking from Xiyouzhaoci.
    Returns ad_traffic_ratio and top organic search terms.
    """
    if not ctx.config.get("enable_xiyou", True):
        return {}

    asin    = item.get("asin")
    country = ctx.config.get("country", "US")
    if not asin:
        return {}

    try:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
        api   = XiyouZhaociAPI(tenant_id=ctx.config.get("tenant_id", "default"))
        scores = api.get_traffic_scores(country=country, asins=[asin])

        if not scores.get("success") or not scores.get("data"):
            return {}

        entry = scores["data"][0] if scores["data"] else {}
        return {
            "ad_traffic_ratio":   entry.get("advertisingTrafficScoreRatio"),
            "organic_traffic_ratio": entry.get("naturalTrafficScoreRatio"),
            "traffic_growth_7d":  entry.get("totalTrafficScoreGrowthRate"),
            "xiyou_scores_raw":   entry,
        }
    except Exception as e:
        logger.warning(f"Xiyou traffic scores failed for {asin}: {e}")
        return {}


# ---------------------------------------------------------------------------
# Workflow builder
# ---------------------------------------------------------------------------

@WorkflowRegistry.register("ad_diagnosis")
def build_ad_diagnosis(config: dict) -> Workflow:
    """
    Build the ad diagnosis workflow.

    Each input item must contain at least {"asin": "B0XXXXXXXX"}.
    Optional per-item fields: sku, cogs, price, daily_sales.
    """
    steps = [
        # ── Stage 1: product & inventory context (parallel, independent) ──
        EnrichStep(
            name="fetch_catalog",
            extractor_fn=_enrich_catalog,
            parallel=True,
            concurrency=5,
        ),
        EnrichStep(
            name="fetch_inventory",
            extractor_fn=_enrich_inventory,
            parallel=True,
            concurrency=5,
        ),

        # ── Stage 2: campaign structure (fetch account-level once, filter per ASIN) ──
        EnrichStep(
            name="fetch_campaigns",
            extractor_fn=_enrich_campaigns,
            parallel=True,
            concurrency=5,
        ),

        # ── Stage 3: performance + keywords (depend on campaign_ids from stage 2) ──
        EnrichStep(
            name="fetch_performance",
            extractor_fn=_enrich_performance,
            parallel=True,
            concurrency=5,
        ),
        EnrichStep(
            name="fetch_keywords",
            extractor_fn=_enrich_keywords,
            parallel=True,
            concurrency=5,
        ),

        # ── Stage 4: keyword-level performance (spKeywords report) ──────────
        EnrichStep(
            name="fetch_keyword_performance",
            extractor_fn=_enrich_keyword_performance,
            parallel=True,
            concurrency=5,
        ),

        # ── Stage 5: LP budget optimisation (pure Python, OR-Tools) ──────────
        ProcessStep(
            name="optimize_budget",
            fn=_optimize_budget,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),

        # ── Stage 6: organic keyword rankings from Xiyouzhaoci ───────────────
        EnrichStep(
            name="fetch_xiyou_rankings",
            extractor_fn=_enrich_xiyou_rankings,
            parallel=True,
            concurrency=3,
            enabled=config.get("enable_xiyou", True),
        ),

        # ── Stage 7: LLM diagnostic synthesis ────────────────────────────────
        ProcessStep(
            name="ad_diagnosis_llm",
            prompt_template=(
                "You are an Amazon advertising specialist. Analyze the following advertising "
                "data for {count} ASIN(s) and produce a structured diagnostic report.\n\n"
                "Data snapshot (JSON):\n{items_json}\n\n"
                "For each ASIN, evaluate and diagnose:\n"
                "1. **Budget** — Is daily_budget sufficient? Is budget_likely_exhausted=True "
                "   indicating lost impressions?\n"
                "2. **Bids** — Are avg_bid levels competitive? Compare against high-ACOS keywords.\n"
                "3. **Bidding strategy** — Are bidding_strategies appropriate? "
                "   Are placement adjustments set?\n"
                "4. **Keywords** — Are keyword_count and match_type_dist healthy? "
                "   Flag keywords with ACOS above threshold in high_acos_campaigns.\n"
                "5. **Organic ranking** — Does ad_traffic_ratio suggest over-reliance on ads? "
                "   Is organic_traffic_ratio growing or declining?\n"
                "6. **Inventory** — Is inventory_risk=True? What is can_sell_days? "
                "   Will a stockout hurt ad performance?\n"
                "7. **ACOS & profitability** — Is account_acos acceptable? "
                "   If cogs and price are provided, calculate true profit after ad spend.\n"
                "8. **LP Budget Optimisation** — Use lp_summary to quantify budget efficiency:\n"
                "   - order_gap > 0 means the current allocation leaves orders on the table.\n"
                "   - lp_zero_keywords are statistically confirmed low-CVR keywords — recommend pausing.\n"
                "   - lp_maxed_keywords have hit their traffic ceiling — recommend raising bid or budget.\n"
                "   - lp_top_allocations shows where spend should be concentrated.\n\n"
                "Output format:\n"
                "- One section per ASIN with a severity rating: 🟢 Healthy / 🟡 Warning / 🔴 Critical\n"
                "- Bullet-point findings per dimension\n"
                "- A prioritised action list (top 5 actions, most impactful first, "
                "  referencing specific keyword names and dollar amounts where available)\n"
                "- An overall account health summary at the end"
            ),
            compute_target=ComputeTarget.CLOUD_LLM,
        ),
    ]

    return Workflow(name="ad_diagnosis", steps=steps)
