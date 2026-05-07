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
  - Bidding strategy       (AUTO vs MANUAL, placement adjustments, actual placement ACOS)
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
  rank_lookback_months    int   6        organic rank history depth (max 24 calendar months)
                                         covariate_series extends to match this window for ITS alignment
  enable_causal_analysis  bool  True     run ITS / CausalImpact / DML on change events
  causal_metric           str   "orders" metric to model: orders | acos | clicks | spend
  stock_gate_days         int   21       min effective stock days before spend-up actions are P0/P1
  inbound_lead_days       int   30       assumed transit days for inbound_shipped (30=sea, 10=domestic US)
"""

import asyncio
import functools
import hashlib
import io
import logging
import math
import os
import re
from datetime import datetime, timedelta, date as _date_cls
from zoneinfo import ZoneInfo
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch

from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow, WorkflowContext
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import ComputeTarget
from src.intelligence.processors.causal_analysis import (
    ATTR_PRE_START, ATTR_POST_END,
    YOY_OFFSET_DAYS, TRAILING_START,
)
from src.core.data_cache import data_cache as _data_cache

logger = logging.getLogger(__name__)

# ── Cache keys (shared across per-item enrichers to avoid duplicate API calls) ──
_KEY_CAMPAIGNS        = "ad_diag:campaigns"
_KEY_PERFORMANCE      = "ad_diag:performance"
_KEY_KEYWORDS         = "ad_diag:keywords"
_KEY_KW_PERFORMANCE   = "ad_diag:kw_performance"
_KEY_DAILY_PERF       = "ad_diag:daily_performance"
_KEY_CHANGE_HISTORY   = "ad_diag:change_history"
_KEY_PLACEMENT        = "ad_diag:placement"
_KEY_COVARIATES       = "ad_diag:covariates"
_KEY_YOY_PERF         = "ad_diag:yoy_perf"        # ERP YoY post-window (364d back)
_KEY_TRAILING_EXT     = "ad_diag:trailing_ext"     # ERP trailing 3M extension

# ── L2 cache helpers (DataCache-backed, multi-tenant safe) ──────────────────
# Key format: {tenant_id}:{store_id}:{part...}
# - tenant_id isolates different seller accounts (multi-user safety)
# - store_id isolates marketplaces (US / EU / JP)
# - extra parts carry data-type-specific discriminators (days, asin, ids_hash)
#
# DataCache auto-selects Redis (if REDIS_URL set) or JSON-file backend.
# L1 (ctx.cache) is always checked first — L2 is only hit on job start / resume.

_L2_DOMAIN = "ad_diag"
_TTL_STATIC = 3600    # campaigns, keywords — account config, stable within a session
_TTL_PERF   = 14400   # performance reports — fetched once per day range
_TTL_CHANGE = 1800    # change history — more volatile, shorter TTL
_TTL_YOY    = 86400   # YoY / trailing-ext ERP data — historical, rarely changes


def _l2_key(ctx: WorkflowContext, *parts) -> str:
    tid = ctx.tenant_id or "default"
    sid = ctx.config.get("store_id", "US")
    return ":".join(str(p) for p in (tid, sid) + parts)


def _l2_get(ctx: WorkflowContext, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx: WorkflowContext, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)


def _l2_cached(
    l1_key_fn:   Callable[..., str],
    l2_ttl:      int,
    l2_parts_fn: Callable[..., Tuple],
) -> Callable:
    """
    Two-level cache decorator for async _ensure_* fetchers.

    Lookup order: L1 (ctx.cache, in-process) → L2 (_data_cache, persistent)
    → decorated function (live API call).  The return value is written to both
    levels so subsequent calls in the same job and resumed jobs are cache hits.

    Parameters
    ----------
    l1_key_fn   : (ctx, *args, **kwargs) → str   — key for ctx.cache dict
    l2_ttl      : int                             — L2 TTL in seconds
    l2_parts_fn : (ctx, *args, **kwargs) → tuple  — variable-length parts
                  forwarded to _l2_get / _l2_set as positional *parts
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        async def wrapper(ctx: WorkflowContext, *args, **kwargs):
            l1_key = l1_key_fn(ctx, *args, **kwargs)
            if l1_key in ctx.cache:
                return ctx.cache[l1_key]
            l2_parts = l2_parts_fn(ctx, *args, **kwargs)
            hit = _l2_get(ctx, l2_ttl, *l2_parts)
            if hit is not None:
                ctx.cache[l1_key] = hit
                return hit
            value = await fn(ctx, *args, **kwargs)
            ctx.cache[l1_key] = value
            _l2_set(ctx, value, *l2_parts)
            return value
        return wrapper
    return decorator


def _ads_client(ctx: WorkflowContext):
    """Construct AmazonAdsClient from workflow config."""
    from src.mcp.servers.amazon.ads.client import AmazonAdsClient
    return AmazonAdsClient(
        store_id=ctx.config.get("store_id"),
        region=ctx.config.get("region", "NA"),
    )


def _campaign_ids_hash(campaign_ids: List[str]) -> str:
    return hashlib.md5(",".join(sorted(campaign_ids)).encode()).hexdigest()[:12]


# Noise thresholds — changes smaller than these are low-signal
_NOISE_BID_PCT    = 0.05  # bid changes < 5% → low_weight
_NOISE_BUDGET_PCT = 0.10  # budget changes < 10% → low_weight

# Attribution priority: higher = analysed first in the LLM prompt
_CHANGE_PRIORITY: Dict[str, int] = {
    "SMART_BIDDING_STRATEGY": 6,
    "PLACEMENT_GROUP":        5,
    "BID_AMOUNT":             4,
    "BUDGET_AMOUNT":          3,
    "STATUS":                 2,
    "IN_BUDGET":              1,  # auto-generated by Amazon, not a manual action
}


def _store_tz(ctx: WorkflowContext) -> ZoneInfo:
    """
    Return the store's reporting timezone.

    Three data sources must share the same calendar date definition:
      - Amazon Ads change history  : UTC epoch-ms → must be converted here
      - Amazon Ads performance report dates : already in this timezone
      - Xiyouzhaoci localDate      : ISO with tz offset (e.g. -08:00/-07:00 Pacific)

    Config key "timezone" defaults to "America/Los_Angeles" (US stores).
    Other common values: "America/New_York", "Europe/London", "Asia/Tokyo".
    """
    return ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))


# ---------------------------------------------------------------------------
# Shared account-level fetchers (fetch once, cached in ctx.cache)
# ---------------------------------------------------------------------------

@_l2_cached(
    l1_key_fn   = lambda ctx: _KEY_CAMPAIGNS,
    l2_ttl      = _TTL_STATIC,
    l2_parts_fn = lambda ctx: ("campaigns",),
)
async def _ensure_campaigns(ctx: WorkflowContext) -> List[Dict]:
    campaigns = await _ads_client(ctx).list_campaigns(states=["ENABLED", "PAUSED"], max_results=2000)
    logger.info(f"Fetched {len(campaigns)} campaigns from Ads API")
    return campaigns


@_l2_cached(
    l1_key_fn   = lambda ctx: _KEY_PERFORMANCE,
    l2_ttl      = _TTL_PERF,
    l2_parts_fn = lambda ctx: ("sp_performance", ctx.config.get("days", 30)),
)
async def _ensure_performance(ctx: WorkflowContext) -> List[Dict]:
    records = await _ads_client(ctx).get_performance_report(
        report_type="spCampaigns", days=ctx.config.get("days", 30)
    )
    logger.info(f"Fetched {len(records)} campaign performance records")
    return records


@_l2_cached(
    l1_key_fn   = lambda ctx: _KEY_KW_PERFORMANCE,
    l2_ttl      = _TTL_PERF,
    l2_parts_fn = lambda ctx: ("kw_performance", ctx.config.get("days", 30)),
)
async def _ensure_keyword_performance(ctx: WorkflowContext) -> List[Dict]:
    """spSearchTerm groups by search term but includes keywordText+matchType for keyword-level aggregation."""
    records = await _ads_client(ctx).get_performance_report(
        report_type="spSearchTerm", days=ctx.config.get("days", 30)
    )
    logger.info(f"Fetched {len(records)} keyword performance records")
    return records


@_l2_cached(
    l1_key_fn   = lambda ctx, campaign_ids: f"{_KEY_KEYWORDS}:{','.join(sorted(campaign_ids))}",
    l2_ttl      = _TTL_STATIC,
    l2_parts_fn = lambda ctx, campaign_ids: ("keywords", _campaign_ids_hash(campaign_ids)),
)
async def _ensure_keywords(ctx: WorkflowContext, campaign_ids: List[str]) -> List[Dict]:
    """Fetch keywords for a set of campaign_ids, cached by sorted id-tuple."""
    return await _ads_client(ctx).list_keywords(
        campaign_ids=campaign_ids, states=["ENABLED", "PAUSED"]
    )


_ADVERT_PROD_MAX_DAYS = 31  # spAdvertisedProduct API window limit


@_l2_cached(
    l1_key_fn   = lambda ctx, asin: f"{_KEY_DAILY_PERF}:{asin}",
    l2_ttl      = _TTL_PERF,
    l2_parts_fn = lambda ctx, asin: ("daily_perf", asin, ctx.config.get("days", 30)),
)
async def _ensure_daily_performance(ctx: WorkflowContext, asin: str) -> List[Dict]:
    """
    Fetch spAdvertisedProduct daily report for the target ASIN.

    Issues multiple 31-day chunks backwards from yesterday (API window limit)
    and filters client-side to advertised_asin == asin.
    Cache key is per-ASIN so concurrent items don't collide.
    """
    client      = _ads_client(ctx)
    days        = ctx.config.get("days", 30)
    days_needed = days + abs(ATTR_PRE_START) + abs(ATTR_POST_END) + 1
    today       = datetime.utcnow().date()
    chunk_end   = today - timedelta(days=1)
    remaining   = days_needed
    all_raw: List[Dict] = []

    while remaining > 0:
        chunk       = min(remaining, _ADVERT_PROD_MAX_DAYS)
        chunk_start = chunk_end - timedelta(days=chunk - 1)
        batch = await client.get_performance_report(
            report_type="spAdvertisedProduct",
            start_date=str(chunk_start),
            end_date=str(chunk_end),
            time_unit="DAILY",
        )
        all_raw.extend(batch)
        chunk_end  = chunk_start - timedelta(days=1)
        remaining -= chunk

    records = [r for r in all_raw if r.get("advertised_asin") == asin]
    chunks  = -(-days_needed // _ADVERT_PROD_MAX_DAYS)
    logger.info(f"[daily_perf] {asin}: {len(records)}/{len(all_raw)} records ({days_needed}d, {chunks} chunk(s))")
    return records


@_l2_cached(
    l1_key_fn   = lambda ctx: _KEY_PLACEMENT,
    l2_ttl      = _TTL_PERF,
    l2_parts_fn = lambda ctx: ("placement", ctx.config.get("days", 30)),
)
async def _ensure_placement_performance(ctx: WorkflowContext) -> List[Dict]:
    """Fetch account-wide spCampaignsPlacement report, cached once per run."""
    records = await _ads_client(ctx).get_performance_report(
        report_type="spCampaignsPlacement", days=ctx.config.get("days", 30)
    )
    logger.info(f"Fetched {len(records)} placement performance records")
    return records


@_l2_cached(
    l1_key_fn   = lambda ctx: _KEY_CHANGE_HISTORY,
    l2_ttl      = _TTL_CHANGE,
    l2_parts_fn = lambda ctx: ("change_history", ctx.config.get("days", 30)),
)
async def _ensure_change_history(ctx: WorkflowContext) -> List[Dict]:
    """Fetch change history for the lookback window + attribution tail, cached once."""
    client   = _ads_client(ctx)
    days     = ctx.config.get("days", 30)
    tz       = _store_tz(ctx)
    today    = datetime.now(tz=tz).date()
    # Extend window by ATTR_POST_END days so attribution tail is covered.
    # Boundaries must be in store tz so the API returns events for the correct
    # local calendar days (e.g. an event at 23:30 PST would be next UTC day).
    end_dt   = today - timedelta(days=1)
    start_dt = today - timedelta(days=days + abs(ATTR_POST_END))
    to_ms    = int(datetime(end_dt.year,   end_dt.month,   end_dt.day,   23, 59, 59, tzinfo=tz).timestamp() * 1000)
    from_ms  = int(datetime(start_dt.year, start_dt.month, start_dt.day,  0,  0,  0, tzinfo=tz).timestamp() * 1000)
    # No campaign_ids: API only supports useProfileIdAdvertiser:true (profile-wide).
    # Per-ASIN filtering happens client-side in _enrich_change_history.
    result = await client.get_change_history(
        from_date=from_ms, to_date=to_ms, count=200, sort_direction="DESC",
    )
    events = result.get("events", [])
    logger.info(f"Fetched {len(events)} change history events")
    return events


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
        total_available    = sum(r.get("available_quantity", 0)  for r in matched)
        inbound_receiving  = sum(r.get("inbound_receiving",  0)  for r in matched)
        inbound_shipped    = sum(r.get("inbound_shipped",    0)  for r in matched)
        inbound_working    = sum(r.get("inbound_working",    0)  for r in matched)
        total_inbound      = inbound_receiving + inbound_shipped  # confirmed in-transit only
        # Estimate can-sell days using item daily sales if provided
        daily_sales = item.get("daily_sales") or 0
        can_sell_days = (
            round(total_available / daily_sales) if daily_sales > 0 else None
        )
        return {
            "inventory_records":  matched,
            "total_available":    total_available,
            "inbound_receiving":  inbound_receiving,
            "inbound_shipped":    inbound_shipped,
            "inbound_working":    inbound_working,
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


async def _enrich_order_metrics(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch total units ordered via SP-API getOrderMetrics for this ASIN.

    Runs AFTER _enrich_inventory so total_available is already in item.
    Uses real (organic + ad) unit sales to compute daily_sales and can_sell_days —
    replacing the ad-orders-only fallback in _enrich_performance.

    TTL: 4h — order data refreshes approximately once per day but we re-check
    frequently in case of intraday corrections.
    """
    from src.mcp.servers.amazon.sp_api.client import SPAPIClient
    asin = item.get("asin")
    if not asin:
        return {}

    days = ctx.config.get("days", 30)
    cached = _l2_get(ctx, 14400, "order_metrics", asin, days)  # 4h TTL
    if cached is not None:
        return cached

    try:
        tz       = _store_tz(ctx)
        today    = datetime.now(tz=tz).date()
        end_dt   = today - timedelta(days=1)
        start_dt = today - timedelta(days=days)

        client  = SPAPIClient(store_id=ctx.config.get("store_id"))
        metrics = await client.get_order_metrics(
            asin=asin,
            start_date=start_dt.isoformat(),
            end_date=end_dt.isoformat(),
            granularity="Total",
            granularity_timezone=ctx.config.get("timezone", "America/Los_Angeles"),
        )

        total_units = sum((m.get("unitCount") or 0) for m in metrics)
        if total_units <= 0:
            result: Dict = {}
        else:
            daily_sales     = round(total_units / days, 2)
            total_available = item.get("total_available") or 0
            result = {
                "daily_sales":        daily_sales,
                "daily_sales_source": "order_metrics",
                "total_units_ordered": total_units,
            }
            if total_available > 0:
                can_sell_days = round(total_available / daily_sales)
                result["can_sell_days"]   = can_sell_days
                result["inventory_risk"]  = (
                    can_sell_days < ctx.config.get("inventory_risk_days", 30)
                )
                logger.info(
                    f"[order_metrics] {asin}: units={total_units}/{days}d "
                    f"→ daily_sales={daily_sales}, can_sell_days={can_sell_days}"
                )

        _l2_set(ctx, result, "order_metrics", asin, days)
        return result

    except Exception as e:
        logger.warning(f"Order metrics fetch failed for {asin}: {e}")
        return {}


async def _enrich_campaigns(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Match account campaigns to this ASIN using three strategies in priority order:

    1. Explicit campaign_ids in config (most reliable — set by caller).
    2. spAdvertisedProduct records — campaigns that actually delivered ads for this ASIN.
       Ground truth: no naming convention assumed.
    3. Name substring fallback (ASIN in campaign name) with a warning.

    Returns empty campaign list only when all three strategies fail.
    """
    asin         = item.get("asin", "").upper()
    all_campaigns = await _ensure_campaigns(ctx)
    camp_by_id   = {str(c["campaign_id"]): c for c in all_campaigns}

    # Strategy 1: explicit campaign_ids from caller config
    explicit_ids = [str(i) for i in (ctx.config.get("campaign_ids") or []) if i]
    if explicit_ids:
        matched = [camp_by_id[cid] for cid in explicit_ids if cid in camp_by_id]
        if matched:
            logger.info(f"[enrich_campaigns] {asin}: matched {len(matched)} campaigns via explicit config ids")
            return _build_campaign_result(matched, "explicit_config")

    # Strategy 2: spAdvertisedProduct — campaigns that actually ran ads for this ASIN
    try:
        adv_records = await _ensure_daily_performance(ctx, asin)
        adv_cids = {str(r["campaign_id"]) for r in adv_records if r.get("campaign_id")}
        if adv_cids:
            matched = [camp_by_id[cid] for cid in adv_cids if cid in camp_by_id]
            if matched:
                logger.info(
                    f"[enrich_campaigns] {asin}: matched {len(matched)} campaigns via "
                    f"spAdvertisedProduct ({len(adv_cids)} unique campaign_ids in report)"
                )
                return _build_campaign_result(matched, "spAdvertisedProduct")
    except Exception as e:
        logger.warning(f"[enrich_campaigns] {asin}: spAdvertisedProduct lookup failed: {e}")

    # Strategy 3: name substring fallback
    matched = [c for c in all_campaigns if asin in (c.get("name") or "").upper()]
    if matched:
        logger.warning(
            f"[enrich_campaigns] {asin}: fell back to name-substring match "
            f"({len(matched)} campaigns). Consider fixing campaign names or passing campaign_ids."
        )
        return _build_campaign_result(matched, "name_substring")

    logger.warning(
        f"[enrich_campaigns] {asin}: no campaigns matched via any strategy. "
        f"Pass campaign_ids in config or ensure spAdvertisedProduct has delivery data."
    )
    return {"campaigns": [], "campaign_ids": [], "total_daily_budget": 0,
            "bidding_strategies": [], "campaign_match_strategy": "none"}


def _build_campaign_result(matched: List[Dict], strategy: str) -> Dict:
    campaign_ids = [str(c["campaign_id"]) for c in matched]
    total_daily_budget = sum(
        c.get("daily_budget") or 0 for c in matched if c.get("state") == "ENABLED"
    )
    strategies = list({c.get("bidding_strategy") for c in matched if c.get("bidding_strategy")})
    return {
        "campaigns":               matched,
        "campaign_ids":            campaign_ids,
        "total_daily_budget":      total_daily_budget,
        "bidding_strategies":      strategies,
        "campaign_match_strategy": strategy,
    }


def _wilson_ci(k: int, n: int, z: float = 1.96):
    """Wilson score 95% CI for a binomial proportion (k successes in n trials)."""
    if n <= 0 or k < 0:
        return None, None
    p = k / n
    denom  = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, center - margin), min(1.0, center + margin)


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

    # Statistical sufficiency
    if total_orders >= 100:  orders_reliability = "high"
    elif total_orders >= 30: orders_reliability = "medium"
    else:                    orders_reliability = "low"

    # CVR 95% Wilson CI → propagate to ACOS CI
    cvr_point = round(total_orders / total_clicks, 6) if total_clicks > 0 else None
    _cvr_lo, _cvr_hi = _wilson_ci(total_orders, total_clicks)
    acos_ci_lo = acos_ci_hi = None
    if account_acos and cvr_point and _cvr_hi and _cvr_lo:
        frac = account_acos / 100  # ACOS as fraction: spend/sales
        # Higher CVR → more orders → lower ACOS (inverse relationship)
        if _cvr_hi > 0:
            acos_ci_lo = round(frac * cvr_point / _cvr_hi * 100, 2)
        if _cvr_lo > 0:
            acos_ci_hi = round(frac * cvr_point / _cvr_lo * 100, 2)

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

    # Backfill can_sell_days: fetch_inventory runs in Stage 1 before performance
    # data is available, so daily_sales is unknown at that point.  Derive it here
    # from ad orders / days (a conservative lower bound — excludes organic orders).
    result: Dict = {
        "performance_records":    matched,
        "total_spend":            total_spend,
        "total_sales":            total_sales,
        "total_orders":           total_orders,
        "total_clicks":           total_clicks,
        "account_acos":           account_acos,
        "orders_reliability":     orders_reliability,
        "cvr_point":              cvr_point,
        "cvr_ci_lo":              round(_cvr_lo, 6) if _cvr_lo is not None else None,
        "cvr_ci_hi":              round(_cvr_hi, 6) if _cvr_hi is not None else None,
        "acos_ci_lo":             acos_ci_lo,
        "acos_ci_hi":             acos_ci_hi,
        "high_acos_campaigns":    high_acos_campaigns,
        "budget_exhaustion_pct":  budget_exhaustion_pct,
        "budget_likely_exhausted": (
            budget_exhaustion_pct is not None
            and budget_exhaustion_pct > budget_pct_threshold
        ),
    }
    # Last-resort can_sell_days backfill: only if neither inventory (daily_sales
    # supplied by caller) nor order_metrics (preferred) set can_sell_days already.
    if item.get("can_sell_days") is None and item.get("daily_sales_source") != "order_metrics" and total_orders > 0:
        # Ad orders are only a fraction of total sales; this is a LOWER BOUND.
        # Do NOT use for inventory risk decisions — flag clearly for the LLM.
        daily_sales_ad = total_orders / days
        total_available = item.get("total_available") or 0
        if total_available > 0:
            can_sell_days = round(total_available / daily_sales_ad)
            result["daily_sales"]        = round(daily_sales_ad, 2)
            result["daily_sales_source"] = "ad_orders_only"
            result["can_sell_days"]      = can_sell_days
            result["can_sell_days_note"] = (
                "upper_bound — derived from ad-attributed orders only (organic excluded); "
                "true daily unit consumption (ad + organic) is higher, "
                "so actual stockout will occur SOONER than can_sell_days suggests"
            )
            result["inventory_risk"] = can_sell_days < ctx.config.get("inventory_risk_days", 30)
            logger.info(
                f"[backfill can_sell_days] {item.get('asin')}: "
                f"ad_orders={total_orders}/{days}d → daily_sales_ad≈{daily_sales_ad:.1f} "
                f"(lower bound), available={total_available}, can_sell_days≈{can_sell_days}"
            )
    return result


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
            agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0, "sales": 0}
        agg[key]["spend"]       += r.get("spend", 0) or 0
        agg[key]["clicks"]      += r.get("clicks", 0) or 0
        agg[key]["orders"]      += r.get("orders", 0) or 0
        agg[key]["impressions"] += r.get("impressions", 0) or 0
        agg[key]["sales"]       += r.get("sales", 0) or 0

    kw_performance = []
    for (kw_text, match_type), v in agg.items():
        clicks = v["clicks"]
        if clicks < min_clicks:
            continue
        avg_cpc      = round(v["spend"] / clicks, 4)
        cvr          = round(v["orders"] / clicks, 4)
        daily_clicks = round(clicks / days, 2)
        # ACOS = ad spend / attributed sales revenue (not spend/orders which gives cost/order)
        acos = round(v["spend"] / v["sales"] * 100, 2) if v["sales"] > 0 else None
        kw_performance.append({
            "keyword_text":  kw_text,
            "match_type":    match_type,
            "total_spend":   round(v["spend"], 2),
            "total_sales":   round(v["sales"], 2),
            "total_clicks":  clicks,
            "total_orders":  v["orders"],
            "impressions":   v["impressions"],
            "avg_cpc":       avg_cpc,
            "cvr":           cvr,
            "daily_clicks":  daily_clicks,
            "acos":          acos,
        })

    # Sort by spend descending (most important keywords first)
    kw_performance.sort(key=lambda x: x["total_spend"], reverse=True)

    return {"keyword_performance": kw_performance}


async def _enrich_placement(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Aggregate spCampaignsPlacement report to per-placement spend/ACOS for this
    ASIN's campaigns.  Compares actual placement performance to configured bid
    adjustments so the LLM can recommend raising/lowering placement modifiers.
    """
    campaign_ids = set(item.get("campaign_ids", []))
    all_records  = await _ensure_placement_performance(ctx)

    # Filter to this ASIN's campaigns
    relevant = [
        r for r in all_records
        if str(r.get("campaign_id")) in campaign_ids
    ] if campaign_ids else all_records

    # Aggregate by placement label
    agg: Dict[str, Dict] = {}
    for r in relevant:
        p = r.get("placement") or "UNKNOWN"
        slot = agg.setdefault(p, {"spend": 0.0, "sales": 0.0, "clicks": 0, "impressions": 0, "orders": 0})
        slot["spend"]       += r.get("spend") or r.get("cost") or 0
        slot["sales"]       += r.get("sales") or 0
        slot["clicks"]      += r.get("clicks") or 0
        slot["impressions"] += r.get("impressions") or 0
        slot["orders"]      += r.get("orders") or 0

    placement_performance: Dict[str, Dict] = {}
    total_spend = sum(v["spend"] for v in agg.values()) or 1  # avoid /0
    for p, m in agg.items():
        spend = m["spend"]
        sales = m["sales"]
        placement_performance[p] = {
            "spend":         round(spend, 2),
            "sales":         round(sales, 2),
            "clicks":        m["clicks"],
            "impressions":   m["impressions"],
            "orders":        m["orders"],
            "acos":          round(spend / sales * 100, 2) if sales > 0 else None,
            "spend_share":   round(spend / total_spend * 100, 2),
            "ctr":           round(m["clicks"] / m["impressions"] * 100, 4) if m["impressions"] > 0 else None,
        }

    # Attach configured bid adjustments from campaign data for easy comparison
    campaigns = item.get("campaigns", [])
    configured_adjustments: Dict[str, Optional[float]] = {}
    for c in campaigns:
        for key, label in [
            ("placement_top_of_search_pct", "PLACEMENT_TOP_OF_SEARCH"),
            ("placement_product_page_pct",  "PLACEMENT_PRODUCT_PAGE"),
        ]:
            pct = c.get(key)
            if pct is not None:
                existing = configured_adjustments.get(label)
                configured_adjustments[label] = max(existing or 0, pct)

    return {
        "placement_performance":      placement_performance,
        "placement_configured_pcts":  configured_adjustments,
    }


async def _enrich_change_history(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Filter change history to this ASIN's campaigns, apply noise filter,
    detect compound changes (multiple dimensions within 48h on same campaign).

    _ensure_daily_performance is gathered concurrently here intentionally:
    _correlate_changes is a synchronous ProcessStep that reads daily_perf from
    ctx.cache. It runs AFTER all EnrichStep items complete, so the cache must
    be populated before then. Fetching it here (in parallel with change_history)
    ensures it is warmed — do NOT remove this gather without moving the fetch elsewhere.
    """
    asin         = item.get("asin", "").upper()
    campaign_ids = set(item.get("campaign_ids") or [])
    all_events, _ = await asyncio.gather(
        _ensure_change_history(ctx),
        _ensure_daily_performance(ctx, asin),
    )

    relevant = []
    for ev in all_events:
        # Campaign ID: prefer metadata.campaignId (AD_GROUP/KEYWORD events),
        # fall back to entityId for CAMPAIGN-level events.
        meta = ev.get("metadata") or {}
        cid  = str(meta.get("campaignId") or ev.get("entityId") or "")

        # Filter to this ASIN's campaigns since we now fetch profile-wide
        if campaign_ids and cid not in campaign_ids:
            continue

        # Noise filter: IN_BUDGET is auto-generated (not a human action); tiny tweaks are low-signal
        change_type = ev.get("changeType", "")

        # CREATED events record the initial state at campaign/keyword creation —
        # they are not actionable changes and add noise to attribution analysis.
        if change_type == "CREATED":
            continue

        old_val = ev.get("previousValue")
        new_val = ev.get("newValue")
        is_low_weight = change_type == "IN_BUDGET"  # always low_weight — Amazon auto-event
        if not is_low_weight and change_type in ("BID_AMOUNT", "BUDGET_AMOUNT") and old_val and new_val:
            try:
                old_f, new_f = float(old_val), float(new_val)
                if old_f > 0:
                    threshold = _NOISE_BID_PCT if change_type == "BID_AMOUNT" else _NOISE_BUDGET_PCT
                    if abs(new_f - old_f) / old_f < threshold:
                        is_low_weight = True
            except (TypeError, ValueError):
                pass

        relevant.append({
            "event_id":     ev.get("eventId"),
            "campaign_id":  cid,
            "entity_type":  ev.get("entityType"),
            "entity_id":    ev.get("entityId"),
            "change_type":  change_type,
            "old_value":    old_val,
            "new_value":    new_val,
            "changed_at":   ev.get("changedAt") or ev.get("timestamp"),
            "priority":     _CHANGE_PRIORITY.get(change_type, 0),
            "low_weight":   is_low_weight,
            "keyword":      meta.get("keyword"),
            "keyword_type": meta.get("keywordType"),
            "ad_group_id":  meta.get("adGroupId"),
        })

    # Compound change detection: same campaign, >1 dimension within 48h
    compound_flag: Dict[str, bool] = {}
    for ev in relevant:
        cid = ev["campaign_id"]
        ts  = ev.get("changed_at")
        if not ts:
            continue
        try:
            t0 = int(ts) / 1000  # epoch ms → seconds
        except (TypeError, ValueError):
            continue
        siblings = [
            e for e in relevant
            if e["campaign_id"] == cid
            and e["change_type"] != ev["change_type"]
            and e.get("changed_at")
            and abs(int(e["changed_at"]) / 1000 - t0) <= 48 * 3600
        ]
        if siblings:
            compound_flag[cid] = True

    for ev in relevant:
        ev["compound_change"] = compound_flag.get(ev["campaign_id"], False)

    # Surface only meaningful (non-low-weight) changes, up to 50
    notable = [e for e in relevant if not e["low_weight"]]
    return {
        "change_events":       notable[:50],
        "change_event_count":  len(notable),
        "has_compound_change": any(e["compound_change"] for e in notable),
    }


def _compute_placement_multiplier(placement_perf: Dict, placement_mods: Dict) -> float:
    total_pl_spend = sum(v.get("spend", 0) for v in placement_perf.values()) or 1.0
    result = 0.0
    for pl_key, pl_data in placement_perf.items():
        share    = pl_data.get("spend", 0) / total_pl_spend
        modifier = placement_mods.get(pl_key, 0) / 100.0
        result  += share * (1.0 + modifier)
    return result if result >= 0.5 else 1.0


def _build_kw_to_campaign_map(raw_kw_perf: List[Dict], campaign_ids: set) -> Dict[tuple, str]:
    kw_clicks_by_camp: Dict[tuple, Dict[str, int]] = {}
    for r in raw_kw_perf:
        cid = str(r.get("campaign_id", ""))
        if cid not in campaign_ids:
            continue
        key = (r.get("keyword_text", ""), r.get("match_type", ""))
        kw_clicks_by_camp.setdefault(key, {})
        kw_clicks_by_camp[key][cid] = (
            kw_clicks_by_camp[key].get(cid, 0) + int(r.get("clicks", 0) or 0)
        )
    return {key: max(cc, key=cc.get) for key, cc in kw_clicks_by_camp.items()}


def _p90_headroom(daily_perf: List[Dict], fallback: float) -> float:
    """
    Derive a data-driven click-ceiling multiplier from ASIN-level daily_perf.

    p90_ratio = p90(daily_clicks) / mean(daily_clicks) — measures how far
    the 90th-percentile day sits above the average.  Multiplied by 1.5 to give
    the LP room to model aggressive-bid scenarios beyond the historical peak.

    Capped at fallback (default 3.0) to prevent outlier-driven over-allocation.
    Requires ≥7 days of data; falls back to `fallback` otherwise.
    """
    clicks = [float(d.get("clicks") or 0) for d in daily_perf if d.get("clicks")]
    if len(clicks) < 7:
        return fallback
    avg = sum(clicks) / len(clicks)
    if avg <= 0:
        return fallback
    p90 = sorted(clicks)[int(len(clicks) * 0.9)]
    return min(round(p90 / avg * 1.5, 2), fallback)


def _build_lp_input(
    kw_perf: List[Dict],
    kw_to_campaign: Dict[tuple, str],
    camp_meta: Dict[str, Dict],
    brand_kws: set,
    headroom: float,
    placement_multiplier: float,
    daily_perf: Optional[List[Dict]] = None,
) -> List[Dict]:
    click_headroom = _p90_headroom(daily_perf or [], headroom)
    lp_input: List[Dict] = []
    for kw in kw_perf:
        if not kw.get("avg_cpc") or not kw.get("cvr"):
            continue
        kw_text    = kw["keyword_text"]
        match_type = kw["match_type"]
        cid        = kw_to_campaign.get((kw_text, match_type), "")
        strategy   = camp_meta.get(cid, {}).get("bidding_strategy", "")
        is_brand   = kw_text.lower() in {b.lower() for b in brand_kws}
        max_daily  = max(round(kw["daily_clicks"] * click_headroom, 1), 1.0)
        min_daily  = round(kw["daily_clicks"] * 0.3, 1) if is_brand else 0.0
        lp_input.append({
            "name":                f"{kw_text}|{match_type}",
            "avg_cpc":             kw["avg_cpc"],
            "estimated_cvr":       kw["cvr"],
            "sample_clicks":       kw.get("total_clicks", 0),
            "max_daily_clicks":    max_daily,
            "min_daily_clicks":    min_daily,
            "campaign_id":         cid,
            "bidding_strategy":    strategy,
            "placement_multiplier": placement_multiplier,
        })
    return lp_input


def _classify_lp_keywords(
    kw_perf: List[Dict], alloc: List[Dict], kw_map: Dict[str, Dict]
) -> Tuple[List[str], List[str]]:
    alloc_names = {a["keyword"] for a in alloc}
    seen_zero: set = set()
    zero_kws: List[str] = []
    for kw in kw_perf:
        composed = f"{kw['keyword_text']}|{kw['match_type']}"
        if composed not in alloc_names and kw.get("avg_cpc") and composed not in seen_zero:
            seen_zero.add(composed)
            zero_kws.append(f"{kw['keyword_text']} ({kw['match_type']})")
    seen_maxed: set = set()
    maxed_kws: List[str] = []
    for a in alloc:
        cap = kw_map.get(a["keyword"], {}).get("max_daily_clicks", 0)
        if cap and a["optimized_clicks"] >= cap * 0.95 and a["keyword"] not in seen_maxed:
            seen_maxed.add(a["keyword"])
            parts = a["keyword"].split("|")
            maxed_kws.append(f"{parts[0]} ({parts[1]})" if len(parts) > 1 else parts[0])
    return zero_kws, maxed_kws


def _build_lp_kw_id_map(ctx: WorkflowContext, campaign_ids: set) -> Dict[tuple, Dict]:
    kw_cache_key = f"{_KEY_KEYWORDS}:{','.join(sorted(campaign_ids))}"
    kw_id_map: Dict[tuple, Dict] = {}
    for k in ctx.cache.get(kw_cache_key, []):
        key3 = (
            (k.get("keyword_text") or "").lower(),
            (k.get("match_type") or "").upper(),
            str(k.get("campaign_id") or ""),
        )
        kw_id_map[key3] = k
    return kw_id_map


_CAMP_SPEND_UP = {"increase_budget", "enable_and_increase_budget", "enable_and_review_bids"}


def _build_campaign_actions(
    camp_meta: Dict[str, Dict],
    camp_spend: Dict[str, float],
    performance_records: List[Dict],
    days: int,
    target_acos: Optional[float],
    inv_gate: Optional[Dict] = None,
    order_gap: float = 0.0,
    spend_ceiling_bound: bool = False,
    budget_binding: bool = False,
    lp_scoped_cids: Optional[set] = None,
) -> List[Dict]:
    camp_actual_spend: Dict[str, float] = {}
    for r in performance_records:
        cid = str(r.get("campaign_id", ""))
        camp_actual_spend[cid] = camp_actual_spend.get(cid, 0.0) + float(r.get("spend", 0) or 0)

    # Campaigns that contributed keyword data to the LP; auto/PT campaigns won't appear here.
    _lp_scoped = lp_scoped_cids or set()

    actions: List[Dict] = []
    for cid, meta in camp_meta.items():
        camp_budget  = float(meta.get("daily_budget") or 0)
        lp_spend     = camp_spend.get(cid, 0.0)
        actual_spend = camp_actual_spend.get(cid, 0.0) / days
        camp_state   = (meta.get("state") or "").upper()
        is_paused    = camp_state == "PAUSED"

        if camp_budget <= 0:
            continue

        budget_util  = round(actual_spend / camp_budget, 3)
        lp_saturated = lp_spend >= camp_budget * 0.90
        in_lp_scope  = bool(_lp_scoped) and (cid in _lp_scoped)

        camp_perf         = [r for r in performance_records if str(r.get("campaign_id")) == cid]
        camp_sales        = sum(float(r.get("sales", 0) or 0) for r in camp_perf)
        camp_spend_total  = sum(float(r.get("spend", 0) or 0) for r in camp_perf)
        camp_acos         = round(camp_spend_total / camp_sales * 100, 1) if camp_sales > 0 else None
        camp_orders_total = sum(float(r.get("orders", 0) or 0) for r in camp_perf)
        camp_daily_orders = camp_orders_total / days if days > 0 else 0.0
        camp_cpo          = round(camp_spend_total / camp_orders_total, 2) if camp_orders_total > 0 else None

        target_acos_pct = (target_acos or 0.35) * 100
        suggested = None

        # ── Auto / Product-Targeting campaigns: LP allocates $0 (not in scope) ──
        # Applying LP-allocation thresholds to these campaigns produces false
        # pause/archive recommendations.  Evaluate them on ACOS alone.
        if not in_lp_scope and camp_spend_total > 0:
            if camp_acos is None:
                # Spending but zero orders → conversion failure
                action, priority = "pause_candidate", "P1"
                rationale = (
                    f"No conversions in period (spend ${camp_spend_total:.0f} total, 0 orders) — "
                    f"add negative targets or pause (auto/PT, outside LP scope)"
                )
            elif camp_acos > target_acos_pct * 1.3:
                suggested = round(camp_budget * 0.75, 0)
                action, priority = "decrease_budget", "P0"
                rationale = (
                    f"ACOS {camp_acos}% > 130% of target {target_acos_pct:.0f}% — "
                    f"reduce budget and add negative keywords/targets (auto/PT, outside LP scope)"
                )
            elif camp_acos > target_acos_pct:
                action, priority = "review_bids", "P1"
                rationale = (
                    f"ACOS {camp_acos}% above target {target_acos_pct:.0f}% — "
                    f"lower bids or add negatives (auto/PT, outside LP scope)"
                )
            elif budget_util >= 0.9:
                suggested = round(camp_budget * 1.2, 0)
                action, priority = "increase_budget", "P0"
                rationale = (
                    f"ACOS {camp_acos}% ≤ target, budget util {budget_util:.0%} — "
                    f"safe to scale (auto/PT, outside LP scope)"
                )
            else:
                action, priority = "maintain", "P2"
                rationale = (
                    f"ACOS {camp_acos}% ≤ target, {budget_util:.0%} utilisation — "
                    f"healthy (auto/PT, outside LP scope)"
                )
            if is_paused:
                if action == "increase_budget":
                    action = "enable_and_increase_budget"
                elif action in ("maintain", "review_bids"):
                    action = "enable_and_review_bids"
                elif action in ("decrease_budget", "pause_candidate"):
                    # Campaign is already PAUSED — decreasing budget or pausing again
                    # has no effect. Convert to maintain so no spend-reducing action
                    # is surfaced for an already-stopped campaign.
                    action, priority = "maintain", "P2"
                    rationale = f"Already PAUSED — {rationale}. No further action while paused."
        elif not in_lp_scope:
            # No spend at all: inactive campaign outside LP scope — low priority
            action, priority = "maintain", "P2"
            rationale = "No spend recorded in period (auto/PT, outside LP scope)"
        elif is_paused and lp_spend >= camp_budget * 0.10:
            action, priority = "enable_and_review_bids", "P1"
            rationale = f"Campaign is PAUSED; LP projects ${lp_spend:.0f}/day potential — evaluate re-enabling after bid review"
        elif lp_spend < camp_budget * 0.10:
            if is_paused:
                action, priority = "archive_candidate", "P2"
                rationale = f"Campaign is PAUSED and LP allocates only ${lp_spend:.0f}/day — consider archiving"
            elif camp_acos is not None and camp_acos <= target_acos_pct:
                # LP has no click data for this campaign's keywords (filtered out),
                # but historical ACOS is efficient — do not pause, flag for bid review.
                action, priority = "review_bids", "P1"
                rationale = (
                    f"LP allocates only ${lp_spend:.0f}/day (keywords lack click data for LP model), "
                    f"but campaign ACOS {camp_acos}% ≤ target {target_acos_pct:.0f}% — "
                    f"review bids to improve data coverage before pausing"
                )
            else:
                action, priority = "pause_candidate", "P1"
                rationale = f"LP allocates only ${lp_spend:.0f}/day (< 10% of ${camp_budget:.0f} budget) — keywords inefficient"
        elif lp_saturated and (camp_acos is None or camp_acos <= target_acos_pct):
            suggested = round(min(lp_spend * 1.15, camp_budget * 1.5), 0)
            # spend_ceiling_bound=True: click ceilings are the real cap — raise bids/expand keywords.
            # order_gap < 0 with budget NOT binding: LP truly underperforms — review bids first.
            # order_gap < 0 with budget binding: pessimistic CVR artifact — Wilson shrinkage causes
            #   LP's estimated orders < actual historical when budget is exhausted, NOT because the
            #   current allocation outperforms LP.  Allow increase_budget when ACOS is healthy.
            if spend_ceiling_bound or (order_gap < 0 and not budget_binding):
                action, priority = "review_bids", "P1"
                if spend_ceiling_bound:
                    reason = "click ceilings are the binding constraint"
                else:
                    reason = f"order_gap={order_gap:+.2f} (LP cannot outperform current allocation)"
                rationale = (
                    f"LP saturates budget cap (${lp_spend:.0f}/day vs ${camp_budget:.0f}), "
                    f"but {reason} — raise bids or expand keywords before increasing budget"
                )
                if is_paused:
                    action = "enable_and_review_bids"
                    rationale = f"Campaign is PAUSED; {rationale}"
            elif is_paused:
                action, priority = "enable_and_increase_budget", "P0"
                rationale = (
                    f"Campaign is PAUSED; LP saturates budget (needs ${lp_spend:.0f}/day vs ${camp_budget:.0f} cap); "
                    f"ACOS {camp_acos}% ≤ target {target_acos_pct:.0f}% — re-enable then raise budget to ${suggested:.0f}"
                )
            else:
                action, priority = "increase_budget", "P0"
                rationale = (
                    f"LP saturates budget (needs ${lp_spend:.0f}/day vs ${camp_budget:.0f} cap); "
                    f"ACOS {camp_acos}% ≤ target {target_acos_pct:.0f}% — safe to scale"
                )
        elif camp_acos and camp_acos > target_acos_pct * 1.3:
            if is_paused:
                # Already paused — high historical ACOS is why it was paused.
                # No budget action is actionable until the campaign is re-enabled.
                action, priority = "maintain", "P2"
                rationale = (
                    f"Already PAUSED (historical ACOS {camp_acos}% exceeds 130% of target "
                    f"{target_acos_pct:.0f}%) — review bids/negatives before re-enabling"
                )
            else:
                suggested = round(camp_budget * 0.75, 0)
                action, priority = "decrease_budget", "P0"
                rationale = f"ACOS {camp_acos}% exceeds 130% of target {target_acos_pct:.0f}% — cut budget to reduce losses"
        elif camp_acos and target_acos_pct < camp_acos <= target_acos_pct * 1.3:
            if is_paused:
                action, priority = "maintain", "P2"
                rationale = (
                    f"Already PAUSED (historical ACOS {camp_acos}% above target "
                    f"{target_acos_pct:.0f}%) — evaluate bids before re-enabling"
                )
            else:
                action, priority = "review_bids", "P1"
                rationale = f"ACOS {camp_acos}% above target — lower bids on high-ACOS keywords before scaling"
        else:
            action, priority = "maintain", "P2"
            rationale = f"Budget util {budget_util:.0%}, ACOS {camp_acos}% — within healthy range"

        # Inventory gate: downgrade spend-increasing actions when effective stock < threshold
        prerequisite: Optional[Dict] = None
        if inv_gate and action in _CAMP_SPEND_UP:
            eff = inv_gate["effective_stock_days"]
            gate = inv_gate["stock_gate_days"]
            if eff is not None and eff < gate:
                priority = "P2"
                prerequisite = {
                    "condition":            f"effective_stock_days >= {gate}",
                    "effective_stock_days": eff,
                    "can_sell_days":        inv_gate["can_sell_days"],
                    "inbound_receiving":    inv_gate["inbound_receiving"],
                    "inbound_shipped":      inv_gate["inbound_shipped"],
                    "inbound_lead_days":    inv_gate["inbound_lead_days"],
                    "note": (
                        f"Current stock ~{inv_gate['can_sell_days']}d + confirmed inbound = {eff}d effective. "
                        f"Increasing spend now risks stockout before restock arrives. "
                        f"Activate after stock reaches {gate}+ days."
                    ),
                }

        _order_delta = None
        _spend_delta = None
        if action in ("pause_candidate", "archive_candidate"):
            _order_delta = -round(camp_daily_orders, 2)
            _spend_delta = -round(actual_spend, 2)
        elif action in ("increase_budget", "enable_and_increase_budget") and suggested and camp_cpo:
            delta_budget = suggested - camp_budget
            _order_delta = round(delta_budget / camp_cpo, 2)
            _spend_delta = round(delta_budget, 2)
        elif action == "decrease_budget" and suggested:
            delta_budget = suggested - camp_budget
            _order_delta = round(delta_budget / camp_cpo, 2) if camp_cpo else None
            _spend_delta = round(delta_budget, 2)

        entry: Dict = {
            "campaign_id":          cid,
            "campaign_name":        meta.get("name", ""),
            "campaign_state":       camp_state or "UNKNOWN",
            "bidding_strategy":     meta.get("bidding_strategy", ""),
            "current_budget":       camp_budget,
            "lp_optimal_spend":     round(lp_spend, 2),
            "actual_daily_spend":   round(actual_spend, 2),
            "budget_util":          budget_util,
            "campaign_acos":        camp_acos,
            "action":               action,
            "priority":             priority,
            "rationale":            rationale,
            "expected_order_delta": _order_delta,
            "expected_spend_delta": _spend_delta,
        }
        if suggested is not None:
            entry["suggested_budget"] = suggested
        if prerequisite is not None:
            entry["prerequisite"] = prerequisite
        actions.append(entry)

    actions.sort(key=lambda x: ("P0", "P1", "P2").index(x["priority"]))
    return actions


def _build_keyword_actions(
    lp_input: List[Dict],
    alloc: List[Dict],
    kw_id_map: Dict[tuple, Dict],
    brand_kws: set,
    headroom: float,
    avg_price: Optional[float],
    inv_gate: Optional[Dict] = None,
    paused_campaign_ids: Optional[set] = None,
    spend_ceiling_bound: bool = False,
) -> List[Dict]:
    alloc_map: Dict[str, Dict] = {a["keyword"]: a for a in alloc}
    actions: List[Dict] = []
    for lp_kw in lp_input:
        kw_name    = lp_kw["name"]
        kw_text    = kw_name.split("|")[0]
        match_type = kw_name.split("|")[1] if "|" in kw_name else ""
        cid        = lp_kw.get("campaign_id", "")
        is_brand   = kw_text.lower() in {b.lower() for b in brand_kws}

        _lookup = kw_id_map.get((kw_text.lower(), match_type.upper(), str(cid)), {})
        kw_id   = _lookup.get("keyword_id")
        cur_bid = _lookup.get("bid")

        a           = alloc_map.get(kw_name)
        raw_cvr     = lp_kw["estimated_cvr"]
        avg_cpc     = lp_kw["avg_cpc"]
        kw_acos_pct = (
            round(avg_cpc / (raw_cvr * avg_price) * 100, 1)
            if avg_price and raw_cvr > 0 else None
        )

        if a is None:
            camp_is_paused = paused_campaign_ids and str(cid) in paused_campaign_ids
            if not is_brand and not camp_is_paused:
                cur_clicks = lp_kw["max_daily_clicks"] / headroom
                actions.append({
                    "action":               "pause_keyword",
                    "priority":             "P1",
                    "keyword_text":         kw_text,
                    "match_type":           match_type,
                    "campaign_id":          cid,
                    "keyword_id":           kw_id,
                    "current_bid":          cur_bid,
                    "keyword_acos_pct":     kw_acos_pct,
                    "expected_order_delta": -round(cur_clicks * raw_cvr, 2),
                    "expected_spend_delta": -round(cur_clicks * avg_cpc, 2),
                    "rationale": (
                        f"LP assigned 0 clicks — CVR {raw_cvr:.3f} × "
                        f"CPC ${avg_cpc:.2f} → keyword ACOS {kw_acos_pct}% "
                        f"exceeds budget efficiency threshold"
                    ),
                })
        else:
            opt_clicks = a["optimized_clicks"]
            cur_clicks = lp_kw["max_daily_clicks"] / headroom
            cap        = lp_kw["max_daily_clicks"]
            at_ceiling = opt_clicks >= cap * 0.95

            if at_ceiling:
                order_per_10pct_bid = round(opt_clicks * 0.10 * raw_cvr, 2)
                kw_priority = "P1"
                kw_prerequisite: Optional[Dict] = None
                if inv_gate:
                    eff  = inv_gate["effective_stock_days"]
                    gate = inv_gate["stock_gate_days"]
                    if eff is not None and eff < gate:
                        kw_priority = "P2"
                        kw_prerequisite = {
                            "condition":            f"effective_stock_days >= {gate}",
                            "effective_stock_days": eff,
                            "can_sell_days":        inv_gate["can_sell_days"],
                            "inbound_receiving":    inv_gate["inbound_receiving"],
                            "inbound_shipped":      inv_gate["inbound_shipped"],
                            "inbound_lead_days":    inv_gate["inbound_lead_days"],
                            "note": (
                                f"Current stock ~{inv_gate['can_sell_days']}d + confirmed inbound = {eff}d effective. "
                                f"Raising bids accelerates spend and risks stockout. "
                                f"Activate after stock reaches {gate}+ days."
                            ),
                        }
                kw_entry: Dict = {
                    "action":                               "increase_bid",
                    "priority":                             kw_priority,
                    "keyword_text":                         kw_text,
                    "match_type":                           match_type,
                    "campaign_id":                          cid,
                    "keyword_id":                           kw_id,
                    "current_bid":                          cur_bid,
                    "keyword_acos_pct":                     kw_acos_pct,
                    "estimated_order_uplift_per_10pct_bid": order_per_10pct_bid,
                    "expected_spend_per_10pct_bid":         round(opt_clicks * 0.10 * avg_cpc, 2),
                    "rationale": (
                        f"LP maxed click ceiling ({opt_clicks:.0f} clicks/day, ACOS {kw_acos_pct}%); "
                        f"+10% bid ≈ +{order_per_10pct_bid} orders/day (linear impression elasticity assumed)"
                    ),
                }
                if kw_prerequisite:
                    kw_entry["prerequisite"] = kw_prerequisite
                actions.append(kw_entry)
            elif cur_clicks > 0 and opt_clicks < cur_clicks * 0.4:
                delta_clicks = opt_clicks - cur_clicks
                if spend_ceiling_bound:
                    # When LP is globally ceiling-bound, cutting this keyword's bid saves
                    # budget that cannot be reallocated to ceiling-bound keywords, AND will
                    # lower the click ceiling in future runs. Monitor rather than cut.
                    actions.append({
                        "action":               "review_bids",
                        "priority":             "P2",
                        "keyword_text":         kw_text,
                        "match_type":           match_type,
                        "campaign_id":          cid,
                        "keyword_id":           kw_id,
                        "current_bid":          cur_bid,
                        "keyword_acos_pct":     kw_acos_pct,
                        "expected_order_delta": round(delta_clicks * raw_cvr, 2),
                        "expected_spend_delta": round(delta_clicks * avg_cpc, 2),
                        "rationale": (
                            f"LP suggests {opt_clicks:.0f} clicks/day vs current ~{cur_clicks:.0f} "
                            f"(ACOS {kw_acos_pct}%), but account is ceiling-bound: "
                            f"freed budget cannot reach ceiling-constrained keywords — "
                            f"monitor for efficiency; do NOT cut bids until coverage is expanded"
                        ),
                    })
                else:
                    actions.append({
                        "action":               "decrease_bid",
                        "priority":             "P2",
                        "keyword_text":         kw_text,
                        "match_type":           match_type,
                        "campaign_id":          cid,
                        "keyword_id":           kw_id,
                        "current_bid":          cur_bid,
                        "keyword_acos_pct":     kw_acos_pct,
                        "expected_order_delta": round(delta_clicks * raw_cvr, 2),
                        "expected_spend_delta": round(delta_clicks * avg_cpc, 2),
                        "rationale": (
                            f"LP suggests {opt_clicks:.0f} clicks/day vs current ~{cur_clicks:.0f} "
                            f"(ACOS {kw_acos_pct}%) — reduce bid; "
                            f"est. {round(delta_clicks * raw_cvr, 2):+.2f} orders/day, "
                            f"{round(delta_clicks * avg_cpc, 2):+.2f} $/day spend"
                        ),
                    })

    actions.sort(key=lambda x: ("P0", "P1", "P2").index(x["priority"]))
    return actions


def _optimize_budget(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    ProcessStep (pure Python): run LP budget optimisation for each item.

    LP formulation (OR-Tools GLOP) — full constraint set:
      Maximise  Σ clicks_i × pessimistic_cvr_i

      C1  Global budget:       Σ clicks_i × eff_cpc_i ≤ total_budget
      C2  Per-campaign caps:   Σ_{i∈c} clicks_i × eff_cpc_i ≤ budget_c  ∀c
      C3  Target ACOS:         Σ clicks_i × (eff_cpc_i − tacos × pess_cvr_i × price) ≤ 0
      C4  Click bounds:        min_daily_clicks_i ≤ clicks_i ≤ max_daily_clicks_i

      Note: inventory is NOT a LP constraint — replenishment timing is too unstable
      to model as a hard bound. Instead, lp_summary.recommended_stock_units shows
      how many units are needed to safely execute the LP plan.

      eff_cpc_i  = avg_cpc_i × bidding_strategy_multiplier × placement_multiplier
      pess_cvr_i = cvr_i × √(clicks_i / (clicks_i + 30))

    Adds to each item:
      lp_summary, lp_top_allocations, lp_zero_keywords, lp_maxed_keywords,
      campaign_actions, keyword_actions
    """
    from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer

    headroom    = ctx.config.get("lp_headroom_factor", 3.0)
    target_acos = ctx.config.get("target_acos")
    days        = ctx.config.get("days", 30)
    brand_kws   = set(ctx.config.get("brand_keywords", []))
    optimizer   = AdBudgetOptimizer()

    raw_kw_perf: List[Dict] = ctx.cache.get(_KEY_KW_PERFORMANCE, [])

    for item in items:
        campaigns    = item.get("campaigns") or []
        kw_perf      = item.get("keyword_performance", [])
        daily_budget = item.get("total_daily_budget", 0) or 0
        campaign_ids = set(item.get("campaign_ids", []))

        # ── Reconcile budget snapshot vs historical spend ─────────────────
        # total_daily_budget = ENABLED campaigns only (current snapshot).
        # During the period, campaigns may have been PAUSED or had budgets
        # changed, so historical spend can exceed the snapshot.
        # Amazon also allows up to 25% daily budget overage for traffic spikes.
        #
        # Strategy:
        #   discrepancy ≤ 25% → Amazon pacing; keep snapshot, expose ratio.
        #   discrepancy > 25% → structural (budget changed / campaigns paused);
        #                        use historical_daily_spend × 0.90 as LP budget.
        perf_records_all = item.get("performance_records") or []
        historical_spend_total = sum(
            float(r.get("spend", 0) or 0)
            for r in perf_records_all
            if str(r.get("campaign_id")) in campaign_ids
        )
        historical_daily_spend = round(historical_spend_total / days, 2) if days > 0 else 0.0
        # Amazon's pacing ceiling: campaigns can overspend by up to this factor per day
        _AMAZON_PACING_MAX = 0.25
        if daily_budget > 0:
            budget_overage_ratio = round(historical_daily_spend / daily_budget, 3)
            if budget_overage_ratio > 1 + _AMAZON_PACING_MAX:
                # Structural mismatch: budget snapshot understates real capacity
                daily_budget = round(historical_daily_spend * 0.90, 2)
                budget_source = "historical_avg_spend"
            else:
                budget_source = "campaign_snapshot"
        else:
            budget_overage_ratio = None
            budget_source = "campaign_snapshot"
            # Fall back: if current snapshot is 0 but account was actually spending, use history
            if historical_daily_spend > 0:
                daily_budget = round(historical_daily_spend * 0.90, 2)
                budget_source = "historical_avg_spend"

        if not kw_perf or daily_budget <= 0:
            item["lp_summary"] = {"skipped": True, "reason": "no keyword data or zero budget"}
            continue

        camp_meta: Dict[str, Dict] = {
            str(c["campaign_id"]): c for c in campaigns if c.get("campaign_id")
        }
        campaign_budgets: Dict[str, float] = {
            cid: float(c.get("daily_budget") or 0)
            for cid, c in camp_meta.items()
            if c.get("daily_budget")
        }

        placement_perf       = item.get("placement_performance") or {}
        placement_mods       = item.get("placement_configured_pcts") or {}
        placement_multiplier = _compute_placement_multiplier(placement_perf, placement_mods)
        kw_to_campaign       = _build_kw_to_campaign_map(raw_kw_perf, campaign_ids)

        total_orders    = item.get("total_orders") or 0
        total_sales     = item.get("total_sales")  or 0
        avg_price       = round(total_sales / total_orders, 2) if total_orders > 0 else None
        can_sell_days   = item.get("can_sell_days")
        total_available = item.get("total_available") or 0

        asin       = (item.get("asin") or "").upper()
        asin_daily = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])
        lp_input = _build_lp_input(
            kw_perf, kw_to_campaign, camp_meta, brand_kws, headroom, placement_multiplier,
            daily_perf=asin_daily,
        )
        if not lp_input:
            item["lp_summary"] = {"skipped": True, "reason": "all keywords filtered (insufficient clicks)"}
            continue

        # ── LP-scope budget: use only LP-scope campaign budgets as global cap ──
        # Non-LP campaigns (auto/PT) consume their own separate campaign budgets.
        # Giving LP the total account budget ($239) inflates the constraint beyond
        # what LP-scope campaigns can actually spend; per-campaign Constraint 2
        # becomes the real binding cap anyway.  Using LP-scope budget makes the
        # global constraint match the actual LP operating budget and prevents the
        # LLM from summing lp_optimal_spend + non_lp_scope_daily_spend against
        # the total account budget (which would always look contradictory).
        lp_scope_cids_pre = {str(kw.get("campaign_id", "")) for kw in lp_input if kw.get("campaign_id")}
        lp_scope_campaign_budget_raw = sum(
            float(camp_meta[cid].get("daily_budget") or 0)
            for cid in lp_scope_cids_pre
            if cid in camp_meta and (camp_meta[cid].get("state") or "").upper() == "ENABLED"
        )
        # Reconcile LP-scope budget against LP-scope historical spend (same logic
        # as the account-level reconciliation above).
        lp_scope_hist_spend_total = sum(
            float(r.get("spend", 0) or 0)
            for r in perf_records_all
            if str(r.get("campaign_id", "")) in lp_scope_cids_pre
        )
        lp_scope_hist_daily = round(lp_scope_hist_spend_total / days, 2) if days > 0 else 0.0
        if lp_scope_campaign_budget_raw > 0:
            lp_scope_budget_ratio = round(lp_scope_hist_daily / lp_scope_campaign_budget_raw, 3)
            if lp_scope_budget_ratio > 1 + _AMAZON_PACING_MAX:
                lp_budget = round(lp_scope_hist_daily * 0.90, 2)
            else:
                lp_budget = lp_scope_campaign_budget_raw
        else:
            lp_budget = lp_scope_hist_daily * 0.90 if lp_scope_hist_daily > 0 else daily_budget

        # ── CVR deflation: correct for cross-keyword attribution overlap ──────
        # item["total_orders"] = spAdvertisedProduct groupBy=advertiser (ASIN-level,
        #   deduplicated within the ASIN — one purchase counted once across campaigns).
        # sum(kw["total_orders"]) = keyword-level report (the same purchase can be
        #   claimed by BROAD, AUTO, and PT simultaneously within the attribution window).
        # Their ratio directly measures the within-LP double-counting factor.
        kw_attributed_orders = sum(k.get("total_orders", 0) for k in kw_perf)
        cvr_deflation = (
            min(1.0, total_orders / kw_attributed_orders)
            if kw_attributed_orders > 0 and total_orders > 0
            else 1.0
        )
        if cvr_deflation < 1.0:
            for kw in lp_input:
                kw["estimated_cvr"] *= cvr_deflation
            logger.debug(
                f"[lp] {asin}: cvr_deflation={cvr_deflation:.3f} "
                f"(asin_orders={total_orders}, kw_orders={kw_attributed_orders})"
            )

        # ── order_gap baseline: use keyword-level scope (same as LP) ────────
        # total_orders comes from spCampaigns (campaign-level), which includes
        # auto / product-targeting campaigns whose orders are NOT part of the LP
        # input (kw_perf only covers manual keyword targeting).  Using the
        # campaign-level total as the baseline makes order_gap systematically
        # negative by ~auto_pt_daily_orders, and incorrectly downgrades budget
        # recommendations to "review_bids" in _build_campaign_actions.
        #
        # Fix: compare LP output against keyword-attributed orders only.
        # The auto/PT contribution is surfaced separately as auto_pt_daily_orders
        # so the LLM can still reason about the full picture.
        kw_scope_orders       = kw_attributed_orders          # spSearchTerm, keyword scope
        actual_daily_ad_orders = round(kw_scope_orders / days, 2)
        # auto_pt_daily_orders: orders from auto / product-targeting (not in LP scope).
        # Clamped to ≥ 0: if kw_scope > total_orders it means keyword-level report
        # double-counts across match types within the same purchase.
        auto_pt_daily_orders   = max(0.0, round((total_orders - kw_scope_orders) / days, 2))

        # Inventory is NOT a LP constraint — replenishment timing is unstable.
        # Instead, we compute a recommended stock level after optimisation.
        result = optimizer.optimize(
            keywords         = lp_input,
            total_budget     = lp_budget,
            campaign_budgets = campaign_budgets or None,
            target_acos      = target_acos,
            avg_price        = avg_price,
        )
        if result.get("status") != "OPTIMAL":
            item["lp_summary"] = {"skipped": True, "reason": result.get("message")}
            continue

        summary    = result["summary"]
        alloc      = result["allocation"]
        camp_spend = result.get("camp_spend", {})
        kw_map     = {lp["name"]: lp for lp in lp_input}

        raw_cvr_map   = {kw["name"]: kw["estimated_cvr"] for kw in lp_input}
        lp_raw_orders = round(
            sum(a["optimized_clicks"] * raw_cvr_map.get(a["keyword"], 0) for a in alloc), 2
        )

        lp_spend_total      = summary["actual_spend"]
        spend_ceiling_bound = lp_spend_total < lp_budget * 0.6
        budget_binding      = lp_spend_total >= lp_budget * 0.85
        # Campaign IDs that contributed at least one keyword to the LP model.
        # Auto / product-targeting campaigns never appear here.
        lp_scoped_cids      = {str(kw.get("campaign_id", "")) for kw in lp_input if kw.get("campaign_id")}
        # Historical spend split: LP-scope keywords vs non-LP (auto/PT + untracked keywords).
        # Explains the gap between lp_optimal_spend and historical_daily_spend to the LLM.
        lp_scope_hist_spend = round(
            sum(float(r.get("spend", 0) or 0) for r in perf_records_all
                if str(r.get("campaign_id", "")) in lp_scoped_cids) / days, 2
        ) if days > 0 else 0.0
        non_lp_scope_hist_spend = round(max(0.0, historical_daily_spend - lp_scope_hist_spend), 2)
        zero_kws, maxed_kws = _classify_lp_keywords(kw_perf, alloc, kw_map)
        kw_id_map           = _build_lp_kw_id_map(ctx, campaign_ids)

        # ── Inventory gate ────────────────────────────────────────────────
        stock_gate_days   = ctx.config.get("stock_gate_days", 21)
        inbound_lead_days = ctx.config.get("inbound_lead_days", 30)
        daily_sales_val   = item.get("daily_sales") or 0
        inbound_receiving = item.get("inbound_receiving") or 0
        inbound_shipped   = item.get("inbound_shipped") or 0
        effective_stock_days: Optional[int] = None
        if can_sell_days and daily_sales_val > 0:
            # inbound_shipped only "catches" the stockout if it arrives before current stock runs out
            catchable = inbound_shipped if inbound_lead_days < can_sell_days else 0
            eff_units = total_available + inbound_receiving + catchable
            effective_stock_days = round(eff_units / daily_sales_val)
        inv_gate: Optional[Dict] = (
            {
                "stock_gate_days":    stock_gate_days,
                "effective_stock_days": effective_stock_days,
                "can_sell_days":      can_sell_days,
                "inbound_receiving":  inbound_receiving,
                "inbound_shipped":    inbound_shipped,
                "inbound_lead_days":  inbound_lead_days,
            }
            if effective_stock_days is not None else None
        )

        order_gap = lp_raw_orders - actual_daily_ad_orders
        campaign_actions = _build_campaign_actions(
            camp_meta, camp_spend, item.get("performance_records") or [], days, target_acos,
            inv_gate=inv_gate,
            order_gap=order_gap,
            spend_ceiling_bound=spend_ceiling_bound,
            budget_binding=budget_binding,
            lp_scoped_cids=lp_scoped_cids,
        )
        paused_cids = {str(cid) for cid, meta in camp_meta.items()
                       if (meta.get("state") or "").upper() == "PAUSED"}
        keyword_actions = _build_keyword_actions(
            lp_input, alloc, kw_id_map, brand_kws, headroom, avg_price,
            inv_gate=inv_gate,
            paused_campaign_ids=paused_cids,
            spend_ceiling_bound=spend_ceiling_bound,
        )

        placement_data_unknown = set(placement_perf.keys()) <= {"UNKNOWN", ""}

        # ── Stock recommendation (replaces LP inventory cap) ──────────────
        # Use total daily consumption (ad + organic) when available via order_metrics;
        # fall back to ad-orders-only (underestimates consumption → optimistic).
        daily_consumption     = item.get("daily_sales") or actual_daily_ad_orders
        daily_consumption_src = item.get("daily_sales_source") or "ad_orders_only"
        # LP adds lp_raw_orders ad orders/day on top of organic baseline
        organic_daily  = max(0.0, daily_consumption - actual_daily_ad_orders)
        lp_total_daily = organic_daily + lp_raw_orders
        recommended_stock = round(lp_total_daily * stock_gate_days)
        # Inbound inventory offsets the procurement need:
        #   receiving = at FC, available 1-2 days (certain)
        #   shipped   = in transit from seller (10-30d ETA depending on route)
        #   working   = NOT counted — plan only, not yet handed to carrier
        inbound_recv    = item.get("inbound_receiving") or 0
        inbound_ship    = item.get("inbound_shipped") or 0
        inbound_work    = item.get("inbound_working") or 0
        confirmed_inbound = inbound_recv + inbound_ship
        stock_shortfall = max(0, recommended_stock - total_available - confirmed_inbound)

        item["lp_summary"] = {
            # lp_scope_campaign_daily_budget: sum of LP-scope campaign budgets only.
            # This is the actual global constraint given to the LP optimizer.
            # Non-LP campaigns (auto/PT) draw from their OWN separate budgets —
            # lp_optimal_spend and non_lp_scope_daily_spend must NOT be summed
            # against each other or against total_account_daily_budget.
            "lp_scope_campaign_daily_budget": lp_budget,
            "total_account_daily_budget":     daily_budget,
            "lp_optimal_spend":              lp_spend_total,
            "lp_optimal_orders_pessimistic": summary["total_expected_orders"],
            "lp_optimal_orders_raw":         lp_raw_orders,
            "actual_daily_ad_orders":        round(actual_daily_ad_orders, 2),
            "auto_pt_daily_orders":          round(auto_pt_daily_orders, 2),
            "order_gap":                     round(order_gap, 2),
            "spend_ceiling_bound":           spend_ceiling_bound,
            "budget_binding":                budget_binding,
            "click_headroom":               _p90_headroom(asin_daily, headroom),
            "avg_effective_cpc":             summary["avg_effective_cpc"],
            "placement_multiplier":          round(placement_multiplier, 3),
            "placement_data_unknown":        placement_data_unknown,
            "target_acos_applied":           target_acos,
            # Stock recommendation: units needed to safely execute LP plan for stock_gate_days
            "recommended_stock_units":       recommended_stock,
            "stock_shortfall":               stock_shortfall,
            "stock_gate_days":               stock_gate_days,
            "daily_consumption":             round(lp_total_daily, 2),
            "daily_consumption_source":      daily_consumption_src,
            "keywords_in_lp":               len(lp_input),
            "keywords_allocated":           len(alloc),
            "keywords_zeroed":              len(zero_kws),
            "keywords_maxed":               len(maxed_kws),
            # Attribution deflation: ratio of ASIN-level to keyword-level attributed orders.
            # < 1.0 means cross-keyword / cross-match-type overlap was corrected.
            "cvr_deflation":                round(cvr_deflation, 3),
            "cvr_deflation_source":         "asin_vs_keyword_orders",
            # Budget reconciliation
            "historical_daily_spend":       historical_daily_spend,
            "budget_overage_ratio":         budget_overage_ratio,
            "budget_source":                budget_source,
            # LP scope vs non-LP spend split:
            # lp_optimal_spend covers only lp_scope keywords; non_lp_scope_daily_spend
            # explains why total_historical_daily_spend >> lp_optimal_spend.
            "lp_scope_daily_spend":         lp_scope_hist_spend,
            "non_lp_scope_daily_spend":     non_lp_scope_hist_spend,
        }
        item["lp_top_allocations"] = [
            {
                **a,
                "keyword":    a["keyword"].split("|")[0],
                "match_type": a["keyword"].split("|")[1] if "|" in a["keyword"] else "",
            }
            for a in alloc[:10]
        ]
        item["lp_zero_keywords"]  = zero_kws[:20]
        item["lp_maxed_keywords"] = maxed_kws[:10]
        item["campaign_actions"]  = campaign_actions
        item["keyword_actions"]   = keyword_actions[:30]

    return items


async def _enrich_covariates(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch daily price / promotion / rating time series via Xiyouzhaoci get_asin_daily_trends.

    Covers the same window as the change-history lookback so that run_causal_analysis
    can annotate each change event with the covariate state on the change date.

    Returned covariate_series shape:
        {"2026-04-01": {"list_price": 29.99, "sale_price": 24.99,
                        "promotion_flag": true, "rating": 4.6, "review_count": 312}, ...}

    Note: get_asin_daily_trends is historical time-series data (Xiyouzhaoci).
          ProductDetailsExtractor gives only a point-in-time page scrape — not suitable
          for ITS/CausalImpact/DML which require day-indexed covariate vectors.
    """
    if not ctx.config.get("enable_xiyou", True):
        return {}

    asin    = item.get("asin")
    country = ctx.config.get("country", "US")
    days    = ctx.config.get("days", 30)
    if not asin:
        return {}

    try:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
        api = XiyouZhaociAPI(tenant_id=ctx.config.get("tenant_id", "default"))

        tz     = ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))
        today  = datetime.now(tz=tz).date()
        end_dt = today - timedelta(days=1)

        # Two-tier window:
        #   Attribution window  (days):                 aligns with change_history for Before/After
        #   Causal baseline window (rank_lookback_months): aligns with natural_rank_series for ITS
        # Use the longer of the two so covariate_series covers the full ITS baseline.
        import calendar as _cal
        rank_months = min(ctx.config.get("rank_lookback_months", 6), 24)
        rm_y = end_dt.year - (rank_months // 12)
        rm_m = end_dt.month - (rank_months % 12)
        if rm_m <= 0:
            rm_m += 12; rm_y -= 1
        from datetime import date as _date
        causal_start = _date(rm_y, rm_m, min(end_dt.day, _cal.monthrange(rm_y, rm_m)[1]))
        attr_start   = today - timedelta(days=days + abs(ATTR_POST_END))
        start_dt     = min(causal_start, attr_start)

        raw = api.get_asin_daily_trends(
            country=country,
            asin=asin,
            start_date=start_dt.strftime("%Y-%m-%d"),
            end_date=end_dt.strftime("%Y-%m-%d"),
        )
        if not raw:
            return {}

        # Response shape: {"entities": [{"country": ..., "asin": ..., "trends": [...]}]}
        # Each trend record: {"localDate": "2026-01-01T00:00:00-08:00", "price": 43.99,
        #   "ratings": 5325, "stars": 4,
        #   "priceDistribution": {"deal": false, "originPrice": 54.99, "prime": 43.99}}
        entities = raw.get("entities") or []
        entity   = next((e for e in entities if e.get("asin") == asin), entities[0] if entities else {})
        records  = entity.get("trends") or []

        tz               = ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))
        covariate_series: Dict[str, Dict] = {}
        for r in records:
            # localDate carries a UTC offset (e.g. "2026-01-01T00:00:00-08:00").
            # Parse the full ISO string so DST transitions are handled correctly,
            # then express the date in the store timezone — matching the date key
            # used by performance-report records and change-history anchors.
            raw_date = r.get("localDate") or ""
            try:
                dt_with_offset = datetime.fromisoformat(str(raw_date))
                date = dt_with_offset.astimezone(tz).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date = str(raw_date)[:10]
            if len(date) != 10:
                continue

            price_dist  = r.get("priceDistribution") or {}
            list_price  = price_dist.get("originPrice")          # original/list price
            sale_price  = r.get("price")                         # effective price (prime)
            is_deal     = bool(price_dist.get("deal", False))
            rating      = r.get("stars")
            review_cnt  = r.get("ratings")

            # Promotion flag: explicit deal OR effective price < 95% of list price
            try:
                price_promo = bool(
                    list_price and sale_price and
                    float(sale_price) < float(list_price) * 0.95
                )
            except (TypeError, ValueError):
                price_promo = False
            promo = is_deal or price_promo

            covariate_series[date] = {
                "list_price":     float(list_price)  if list_price  is not None else None,
                "sale_price":     float(sale_price)  if sale_price  is not None else None,
                "is_deal":        is_deal,
                "promotion_flag": promo,
                "rating":         float(rating)      if rating      is not None else None,
                "review_count":   int(review_cnt)    if review_cnt  is not None else None,
            }

        if not covariate_series:
            logger.warning(f"No covariate records parsed for {asin}")
            return {}

        prices  = [v["sale_price"]  for v in covariate_series.values() if v["sale_price"]  is not None]
        ratings = [v["rating"]      for v in covariate_series.values() if v["rating"]      is not None]
        promo_days = sum(1 for v in covariate_series.values() if v["promotion_flag"])

        logger.info(f"Covariates for {asin}: {len(covariate_series)} days, "
                    f"{promo_days} promo days, price {min(prices, default=0):.2f}–{max(prices, default=0):.2f}")
        return {
            "covariate_series": covariate_series,
            "price_min":        round(min(prices), 2)                         if prices  else None,
            "price_max":        round(max(prices), 2)                         if prices  else None,
            "price_mean":       round(sum(prices) / len(prices), 2)           if prices  else None,
            "promotion_days":   promo_days,
            "rating_latest":    ratings[-1]                                   if ratings else None,
            "rating_delta":     round(ratings[-1] - ratings[0], 2)            if len(ratings) >= 2 else None,
        }

    except Exception as e:
        logger.warning(f"Covariate fetch failed for {asin}: {e}")
        return {}


async def _enrich_competitor_prices(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Identify competitor ASINs via keyword-level topAsins data, then fetch their
    daily price history as an external covariate for ITS/CausalImpact/DML.

    Pipeline:
      1. get_asin_keywords  → top keywords for this ASIN (synchronous list endpoint)
      2. Filter brand keywords: term contains brand name OR target ASIN click share > 50%
      3. Top 5 non-brand keywords by weeklySearchVolume
      4. topAsins[:3] per keyword → deduplicate → ≤15 competitor ASINs
      5. get_asin_daily_trends for each competitor (parallel via asyncio.to_thread)
      6. Build per-date summary {min, max, median} across all competitors

    Depends on: fetch_catalog (provides brand name for keyword filtering).
    Must run after Stage 1 catalog enrichment.
    """
    if not ctx.config.get("enable_xiyou", True):
        return {}

    asin    = item.get("asin")
    brand   = (item.get("brand") or "").lower().strip()
    country = ctx.config.get("country", "US")
    days    = ctx.config.get("days", 30)
    tz      = _store_tz(ctx)
    if not asin:
        return {}

    try:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
        api = XiyouZhaociAPI(tenant_id=ctx.config.get("tenant_id", "default"))

        today    = datetime.now(tz=tz).date()
        kw_end   = today - timedelta(days=1)
        kw_start = today - timedelta(days=30)   # keyword data: last 30 days

        kw_data = await asyncio.to_thread(
            api.get_asin_keywords,
            country, asin,
            kw_start.strftime("%Y-%m-%d"),
            kw_end.strftime("%Y-%m-%d"),
            1, 20,
        )
        kw_list = kw_data.get("list") or []

        # ── Filter brand keywords, rank by search volume ──────────────────
        non_brand: List[tuple] = []
        for kw in kw_list:
            term = (kw.get("searchTerm") or "").lower()
            # Brand keyword: search term contains the brand name
            if brand and brand in term:
                continue
            # Brand keyword: target ASIN dominates click share (>50%)
            top_asins = (kw.get("topAsins") or {}).get("list") or []
            own_share = next(
                (a.get("clickShare", 0) for a in top_asins if a.get("asin") == asin), 0
            )
            if own_share > 0.5:
                continue
            vol = (kw.get("searchTermReport") or {}).get("weeklySearchVolume", 0) or 0
            non_brand.append((vol, kw))

        non_brand.sort(key=lambda x: x[0], reverse=True)
        top_kws = [kw for _, kw in non_brand[:5]]

        # ── Collect top 3 competitor ASINs per keyword, deduplicate ──────
        competitor_asins: List[str] = []
        seen = {asin}
        for kw in top_kws:
            for entry in ((kw.get("topAsins") or {}).get("list") or [])[:3]:
                comp = entry.get("asin")
                if comp and comp not in seen:
                    competitor_asins.append(comp)
                    seen.add(comp)

        if not competitor_asins:
            logger.warning(f"No competitor ASINs found for {asin}")
            return {"competitor_asins": [], "competitor_price_summary": {}}

        # ── Fetch daily price history for each competitor (parallel) ─────
        cov_end   = today - timedelta(days=1)
        cov_start = today - timedelta(days=days + abs(ATTR_POST_END))

        async def _fetch_prices(comp_asin: str):
            try:
                raw = await asyncio.to_thread(
                    api.get_asin_daily_trends,
                    country, comp_asin,
                    cov_start.strftime("%Y-%m-%d"),
                    cov_end.strftime("%Y-%m-%d"),
                )
                entities = raw.get("entities") or []
                entity   = next(
                    (e for e in entities if e.get("asin") == comp_asin),
                    entities[0] if entities else {},
                )
                prices: Dict[str, float] = {}
                for r in entity.get("trends") or []:
                    try:
                        dt_local = datetime.fromisoformat(str(r.get("localDate") or ""))
                        date  = dt_local.astimezone(tz).strftime("%Y-%m-%d")
                        price = r.get("price")
                        if price is not None:
                            prices[date] = float(price)
                    except (ValueError, TypeError):
                        continue
                return comp_asin, prices
            except Exception as e:
                logger.warning(f"Competitor price fetch failed for {comp_asin}: {e}")
                return comp_asin, {}

        results = await asyncio.gather(*[_fetch_prices(a) for a in competitor_asins])
        comp_price_by_asin: Dict[str, Dict[str, float]] = dict(results)

        # ── Build per-date summary (min/max/median across competitors) ───
        all_dates = sorted({d for prices in comp_price_by_asin.values() for d in prices})
        competitor_price_summary: Dict[str, Dict] = {}
        for date in all_dates:
            day_entries = sorted(
                (comp_price_by_asin[a][date], a)
                for a in comp_price_by_asin
                if comp_price_by_asin[a].get(date) is not None
            )
            if not day_entries:
                continue
            day_prices   = [p for p, _ in day_entries]
            day_asins    = [a for _, a in day_entries]
            n            = len(day_prices)
            median       = day_prices[n // 2] if n % 2 else (day_prices[n//2-1] + day_prices[n//2]) / 2
            competitor_price_summary[date] = {
                "min":               round(min(day_prices), 2),
                "max":               round(max(day_prices), 2),
                "median":            round(median, 2),
                "count":             n,
                "contributor_asins": day_asins[:3],  # up to 3 for easy lookup
            }

        logger.info(
            f"Competitor prices for {asin}: {len(competitor_asins)} ASINs, "
            f"{len(competitor_price_summary)} date points"
        )
        return {
            "competitor_asins":         competitor_asins,
            "top_competitor_asins":     competitor_asins[:3],
            "competitor_price_by_asin": comp_price_by_asin,
            "competitor_price_summary": competitor_price_summary,
        }

    except Exception as e:
        logger.warning(f"Competitor price enrichment failed for {asin}: {e}")
        return {"competitor_asins": [], "competitor_price_summary": {}}


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

        entities = scores.get("entities") or []
        if not entities:
            return {}

        entry = next((e for e in entities if e.get("asin") == asin), entities[0])
        return {
            "ad_traffic_ratio":      entry.get("advertisingTrafficScoreRatio"),
            "organic_traffic_ratio": entry.get("organicTrafficScoreRatio"),
            "traffic_growth_7d":     entry.get("totalTrafficScoreGrowthRate"),
            "xiyou_scores_raw":      entry,
        }
    except Exception as e:
        logger.warning(f"Xiyou traffic scores failed for {asin}: {e}")
        return {}


def _select_rank_keywords(item: Dict, top_n: int = 3) -> List[str]:
    """
    Choose the most signal-rich keywords for organic rank and market trend tracking.

    Priority order:
      1. lp_top_allocations — LP-validated efficient terms (high spend + positive CVR).
      2. keyword_performance sorted by total_spend, filtered ACOS ≤ 80%
         (avoid zero-CVR keywords that burn budget with no organic signal).

    Returns at most `top_n` unique lowercase keyword strings.
    """
    seen: list = []

    for entry in item.get("lp_top_allocations") or []:
        # lp_top_allocations stores "keyword text|MATCH_TYPE" — strip the suffix
        kw = (entry.get("keyword") or "").split("|")[0].strip().lower()
        if kw and kw not in seen:
            seen.append(kw)
        if len(seen) >= top_n:
            return seen

    kw_perf = sorted(
        item.get("keyword_performance") or [],
        key=lambda x: x.get("total_spend", 0),
        reverse=True,
    )
    for entry in kw_perf:
        acos = entry.get("acos")
        if acos is not None and acos > 80:
            continue
        kw = (entry.get("keyword_text") or "").strip().lower()
        if kw and kw not in seen:
            seen.append(kw)
        if len(seen) >= top_n:
            return seen

    return seen


async def _enrich_keyword_signals(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch daily organic rank and weekly ABA market trends for the top-N keywords
    in a single enricher, avoiding duplicate keyword selection and API client setup.

    Keyword selection: _select_rank_keywords() called once — LP allocations first,
    fallback to top-spend keywords with ACOS ≤ 80%.

    Both API calls (rank trends, SFR trends) are issued concurrently via asyncio.gather.

    Returned keys:
        natural_rank_series   {keyword: {date: {page, pageRank, totalRank}}}
        rank_tracked_keywords list of keywords that returned rank data
        market_trends         {keyword: {YYYY-Www: {sfr, weekly_searches}}}
        market_trends_meta    {keyword: {current_sfr, current_weekly_searches}}
    """
    if not ctx.config.get("enable_xiyou", True):
        return {}

    asin    = item.get("asin")
    country = ctx.config.get("country", "US")
    if not asin:
        return {}

    keywords = _select_rank_keywords(item)
    if not keywords:
        logger.info(f"[keyword_signals] No keywords for {asin}, skipping.")
        return {}

    import calendar as _cal
    from datetime import date as _date

    rank_months  = min(ctx.config.get("rank_lookback_months", 6), 24)
    weeks_needed = rank_months * 4 + 4   # slight overestimate; API caps internally

    tz     = ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))
    today  = datetime.now(tz=tz).date()
    end_dt = today - timedelta(days=1)

    # Calendar-month start date for rank series
    y, m = end_dt.year - (rank_months // 12), end_dt.month - (rank_months % 12)
    if m <= 0:
        m += 12; y -= 1
    rank_start = _date(y, m, min(end_dt.day, _cal.monthrange(y, m)[1]))

    try:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
        api = XiyouZhaociAPI(tenant_id=ctx.config.get("tenant_id", "default"))

        # ── Fetch rank trends and SFR trends concurrently ─────────────────────
        async def _fetch_rank() -> Dict:
            raw      = await asyncio.to_thread(
                api.get_asin_search_term_rank_trends,
                country, asin, keywords,
                rank_start.strftime("%Y-%m-%d"),
                end_dt.strftime("%Y-%m-%d"),
            )
            entities = raw.get("entities") or []
            series: Dict[str, Dict] = {}
            for entity in entities:
                term  = (entity.get("searchTerm") or "").strip().lower()
                daily: Dict[str, Dict] = {}
                for trend in entity.get("trends") or []:
                    try:
                        dt = datetime.fromisoformat(str(trend.get("localDate") or "")).astimezone(tz)
                        date_key = dt.strftime("%Y-%m-%d")
                    except Exception:
                        continue
                    or_pos = (trend.get("displayPositions") or {}).get("or") or {}
                    if or_pos:
                        daily[date_key] = {
                            "page":      or_pos.get("page"),
                            "pageRank":  or_pos.get("pageRank"),
                            "totalRank": or_pos.get("totalRank"),
                        }
                if daily:
                    series[term] = daily
            return series

        async def _fetch_trends() -> tuple:
            latest_monday = today - timedelta(days=today.weekday() + 7)
            trends: Dict[str, Dict] = {}
            meta:   Dict[str, Dict] = {}
            for kw in keywords:
                raw     = await asyncio.to_thread(api.get_search_term_trends, country, kw, weeks_needed)
                entries = raw.get("searchTerms") or []
                if not entries:
                    continue
                entry      = entries[0]
                sfr_arr    = (entry.get("trends") or {}).get("searchFrequencyRank") or []
                search_arr = (entry.get("trends") or {}).get("weekSearch") or []
                n = len(sfr_arr)
                if not n:
                    continue
                weekly: Dict[str, Dict] = {}
                for i, sfr in enumerate(sfr_arr):
                    week_monday = latest_monday - timedelta(days=(n - 1 - i) * 7)
                    weekly[week_monday.strftime("%G-W%V")] = {
                        "sfr":             sfr,
                        "weekly_searches": search_arr[i] if i < len(search_arr) else None,
                    }
                trends[kw] = weekly
                current    = entry.get("values") or {}
                meta[kw]   = {
                    "current_sfr":             current.get("searchFrequencyRank"),
                    "current_weekly_searches": current.get("weekSearch"),
                }
            return trends, meta

        rank_series, (market_trends, market_trends_meta) = await asyncio.gather(
            _fetch_rank(),
            _fetch_trends(),
        )

        logger.info(
            f"[keyword_signals] {asin}: rank={list(rank_series.keys())}, "
            f"trends={list(market_trends.keys())}"
        )
        return {
            "natural_rank_series":   rank_series,
            "rank_tracked_keywords": list(rank_series.keys()),
            "market_trends":         market_trends,
            "market_trends_meta":    market_trends_meta,
        }

    except Exception as e:
        logger.warning(f"[keyword_signals] Failed for {asin}: {e}")
        return {}


# ---------------------------------------------------------------------------
# ── ERP backtest baseline helpers ────────────────────────────────────────────

def _yoy_date_range(ctx: WorkflowContext) -> Tuple[str, str]:
    """
    Date range covering all YoY post-windows for change events in the analysis
    window.  Shifted exactly YOY_OFFSET_DAYS (364 days = 52 weeks) back so the
    same day-of-week is preserved.
    """
    days  = ctx.config.get("days", 30)
    today = _date_cls.today()
    # Post-windows of events from [today-days, today-0] land in YoY space at:
    #   [today - days - YOY_OFFSET_DAYS + ATTR_POST_START,
    #    today - 1    - YOY_OFFSET_DAYS + ATTR_POST_END  ]
    # Add a 5-day buffer on each side for safety.
    end   = today - timedelta(days=YOY_OFFSET_DAYS - abs(ATTR_POST_END) - 1 - 5)
    start = today - timedelta(days=YOY_OFFSET_DAYS + days + 5)
    return str(start), str(end)


def _trailing_ext_date_range(ctx: WorkflowContext) -> Tuple[str, str]:
    """
    Date range for the trailing ~3M extension window that fills the gap between
    daily_perf (covers ~49 days) and the full [anchor-97, anchor-11] baseline.

    Overlaps daily_perf by a few days on the right — asin_date_index (Ads API)
    takes priority for overlapping dates inside _normalized_delta_orders.
    """
    days     = ctx.config.get("days", 30)
    today    = _date_cls.today()
    # daily_perf starts at approximately today - (days + |ATTR_PRE_START| + |ATTR_POST_END| + 2)
    dp_start_offset = days + abs(ATTR_PRE_START) + abs(ATTR_POST_END) + 2
    ext_end   = today - timedelta(days=dp_start_offset - 3)   # 3-day overlap
    ext_start = today - timedelta(days=days + abs(TRAILING_START) + 10)
    return str(ext_start), str(ext_end)


def _normalise_erp_daily(resp: Dict) -> Dict[str, Dict]:
    """Convert ERP ad report response → {date_str: {orders, spend, clicks}}."""
    result: Dict[str, Dict] = {}
    for row in resp.get("data", []):
        d = row.get("date_day")
        if not d:
            continue   # aggregate row (date_day=null)
        if d not in result:
            result[d] = {"orders": 0.0, "spend": 0.0, "clicks": 0.0}
        result[d]["orders"] += float(row.get("orders") or 0)
        result[d]["spend"]  += float(row.get("spends")  or 0)   # ERP field name
        result[d]["clicks"] += float(row.get("clicks")  or 0)
    return result


def _fetch_erp_baseline_sync(
    ctx: WorkflowContext,
    asin: str,
    l1_key: str,
    l2_parts: Tuple,
    date_range: Tuple[str, str],
    label: str,
) -> Dict[str, Dict]:
    """
    Generic sync fetcher for ERP backtest baselines (YoY / trailing-ext).
    Checks L1 ctx.cache → L2 DataCache → live ERP call.  Non-fatal on error.
    """
    if l1_key in ctx.cache:
        return ctx.cache[l1_key]

    hit = _l2_get(ctx, _TTL_YOY, *l2_parts)
    if hit is not None:
        ctx.cache[l1_key] = hit
        return hit

    store_id   = ctx.config.get("store_id", "US")
    profile_id = os.getenv(f"AMAZON_ADS_PROFILE_ID_{store_id}", "")
    if not profile_id:
        logger.warning(f"[{label}] No AMAZON_ADS_PROFILE_ID_{store_id} — skipping ERP fetch")
        return {}

    start_date, end_date = date_range
    try:
        from src.mcp.servers.erp.lingxing.client import LingxingClient
        client = LingxingClient()
        resp   = client.get_sp_campaign_ad_report(
            profile_id=profile_id,
            report_date=f"{start_date} - {end_date}",
            asin=[asin],
            is_daily=1,
            length=500,
            fetch_all=True,
        )
        result = _normalise_erp_daily(resp)
        logger.info(f"[{label}] {asin}: {len(result)} days ({start_date} → {end_date})")
    except Exception as e:
        logger.warning(f"[{label}] ERP fetch failed for {asin}: {e}")
        result = {}

    ctx.cache[l1_key] = result
    _l2_set(ctx, result, *l2_parts)
    return result


def _fetch_yoy_sync(ctx: WorkflowContext, asin: str) -> Dict[str, Dict]:
    date_range = _yoy_date_range(ctx)
    return _fetch_erp_baseline_sync(
        ctx, asin,
        l1_key   = f"{_KEY_YOY_PERF}:{asin}",
        l2_parts = ("yoy_daily_perf", asin, date_range[0], date_range[1]),
        date_range = date_range,
        label    = "yoy_perf",
    )


def _fetch_trailing_ext_sync(ctx: WorkflowContext, asin: str) -> Dict[str, Dict]:
    date_range = _trailing_ext_date_range(ctx)
    return _fetch_erp_baseline_sync(
        ctx, asin,
        l1_key   = f"{_KEY_TRAILING_EXT}:{asin}",
        l2_parts = ("trailing_ext_perf", asin, date_range[0], date_range[1]),
        date_range = date_range,
        label    = "trailing_ext",
    )


# Causal analysis wrapper (delegates to intelligence/processors/causal_analysis)
# ---------------------------------------------------------------------------

def _run_causal_analysis(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    ProcessStep wrapper: runs the full attribution + causal pipeline
    (window attribution, ITS, CausalImpact, DML) for each item.

    Backtest baseline priority:
      P1 YoY (364d back, ERP)  → P2 trailing 3M (ERP extension) → P3 pre-window fallback
    Both ERP fetches are sync (curl_cffi), L1+L2 cached, non-fatal on error.
    """
    from src.intelligence.processors.causal_analysis import run_causal_analysis
    for item in items:
        try:
            asin               = item.get("asin", "").upper()
            daily_perf         = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])
            yoy_date_index     = _fetch_yoy_sync(ctx, asin)
            trailing_ext_index = _fetch_trailing_ext_sync(ctx, asin)
            result = run_causal_analysis(
                item, ctx.config,
                daily_perf         = daily_perf,
                yoy_date_index     = yoy_date_index     or None,
                trailing_ext_index = trailing_ext_index or None,
            )
            item.update(result)
        except Exception as e:
            logger.warning(f"[causal_analysis] Failed for {item.get('asin', '?')}: {e}")
    return items


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

_CHART_PALETTE = {
    "blue":       "#2563EB",
    "orange":     "#F59E0B",
    "red":        "#EF4444",
    "green":      "#10B981",
    "purple":     "#8B5CF6",
    "grey":       "#9CA3AF",
    "light_blue": "#BFDBFE",
    "light_red":  "#FEE2E2",
    "bg":         "#F9FAFB",
}
_C = _CHART_PALETTE  # shorthand


def _fig_to_png(fig: plt.Figure) -> bytes:
    buf = io.BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        return buf.getvalue()
    finally:
        plt.close(fig)


def _chart_lp_waterfall(item: Dict) -> Optional[bytes]:
    actions = item.get("campaign_actions") or []
    rows = [a for a in actions if a.get("current_budget", 0) > 0]
    if not rows:
        return None
    rows = sorted(rows, key=lambda a: a.get("lp_optimal_spend", 0) or 0, reverse=True)[:8]

    names   = [(a.get("campaign_name") or a.get("campaign_id") or "?")[:28] for a in rows]
    current = [a.get("actual_daily_spend", 0) or 0 for a in rows]
    lp_opt  = [a.get("lp_optimal_spend",  0) or 0 for a in rows]
    budgets = [a.get("current_budget",     0) or 0 for a in rows]
    y = np.arange(len(rows))

    fig, ax = plt.subplots(figsize=(10, max(3.5, len(rows) * 0.6)), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    ax.barh(y - 0.22, budgets,  0.22, color=_C["grey"],       alpha=0.45, label="Budget cap")
    ax.barh(y,        current,  0.22, color=_C["light_blue"], alpha=0.90, label="Actual daily spend")
    ax.barh(y + 0.22, lp_opt,   0.22, color=_C["blue"],       alpha=0.90, label="LP optimal spend")
    ax.set_yticks(y); ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel("Daily Spend ($)", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — LP Budget Allocation vs Actual  "
                 f"(+/- = LP reallocation delta)", fontsize=10, pad=6)
    ax.legend(fontsize=8, loc="lower right")
    for i, row in enumerate(rows):
        delta = (row.get("lp_optimal_spend") or 0) - (row.get("actual_daily_spend") or 0)
        if abs(delta) >= 0.5:
            ax.text(max(lp_opt[i], current[i]) + 0.5, y[i] + 0.22,
                    f"{delta:+.0f}", fontsize=7, va="center", color=_C["blue"])
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_rank_trend(item: Dict) -> Optional[bytes]:
    rank_series: Dict = item.get("natural_rank_series") or {}
    if not rank_series:
        return None
    all_dates: set = set()
    for kw_data in rank_series.values():
        all_dates.update(kw_data.keys())
    dates = sorted(all_dates)
    if len(dates) < 5:
        return None
    dt_objs = [_date_cls.fromisoformat(d) for d in dates]
    change_dates = {a.get("changed_at") for a in (item.get("change_attributions") or [])
                   if a.get("changed_at")}

    palette = [_C["blue"], _C["orange"], _C["green"], _C["purple"], _C["red"]]
    fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    for idx, (kw, kw_data) in enumerate(list(rank_series.items())[:5]):
        y_vals = [
            float(kw_data[d]["totalRank"]) if d in kw_data and kw_data[d].get("totalRank") is not None
            else float("nan")
            for d in dates
        ]
        ax.plot(dt_objs, y_vals, color=palette[idx % len(palette)],
                lw=1.5, marker="o", markersize=3, label=kw[:30])
    for cd in change_dates:
        try:
            ax.axvline(_date_cls.fromisoformat(cd), color=_C["orange"],
                       lw=1.2, linestyle="--", alpha=0.7, zorder=4)
        except Exception:
            pass
    ax.invert_yaxis()
    ax.set_ylabel("Organic Rank (lower = better)", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — Organic Rank Trend  "
                 f"(n={len(rank_series)} keywords; ↓ = improved)", fontsize=10, pad=6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    fig.autofmt_xdate(rotation=30)
    ax.legend(fontsize=7, loc="upper right")
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_upload(png: bytes, key: str) -> Optional[str]:
    try:
        from src.core.storage import get_storage_backend
        return get_storage_backend().upload(key, png, "image/png")
    except Exception as e:
        logger.warning(f"[charts] upload failed for {key}: {e}")
        return None


def _chart_daily_trend(item: Dict, daily_perf: List[Dict]) -> Optional[bytes]:
    if not daily_perf:
        return None
    by_date: Dict[str, Dict] = {}
    for r in daily_perf:
        d = r.get("date") or r.get("report_date", "")
        if not d:
            continue
        slot = by_date.setdefault(d, {"spend": 0.0, "orders": 0, "sales": 0.0})
        slot["spend"]  += float(r.get("spend",  0) or 0)
        slot["orders"] += int(r.get("orders", 0) or 0)
        slot["sales"]  += float(r.get("sales",  0) or 0)
    dates = sorted(by_date)
    if len(dates) < 3:
        return None
    dt_objs = [_date_cls.fromisoformat(d) for d in dates]
    spends  = [by_date[d]["spend"]  for d in dates]
    orders  = [by_date[d]["orders"] for d in dates]
    sales   = [by_date[d]["sales"]  for d in dates]
    acos    = [round(spends[i] / sales[i] * 100, 1) if sales[i] > 0 else None
               for i in range(len(dates))]
    change_dates = {a["changed_at"] for a in (item.get("change_attributions") or [])
                    if a.get("changed_at")}

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True,
                                   facecolor=_C["bg"])
    ax1.set_facecolor(_C["bg"]); ax2.set_facecolor(_C["bg"])

    bar_w = max(0.6, 0.8 * (dt_objs[-1] - dt_objs[0]).days / len(dt_objs))
    ax1.bar(dt_objs, spends, width=bar_w, color=_C["light_blue"], label="Spend ($)", zorder=2)
    ax1r = ax1.twinx()
    ax1r.plot(dt_objs, orders, color=_C["blue"], lw=1.8, marker="o", markersize=3,
              label="Orders", zorder=3)
    ax1r.set_ylabel("Orders", color=_C["blue"], fontsize=9)
    ax1r.tick_params(axis="y", labelcolor=_C["blue"])
    ax1.set_ylabel("Spend ($)", fontsize=9)
    ax1.legend(loc="upper left", fontsize=8); ax1r.legend(loc="upper right", fontsize=8)
    ax1.set_title(f"{item.get('asin','?')} — Daily Performance", fontsize=11, pad=6)
    for cd in change_dates:
        try:
            ax1.axvline(_date_cls.fromisoformat(cd), color=_C["orange"],
                        lw=1.2, linestyle="--", alpha=0.8, zorder=4)
        except Exception:
            pass

    acos_valid = [(dt_objs[i], acos[i]) for i in range(len(acos)) if acos[i] is not None]
    if acos_valid:
        ax2_dates, ax2_vals = zip(*acos_valid)
        ax2.plot(ax2_dates, ax2_vals, color=_C["purple"], lw=1.8, marker="o",
                 markersize=3, label="ACOS (%)")
        warn = item.get("acos_warn_threshold", 0.30)
        warn_pct = warn * 100 if warn < 2 else warn
        ax2.axhline(warn_pct, color=_C["red"], lw=1, linestyle=":", alpha=0.7,
                    label=f"Warn {warn_pct:.0f}%")
        ci_lo, ci_hi = item.get("acos_ci_lo"), item.get("acos_ci_hi")
        if ci_lo and ci_hi:
            ax2.fill_between(ax2_dates, ci_lo, ci_hi, alpha=0.15,
                             color=_C["purple"], label="ACOS 95% CI")
        ax2.set_ylabel("ACOS (%)", fontsize=9)
        ax2.legend(loc="upper left", fontsize=8)
    for cd in change_dates:
        try:
            ax2.axvline(_date_cls.fromisoformat(cd), color=_C["orange"],
                        lw=1.2, linestyle="--", alpha=0.8)
        except Exception:
            pass
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    fig.autofmt_xdate(rotation=30)
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_its_causal(item: Dict, daily_perf: List[Dict]) -> Optional[bytes]:
    attributions = item.get("change_attributions") or []
    if not attributions or not daily_perf:
        return None
    attr = next((a for a in attributions if not (a.get("its") or {}).get("skipped")), None)
    if not attr:
        return None
    change_date_str = attr.get("changed_at", "")
    if not change_date_str:
        return None

    col = {"orders": "orders", "spend": "spend", "clicks": "clicks"}.get(
        item.get("causal_metric", "orders"), "orders")
    by_date: Dict[str, float] = {}
    for r in daily_perf:
        d = r.get("date") or r.get("report_date", "")
        if d:
            by_date[d] = by_date.get(d, 0.0) + float(r.get(col, 0) or 0)

    dates = sorted(by_date)
    if len(dates) < 5:
        return None
    try:
        t0 = dates.index(change_date_str)
    except ValueError:
        cd = _date_cls.fromisoformat(change_date_str)
        t0 = min(range(len(dates)),
                 key=lambda i: abs((_date_cls.fromisoformat(dates[i]) - cd).days))
    if t0 < 3 or t0 >= len(dates) - 1:
        return None

    y       = np.array([by_date[d] for d in dates], dtype=float)
    dt_objs = [_date_cls.fromisoformat(d) for d in dates]
    A       = np.column_stack([np.ones(t0), np.arange(t0)])
    try:
        beta, _, _, _ = np.linalg.lstsq(A, y[:t0], rcond=None)
    except Exception:
        return None
    fitted = beta[0] + beta[1] * np.arange(len(dates))

    its          = attr.get("its") or {}
    level_shift  = its.get("level_shift", 0)
    ls_ci_lo     = its.get("level_shift_ci_lo")
    ls_ci_hi     = its.get("level_shift_ci_hi")

    fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    ax.scatter(dt_objs, y, color=_C["grey"], s=20, zorder=3, label="Actual")
    ax.plot(dt_objs[:t0], fitted[:t0], color=_C["blue"], lw=1.8, label="Pre-trend (fitted)")
    ax.plot(dt_objs[t0:], fitted[t0:], color=_C["blue"], lw=1.5, linestyle="--",
            alpha=0.6, label="Counterfactual")
    ax.plot(dt_objs[t0:], y[t0:], color=_C["orange"], lw=1.8, label="Post-change actual")
    ax.fill_between(dt_objs[t0:], fitted[t0:], y[t0:], alpha=0.18,
                    color=_C["green"], label="Estimated effect")
    ax.axvline(dt_objs[t0], color=_C["red"], lw=1.5, alpha=0.8, zorder=4)
    ylim = ax.get_ylim()
    ax.text(dt_objs[t0], ylim[1] * 0.95,
            f"  {attr.get('change_type','change')}", color=_C["red"], fontsize=8, va="top")
    ci_str = (f"\n95% CI [{ls_ci_lo:+.2f}, {ls_ci_hi:+.2f}]"
              if ls_ci_lo is not None and ls_ci_hi is not None else "")
    metric_lbl = (item.get("causal_metric") or "orders").capitalize()
    ax.set_title(f"{item.get('asin','?')} — ITS Causal Fit  "
                 f"(level_shift={level_shift:+.2f}{ci_str})", fontsize=10, pad=6)
    ax.set_ylabel(metric_lbl, fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    fig.autofmt_xdate(rotation=30)
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_kw_quadrant(item: Dict) -> Optional[bytes]:
    kw_perf = item.get("keyword_performance") or []
    if not kw_perf:
        return None
    valid = [i for i in range(len(kw_perf))
             if kw_perf[i].get("acos") is not None and kw_perf[i].get("total_orders") is not None]
    if not valid:
        return None
    acos_v   = np.array([kw_perf[i]["acos"]                    for i in valid], dtype=float)
    orders_v = np.array([kw_perf[i]["total_orders"]            for i in valid], dtype=float)
    spend_v  = np.array([kw_perf[i].get("total_spend") or 0   for i in valid], dtype=float)
    labels_v = [kw_perf[i].get("keyword", "")                 for i in valid]

    acos_thresh  = (item.get("acos_warn_threshold") or 0.30) * 100
    orders_mid   = float(np.median(orders_v))
    bubble_sizes = np.clip(spend_v / (spend_v.max() + 1e-9) * 600, 20, 600)
    colors = [
        _C["green"]  if a <= acos_thresh and o >= orders_mid else
        _C["red"]    if a >  acos_thresh and o <  orders_mid else
        _C["blue"]   if a <= acos_thresh else _C["orange"]
        for a, o in zip(acos_v, orders_v)
    ]

    fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    ax.scatter(orders_v, acos_v, s=bubble_sizes, c=colors, alpha=0.75,
               edgecolors="white", lw=0.5, zorder=3)
    ax.axhline(acos_thresh, color=_C["red"],  lw=1, linestyle="--", alpha=0.6,
               label=f"ACOS warn {acos_thresh:.0f}%")
    ax.axvline(orders_mid,  color=_C["grey"], lw=1, linestyle="--", alpha=0.6,
               label=f"Median orders {orders_mid:.0f}")
    for i in np.argsort(spend_v)[-5:]:
        ax.annotate(labels_v[i][:20], (orders_v[i], acos_v[i]),
                    fontsize=7, xytext=(4, 4), textcoords="offset points", alpha=0.85)
    ax.legend(handles=[
        Patch(color=_C["green"],  label="Efficient + High volume"),
        Patch(color=_C["blue"],   label="Efficient + Low volume"),
        Patch(color=_C["orange"], label="Inefficient + High volume"),
        Patch(color=_C["red"],    label="Inefficient + Low volume"),
    ], fontsize=7, loc="upper right")
    ax.set_xlabel("Orders", fontsize=9)
    ax.set_ylabel("ACOS (%)", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — Keyword ACOS × Orders  "
                 f"(bubble = spend, n={len(valid)})", fontsize=10, pad=6)
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_placement_bar(item: Dict) -> Optional[bytes]:
    placement  = item.get("placement_performance") or {}
    configured = item.get("placement_configured_pcts") or {}
    keys = [k for k in placement if placement[k].get("acos") is not None]
    if not keys:
        return None
    label_map = {
        "PLACEMENT_TOP_OF_SEARCH":  "Top of Search",
        "PLACEMENT_REST_OF_SEARCH": "Rest of Search",
        "PLACEMENT_PRODUCT_PAGE":   "Product Page",
    }
    display  = [label_map.get(k, k)               for k in keys]
    act_acos = [placement[k]["acos"]               for k in keys]
    cfg_pct  = [configured.get(k) or 0            for k in keys]
    spend_sh = [placement[k].get("spend_share", 0) for k in keys]
    x = np.arange(len(keys)); w = 0.35

    fig, ax = plt.subplots(figsize=(10, 4.0), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    bars1 = ax.bar(x - w/2, act_acos, w, color=_C["blue"],   alpha=0.85, label="Actual ACOS (%)")
    bars2 = ax.bar(x + w/2, cfg_pct,  w, color=_C["orange"], alpha=0.85, label="Configured Bid Adj. (%)")
    warn_pct = (item.get("acos_warn_threshold") or 0.30) * 100
    ax.axhline(warn_pct, color=_C["red"], lw=1, linestyle=":", alpha=0.7,
               label=f"ACOS warn {warn_pct:.0f}%")
    for bar, sh in zip(bars1, spend_sh):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{sh:.0f}%", ha="center", va="bottom", fontsize=8, color=_C["blue"])
    ax.set_xticks(x); ax.set_xticklabels(display, fontsize=9)
    ax.set_ylabel("Percent (%)", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — Placement Performance  (spend share labeled)",
                 fontsize=10, pad=6)
    ax.legend(fontsize=8)
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_inventory_burndown(item: Dict) -> Optional[bytes]:
    total     = item.get("total_available")
    daily     = item.get("daily_sales")
    risk_days = 30
    if not total or not daily or daily <= 0:
        return None
    can_sell = int(total / daily)
    x = np.arange(0, can_sell + 1)
    y = total - daily * x

    fig, ax = plt.subplots(figsize=(10, 3.8), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    ax.plot(x, y, color=_C["blue"], lw=2, label="Remaining inventory")
    ax.axhline(0, color=_C["red"], lw=1, alpha=0.5)
    risk_end = min(risk_days, can_sell)
    ax.axvspan(0, risk_end, color=_C["light_red"], alpha=0.35,
               label=f"Risk zone (<{risk_days}d)")
    ax.axvline(can_sell, color=_C["red"], lw=1.5, linestyle="--", alpha=0.8)
    stockout_date = _date_cls.today() + timedelta(days=can_sell)
    ax.text(can_sell, total * 0.05, f"  Stockout\n  {stockout_date.isoformat()}",
            color=_C["red"], fontsize=8, va="bottom")
    ax.fill_between(x, y, 0, alpha=0.08, color=_C["blue"])
    risk_flag = "⚠️ " if item.get("inventory_risk") else ""
    ax.set_xlabel("Days from today", fontsize=9)
    ax.set_ylabel("Units available", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — {risk_flag}Inventory Burn-down  "
                 f"({total:,} units / {daily:.1f}/day → {can_sell}d)", fontsize=10, pad=6)
    ax.legend(fontsize=8)
    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_comp_price_box(item: Dict) -> Optional[bytes]:
    price_by_asin: Dict = item.get("competitor_price_by_asin") or {}
    if not price_by_asin:
        return None
    asin_prices = {a: [v for v in prices.values() if v is not None]
                   for a, prices in price_by_asin.items()}
    asin_prices = {a: v for a, v in asin_prices.items() if len(v) >= 2}
    if not asin_prices:
        return None
    sorted_asins = sorted(asin_prices, key=lambda a: float(np.median(asin_prices[a])))
    data         = [asin_prices[a]   for a in sorted_asins]
    short_labels = [a[-6:]           for a in sorted_asins]
    own_price    = item.get("price") or item.get("sale_price")

    fig, ax = plt.subplots(figsize=(10, 4.5), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])
    ax.boxplot(data, labels=short_labels, patch_artist=True,
               boxprops=dict(facecolor=_C["light_blue"], color=_C["blue"]),
               medianprops=dict(color=_C["blue"], lw=2),
               whiskerprops=dict(color=_C["grey"]),
               capprops=dict(color=_C["grey"]),
               flierprops=dict(marker="o", color=_C["grey"], alpha=0.4, markersize=4))
    if own_price:
        ax.axhline(float(own_price), color=_C["red"], lw=1.8, linestyle="--",
                   label=f"Own price ${own_price:.2f}")
        ax.legend(fontsize=8)
    ax.set_xlabel("Competitor ASIN (last 6 chars)", fontsize=9)
    ax.set_ylabel("Price ($)", fontsize=9)
    ax.set_title(f"{item.get('asin','?')} — Competitor Price Distribution  "
                 f"(n={len(sorted_asins)} competitors)", fontsize=10, pad=6)
    fig.tight_layout()
    return _fig_to_png(fig)


def _generate_charts(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """Generate diagnostic PNG charts per ASIN, upload to storage, store URLs in item['chart_urls']."""
    import datetime as _dt
    from concurrent.futures import ThreadPoolExecutor, Future
    from typing import Tuple as _Tuple

    date_str    = _dt.date.today().isoformat()
    max_workers = ctx.config.get("chart_workers", 8)

    def _run_one(asin: str, name: str, fn, upload_key: str) -> Optional[str]:
        try:
            png = fn()
            if png is None:
                logger.debug(f"[charts] {asin}/{name}: skipped (no data)")
                return None
            url = _chart_upload(png, upload_key)
            if url:
                logger.info(f"[charts] {asin}/{name} → {url}")
            return url or None
        except Exception as e:
            logger.warning(f"[charts] {asin}/{name} failed: {e}", exc_info=True)
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        submitted: list[_Tuple[int, str, Future]] = []
        for item in items:
            asin       = (item.get("asin") or "unknown").upper()
            daily_perf = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])
            generators = [
                ("daily_trend",        lambda _i=item, _p=daily_perf: _chart_daily_trend(_i, _p)),
                ("its_causal",         lambda _i=item, _p=daily_perf: _chart_its_causal(_i, _p)),
                ("kw_quadrant",        lambda _i=item: _chart_kw_quadrant(_i)),
                ("placement_bar",      lambda _i=item: _chart_placement_bar(_i)),
                ("inventory_burndown", lambda _i=item: _chart_inventory_burndown(_i)),
                ("comp_price_box",     lambda _i=item: _chart_comp_price_box(_i)),
                ("lp_waterfall",       lambda _i=item: _chart_lp_waterfall(_i)),
                ("rank_trend",         lambda _i=item: _chart_rank_trend(_i)),
            ]
            for name, fn in generators:
                key = f"reports/ad_diagnosis/{asin}/{date_str}/{name}.png"
                submitted.append((id(item), name, pool.submit(_run_one, asin, name, fn, key)))

        urls_by_item: Dict[int, Dict[str, str]] = {id(i): {} for i in items}
        for item_id, name, future in submitted:
            url = future.result()
            if url:
                urls_by_item[item_id][name] = url

    for item in items:
        urls = urls_by_item[id(item)]
        item["chart_urls"] = urls
        asin = (item.get("asin") or "unknown").upper()
        logger.info(f"[charts] {asin}: {len(urls)}/8 charts uploaded")

    return items


# ---------------------------------------------------------------------------
# LLM pre-enrichment (summary injection only — no field stripping)
# ---------------------------------------------------------------------------

def _causal_reliability_tier(
    backtest_hit_rate: Optional[float],
    events_significant_pct: Optional[float],
) -> str:
    """
    AND-gate: 'high' requires BOTH historical calibration (hit_rate ≥0.70)
    AND at least one event being statistically significant in this run.
    Downgrade to 'low' if either condition fails; 'none' if no data at all.
    """
    has_calibration  = (backtest_hit_rate or 0) >= 0.70
    has_significance = (events_significant_pct is not None) and events_significant_pct > 0

    if has_calibration and has_significance:
        return "high"
    if (backtest_hit_rate or 0) > 0 or has_significance:
        return "low"
    return "none"


def _build_item_summary(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Pre-compute a flat highlights dict from a fully enriched item.
    Python-side extraction: 100% accurate, zero LLM token cost.
    Mirrors the highlights dict in the live test's _print_result.
    """
    from datetime import date as _date

    rank_series: Dict = item.get("natural_rank_series") or {}
    market_trends: Dict = item.get("market_trends") or {}
    attributions: List = item.get("change_attributions") or []
    campaigns: List = item.get("campaigns") or []
    days = ctx.config.get("days", 30)
    today = _date.today()
    data_end_date   = (today - timedelta(days=1)).isoformat()
    data_start_date = (today - timedelta(days=days)).isoformat()
    active_campaign_count = sum(1 for c in campaigns if c.get("state") == "ENABLED")
    paused_campaign_count = sum(1 for c in campaigns if c.get("state") == "PAUSED")
    return {
        "asin":                      item.get("asin"),
        "title":                     item.get("title"),
        "brand":                     item.get("brand"),
        "lookback_days":             days,
        "data_start_date":           data_start_date,
        "data_end_date":             data_end_date,
        "total_available":           item.get("total_available"),
        "inbound_receiving":         item.get("inbound_receiving"),
        "inbound_shipped":           item.get("inbound_shipped"),
        "inbound_working":           item.get("inbound_working"),
        "total_inbound":             item.get("total_inbound"),
        "can_sell_days":             item.get("can_sell_days"),
        "inventory_risk":            item.get("inventory_risk"),
        "campaign_count":            len(campaigns),
        "campaign_match_strategy":   item.get("campaign_match_strategy", "unknown"),
        "active_campaign_count":     active_campaign_count,
        "paused_campaign_count":     paused_campaign_count,
        "total_daily_budget":        item.get("total_daily_budget"),
        "bidding_strategies":        item.get("bidding_strategies"),
        "total_spend":               item.get("total_spend"),
        "total_sales":               item.get("total_sales"),
        "total_orders":              item.get("total_orders"),
        "account_acos":              item.get("account_acos"),
        "budget_exhaustion_pct":     item.get("budget_exhaustion_pct"),
        "budget_likely_exhausted":   item.get("budget_likely_exhausted"),
        "keyword_count":             item.get("keyword_count"),
        "avg_bid":                   item.get("avg_bid"),
        "match_type_dist":           item.get("match_type_dist"),
        "kw_performance_count":      len(item.get("keyword_performance", [])),
        "lp_summary":                item.get("lp_summary"),
        "lp_top_allocations":        (item.get("lp_top_allocations") or [])[:3],
        "lp_zero_keywords":          (item.get("lp_zero_keywords") or [])[:5],
        "lp_maxed_keywords":         (item.get("lp_maxed_keywords") or [])[:5],
        "campaign_actions":          (item.get("campaign_actions") or [])[:5],
        "keyword_actions":           (item.get("keyword_actions") or [])[:10],
        "ad_traffic_ratio":          item.get("ad_traffic_ratio"),
        "organic_traffic_ratio":     item.get("organic_traffic_ratio"),
        "rank_tracked_keywords":     item.get("rank_tracked_keywords"),
        "rank_series_days":          len(next(iter(rank_series.values()), {})),
        "market_trends_keywords":    list(market_trends.keys()),
        "change_attributions_count":  len(attributions),
        "attribution_suspect_count":  sum(1 for a in attributions if a.get("attribution_suspect")),
        "causal_consensus_sample":    attributions[0].get("consensus") if attributions else None,
        # Statistical sufficiency & ACOS CI
        "orders_reliability":         item.get("orders_reliability"),
        "acos_ci_lo":                 item.get("acos_ci_lo"),
        "acos_ci_hi":                 item.get("acos_ci_hi"),
        # Directional backtest calibration
        "backtest_hit_rate":          item.get("backtest_hit_rate"),
        "backtest_strong_hit_rate":   item.get("backtest_strong_hit_rate"),
        "backtest_total":             item.get("backtest_total"),
        # Per-event statistical significance (pre-computed by causal_analysis)
        "events_significant_count":   item.get("events_significant_count"),
        "events_significant_pct":     item.get("events_significant_pct"),
        # Pre-computed reliability tier — AND-gate of historical calibration AND
        # current-run event significance so LLM cannot overclaim on insignificant results:
        #   "high"   backtest_hit_rate ≥0.70 AND ≥1 event statistically significant
        #   "low"    any calibration data OR some significant events, but not both conditions
        #   "none"   no backtest data and no significant events
        "causal_reliability": _causal_reliability_tier(
            backtest_hit_rate     = item.get("backtest_hit_rate"),
            events_significant_pct = item.get("events_significant_pct"),
        ),
    }


def _prepare_for_llm(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    PURE_PYTHON step immediately before ad_diagnosis_llm.

    Injects _summary_json (Python-exact highlights) as a scalar field so
    ProcessStep auto-substitutes it into {_summary_json} in the prompt.
    Full item data is preserved — no fields are stripped.
    """
    import json as _json
    for item in items:
        item["_summary_json"] = _json.dumps(
            _build_item_summary(item, ctx), ensure_ascii=False, default=str
        )
    return items


_CHART_META: Dict[str, str] = {
    "daily_trend":        "Daily Performance Trend",
    "its_causal":         "ITS Causal Analysis",
    "kw_quadrant":        "Keyword ACOS × Orders",
    "placement_bar":      "Placement Performance",
    "inventory_burndown": "Inventory Burn-down",
    "comp_price_box":     "Competitor Price Distribution",
    "lp_waterfall":       "LP Budget Allocation",
    "rank_trend":         "Organic Rank Trend",
}

# Matches [CHART:chart_name] anywhere in the text (LLM-inserted placeholder)
_CHART_PLACEHOLDER_RE = re.compile(r'\[CHART:(\w+)\]')


def _chart_interpretation(item: Dict, name: str) -> str:
    """One-sentence business interpretation for each chart type."""
    acos_warn    = (item.get("acos_warn_threshold") or 0.30) * 100
    account_acos = item.get("account_acos")

    if name == "daily_trend":
        exh   = item.get("budget_exhaustion_pct") or 0
        acos_s = f"ACOS {account_acos:.0f}%" if account_acos else "ACOS N/A"
        above  = account_acos and account_acos > acos_warn
        return (f"Budget exhausted {exh:.0f}% of days; {acos_s} — "
                f"{'above' if above else 'at or below'} target {acos_warn:.0f}%. "
                f"Orange dashed lines = change events.")

    if name == "its_causal":
        attrs = item.get("change_attributions") or []
        if attrs:
            top = attrs[0]
            its = top.get("its") or {}
            ci  = top.get("causal_impact") or {}
            if not its.get("skipped") and its.get("level_shift") is not None:
                ls_s = f"ITS level shift {its['level_shift']:+.2f} orders/day"
            elif not ci.get("skipped") and ci.get("point_effect") is not None:
                ls_s = f"CausalImpact point effect {ci['point_effect']:+.2f} orders/day (ITS skipped)"
            else:
                ls_s = "causal models skipped (insufficient data)"
            return (f"Top event: {top.get('change_type','?')} ({top.get('changed_at','?')}) "
                    f"→ {ls_s}. Shaded area = estimated causal effect.")
        return "No attributable change event in this window."

    if name == "kw_quadrant":
        kw_perf = item.get("keyword_performance") or []
        kwd = [k for k in kw_perf if k.get("acos") is not None and k.get("total_orders") is not None]
        if kwd:
            mid = float(np.median([k["total_orders"] for k in kwd]))
            pause = sum(1 for k in kwd if k["acos"] > acos_warn and k["total_orders"] < mid)
            scale = sum(1 for k in kwd if k["acos"] <= acos_warn and k["total_orders"] >= mid)
            return (f"{pause} pause candidates (top-right: high ACOS + low vol); "
                    f"{scale} scale candidates (bottom-right: efficient + high vol). Bubble = spend.")
        return "Keyword ACOS vs orders quadrant. Bubble size = spend."

    if name == "placement_bar":
        tos      = (item.get("placement_performance") or {}).get("PLACEMENT_TOP_OF_SEARCH") or {}
        tos_acos = tos.get("acos")
        if tos_acos:
            rec = "reduce TOS bid adjustment" if tos_acos > acos_warn else "TOS within target"
            return f"TOS ACOS {tos_acos:.0f}% vs target {acos_warn:.0f}% → {rec}. Bar height = %; label = spend share."
        return "Compare actual ACOS (blue) vs configured bid adjustment (orange) per placement."

    if name == "inventory_burndown":
        can_sell   = item.get("can_sell_days") or 0
        risk       = item.get("inventory_risk", False)
        inb_recv   = item.get("inbound_receiving") or 0
        inb_ship   = item.get("inbound_shipped") or 0
        inb_work   = item.get("inbound_working") or 0
        inb_parts  = []
        if inb_recv: inb_parts.append(f"{inb_recv} units receiving (1-2d)")
        if inb_ship: inb_parts.append(f"{inb_ship} units shipped (10-30d ETA)")
        if inb_work: inb_parts.append(f"{inb_work} units planned (not yet shipped)")
        inb_note   = f" Inbound: {', '.join(inb_parts)}." if inb_parts else ""
        lp  = item.get("lp_summary") or {}
        rec = lp.get("recommended_stock_units")
        gap = lp.get("stock_shortfall") or 0
        rec_note = (
            f" LP plan requires {rec} units ({lp.get('stock_gate_days',21)}d buffer); "
            f"shortfall {gap} units — procure before activating gated actions."
            if rec and gap > 0 else
            f" LP plan requires {rec} units — current stock sufficient." if rec else ""
        )
        if risk:
            return (
                f"⚠ Current stock ~{can_sell:.0f} days — budget/bid increases are gated until "
                f"effective stock ≥ 21 days.{inb_note}{rec_note}"
            )
        return f"Inventory covers ~{can_sell:.0f} days — sufficient runway for current scaling plans.{inb_note}{rec_note}"

    if name == "comp_price_box":
        own_price  = item.get("price") or item.get("sale_price")
        comp_flat  = [v for prices in (item.get("competitor_price_by_asin") or {}).values()
                      for v in prices.values() if v is not None]
        if own_price and comp_flat:
            pct = sum(1 for p in comp_flat if p < float(own_price)) / len(comp_flat) * 100
            pos = "above" if float(own_price) > float(np.median(comp_flat)) else "below"
            return f"Own price ${float(own_price):.2f} is {pos} competitor median; higher than {pct:.0f}% of sampled prices."
        return "Competitor price distribution vs own price (red dashed line)."

    if name == "lp_waterfall":
        lp      = item.get("lp_summary") or {}
        gap     = lp.get("order_gap") or 0
        ceiling = lp.get("spend_ceiling_bound", False)
        if ceiling:
            return (f"LP is ceiling-bound (spend ${lp.get('lp_optimal_spend',0):.0f} "
                    f"vs budget ${lp.get('daily_budget',0):.0f}) — "
                    f"expand keyword coverage to unlock remaining budget.")
        return (f"Ad order gap {gap:+.1f}/day (LP-projected vs actual ad-attributed orders) — "
                f"rebalancing spend could gain {abs(gap):.1f} ad orders/day. "
                f"Blue bar = LP target; grey = budget cap. Organic orders not included.")

    if name == "rank_trend":
        n = len(item.get("natural_rank_series") or {})
        return (f"Organic rank for {n} keyword(s). Downward slope = improving position. "
                f"Orange lines = ad change events. Correlate rank drops with bid/budget cuts.")

    return ""


def _export_report(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    PURE_PYTHON step after ad_diagnosis_llm.

    Writes the LLM report to data/reports/ad_diagnosis_{ASIN}_{date}.md and
    sets item["report_file_path"] so FeishuCallback.on_complete sends it as a
    file attachment automatically.

    Also sets item["response"] to a short preview so the card branch in
    on_complete shows a summary card instead of trying to upload the full
    text as a second attachment.
    """
    import os
    import datetime as _dt

    report_dir = os.path.abspath("data/reports")
    os.makedirs(report_dir, exist_ok=True)
    date_str = _dt.date.today().isoformat()

    for item in items:
        text = item.get("ad_diagnosis_llm") or ""
        if not text:
            continue
        asin = (item.get("asin") or "unknown").upper()

        chart_urls: dict = item.get("chart_urls") or {}
        if chart_urls:
            interps = {name: _chart_interpretation(item, name) for name in chart_urls}
            placed: set = set()

            def _replace(m, _urls=chart_urls, _interps=interps, _placed=placed):
                name = m.group(1)
                url = _urls.get(name)
                if not url:
                    return ""  # chart not generated; strip placeholder
                label  = _CHART_META.get(name, name)
                interp = _interps.get(name, "")
                _placed.add(name)
                return (
                    f"\n> *{interp}*\n\n![{label}]({url})\n"
                    if interp else f"\n![{label}]({url})\n"
                )

            text = _CHART_PLACEHOLDER_RE.sub(_replace, text)

            # Append chart URLs that had no placeholder in the LLM output
            remaining = {n: u for n, u in chart_urls.items() if n not in placed}
            if remaining:
                lines = ["\n\n---\n\n## 📊 Diagnostic Charts\n\n"]
                for name, url in remaining.items():
                    label  = _CHART_META.get(name, name)
                    interp = interps.get(name, "")
                    lines.append(f"### {label}\n\n")
                    if interp:
                        lines.append(f"> *{interp}*\n\n")
                    lines.append(f"![{label}]({url})\n\n")
                text = text + "".join(lines)

        filename = f"ad_diagnosis_{asin}_{date_str}.md"
        file_path = os.path.join(report_dir, filename)
        chart_note = f"，含 {len(chart_urls)} 张诊断图表" if chart_urls else ""
        # Set response before file write so it's always available even if write fails.
        item["response"] = (
            text[:400].rstrip()
            + f"\n\n…（完整报告已保存为 `{filename}`{chart_note}，正在发送为附件）"
        )
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(text)
            item["report_file_path"] = file_path
            logger.info(f"[export_report] {asin}: {len(text)} chars, "
                        f"{len(chart_urls)} charts → {file_path}")
        except OSError as e:
            logger.error(f"[export_report] {asin}: failed to write report file: {e}")
    return items


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
        # Order metrics: fetch real (organic + ad) unit sales via SP-API getOrderMetrics.
        # Runs after fetch_inventory so total_available is already set, allowing
        # can_sell_days to be computed accurately here rather than relying on the
        # ad-orders-only fallback in _enrich_performance.
        EnrichStep(
            name="fetch_order_metrics",
            extractor_fn=_enrich_order_metrics,
            parallel=True,
            concurrency=3,
        ),
        # Covariate time series: daily price/promotion/rating from Xiyouzhaoci.
        # Runs in Stage 1 (independent of campaign data) so the series is ready
        # before run_causal_analysis annotates each change event with covariate context.
        EnrichStep(
            name="fetch_covariates",
            extractor_fn=_enrich_covariates,
            parallel=True,
            concurrency=3,
            enabled=config.get("enable_xiyou", True),
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

        # ── Stage 4: keyword-level + placement + competitor prices (parallel) ─
        EnrichStep(
            name="fetch_keyword_performance",
            extractor_fn=_enrich_keyword_performance,
            parallel=True,
            concurrency=5,
        ),
        EnrichStep(
            name="fetch_placement",
            extractor_fn=_enrich_placement,
            parallel=True,
            concurrency=5,
        ),
        # Depends on fetch_catalog (brand name for keyword filtering).
        # Runs here (after Stage 1) so brand is already in item.
        EnrichStep(
            name="fetch_competitor_prices",
            extractor_fn=_enrich_competitor_prices,
            parallel=True,
            concurrency=2,   # each call spawns up to 15 sub-requests; keep low
            enabled=config.get("enable_xiyou", True),
        ),

        # ── Stage 5a: temporal — daily performance + change history ───────────
        EnrichStep(
            name="fetch_change_history",
            extractor_fn=_enrich_change_history,
            parallel=True,
            concurrency=5,
        ),

        # ── Stage 5b: LP budget optimisation (pure Python, OR-Tools) ─────────
        ProcessStep(
            name="optimize_budget",
            fn=_optimize_budget,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),

        # ── Stage 5c: causal analysis (window attribution + ITS/CI/DML) ──────
        # Absorbs the old correlate_changes step: produces change_attributions
        # with ITS/CausalImpact/DML results embedded per event.
        # Runs before stage 6 so statistical evidence reaches the LLM prompt
        # even when rank/trends data is not yet available.
        ProcessStep(
            name="run_causal_analysis",
            fn=_run_causal_analysis,
            compute_target=ComputeTarget.PURE_PYTHON,
            enabled=config.get("enable_causal_analysis", True),
        ),

        # ── Stage 6: Xiyouzhaoci keyword signals ─────────────────────────────
        EnrichStep(
            name="fetch_xiyou_rankings",
            extractor_fn=_enrich_xiyou_rankings,
            parallel=True,
            concurrency=3,
            enabled=config.get("enable_xiyou", True),
        ),
        # Fetches natural_rank_series + market_trends in one step.
        # Keywords selected once via _select_rank_keywords; both API calls
        # (rank trends, SFR trends) run concurrently inside the enricher.
        # Requires keyword_performance (Stage 4) and lp_top_allocations (Stage 5b).
        EnrichStep(
            name="fetch_keyword_signals",
            extractor_fn=_enrich_keyword_signals,
            parallel=True,
            concurrency=2,   # each call issues 2 Xiyou requests; keep low
            enabled=config.get("enable_xiyou", True),
        ),

        # ── Stage 7a: inject pre-computed summary before LLM ─────────────────
        # Injects _summary_json (Python-exact highlights) into each item so the
        # prompt can reference pre-computed values without asking the LLM to
        # re-derive them from the full JSON. No fields are removed.
        ProcessStep(
            name="prepare_for_llm",
            fn=_prepare_for_llm,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),

        # ── Stage 7b: LLM diagnostic synthesis ───────────────────────────────
        # batch_threshold=1: always use Batch API (data collection takes 30+ min,
        # so async batch adds no perceived latency; 50% cost saving on large payloads).
        ProcessStep(
            name="ad_diagnosis_llm",
            batch_threshold=1,
            prompt_template=(
                "You are a senior Amazon advertising analyst. Analyse the following enriched "
                "advertising dataset for {count} ASIN(s) and produce a structured diagnostic "
                "report strictly following the output format below.\n\n"
                "Report date: {report_date}  (use this to interpret relative dates — "
                "e.g., a change_attribution event on 2026-04-17 is 10 days ago; "
                "the last entry in natural_rank_series is today's rank.)\n\n"
                "Pre-computed highlights (authoritative — do not re-derive these values):\n"
                "{_summary_json}\n\n"
                "Full enriched data (use for keyword-level, attribution, rank-series, "
                "trend, and competitor analysis):\n{items_json}\n\n"

                # ── Output format ────────────────────────────────────────────
                "==== OUTPUT FORMAT (repeat for each ASIN) ====\n\n"

                "## ASIN: {{asin}} – Severity: 🟢 Healthy / 🟡 Warning / 🔴 Critical\n\n"

                "### Quick Metrics Snapshot\n\n"
                "The block below is the authoritative pre-computed summary. "
                "Render ALL fields as a three-column table (Field | Value | Source / How derived). "
                "Use `null` as-is — do NOT substitute guesses. "
                "Do NOT re-derive any value from the full JSON.\n\n"
                "Column definitions:\n"
                "- **Field**: exact key name from the JSON below.\n"
                "- **Value**: exact value (keep null as `—`).\n"
                "- **Source / How derived**: one short phrase from the legend below. "
                "⚠️ Do NOT write 'Pre-computed highlights', 'summary', or any generic phrase — "
                "every row must have a *specific* source (e.g. 'SP-API Catalog', "
                "'FBA Inventory API', 'Ads API spCampaigns report', etc.).\n\n"
                "Legend (Field → derivation):\n"
                "  title / brand / size / bullet_point_count → SP-API Catalog\n"
                "  lookback_days → days config (default 30); period covered: data_start_date → data_end_date\n"
                "  data_start_date / data_end_date → reporting window used for all time-series fields "
                "(today − lookback_days → yesterday; use these when describing 'past N days')\n"
                "  total_available → fulfillable FBA units (point-in-time)\n"
                "  inbound_receiving / inbound_shipped / inbound_working → FBA inbound tiers: "
                "receiving=at FC being checked in (1-2d, certain); "
                "shipped=in transit from seller (10-30d depending on sea/domestic); "
                "working=shipment plan created, not yet shipped (uncertain ETA — do NOT count as arriving soon)\n"
                "  total_inbound = inbound_receiving + inbound_shipped (confirmed in-transit only)\n"
                "  effective_stock_days (in campaign_actions/keyword_actions prerequisite) = "
                "(total_available + inbound_receiving + catchable_shipped) / daily_sales; "
                "catchable_shipped = inbound_shipped only when inbound_lead_days < can_sell_days\n"
                "  prerequisite field on actions → inventory gate triggered: action downgraded to P2 "
                "because effective_stock_days < stock_gate_days (default 21). "
                "When reporting these actions, state the prerequisite condition clearly and recommend "
                "the seller confirm inbound ETA before activating.\n"
                "  daily_sales / daily_sales_source → "
                "daily_sales = total units sold ÷ lookback_days (ALL channels: ad-attributed + organic). "
                "Source priority: "
                "(1) caller-supplied, "
                "(2) order_metrics = SP-API getOrderMetrics (ad + organic, most accurate), "
                "(3) ad_orders_only = spCampaigns ad-attributed orders ÷ days only — "
                "organic orders excluded, so daily_sales is a LOWER BOUND on true consumption velocity. "
                "When reporting inventory runway, always state the source. "
                "When daily_sales_source=ad_orders_only, warn that true stockout will occur SOONER "
                "than can_sell_days indicates (organic sales consume inventory too).\n"
                "  can_sell_days → total_available ÷ daily_sales "
                "(null if daily_sales unavailable; "
                "when daily_sales_source=ad_orders_only: this is an UPPER BOUND — "
                "actual stockout is sooner because organic orders are excluded from the denominator; "
                "can_sell_days_note field contains a human-readable caveat)\n"
                "  inventory_risk → can_sell_days < inventory_risk_days config (default 30 d)\n"
                "  campaign_count → count of campaigns whose name contains the ASIN\n"
                "  campaign_match_strategy → how campaigns were matched to this ASIN: "
                "explicit_config (most reliable) | spAdvertisedProduct (ground truth) | "
                "name_substring (fragile — ASIN in campaign name; may include unrelated campaigns) | none\n"
                "  active_campaign_count → campaigns with state=ENABLED\n"
                "  paused_campaign_count → campaigns with state=PAUSED\n"
                "  total_daily_budget → sum of ENABLED campaign daily budgets (Ads API, current config)\n"
                "  bidding_strategies → distinct strategies across matched campaigns\n"
                "  total_spend / total_sales / total_orders / total_clicks → "
                "spCampaigns ad-attributed performance (ad orders only, organic excluded), "
                "summed over matched campaigns, period: data_start_date → data_end_date. "
                "NOTE: total_orders here is AD-ATTRIBUTED orders only. "
                "True total unit sales (ad + organic) = daily_sales × lookback_days "
                "when daily_sales_source=order_metrics.\n"
                "  account_acos → total_spend ÷ total_sales × 100 "
                "(data_start_date → data_end_date)\n"
                "  budget_exhaustion_pct → total_spend ÷ (total_daily_budget × lookback_days) "
                "(data_start_date → data_end_date)\n"
                "  budget_likely_exhausted → budget_exhaustion_pct > 90% threshold\n"
                "  keyword_count / avg_bid / min_bid / max_bid / match_type_dist → "
                "Ads API keyword list for matched campaigns (current config, not time-series)\n"
                "  kw_performance_count → rows in spSearchTerm report with ≥ min_clicks_for_cvr clicks "
                "(data_start_date → data_end_date)\n"
                "  lp_summary / lp_top_allocations / lp_zero_keywords / lp_maxed_keywords → "
                "OR-Tools LP (maximise ad orders; constraints: C1 global budget, C2 per-campaign budget caps, "
                "C3 target ACOS linearised, C4 click floor/ceiling; "
                "inventory is NOT a LP constraint — replenishment timing is too unstable to model as a hard bound; "
                "eff_cpc = avg_cpc × bidding_strategy_multiplier × placement_multiplier; "
                "pessimistic CVR shrinkage applied for low-sample keywords). "
                "lp_optimal_orders_pessimistic uses shrunk CVR (may be < actual_daily_ad_orders — expected). "
                "lp_optimal_orders_raw uses the same raw CVR as actual_daily_ad_orders. "
                "actual_daily_ad_orders = spCampaigns ad-attributed orders ÷ days (AD ORDERS ONLY — "
                "organic orders not included; true daily total consumption is higher). "
                "order_gap = lp_optimal_orders_raw − actual_daily_ad_orders "
                "(positive = LP reallocates budget to gain more ad orders; "
                "negative — two distinct causes: "
                "(1) spend_ceiling_bound=true: click ceilings prevent the LP from using the full budget — "
                "order_gap is ceiling-limited, primary action is expand keywords / raise bids; "
                "(2) budget_binding=true (lp_optimal_spend ≥ 85% of daily_budget): budget is exhausted and "
                "Wilson CVR shrinkage causes LP's estimated orders to fall below actual historical — "
                "this is an artifact of pessimistic estimation, NOT evidence the current allocation beats LP; "
                "campaign_actions already corrects for this by allowing increase_budget when ACOS is healthy). "
                "CRITICAL — LP budget split (read carefully before writing the Budget section):\n"
                "  lp_scope_campaign_daily_budget = sum of LP-scope campaign daily budgets — the actual global "
                "constraint given to LP. LP cannot spend more than this.\n"
                "  total_account_daily_budget = sum of ALL enabled campaign daily budgets (LP-scope + non-LP-scope).\n"
                "  lp_scope_daily_spend = historical daily spend for LP-scope campaigns (what they actually spent).\n"
                "  non_lp_scope_daily_spend = historical daily spend for non-LP campaigns (auto/PT + untracked keywords).\n"
                "  lp_optimal_spend = what LP recommends spending across LP-scope keywords.\n"
                "FORBIDDEN: do NOT add lp_optimal_spend + non_lp_scope_daily_spend and compare to "
                "total_account_daily_budget — these come from SEPARATE campaign budget pools and adding them "
                "produces a meaningless number that is always larger than the account budget. "
                "The correct comparisons are:\n"
                "  lp_optimal_spend vs lp_scope_campaign_daily_budget (is LP using its allocated budget?)\n"
                "  lp_scope_daily_spend vs lp_scope_campaign_daily_budget (did LP-scope campaigns historically hit cap?)\n"
                "  non_lp_scope_daily_spend vs (total_account_daily_budget − lp_scope_campaign_daily_budget) "
                "(did non-LP campaigns hit their budget cap?)\n"
                "budget_source: 'campaign_snapshot' = current campaign budget snapshot; "
                "'historical_avg_spend' = historical_daily_spend × 0.90 used instead "
                "because discrepancy exceeded Amazon's 25% daily pacing allowance (structural budget change detected). "
                "budget_overage_ratio = historical_daily_spend ÷ campaign_snapshot_budget: "
                "ratio ≤ 1.25 is normal Amazon pacing (campaigns regularly hit cap → Amazon allows up to 25% daily overage); "
                "ratio > 1.25 indicates campaigns ran at higher budgets during the period (budget was cut recently). "
                "When budget_overage_ratio is 1.20–1.25 and budget_source='campaign_snapshot': "
                "actual achievable spend ≈ total_account_daily_budget × budget_overage_ratio — "
                "mention this in the report as 'Amazon budget pacing: account consistently hits cap, "
                "actual spend runs ~X% above set budget'; this is NOT a data error. "
                "Do NOT flag budget_overage_ratio ≤ 1.25 as a discrepancy or data quality issue. "
                "spend_ceiling_bound=true means click ceilings (C4) are the binding constraint, NOT the budget "
                "(lp_optimal_spend < 60% of daily_budget); in this case: "
                "(a) lp_zero/maxed keyword signals remain valid as ROI signals; "
                "(b) primary recommendation is expand keyword coverage or raise bids (not increase budget). "
                "budget_binding=true means the global budget cap is fully consumed; "
                "a negative order_gap when budget_binding=true is expected and should NOT be flagged as a caveat. "
                "placement_data_unknown=true means all spend was reported under 'UNKNOWN' placement — "
                "do NOT make placement-specific recommendations in this case. "
                "recommended_stock_units = (organic_daily + lp_optimal_orders_raw) × stock_gate_days — "
                "total units required to safely execute the LP plan for stock_gate_days (default 21d) without stockout risk; "
                "daily_consumption = organic daily orders + lp_optimal_orders_raw (total units consumed per day if LP is executed); "
                "daily_consumption_source: 'order_metrics' = ad+organic (accurate), 'ad_orders_only' = underestimate (organic excluded — real shortfall is larger); "
                "inbound_receiving / inbound_shipped / inbound_working / total_inbound are top-level snapshot fields (not inside lp_summary); "
                "confirmed_inbound = inbound_receiving + inbound_shipped (units already in transit, offset against shortfall); "
                "inbound_working = shipment plan not yet shipped — NOT counted in confirmed_inbound (arrival timing unknown); "
                "stock_shortfall = max(0, recommended_stock_units − total_available − confirmed_inbound) — "
                "net units still to procure after accounting for current stock and confirmed inbound; "
                "when stock_shortfall > 0: lead with inventory procurement recommendation before bid/budget actions; "
                "when reporting: state the full breakdown — current stock X units + inbound_receiving Y units (1-2d) + inbound_shipped Z units (ETA varies) = coverage; "
                "if inbound_working > 0, note it separately as 'planned but not shipped — confirm dispatch urgently'.\n"
                "  campaign_actions → two evaluation paths based on LP scope:\n"
                "    (a) LP-scoped campaigns (manual keyword targeting with click data): "
                "action derived from LP camp_spend vs actual campaign budget; "
                "rationale will NOT contain 'outside LP scope'.\n"
                "    (b) Non-LP-scope campaigns (auto-targeting, product-targeting, or manual with zero click data): "
                "action derived from ACOS only — high ACOS → decrease_budget/review_bids, "
                "healthy ACOS + saturated → increase_budget, otherwise maintain; "
                "rationale will contain '(auto/PT, outside LP scope)'; "
                "for high-ACOS auto campaigns recommend adding NEGATIVE keywords/targets, NOT lowering keyword bids; "
                "for high-ACOS PT campaigns recommend removing or negating underperforming targets.\n"
                "  campaign_state field present; action ∈ {{increase_budget, decrease_budget, review_bids, "
                "pause_candidate, archive_candidate, enable_and_increase_budget, enable_and_review_bids, maintain}}; "
                "priority ∈ {{P0, P1, P2}}; actions prefixed enable_and_* mean campaign is PAUSED — "
                "re-enable BEFORE any budget/bid change\n"
                "  keyword_actions → LP-derived keyword-level recommendations; "
                "action ∈ {{pause_keyword, increase_bid, decrease_bid}}; "
                "fields: keyword_text, match_type, campaign_id, keyword_id (Ads API id for direct API call), "
                "current_bid, keyword_acos_pct, rationale; "
                "pause_keyword: expected_order_delta (orders/day lost by pausing, negative), "
                "expected_spend_delta (spend/day saved, negative); "
                "increase_bid: estimated_order_uplift_per_10pct_bid, expected_spend_per_10pct_bid; "
                "decrease_bid: expected_order_delta (negative), expected_spend_delta (negative = savings); "
                "campaign_actions: expected_order_delta and expected_spend_delta quantify daily impact\n"
                "  ad_traffic_ratio / organic_traffic_ratio / traffic_growth_7d → "
                "Xiyouzhaoci traffic score API (latest available snapshot)\n"
                "  rank_tracked_keywords / rank_series_days → "
                "Xiyouzhaoci daily organic rank trends; rank_series_days = days with rank data "
                "within data_start_date → data_end_date for the top tracked keyword\n"
                "  market_trends_keywords → keywords with SFR weekly trend data (Xiyouzhaoci)\n"
                "  change_attributions_count → change events that passed noise filter (Ads API history, "
                "data_start_date → data_end_date)\n"
                "  causal_consensus_sample → consensus label of first change_attribution entry "
                "(ITS + CausalImpact + DML agreement for THAT SPECIFIC EVENT: "
                "Strong / Moderate / Weak / Confounded / Conflicting / Skipped). "
                "This is PER-EVENT model agreement — entirely separate from causal_reliability:\n"
                "    causal_reliability = historical calibration quality (how accurate the model has been ACROSS PAST EVENTS)\n"
                "    causal_consensus_sample = whether the 3 models agree ON THIS SPECIFIC EVENT\n"
                "  These two can contradict: high causal_reliability + Conflicting consensus is valid and means "
                "'the model is generally well-calibrated, but the 3 models disagree on this particular event — "
                "treat this event's causal direction as uncertain'. "
                "Do NOT use causal_reliability='high' to override a Conflicting/Weak consensus on a specific event.\n"
                "  orders_reliability → statistical sufficiency of orders sample: "
                "high (≥100 orders), medium (30–99), low (<30)\n"
                "  acos_ci_lo / acos_ci_hi → 95% ACOS confidence interval (Wilson method on CVR "
                "propagated to ACOS via ACOS = spend/sales; see Methodology section)\n"
                "  backtest_hit_rate → stored as 0–1 fraction (e.g. 0.85 = 85%, 1.0 = 100%); "
                "fraction of evaluated change events where model-predicted direction matched observed post-window KPI direction; "
                "threshold: <0.70 = near-random (causal_reliability='low'/'none'), ≥0.70 = reliable ('high')\n"
                "  backtest_strong_hit_rate → same but restricted to 'Strong evidence' events only\n"
                "  events_significant_count / events_significant_pct → how many change events in THIS run "
                "had at least one model produce a statistically significant result (p<0.05 + CI not crossing zero). "
                "events_significant_pct=0.0 means ALL events were insignificant in this run.\n"
                "  causal_reliability → pre-computed AND-gate tier: 'high' requires BOTH backtest_hit_rate ≥0.70 "
                "AND events_significant_pct > 0; 'low' if either condition fails but some data exists; "
                "'none' if no calibration or significance data. "
                "Use this field (not the raw fraction) to apply Rule 4 label-downgrade logic\n\n"
                "```json\n{_summary_json}\n```\n\n"
                "> **Evidence Summary**: 1-2 sentences citing the single most critical metric "
                "visible in the snapshot above "
                "(e.g., ACOS 45% vs. 30% warn threshold, budget exhausted 27/30 days, "
                "organic rank dropped from #12 → #28 over 10 days, "
                "can_sell_days=null → daily_sales not resolved). "
                "If a key field is null, call it out explicitly.\n\n"

                "### Diagnostic Findings\n\n"
                "Each bullet must include a direct data citation in *italics*.\n\n"
                "- **Budget**: [Insight] — "
                "*Source: daily_budget=$val, budget_likely_exhausted=T/F over N days*\n"
                "- **Bids vs. Competition**: [Insight] — "
                "*Source: avg_bid=$val, high_acos_keywords=[kw1(ACOS%), kw2(ACOS%)]*\n"
                "- **Placement Strategy**: [Insight] — "
                "*Source: TOS ACOS=$val (modifier=$x%, spend_share=$y%) | "
                "PP ACOS=$val (modifier=$z%, spend_share=$w%) → mismatch if any*\n"
                "- **Keywords**: [Insight] — "
                "*Source: keyword_count=$n, match_type_dist={{exact:$a%, phrase:$b%}}, "
                "high_acos_keywords=[...]*\n"
                "- **Organic & Market**: [Insight] — "
                "*Source: ad_traffic_ratio=$val, organic_rank_trend=improving/stable/declining "
                "for kw=$kw; SFR trend up/down in week YYYY-Www → demand increased/decreased*\n"
                "- **Inventory**: [Insight] — "
                "*Source: inventory_risk=T/F, can_sell_days=$d*\n"
                "- **Profitability**: [Insight] — "
                "*Source: account_acos=$val%, TACOS=$val% (if tacos available), "
                "net_profit_after_ads=$val (if cogs+price provided)*\n"
                "- **LP Budget Optimisation**: [Insight] — "
                "*Source: order_gap=$val orders left on table, "
                "lp_zero_keywords=[kw1, kw2], lp_maxed_keywords=[kw3], "
                "lp_top_allocations=[kw4: $budget_share%]*\n"
                "- **Change Attribution (past actions)**:\n"
                "  - ✅ Positive: $change_type on $date → Δ orders = +$x "
                "(causal confidence: high/med/low)\n"
                "  - ❌ Negative: $change_type on $date → Δ ACOS = +$y pp "
                "(causal confidence: high/med/low)\n"
                "  - ⚠️ Confounded: $change_type coincided with promotion/price cut → "
                "effect cannot be isolated to bid/budget\n\n"

                "### Causal Confidence Assessment\n\n"
                "For each change_attribution entry cited above:\n"
                "- **Consensus**: Strong / Moderate / Weak / Confounded / Conflicting / Skipped "
                "(read from the `consensus` field directly — this is per-event model agreement, "
                "independent of causal_reliability; "
                "causal_reliability='high' means the model is historically well-calibrated, "
                "but a Conflicting consensus still means THIS event's direction is uncertain — "
                "do NOT use causal_reliability to override a Conflicting/Weak consensus)\n"
                "- **ITS** *(Linden 2015)*: if its.skipped=True write 'Skipped (reason: $reason)'; "
                "otherwise level_shift=$val [95% CI: level_shift_ci_lo – level_shift_ci_hi], "
                "p=$p_val (significant if p < 0.10)\n"
                "- **CausalImpact** *(Brodersen et al. 2015)*: if causal_impact.skipped=True write 'Skipped (reason: $reason)'; "
                "otherwise point_effect=$val [95% credible interval: ci_lo – ci_hi] (actual − BSTS counterfactual)\n"
                "- **DML** *(Chernozhukov et al. 2018)*: if dml.skipped=True write 'Skipped (reason: $reason)'; "
                "otherwise theta=$val [95% CI: theta_ci_lo – theta_ci_hi] (sandwich-SE; reliable only if r_squared ≥ 0)\n"
                "- **Historical calibration**: backtest_hit_rate=$val "
                "(stored as 0–1 fraction; display as percentage, e.g. 1.0 → '100%'; "
                "model direction vs observed post-window direction for this ASIN)\n"
                "- **Note**: Use 'causally demonstrated' only when consensus = 'Strong evidence' "
                "(all 3 models agree). Use 'suggested' or 'estimated' otherwise.\n"
                "  When had_promotion=True or price_delta_window is large negative: "
                "do not attribute lift solely to the ad change — flag as partially confounded.\n"
                "  Use competitor_price_summary {{min, max, median}} time series to explain "
                "sustained ACOS/CVR shifts that correlate with competitor price moves "
                "rather than internal bid changes.\n\n"

                "### Top 5 Prioritised Actions (most impactful first)\n\n"
                "For each action use this sub-structure:\n\n"
                "> **Action #N**: [Specific action with concrete numbers, "
                "e.g., increase TOS modifier from 10% → 30%]\n"
                "> - **Rationale**: [1-2 sentences with data citations]\n"
                ">   *Evidence: placement_performance.TOS.acos=$val, spend_share=$val%*\n"
                "> - **Expected impact**: [Quantified estimate] "
                "*(assuming CVR and CTR remain stable)*\n"
                "> - **Risks / caveats**: [What to monitor; set a time-box, e.g., 7 days]\n"
                "> - **Reference change attribution**: [Cite a past change_attribution entry "
                "if a similar action was taken; include the consensus confidence level. "
                "If none exists, write 'No historical reference available.']\n\n"

                "### Account Health Summary\n\n"
                "- **Overall Health Score**: 🟢 / 🟡 / 🔴 "
                "(based on proportion of ASINs at each severity level)\n"
                "- **Biggest Win (Last 30d)**: $change_type on $date for ASIN $x → "
                "saved/earned $impact (cite change_attribution)\n"
                "- **Biggest Risk**: One sentence on the highest-severity unresolved issue "
                "across all ASINs\n"
                "- **Single Most Important Action (account-wide)**: Repeat the #1 action "
                "if it applies globally\n"
                "- **Monitoring Recommendation**: Which 1-2 metrics to track weekly\n\n"

                "### Statistical Methodology\n\n"
                "| Method | Citation | Role in this report |\n"
                "| --- | --- | --- |\n"
                "| ITS | Linden (2015), *Stata J.* 15(2):480–500 | "
                "Piecewise OLS: y=α+β·t+γ·D+δ·(t−T₀)·D+ε; γ=level_shift (immediate step); "
                "95% CI from OLS t-distribution SE |\n"
                "| CausalImpact | Brodersen et al. (2015), *Ann. Appl. Stat.* 9(1):247–274 | "
                "BSTS counterfactual; point_effect=actual−predicted; 95% posterior credible interval |\n"
                "| DML | Chernozhukov et al. (2018), *Econometrics J.* 21(1):C1–C68 | "
                "Frisch–Waugh–Lovell with RF residualisation; θ=clean causal effect; "
                "95% CI from heteroscedasticity-robust sandwich SE |\n"
                "| LP | Dantzig (1963), *Linear Programming & Extensions* / OR-Tools | "
                "max Σorders_i·x_i s.t. Σx_i≤daily_budget, x_i≤headroom·clicks_i·bid_i |\n"
                "| ACOS CI | Wilson (1927), *JASA* 22:209–212 | "
                "95% CI on CVR (Wilson score) propagated: ACOS_CI = ACOS_point × CVR_point / CVR_CI |\n\n"
                "*All CIs at 95% (α=0.05). Significance threshold: p<0.10 (ITS/DML, one-sided) "
                "given limited post-period samples typical in weekly ad cadence.*\n\n"

                # ── Credibility & actionability rules ────────────────────────
                "==== MANDATORY RULES (violations degrade report quality) ====\n\n"
                "1. Every claim must be accompanied by a direct data reference "
                "(field name + value from the JSON). No assertion without citation.\n"
                "2. When recommending a bid or budget change, provide a numerical bound "
                "(e.g., 'raise from $0.50 to $0.75') AND cite the pre-computed "
                "expected_order_delta / expected_spend_delta from keyword_actions or campaign_actions. "
                "Write it as: 'Expected: $X spend saved/day, −Y orders/day' or "
                "'Expected: +$X spend/day, +Y orders/day (ACOS Z%)'. "
                "For increase_bid, cite estimated_order_uplift_per_10pct_bid. "
                "Cite a historical change_attribution entry if available.\n"
                "3. For any 'expected impact' estimate, state the assumption explicitly "
                "(e.g., 'assuming CVR and CTR remain stable; linear impression-share elasticity assumed'). "
                "Mark increase_bid order estimates as approximate: "
                "'~+N orders/day per +10% bid (impression elasticity unvalidated)'.\n"
                "4. Uncertainty labelling — two independent dimensions, BOTH must pass:\n"
                "   Dimension A — causal_reliability (pre-computed AND-gate, ASIN-level):\n"
                "   causal_reliability='high': backtest_hit_rate ≥0.70 AND ≥1 event statistically significant "
                "(p<0.05, CI not crossing zero) in this run — model is both historically calibrated AND currently significant.\n"
                "   causal_reliability='low'/'none': at least one gate failed — downgrade all labels regardless of consensus.\n"
                "   Dimension B — consensus (per-event model agreement):\n"
                "   consensus='Strong evidence': all 3 models agree on direction for THIS event.\n"
                "   consensus='Conflicting': models disagree on THIS event — direction is uncertain for this event "
                "EVEN IF causal_reliability='high'. Do NOT cite causal_reliability to override a Conflicting consensus.\n"
                "   COMBINED RULE: use 'demonstrated'/'causally linked' ONLY when "
                "causal_reliability='high' AND consensus='Strong evidence' — both conditions required.\n"
                "   When reporting both fields together, always explain the distinction: "
                "'backtest_hit_rate=X% reflects historical model accuracy across N past events; "
                "the consensus for this specific event is Y — [interpret Y independently]'.\n"
                "   causal_reliability='low' (backtest_hit_rate 1–69%): "
                "directional accuracy is near-random — DO NOT use 'demonstrated'/'causally linked'; "
                "downgrade ALL consensus labels by one level (Strong→Moderate, Moderate→Weak, Weak→Inconclusive); "
                "prefix every causal citation with '(low causal reliability: hit_rate=$val%)'; "
                "do NOT promote any action to P0 solely on the basis of change_attribution evidence.\n"
                "   causal_reliability='none' (backtest_hit_rate=0% or missing): "
                "downgrade ALL consensus labels by one level AND treat all causal evidence as unvalidated; "
                "add '(backtest unvalidated)' after every citation; "
                "do NOT use 'demonstrated' or 'causally linked'.\n"
                "   In ALL cases where causal_reliability ≠ 'high': "
                "LP-derived campaign_actions and keyword_actions retain their assigned priority "
                "(they are budget-optimisation signals independent of causal models), "
                "but do NOT cite change_attribution as primary justification for P0 actions.\n"
                "5. If confounders exist (promotion, price cut, competitor move), "
                "do not attribute the outcome solely to ad actions — "
                "rewrite the conclusion to reflect partial or unclear attribution.\n"
                "6. Append a **Caveats & Data Quality** subsection at the end of each ASIN "
                "report if ANY of the following are true:\n"
                "   - lp_summary.spend_ceiling_bound == true: write "
                "'LP click-ceiling bound: LP optimal spend $lp_optimal_spend vs LP-scope campaign budget "
                "$lp_scope_campaign_daily_budget (LP-scope historical spend $lp_scope_daily_spend/day). "
                "Click ceilings on LP-scope keywords are exhausted — LP cannot allocate the full budget. "
                "Non-LP campaigns (auto/PT) run on their own separate budgets ($non_lp_scope_daily_spend/day). "
                "Priority action: expand keyword coverage or raise bids on ceiling-constrained keywords.' "
                "CRITICAL RULES when spend_ceiling_bound=true:\n"
                "  (a) Do NOT cite a negative order_gap as evidence the current strategy is optimal.\n"
                "  (b) Do NOT recommend increasing the budget — more budget does NOT help ceiling-bound keywords.\n"
                "  (c) Do NOT recommend decrease_bid for any keyword — cutting bids lowers future click ceilings "
                "and freed budget cannot reach ceiling-constrained keywords. "
                "keyword_actions with action='review_bids' under ceiling-bound means MONITOR, not cut.\n"
                "  (d) Do NOT suggest that lp_optimal_spend ($X) vs historical_daily_spend ($Y) "
                "means campaigns are under-spending — the gap is explained by non-LP scope spend "
                "(lp_summary.non_lp_scope_daily_spend).\n"
                "   - lp_summary.order_gap < 0 AND lp_summary.budget_binding == false AND lp_summary.spend_ceiling_bound == false: write "
                "'LP order estimate below actual (order_gap=$Z, no binding constraint identified): "
                "possible CVR data gap or keyword mix mismatch — review bid strategy before scaling budget.'\n"
                "   - lp_summary.budget_binding == true AND lp_summary.order_gap < 0: "
                "this is expected (pessimistic CVR shrinkage under-estimates vs actual when budget is fully consumed); "
                "do NOT write a caveat for this case — campaign_actions already reflects the correct recommendation.\n"
                "   - lp_summary.placement_data_unknown == true: "
                "do NOT recommend TOS/PP placement modifiers — "
                "write 'Placement data unavailable (all traffic reported as UNKNOWN)' "
                "and omit any placement-specific action items.\n"
                "   - campaign_match_strategy == 'name_substring' or 'none': write "
                "'Campaign matching used name-substring fallback — some campaigns may be misattributed. "
                "Verify campaign_ids and treat campaign-level metrics with caution.' "
                "Do NOT make high-confidence campaign-level recommendations under name_substring matching.\n"
                "   - causal_reliability != 'high': write the appropriate caveat below "
                "(causal_reliability is pre-computed — use it directly; do NOT re-derive from raw fractions):\n"
                "     causal_reliability='low' because backtest_hit_rate < 0.70: "
                "'Causal model directional accuracy $pct% (threshold 70%) — change_attribution "
                "evidence is near-random; all consensus labels downgraded one tier. "
                "($pct = backtest_hit_rate × 100, e.g. 0.65 → 65%.) "
                "LP budget recommendations remain valid (independent of causal models).'\n"
                "     causal_reliability='low' because events_significant_pct == 0: "
                "'No change event in this run produced a statistically significant causal estimate "
                "(p≥0.05 or CI crosses zero across all models). "
                "Causal labels are descriptive only — do not use them to justify P0 actions.'\n"
                "     causal_reliability='none': "
                "'Causal analysis has no calibration data and no significant results — "
                "all change_attribution evidence is unvalidated; treat as exploratory.'\n"
                "This caveat is MANDATORY when causal_reliability is 'low' or 'none'.\n"
                "   - Any change_attribution entry has attribution_suspect == true: write "
                "'Attribution outlier: [event_date] [change_type] delta_orders=$X exceeds 1.5× pre-window mean "
                "($pre_mean/day) on ASIN-level KPI fallback — seasonal or account-wide trend may dominate; "
                "treat delta magnitude as an upper bound only.' "
                "Do NOT use the raw delta_orders value as a factual order loss — qualify it as an estimate.\n"
                "   - placement_performance is missing or empty\n"
                "   - keyword_count < 10\n"
                "   - More than half of change_attribution entries have skipped=True "
                "for all three causal models\n"
                "   - post_window.days < 5 for the majority of attributions\n"
                "7. Paused campaigns: when campaign_state='PAUSED', do NOT recommend bid or budget "
                "changes for those campaigns — such changes have no effect until the campaign is "
                "re-enabled. CRITICAL: if a campaign_action entry has campaign_state='PAUSED' AND "
                "action='decrease_budget' or 'pause_candidate', treat this as a data artefact — "
                "DO NOT surface it as an action item. The campaign is already paused; any historical "
                "ACOS figure is moot until the campaign is re-enabled. "
                "For paused campaigns, only surface an action if it explicitly says "
                "'enable_and_review_bids', 'enable_and_increase_budget', or 'archive_candidate'. "
                "If all campaigns are paused (active_campaign_count == 0), the #1 priority action "
                "must address the pause decision before any other optimisation.\n"
                "8. Statistical sufficiency: when orders_reliability = 'low' (<30 orders total), "
                "mark ACOS and CVR estimates as 'statistically preliminary — results may shift "
                "significantly with more data' and display the ACOS 95% CI "
                "(acos_ci_lo%–acos_ci_hi%) alongside the point estimate in the Snapshot table. "
                "When orders_reliability = 'medium' (30–99 orders), add a note that "
                "conclusions should be validated over a longer window before committing "
                "to large bid or budget changes (risk of regression-to-the-mean).\n"
                "9. **Chart placeholders**: Diagnostic charts will be injected after the "
                "report is generated. Place each marker at the most relevant position inside "
                "the matching section. Do NOT write URLs or image syntax — just the marker "
                "text. Omit a marker if that data is unavailable "
                "(e.g. no change events → omit `[CHART:its_causal]`).\n"
                "   Available markers:\n"
                "   - `[CHART:daily_trend]` — daily orders/ACOS time series; place in Quick Metrics Snapshot or Overview\n"
                "   - `[CHART:its_causal]` — ITS/CausalImpact causal chart; place in Causal Confidence Assessment\n"
                "   - `[CHART:kw_quadrant]` — keyword ACOS × orders quadrant; place in Diagnostic Findings / Keywords\n"
                "   - `[CHART:placement_bar]` — placement ACOS vs bid adjustment; place in Diagnostic Findings / Placement\n"
                "   - `[CHART:inventory_burndown]` — inventory burn-down; place in Diagnostic Findings / Inventory\n"
                "   - `[CHART:comp_price_box]` — competitor price distribution; place in Diagnostic Findings / Organic & Market\n"
                "   - `[CHART:lp_waterfall]` — LP budget waterfall; place in Diagnostic Findings / LP Budget Optimisation\n"
                "   - `[CHART:rank_trend]` — organic rank trend; place in Diagnostic Findings / Organic & Market\n\n"
                "Begin the report now."
            ),
            compute_target=ComputeTarget.CLOUD_LLM,
        ),

        # ── Stage 8a: generate & upload charts (requires enriched item data) ──
        ProcessStep(
            name="generate_charts",
            fn=_generate_charts,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),

        # ── Stage 8b: write report to .md and set report_file_path ───────────
        # FeishuCallback.on_complete picks up report_file_path and sends it as
        # a file attachment. item["response"] is set to a short preview so the
        # card branch shows a summary without creating a duplicate attachment.
        ProcessStep(
            name="export_report",
            fn=_export_report,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),
    ]

    if config.get("no_llm", False):
        steps = [s for s in steps if s.name not in (
            "prepare_for_llm", "ad_diagnosis_llm", "export_report"
        )]

    return Workflow(name="ad_diagnosis", steps=steps)
