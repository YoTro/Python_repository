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
"""

import asyncio
import functools
import hashlib
import io
import logging
import math
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
from src.intelligence.processors.causal_analysis import ATTR_PRE_START, ATTR_POST_END
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

# ── L2 cache helpers (DataCache-backed, multi-tenant safe) ──────────────────
# Key format: {tenant_id}:{store_id}:{part...}
# - tenant_id isolates different seller accounts (multi-user safety)
# - store_id isolates marketplaces (US / EU / JP)
# - extra parts carry data-type-specific discriminators (days, asin, ids_hash)
#
# DataCache auto-selects Redis (if REDIS_URL set) or JSON-file backend.
# L1 (ctx.cache) is always checked first — L2 is only hit on job start / resume.

_L2_DOMAIN = "ad_diag"
_TTL_STATIC = 3600   # campaigns, keywords — account config, stable within a session
_TTL_PERF   = 14400   # performance reports — fetched once per day range
_TTL_CHANGE = 1800   # change history — more volatile, shorter TTL


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
                "lower_bound — derived from ad-attributed orders only; "
                "true daily sales (including organic) will be higher, "
                "so actual can_sell_days is likely LARGER than shown"
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


def _build_lp_input(
    kw_perf: List[Dict],
    kw_to_campaign: Dict[tuple, str],
    camp_meta: Dict[str, Dict],
    brand_kws: set,
    headroom: float,
    placement_multiplier: float,
) -> Tuple[List[Dict], float]:
    lp_input: List[Dict] = []
    actual_daily_orders  = 0.0
    for kw in kw_perf:
        if not kw.get("avg_cpc") or not kw.get("cvr"):
            continue
        kw_text    = kw["keyword_text"]
        match_type = kw["match_type"]
        cid        = kw_to_campaign.get((kw_text, match_type), "")
        strategy   = camp_meta.get(cid, {}).get("bidding_strategy", "")
        is_brand   = kw_text.lower() in {b.lower() for b in brand_kws}
        max_daily  = max(round(kw["daily_clicks"] * headroom, 1), 1.0)
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
        actual_daily_orders += kw["daily_clicks"] * kw["cvr"]
    return lp_input, actual_daily_orders


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


def _build_campaign_actions(
    camp_meta: Dict[str, Dict],
    camp_spend: Dict[str, float],
    performance_records: List[Dict],
    days: int,
    target_acos: Optional[float],
) -> List[Dict]:
    camp_actual_spend: Dict[str, float] = {}
    for r in performance_records:
        cid = str(r.get("campaign_id", ""))
        camp_actual_spend[cid] = camp_actual_spend.get(cid, 0.0) + float(r.get("spend", 0) or 0)

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

        camp_perf         = [r for r in performance_records if str(r.get("campaign_id")) == cid]
        camp_sales        = sum(float(r.get("sales", 0) or 0) for r in camp_perf)
        camp_spend_total  = sum(float(r.get("spend", 0) or 0) for r in camp_perf)
        camp_acos         = round(camp_spend_total / camp_sales * 100, 1) if camp_sales > 0 else None
        camp_orders_total = sum(float(r.get("orders", 0) or 0) for r in camp_perf)
        camp_daily_orders = camp_orders_total / days if days > 0 else 0.0
        camp_cpo          = round(camp_spend_total / camp_orders_total, 2) if camp_orders_total > 0 else None

        target_acos_pct = (target_acos or 0.35) * 100
        suggested = None
        if is_paused and lp_spend >= camp_budget * 0.10:
            action, priority = "enable_and_review_bids", "P1"
            rationale = f"Campaign is PAUSED; LP projects ${lp_spend:.0f}/day potential — evaluate re-enabling after bid review"
        elif lp_spend < camp_budget * 0.10:
            if is_paused:
                action, priority = "archive_candidate", "P2"
                rationale = f"Campaign is PAUSED and LP allocates only ${lp_spend:.0f}/day — consider archiving"
            else:
                action, priority = "pause_candidate", "P1"
                rationale = f"LP allocates only ${lp_spend:.0f}/day (< 10% of ${camp_budget:.0f} budget) — keywords inefficient"
        elif lp_saturated and (camp_acos is None or camp_acos <= target_acos_pct):
            suggested = round(min(lp_spend * 1.15, camp_budget * 1.5), 0)
            if is_paused:
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
            suggested = round(camp_budget * 0.75, 0)
            action, priority = "decrease_budget", "P0"
            rationale = f"ACOS {camp_acos}% exceeds 130% of target {target_acos_pct:.0f}% — cut budget to reduce losses"
        elif camp_acos and target_acos_pct < camp_acos <= target_acos_pct * 1.3:
            action, priority = "review_bids", "P1"
            rationale = f"ACOS {camp_acos}% above target — lower bids on high-ACOS keywords before scaling"
        else:
            action, priority = "maintain", "P2"
            rationale = f"Budget util {budget_util:.0%}, ACOS {camp_acos}% — within healthy range"

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
            if not is_brand:
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
                actions.append({
                    "action":                               "increase_bid",
                    "priority":                             "P1",
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
                })
            elif cur_clicks > 0 and opt_clicks < cur_clicks * 0.4:
                delta_clicks = opt_clicks - cur_clicks
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
      C4  Inventory cap:       Σ clicks_i × pess_cvr_i ≤ max_daily_orders
      C5  Click bounds:        min_daily_clicks_i ≤ clicks_i ≤ max_daily_clicks_i

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

        total_orders     = item.get("total_orders") or 0
        total_sales      = item.get("total_sales")  or 0
        avg_price        = round(total_sales / total_orders, 2) if total_orders > 0 else None
        can_sell_days    = item.get("can_sell_days")
        total_available  = item.get("total_available") or 0
        max_daily_orders = (
            round(total_available / can_sell_days, 2)
            if can_sell_days and can_sell_days > 0 and total_available > 0 else None
        )

        lp_input, actual_daily_orders = _build_lp_input(
            kw_perf, kw_to_campaign, camp_meta, brand_kws, headroom, placement_multiplier
        )
        if not lp_input:
            item["lp_summary"] = {"skipped": True, "reason": "all keywords filtered (insufficient clicks)"}
            continue

        result = optimizer.optimize(
            keywords         = lp_input,
            total_budget     = daily_budget,
            campaign_budgets = campaign_budgets or None,
            target_acos      = target_acos,
            avg_price        = avg_price,
            max_daily_orders = max_daily_orders,
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
        spend_ceiling_bound = lp_spend_total < daily_budget * 0.6
        zero_kws, maxed_kws = _classify_lp_keywords(kw_perf, alloc, kw_map)
        kw_id_map           = _build_lp_kw_id_map(ctx, campaign_ids)

        campaign_actions = _build_campaign_actions(
            camp_meta, camp_spend, item.get("performance_records") or [], days, target_acos
        )
        keyword_actions = _build_keyword_actions(
            lp_input, alloc, kw_id_map, brand_kws, headroom, avg_price
        )

        placement_data_unknown = set(placement_perf.keys()) <= {"UNKNOWN", ""}
        item["lp_summary"] = {
            "daily_budget":                  daily_budget,
            "lp_optimal_spend":              lp_spend_total,
            "lp_optimal_orders_pessimistic": summary["total_expected_orders"],
            "lp_optimal_orders_raw":         lp_raw_orders,
            "actual_daily_orders":           round(actual_daily_orders, 2),
            "order_gap":                     round(lp_raw_orders - actual_daily_orders, 2),
            "spend_ceiling_bound":           spend_ceiling_bound,
            "avg_effective_cpc":             summary["avg_effective_cpc"],
            "placement_multiplier":          round(placement_multiplier, 3),
            "placement_data_unknown":        placement_data_unknown,
            "target_acos_applied":           target_acos,
            "inventory_cap_applied":         max_daily_orders,
            "keywords_in_lp":               len(lp_input),
            "keywords_allocated":           len(alloc),
            "keywords_zeroed":              len(zero_kws),
            "keywords_maxed":               len(maxed_kws),
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
# Causal analysis wrapper (delegates to intelligence/processors/causal_analysis)
# ---------------------------------------------------------------------------

def _run_causal_analysis(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    ProcessStep wrapper: runs the full attribution + causal pipeline
    (window comparison, ITS, CausalImpact, DML) for each item.

    Passes daily_perf from cache so the processor can build the campaign-level
    daily performance index without depending on workflow internals.
    """
    from src.intelligence.processors.causal_analysis import run_causal_analysis
    for item in items:
        try:
            asin       = item.get("asin", "").upper()
            daily_perf = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])
            result = run_causal_analysis(item, ctx.config, daily_perf=daily_perf)
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
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    return buf.getvalue()


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
    date_str = _dt.date.today().isoformat()

    for item in items:
        asin       = (item.get("asin") or "unknown").upper()
        daily_perf = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])

        # Use explicit closures to avoid the late-binding lambda trap
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

        chart_urls: Dict[str, str] = {}
        for name, fn in generators:
            try:
                png = fn()
                if png is None:
                    logger.debug(f"[charts] {asin}/{name}: skipped (no data)")
                    continue
                url = _chart_upload(png, f"reports/ad_diagnosis/{asin}/{date_str}/{name}.png")
                if url:
                    chart_urls[name] = url
                    logger.info(f"[charts] {asin}/{name} → {url}")
            except Exception as e:
                logger.warning(f"[charts] {asin}/{name} failed: {e}", exc_info=True)

        item["chart_urls"] = chart_urls
        logger.info(f"[charts] {asin}: {len(chart_urls)}/{len(generators)} charts uploaded")

    return items


# ---------------------------------------------------------------------------
# LLM pre-enrichment (summary injection only — no field stripping)
# ---------------------------------------------------------------------------

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
        "causal_consensus_sample":    attributions[0].get("consensus") if attributions else None,
        # Statistical sufficiency & ACOS CI
        "orders_reliability":         item.get("orders_reliability"),
        "acos_ci_lo":                 item.get("acos_ci_lo"),
        "acos_ci_hi":                 item.get("acos_ci_hi"),
        # Directional backtest calibration
        "backtest_hit_rate":          item.get("backtest_hit_rate"),
        "backtest_strong_hit_rate":   item.get("backtest_strong_hit_rate"),
        "backtest_total":             item.get("backtest_total"),
        # Pre-computed reliability tier so LLM does not need to interpret raw %:
        #   "high"   ≥70%  — causal labels trustworthy, may use 'demonstrated'
        #   "low"    1–69% — directional accuracy near-random; downgrade all labels
        #   "none"   0% or no backtest — completely unvalidated
        "causal_reliability": (
            "high" if (item.get("backtest_hit_rate") or 0) >= 70
            else "low"  if (item.get("backtest_hit_rate") or 0) > 0
            else "none"
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


_CHART_META: Dict[str, Dict] = {
    "daily_trend":        {"label": "Daily Performance Trend",       "keywords": ["performance", "trend", "daily", "表现", "趋势", "overview", "概览", "summary", "摘要"]},
    "its_causal":         {"label": "ITS Causal Analysis",           "keywords": ["causal", "attribution", "change", "归因", "变更", "impact", "causalimpact", "分析"]},
    "kw_quadrant":        {"label": "Keyword ACOS × Orders",         "keywords": ["keyword", "关键词", "kw", "bid", "竞价"]},
    "placement_bar":      {"label": "Placement Performance",         "keywords": ["placement", "位置", "广告位"]},
    "inventory_burndown": {"label": "Inventory Burn-down",           "keywords": ["inventor", "库存", "stockout", "stock"]},
    "comp_price_box":     {"label": "Competitor Price Distribution", "keywords": ["compet", "price", "竞品", "价格", "market", "竞争"]},
    "lp_waterfall":       {"label": "LP Budget Allocation",          "keywords": ["budget", "allocation", "lp", "预算", "优化", "recommend", "建议", "action", "行动"]},
    "rank_trend":         {"label": "Organic Rank Trend",            "keywords": ["rank", "organic", "自然", "排名", "flywheel", "飞轮"]},
}


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
            top   = attrs[0]
            ls    = (top.get("its") or {}).get("level_shift")
            ls_s  = f"{ls:+.2f}" if ls is not None else "N/A"
            return (f"Top event: {top.get('change_type','?')} ({top.get('changed_at','?')}) "
                    f"→ ITS level shift {ls_s} orders/day. Shaded area = estimated causal effect.")
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
        can_sell = item.get("can_sell_days") or 0
        risk     = item.get("inventory_risk", False)
        if risk:
            return f"⚠ Stockout in ~{can_sell:.0f} days — avoid budget increases to prevent stranded spend at stockout."
        return f"Inventory covers ~{can_sell:.0f} days — sufficient runway for current scaling plans."

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
        return (f"LP order gap {gap:+.1f}/day — rebalancing spend across campaigns "
                f"can gain {abs(gap):.1f} orders/day. Blue bar = LP target; grey = budget cap.")

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
    import re
    import datetime as _dt

    report_dir = os.path.abspath("data/reports")
    os.makedirs(report_dir, exist_ok=True)
    date_str = _dt.date.today().isoformat()

    _heading_re = re.compile(r'^(#{1,3}\s+.+)$', re.MULTILINE)

    for item in items:
        text = item.get("ad_diagnosis_llm") or ""
        if not text:
            continue
        asin = (item.get("asin") or "unknown").upper()

        chart_urls: dict = item.get("chart_urls") or {}
        if chart_urls:
            # Build per-chart interpretation strings
            interps = {name: _chart_interpretation(item, name) for name in chart_urls}

            # Try to inject each chart right after its matching section heading.
            # Falls back to an appended section for charts that find no match.
            headings = [(m.start(), m.end(), m.group(1).lower())
                        for m in _heading_re.finditer(text)]
            injected: set = set()

            for name, url in list(chart_urls.items()):
                keywords = _CHART_META.get(name, {}).get("keywords", [])
                label    = _CHART_META.get(name, {}).get("label", name)
                interp   = interps.get(name, "")
                img_block = (
                    f"\n\n> *{interp}*\n\n![{label}]({url})\n"
                    if interp else f"\n\n![{label}]({url})\n"
                )
                for i, (hstart, hend, htitle) in enumerate(headings):
                    if any(kw in htitle for kw in keywords):
                        # Insert at the start of the section content (after heading line)
                        insert_pos = hend + 1
                        text = text[:insert_pos] + img_block + text[insert_pos:]
                        shift = len(img_block)
                        headings = [
                            (s + shift if s >= insert_pos else s,
                             e + shift if e >= insert_pos else e,
                             t)
                            for s, e, t in headings
                        ]
                        injected.add(name)
                        break

            # Append charts that had no matching section
            remaining = {n: u for n, u in chart_urls.items() if n not in injected}
            if remaining:
                lines = ["\n\n---\n\n## 📊 Diagnostic Charts\n\n"]
                for name, url in remaining.items():
                    label  = _CHART_META.get(name, {}).get("label", name)
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
                "  total_available / total_inbound → SP-API FBA Inventory (unit count, point-in-time)\n"
                "  daily_sales / daily_sales_source → "
                "daily_sales = total_units ÷ lookback_days; source priority: "
                "(1) caller-supplied, (2) order_metrics = SP-API getOrderMetrics (all channels), "
                "(3) ad_orders_only = spCampaigns orders ÷ days (lower bound; organic excluded)\n"
                "  can_sell_days → total_available ÷ daily_sales "
                "(null if daily_sales unavailable; when daily_sales_source=ad_orders_only, "
                "true can_sell_days is LARGER because organic sales are excluded)\n"
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
                "spCampaigns performance report, summed over matched campaigns, "
                "period: data_start_date → data_end_date\n"
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
                "OR-Tools LP (maximise orders; constraints: C1 global budget, C2 per-campaign budget caps, "
                "C3 target ACOS linearised, C4 inventory order cap, C5 click floor/ceiling; "
                "eff_cpc = avg_cpc × bidding_strategy_multiplier × placement_multiplier; "
                "pessimistic CVR shrinkage applied for low-sample keywords). "
                "lp_optimal_orders_pessimistic uses shrunk CVR (may be < actual_daily_orders — expected). "
                "lp_optimal_orders_raw uses the same raw CVR as actual_daily_orders; order_gap = raw_lp − actual "
                "(positive = LP gains orders by reallocation; negative = click ceilings prevent LP from matching "
                "current performance — NOT evidence the current strategy is optimal). "
                "spend_ceiling_bound=true means click ceilings (C5) are the binding constraint, NOT the budget "
                "(lp_optimal_spend < 60% of daily_budget); in this case: "
                "(a) lp_zero/maxed keyword signals remain valid as ROI signals; "
                "(b) order_gap is ceiling-limited, not budget-limited — the primary recommendation is "
                "expand keyword coverage or raise bids (not increase budget). "
                "placement_data_unknown=true means all spend was reported under 'UNKNOWN' placement — "
                "do NOT make placement-specific recommendations in this case.\n"
                "  campaign_actions → derived from LP camp_spend vs actual campaign budgets; "
                "campaign_state field present; action ∈ {{increase_budget, decrease_budget, review_bids, "
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
                "(ITS + CausalImpact + DML agreement: Strong / Moderate / Weak / Confounded / Skipped)\n"
                "  orders_reliability → statistical sufficiency of orders sample: "
                "high (≥100 orders), medium (30–99), low (<30)\n"
                "  acos_ci_lo / acos_ci_hi → 95% ACOS confidence interval (Wilson method on CVR "
                "propagated to ACOS via ACOS = spend/sales; see Methodology section)\n"
                "  backtest_hit_rate → % of evaluated change events where model-predicted direction "
                "matched observed post-window KPI direction (within-sample calibration); "
                "threshold: <70% = near-random (causal_reliability='low'/'none'), ≥70% = reliable ('high')\n"
                "  backtest_strong_hit_rate → same but restricted to 'Strong evidence' events only\n"
                "  causal_reliability → pre-computed tier: 'high' (≥70%), 'low' (1–69%), 'none' (0%/missing); "
                "use this field (not the raw %) to apply Rule 4 label-downgrade logic\n\n"
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
                "- **Consensus**: Strong / Moderate / Weak / Confounded / Skipped "
                "(read from the `consensus` field directly)\n"
                "- **ITS** *(Linden 2015)*: level_shift=$val [95% CI: level_shift_ci_lo – level_shift_ci_hi], "
                "p=$p_val (significant if p < 0.10)\n"
                "- **CausalImpact** *(Brodersen et al. 2015)*: point_effect=$val "
                "[95% credible interval: ci_lo – ci_hi] (actual − BSTS counterfactual)\n"
                "- **DML** *(Chernozhukov et al. 2018)*: theta=$val "
                "[95% CI: theta_ci_lo – theta_ci_hi] (sandwich-SE; reliable only if r_squared ≥ 0)\n"
                "- **Historical calibration**: backtest_hit_rate=$val% "
                "(model direction vs observed post-window direction for this ASIN)\n"
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
                "4. Uncertainty labelling — mandatory rules based on causal_reliability:\n"
                "   causal_reliability='high' (backtest_hit_rate ≥70%): "
                "may use 'demonstrated'/'causally linked' only when consensus='Strong evidence'.\n"
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
                "   - lp_summary.spend_ceiling_bound == true OR lp_summary.order_gap < 0: write "
                "'LP click-ceiling bound (LP spend $X / budget $Y; order_gap=$Z): "
                "LP cannot outperform current allocation because click ceilings prevent reallocation — "
                "order_gap is negative due to ceiling constraints on LP variables, NOT because "
                "the current strategy is optimal. Priority action: expand keyword coverage or raise bids.' "
                "Do NOT cite a negative order_gap as evidence that the current strategy is better than LP; "
                "do NOT recommend increasing the budget when spend_ceiling_bound=true.\n"
                "   - lp_summary.placement_data_unknown == true: "
                "do NOT recommend TOS/PP placement modifiers — "
                "write 'Placement data unavailable (all traffic reported as UNKNOWN)' "
                "and omit any placement-specific action items.\n"
                "   - campaign_match_strategy == 'name_substring' or 'none': write "
                "'Campaign matching used name-substring fallback — some campaigns may be misattributed. "
                "Verify campaign_ids and treat campaign-level metrics with caution.' "
                "Do NOT make high-confidence campaign-level recommendations under name_substring matching.\n"
                "   - causal_reliability != 'high' (i.e. backtest_hit_rate < 70%): write "
                "'Causal model directional accuracy $val% (threshold 70%) — change_attribution "
                "evidence is near-random; all consensus labels downgraded one tier. "
                "LP budget recommendations remain valid (independent of causal models).' "
                "This caveat is MANDATORY when causal_reliability is 'low' or 'none'.\n"
                "   - placement_performance is missing or empty\n"
                "   - keyword_count < 10\n"
                "   - More than half of change_attribution entries have skipped=True "
                "for all three causal models\n"
                "   - post_window.days < 5 for the majority of attributions\n"
                "7. Paused campaigns: when paused_campaign_count > 0, do NOT recommend bid or budget "
                "changes for those campaigns — such changes have no effect until the campaign is "
                "re-enabled. For each action targeting a paused campaign either (a) recommend "
                "re-enabling it first (with explicit justification: what will improve if re-enabled?), "
                "or (b) recommend restructuring / archiving it. If all campaigns are paused "
                "(active_campaign_count == 0), the #1 priority action must address the pause decision "
                "before any other optimisation.\n"
                "8. Statistical sufficiency: when orders_reliability = 'low' (<30 orders total), "
                "mark ACOS and CVR estimates as 'statistically preliminary — results may shift "
                "significantly with more data' and display the ACOS 95% CI "
                "(acos_ci_lo%–acos_ci_hi%) alongside the point estimate in the Snapshot table. "
                "When orders_reliability = 'medium' (30–99 orders), add a note that "
                "conclusions should be validated over a longer window before committing "
                "to large bid or budget changes (risk of regression-to-the-mean).\n"
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
