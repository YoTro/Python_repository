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
  inbound_lead_days       int   30       assumed transit days for inbound_shipped; overridden by actual
                                         sea-transit p75 from Lingxing when enable_lingxing=True
  enable_lingxing         bool  auto     fetch shipment lead-time from Lingxing ERP; defaults to True
                                         when LINGXING_ACCOUNT env var is set, False otherwise
  keyword_max_results     int   10000    max configured keywords to fetch per ASIN campaign set
"""

import asyncio
import functools
import hashlib
import io
import logging
import math
import os
import re
from datetime import datetime, timedelta, date as _date_cls, timezone
from zoneinfo import ZoneInfo
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch

from src.core.utils.charts import CHART_PALETTE as _CHART_PALETTE, fig_to_png as _fig_to_png, chart_upload as _chart_upload

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
_KEY_LEAD_TIME        = "ad_diag:lead_time"        # Lingxing shipment lead-time (store-wide)

# ── Action priority tiers ────────────────────────────────────────────────────
# Used by campaign_actions, keyword_actions, and mining_actions alike.
# Every function that emits a "priority" field MUST use these constants so the
# tier semantics remain consistent across all action types and are visible to
# both code reviewers and the LLM prompt (the prompt references these strings).
#
# P0 — Immediate / revenue-blocking
#       Situation is actively losing money or at risk of stockout / account suspension.
#       Examples: budget exhausted daily (capping organic-traffic campaigns),
#                 ACOS > 130% of target (burning margin), stockout imminent.
#       LLM instruction: surface in Top 5, do NOT demote below P1.
#
# P1 — High-priority / clear action required
#       A measurable problem exists; act within the current week.
#       Examples: ACOS above target (not extreme), keyword genuinely inefficient
#                 (kw_acos ≥ target → pause_keyword), campaign paused with healthy ACOS
#                 (should re-enable), LP-causal conflict detected.
#       LLM instruction: include in Top 5 unless overridden by a conflict rule.
#
# P2 — Monitor / conditional / low-urgency
#       No urgent action; revisit when a condition changes (budget increases,
#       stock clears gate, more data accumulates).
#       Examples: hold_keyword (efficient but budget-constrained),
#                 pause_keyword with null ACOS (data insufficient),
#                 increase_bid gated behind inventory threshold,
#                 maintain (campaign is healthy).
#       LLM instruction: mention as secondary notes; omit from Top 5 if space
#                        is needed for P0/P1 items.
#
# Sort order for action lists: P0 < P1 < P2  (ascending index = higher urgency).
PRIORITY_SORT = ("P0", "P1", "P2")

# ── L2 cache helpers (DataCache-backed, multi-tenant safe) ──────────────────
# Key format: {tenant_id}:{store_id}:{part...}
# - tenant_id isolates different seller accounts (multi-user safety)
# - store_id isolates marketplaces (US / EU / JP)
# - extra parts carry data-type-specific discriminators (days, asin, ids_hash)
#
# DataCache auto-selects Redis (if REDIS_URL set) or JSON-file backend.
# L1 (ctx.cache) is always checked first — L2 is only hit on job start / resume.

_L2_DOMAIN = "ad_diag"
_TTL_STATIC = 7200    # campaigns, keywords — account config, stable within a session
_TTL_PERF   = 14400   # performance reports — fetched once per day range
_TTL_CHANGE = 21600   # change history — historical data, 6h TTL to reduce /history API calls
_TTL_YOY    = 86400   # YoY / trailing-ext ERP data — historical, rarely changes
_KEYWORD_LIST_MAX_RESULTS = 10000


def _l2_key(ctx: WorkflowContext, *parts) -> str:
    tid = ctx.tenant_id or "default"
    sid = ctx.config.get("store_id", "US")
    return ":".join(str(p) for p in (tid, sid) + parts)


def _l2_get(ctx: WorkflowContext, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx: WorkflowContext, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)


# Per-key asyncio locks to prevent cache stampede: when the cache expires and
# multiple ASIN items are processed concurrently, all would miss L1+L2 and fire
# simultaneous API calls.  The lock ensures only the first coroutine fetches;
# all others wait and then find the result already populated in L1.
_l2_inflight: Dict[str, "asyncio.Lock"] = {}


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

    Stampede protection: an asyncio.Lock per l1_key serialises concurrent
    callers.  The winner fetches and populates both cache levels; latecomers
    re-check L1 after acquiring the lock and return the already-stored value.

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

            # Fast path: already in L1 (no lock needed)
            if l1_key in ctx.cache:
                return ctx.cache[l1_key]

            # Acquire per-key lock to serialise concurrent callers.
            # Pop the entry ONLY after a successful fetch so that on failure the
            # lock stays registered in _l2_inflight.  Without this, a pop in the
            # failure path lets new arrivals create an uncontested fresh lock and
            # race the still-waiting coroutines — a stampede amplifier during
            # API jitter.  Keeping the entry means all callers (waiting or new)
            # queue behind the same lock and the next winner does a single retry.
            if l1_key not in _l2_inflight:
                _l2_inflight[l1_key] = asyncio.Lock()
            _fetched = False
            try:
                async with _l2_inflight[l1_key]:
                    # Re-check L1 after acquiring — a prior waiter may have filled it
                    if l1_key in ctx.cache:
                        return ctx.cache[l1_key]

                    l2_parts = l2_parts_fn(ctx, *args, **kwargs)
                    hit = _l2_get(ctx, l2_ttl, *l2_parts)
                    if hit is not None:
                        ctx.cache[l1_key] = hit
                        _fetched = True
                        return hit

                    value = await fn(ctx, *args, **kwargs)
                    ctx.cache[l1_key] = value
                    _l2_set(ctx, value, *l2_parts)
                    _fetched = True
                    return value
            finally:
                if _fetched:
                    _l2_inflight.pop(l1_key, None)
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
    l1_key_fn   = lambda ctx, campaign_ids: (
        f"{_KEY_KEYWORDS}:v2:{ctx.config.get('keyword_max_results', _KEYWORD_LIST_MAX_RESULTS)}:"
        f"{','.join(sorted(campaign_ids))}"
    ),
    l2_ttl      = _TTL_STATIC,
    l2_parts_fn = lambda ctx, campaign_ids: (
        "keywords_v2",
        ctx.config.get("keyword_max_results", _KEYWORD_LIST_MAX_RESULTS),
        _campaign_ids_hash(campaign_ids),
    ),
)
async def _ensure_keywords(ctx: WorkflowContext, campaign_ids: List[str]) -> List[Dict]:
    """Fetch keywords for a set of campaign_ids, cached by sorted id-tuple."""
    limit = ctx.config.get("keyword_max_results", _KEYWORD_LIST_MAX_RESULTS)
    keywords = await _ads_client(ctx).list_keywords(
        campaign_ids=campaign_ids,
        states=["ENABLED", "PAUSED"],
        max_results=limit,
    )
    if len(keywords) >= limit:
        logger.warning(
            f"Keyword list reached keyword_max_results={limit}; keyword_count may still be truncated"
        )
    return keywords


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
    today       = datetime.now(tz=_store_tz(ctx)).date()
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
    l1_key_fn   = lambda ctx, campaign_ids: f"{_KEY_CHANGE_HISTORY}:{_campaign_ids_hash(list(campaign_ids))}",
    l2_ttl      = _TTL_CHANGE,
    l2_parts_fn = lambda ctx, campaign_ids: ("change_history", ctx.config.get("days", 30),
                                              _campaign_ids_hash(list(campaign_ids))),
)
async def _ensure_change_history(ctx: WorkflowContext, campaign_ids: List[str]) -> List[Dict]:
    """Fetch change history for the given campaigns, scoped to the lookback + attribution tail.

    Uses campaign-batched mode (parents=[{campaignId}]) when campaign_ids is provided —
    this is both more targeted (no cross-ASIN noise) and fully paginated per campaign batch,
    avoiding the 4000-event profile-wide cap that truncates high-volume accounts.
    """
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
    result = await client.get_change_history(
        from_date=from_ms, to_date=to_ms,
        campaign_ids=campaign_ids or None,
        count=200, sort_direction="DESC",
    )
    events = result.get("events", [])
    logger.info(f"Fetched {len(events)} change history events "
                f"({'profile-wide' if not campaign_ids else f'{len(campaign_ids)} campaigns'})")
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


async def _enrich_shipment_lead_time(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Fetch and compute store-wide FBA shipment lead-time distributions.

    Data source priority:
      1. Lingxing ERP  — full phase breakdown (sea transit + FBA processing).
                         Used when sea_transit.n ≥ 5.
      2. SP-API Inbound Plans 2024-03-20 — proxy: plan_creation → fba_receive
                         for CN-source SHIPPED plans only.  sea_transit.p75 will
                         overestimate by roughly the plan-lead-time lag (~7-21d);
                         data_source = 'sp_api_plans' flags this.
      3. No data       — data_source = 'none'; inbound_lead_days falls back to config.

    Result cached under _KEY_LEAD_TIME and shared across all ASINs in the run.
    """
    if _KEY_LEAD_TIME in ctx.cache:
        item["shipment_lead_time"] = ctx.cache[_KEY_LEAD_TIME]
        return item

    from src.intelligence.processors.shipment_lead_time import compute_quarterly_lead_times
    result: Dict = {}

    # ── Primary: Lingxing ERP ─────────────────────────────────────────────
    try:
        from src.mcp.servers.erp.lingxing.client import LingxingClient
        from src.intelligence.processors.shipment_lead_time import adapt_lingxing_shipments

        loop = asyncio.get_event_loop()

        def _fetch_lingxing() -> List[Dict]:
            client = LingxingClient()
            if not client.token:
                raise RuntimeError("Lingxing: no auth token")
            end_dt   = _date_cls.today()
            start_dt = end_dt.replace(year=end_dt.year - 2)
            return client.get_fba_shipment_tracking(
                start_date=start_dt.strftime("%Y-%m-%d"),
                end_date=end_dt.strftime("%Y-%m-%d"),
                fetch_all=True,
            )

        raw        = await loop.run_in_executor(None, _fetch_lingxing)
        normalised = adapt_lingxing_shipments(raw)
        lx = compute_quarterly_lead_times(
            normalised,
            sea_start_field   = "domestic_ship_date",
            sea_end_field     = "overseas_arrival_date",
            ovs_start_field   = "overseas_arrival_date",
            ovs_end_field     = "fba_received_date",
            local_start_field = "domestic_ship_date",
            local_end_field   = "overseas_arrival_date",
            local_min_days    = 0,
            local_max_days    = 12,
            quarter_field     = "overseas_arrival_date",
            sea_min_days      = 13,
            sea_max_days      = 180,
            ovs_min_days      = 0,
            ovs_max_days      = 60,
        )
        sea_n = lx.get("sea_transit", {}).get("overall", {}).get("n", 0)
        if sea_n >= 5:
            lx["data_source"] = "lingxing_erp"
            result = lx
            logger.info(
                f"_enrich_shipment_lead_time [lingxing]: {lx.get('total_input', 0)} shipments, "
                f"sea n={sea_n}, fba n={lx.get('overseas_to_fba', {}).get('overall', {}).get('n', 0)}"
            )
        else:
            logger.warning(
                f"_enrich_shipment_lead_time: Lingxing sea n={sea_n} < 5 — trying SP-API fallback"
            )
    except Exception as e:
        logger.warning(f"_enrich_shipment_lead_time: Lingxing failed ({e}) — trying SP-API fallback")

    # ── Fallback: SP-API Inbound Plans 2024-03-20 ─────────────────────────
    # Proxy measurement: plan_creation (createdAt) → FBA receive (lastUpdatedAt).
    # Captures full plan-to-receive horizon; biased longer than true sea transit
    # by the time between plan creation and actual factory departure (~7-21d).
    if not result:
        try:
            from src.mcp.servers.amazon.sp_api.client import SPAPIClient
            from src.intelligence.processors.shipment_lead_time import adapt_sp_api_plans

            sp_client = SPAPIClient()
            plans = await sp_client.get_inbound_plans(status="SHIPPED")
            normalised_sp = adapt_sp_api_plans(plans, cn_only=True, shipped_only=True)
            sp = compute_quarterly_lead_times(
                normalised_sp,
                sea_start_field = "domestic_ship_date",   # proxy: plan createdAt
                sea_end_field   = "fba_received_date",    # proxy: plan lastUpdatedAt
                ovs_start_field = "overseas_arrival_date",  # always None → ovs_to_fba skipped
                ovs_end_field   = "fba_received_date",
                quarter_field   = "fba_received_date",
                sea_min_days    = 20,   # exclude domestic/air noise; plan pre-date adds ~7-21d
                sea_max_days    = 150,
            )
            sea_n = sp.get("sea_transit", {}).get("overall", {}).get("n", 0)
            if sea_n >= 3:
                sp["data_source"] = "sp_api_plans"
                result = sp
                logger.info(
                    f"_enrich_shipment_lead_time [sp_api_plans]: sea n={sea_n} "
                    f"(proxy plan_creation→fba_receive; p75 may overestimate by ~14d)"
                )
            else:
                logger.warning(f"_enrich_shipment_lead_time: SP-API plans sea n={sea_n} < 3 — no lead-time data")
        except Exception as e:
            logger.warning(f"_enrich_shipment_lead_time: SP-API fallback failed ({e})")

    if not result:
        result = {"data_source": "none"}

    ctx.cache[_KEY_LEAD_TIME] = result
    item["shipment_lead_time"] = result
    return item


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
    strategy_counts: Dict[str, int] = {}
    for c in matched:
        s = c.get("bidding_strategy")
        if s:
            strategy_counts[s] = strategy_counts.get(s, 0) + 1
    return {
        "campaigns":               matched,
        "campaign_ids":            campaign_ids,
        "total_daily_budget":      total_daily_budget,
        "bidding_strategies":      strategy_counts,
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

    if not campaign_ids:
        return {"performance_records": [], "total_spend": 0, "account_acos": None}
    matched = [r for r in all_perf if str(r.get("campaign_id")) in campaign_ids]

    if not matched:
        return {"performance_records": [], "total_spend": 0, "account_acos": None}

    total_spend  = round(sum(r.get("spend",  0) or 0 for r in matched), 2)
    total_sales  = round(sum(r.get("sales",  0) or 0 for r in matched), 2)
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
    high_acos_campaigns = [
        r for r in matched
        if r.get("acos") and r["acos"] > warn_thresh
    ]

    # Budget exhaustion: spend / (daily_budget * days) > threshold
    days = ctx.config.get("days", 30)
    budget_pct_threshold = ctx.config.get("budget_exhaustion_pct", 0.90)
    total_budget_capacity = item.get("total_daily_budget", 0) * days
    budget_exhaustion_pct = (
        round(total_spend / total_budget_capacity * 100, 1)
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
            and budget_exhaustion_pct > budget_pct_threshold * 100
        ),
    }
    # Last-resort can_sell_days backfill: only if neither inventory (daily_sales
    # supplied by caller) nor order_metrics (preferred) set can_sell_days already.
    if item.get("can_sell_days") is None and item.get("daily_sales_source") != "order_metrics" and total_orders > 0:
        # Ad orders exclude organic — daily consumption is understated, so can_sell_days
        # is an upper bound (stock lasts LESS than this estimate in reality).
        # inventory_risk is only set True when even this optimistic upper bound is below
        # the threshold (definite risk). When the upper bound looks safe, inventory_risk
        # is left unset (unknown) — never assert False on an unreliable estimate.
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
            if can_sell_days < ctx.config.get("inventory_risk_days", 30):
                result["inventory_risk"] = True
            # else: leave unset — upper-bound safety does not confirm no risk
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

    if not campaign_ids:
        return {"keyword_performance": []}
    # Filter to this ASIN's campaigns
    relevant = [r for r in all_kw_perf if str(r.get("campaign_id")) in campaign_ids]

    # Aggregate by (campaign_id, keyword_text, match_type) — same keyword in
    # different campaigns gets separate rows so LP budget constraints and
    # keyword_actions are attributed to the correct campaign.
    agg: Dict[tuple, Dict] = {}
    for r in relevant:
        cid = str(r.get("campaign_id", ""))
        key = (cid, r.get("keyword_text", ""), r.get("match_type", ""))
        if key not in agg:
            agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0, "sales": 0}
        agg[key]["spend"]       += r.get("spend", 0) or 0
        agg[key]["clicks"]      += r.get("clicks", 0) or 0
        agg[key]["orders"]      += r.get("orders", 0) or 0
        agg[key]["impressions"] += r.get("impressions", 0) or 0
        agg[key]["sales"]       += r.get("sales", 0) or 0

    kw_performance = []
    for (cid, kw_text, match_type), v in agg.items():
        clicks = v["clicks"]
        if clicks < min_clicks:
            continue
        avg_cpc      = round(v["spend"] / clicks, 4)
        cvr          = round(v["orders"] / clicks, 4)
        daily_clicks = round(clicks / days, 2)
        # ACOS = ad spend / attributed sales revenue (not spend/orders which gives cost/order)
        acos = round(v["spend"] / v["sales"] * 100, 2) if v["sales"] > 0 else None
        kw_performance.append({
            "campaign_id":   cid,
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

    if not campaign_ids:
        return {"placement_performance": {}, "placement_configured_pcts": {}}
    # Filter to this ASIN's campaigns
    relevant = [r for r in all_records if str(r.get("campaign_id")) in campaign_ids]

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
    campaign_ids = list(item.get("campaign_ids") or [])
    all_events, _ = await asyncio.gather(
        _ensure_change_history(ctx, campaign_ids),
        _ensure_daily_performance(ctx, asin),
    )

    cid_set  = set(str(c) for c in campaign_ids)
    relevant = []
    for ev in all_events:
        # Campaign ID: prefer metadata.campaignId (AD_GROUP/KEYWORD events),
        # fall back to entityId for CAMPAIGN-level events.
        meta = ev.get("metadata") or {}
        cid  = str(meta.get("campaignId") or ev.get("entityId") or "")

        # Guard: drop events outside this ASIN's campaigns (shouldn't happen in
        # campaign-batched mode, but protects against stale profile-wide cache entries).
        if cid_set and cid not in cid_set:
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


def _compute_daily_budget_metrics(
    asin_daily: List[Dict],
    cap_by_date: Dict[str, float],
    days: int,
    exhaustion_threshold: float = 0.85,
) -> Dict:
    """
    Compute day-level budget utilization metrics from spAdvertisedProduct daily records.

    Uses per-day effective budget caps (reconstructed from BUDGET_AMOUNT change history)
    so utilization is accurate even when budgets changed mid-period.  The denominator on
    each date reflects the cap that was actually active on that date, not the current
    snapshot — eliminating the previous budget_overage_ratio heuristic entirely.

    cap_by_date: {date_str: total_effective_cap} — built by _compute_campaign_budget_coverage.
    """
    if not asin_daily or not cap_by_date:
        return {}

    from collections import defaultdict
    raw_by_date: dict = defaultdict(float)
    for r in asin_daily:
        dt = r.get("date")
        if dt:
            raw_by_date[dt] += float(r.get("spend") or 0)

    if not raw_by_date:
        return {}

    sorted_dates = sorted(raw_by_date.keys())[-days:]
    # Only include dates that have both spend > 0 and a known cap
    active_pairs = [
        (raw_by_date[d], cap_by_date[d])
        for d in sorted_dates
        if raw_by_date[d] > 0 and cap_by_date.get(d, 0) > 0
    ]
    if not active_pairs:
        return {}

    active_days = len(active_pairs)
    utilizations = [s / c for s, c in active_pairs]
    exhausted = sum(1 for u in utilizations if u >= exhaustion_threshold)
    overdelivery_days = sum(1 for u in utilizations if u > 1.0)
    avg_util = sum(utilizations) / active_days
    sorted_utils = sorted(utilizations)
    p90_util = sorted_utils[int(active_days * 0.9)]
    max_util = sorted_utils[-1]
    exhausted_pct = round(exhausted / active_days * 100, 1)

    if exhausted_pct >= 75:
        pressure = "chronic"
    elif exhausted_pct >= 30:
        pressure = "moderate"
    elif exhausted_pct >= 10:
        pressure = "light"
    else:
        pressure = "none"

    return {
        "budget_active_days":          active_days,
        "budget_exhausted_days":       exhausted,
        "budget_exhausted_days_pct":   exhausted_pct,
        "overdelivery_days":           overdelivery_days,
        "avg_daily_utilization_pct":   round(avg_util * 100, 1),
        "p90_daily_utilization_pct":   round(p90_util * 100, 1),
        "max_daily_utilization_pct":   round(max_util * 100, 1),
        "budget_pressure":             pressure,
    }


def _compute_campaign_budget_coverage(
    camp_meta: Dict[str, Dict],
    asin_daily: List[Dict],
    change_events: List[Dict],
    days: int,
    exhaustion_threshold: float = 0.85,
) -> Tuple[List[Dict], Dict[str, float]]:
    """
    Per-campaign intraday budget coverage + account-level per-day cap dict.

    Algorithm:
    1. Reconstruct the effective budget cap for each historical date using
       BUDGET_AMOUNT change-history events.  Each change carries (prev, new) so
       _cap_on_date uses prev_value directly — no longer approximating via an
       adjacent change's new_value (fixes the single-change-event bug).
    2. For each (campaign, date), compare daily spend vs. effective cap.
       Spend ≥ 85% of cap → campaign likely exhausted budget before midnight.
    3. Accumulate per-date total effective cap across campaigns (cap_by_date),
       used by _compute_daily_budget_metrics for correct per-day utilization.

    Returns (per_campaign_coverage, cap_by_date).
    """
    if not asin_daily or not camp_meta:
        return [], {}

    # Build per-campaign daily spend: {cid: {date: spend}}
    daily_by_cid: Dict[str, Dict[str, float]] = {}
    for r in asin_daily:
        cid = str(r.get("campaign_id", "") or "")
        dt  = r.get("date") or r.get("report_date", "")
        if not cid or not dt:
            continue
        daily_by_cid.setdefault(cid, {})
        daily_by_cid[cid][dt] = daily_by_cid[cid].get(dt, 0.0) + float(r.get("spend") or 0)

    # Reconstruct historical budget caps from BUDGET_AMOUNT change events.
    # Store (ev_date, prev_budget, new_budget) so _cap_on_date can use prev_budget
    # directly instead of inferring it from an adjacent change's new_budget.
    budget_changes: Dict[str, List[Tuple[str, float, float]]] = {}
    for ev in sorted(change_events, key=lambda e: e.get("changed_at") or 0):
        if ev.get("change_type") != "BUDGET_AMOUNT":
            continue
        cid  = str(ev.get("campaign_id") or "")
        ts   = ev.get("changed_at")
        nval = ev.get("new_value")
        pval = ev.get("old_value")
        if not cid or not ts or nval is None or pval is None:
            continue
        try:
            epoch_s = int(ts) / 1000
            ev_date = datetime.fromtimestamp(epoch_s, tz=timezone.utc).strftime("%Y-%m-%d")
            budget_changes.setdefault(cid, []).append(
                (ev_date, float(pval), float(nval))
            )
        except (TypeError, ValueError, OSError):
            continue

    cap_by_date: Dict[str, float] = {}
    results = []

    for cid, meta in camp_meta.items():
        cid_str     = str(cid)
        current_cap = float(meta.get("daily_budget") or 0)
        if current_cap <= 0:
            continue
        camp_daily   = daily_by_cid.get(cid_str, {})
        sorted_dates = sorted(camp_daily.keys())[-days:]
        if not sorted_dates:
            continue

        changes_for_cid = sorted(budget_changes.get(cid_str, []), key=lambda x: x[0])

        def _cap_on_date(dt: str, _changes=changes_for_cid, _cur=current_cap) -> float:
            # Walk in reverse (newest → oldest).  For each change that occurred
            # AFTER dt, the cap active on dt was that change's prev_budget.
            # The innermost such change (earliest post-dt event) gives the cap.
            cap = _cur
            for ev_date, prev_budget, _new in reversed(_changes):
                if ev_date > dt:
                    cap = prev_budget
                else:
                    break
            return cap if cap > 0 else _cur

        exhausted   = 0
        active      = 0
        total_spend = 0.0
        for dt in sorted_dates:
            spend = camp_daily.get(dt, 0.0)
            if spend <= 0:
                continue
            active += 1
            total_spend += spend
            cap = _cap_on_date(dt)
            # Accumulate into account-level per-day cap for utilization denominator
            cap_by_date[dt] = cap_by_date.get(dt, 0.0) + cap
            if cap > 0 and spend / cap >= exhaustion_threshold:
                exhausted += 1

        if active == 0:
            continue

        exhausted_pct   = round(exhausted / active * 100, 1)
        all_day_pct     = round(100 - exhausted_pct, 1)
        avg_daily_spend = round(total_spend / active, 2)
        results.append({
            "campaign_id":      cid_str,
            "campaign_name":    (meta.get("campaign_name") or meta.get("name") or cid_str)[:40],
            "active_days":      active,
            "exhausted_days":   exhausted,
            "exhausted_pct":    exhausted_pct,
            "all_day_pct":      all_day_pct,
            "current_budget":   current_cap,
            "avg_daily_spend":  avg_daily_spend,
        })

    results.sort(key=lambda x: x["exhausted_pct"], reverse=True)
    return results, cap_by_date


_MIN_KWS_FOR_STRATUM = 3   # min keyword count for a match-type-specific μ
_MIN_CLICKS_FOR_MU   = 20  # min clicks per keyword to include in μ calculation


def _compute_cvr_prior(kw_perf: List[Dict]) -> Tuple[Dict[str, float], float]:
    """
    Compute click-weighted mean CVR (μ) per match type and globally.

    Keywords below _MIN_CLICKS_FOR_MU are excluded — too noisy to anchor the prior.
    Match types with fewer than _MIN_KWS_FOR_STRATUM qualifying keywords fall back
    to the global μ so small keyword pools don't produce unstable per-stratum priors.

    Returns (mu_by_match_type, global_mu).
    """
    from collections import defaultdict
    buckets: Dict[str, List[Tuple[int, float]]] = defaultdict(list)
    all_pairs: List[Tuple[int, float]] = []

    for kw in kw_perf:
        clicks = kw.get("total_clicks", 0)
        cvr    = kw.get("cvr")
        if not cvr or clicks < _MIN_CLICKS_FOR_MU:
            continue
        mt = (kw.get("match_type") or "UNKNOWN").upper()
        pair = (clicks, cvr)
        buckets[mt].append(pair)
        all_pairs.append(pair)

    def _wmean(pairs: List[Tuple[int, float]]) -> Optional[float]:
        total_c = sum(c for c, _ in pairs)
        return sum(c * v for c, v in pairs) / total_c if total_c > 0 else None

    global_mu = _wmean(all_pairs) or 0.02

    mu_by_match_type: Dict[str, float] = {}
    for mt, pairs in buckets.items():
        mu_by_match_type[mt] = _wmean(pairs) if len(pairs) >= _MIN_KWS_FOR_STRATUM else global_mu

    return mu_by_match_type, global_mu


def _build_lp_input(
    kw_perf: List[Dict],
    camp_meta: Dict[str, Dict],
    brand_kws: set,
    headroom: float,
    placement_multiplier: float,
    daily_perf: Optional[List[Dict]] = None,
    mu_by_match_type: Optional[Dict[str, float]] = None,
    global_mu: float = 0.02,
) -> List[Dict]:
    click_headroom = _p90_headroom(daily_perf or [], headroom)
    mu_map = mu_by_match_type or {}
    lp_input: List[Dict] = []
    for kw in kw_perf:
        if not kw.get("avg_cpc") or not kw.get("cvr"):
            continue
        kw_text    = kw["keyword_text"]
        match_type = kw["match_type"]
        cid        = str(kw.get("campaign_id", ""))
        strategy   = camp_meta.get(cid, {}).get("bidding_strategy", "")
        is_brand   = kw_text.lower() in {b.lower() for b in brand_kws}
        max_daily  = max(round(kw["daily_clicks"] * click_headroom, 1), 1.0)
        min_daily  = round(kw["daily_clicks"] * 0.3, 1) if is_brand else 0.0
        prior_mu   = mu_map.get(match_type.upper(), global_mu)
        lp_input.append({
            "name":                f"{kw_text}|{match_type}|{cid}",
            "avg_cpc":             kw["avg_cpc"],
            "estimated_cvr":       kw["cvr"],
            "sample_clicks":       kw.get("total_clicks", 0),
            "sample_orders":       kw.get("total_orders", 0),
            "prior_mu":            prior_mu,
            "daily_clicks":        kw["daily_clicks"],   # historical avg; source of truth for cur_clicks
            "max_daily_clicks":    max_daily,
            "min_daily_clicks":    min_daily,
            "campaign_id":         cid,
            "bidding_strategy":    strategy,
            "placement_multiplier": placement_multiplier,
        })
    return lp_input


def _classify_lp_keywords(
    kw_perf: List[Dict],
    alloc: List[Dict],
    kw_map: Dict[str, Dict],
    camp_meta: Dict[str, Dict] = {},
) -> Tuple[List[Dict], List[Dict]]:
    alloc_names = {a["keyword"] for a in alloc}
    seen_zero: set = set()
    zero_kws: List[Dict] = []
    for kw in kw_perf:
        composed = f"{kw['keyword_text']}|{kw['match_type']}|{kw.get('campaign_id', '')}"
        display_key = (kw['keyword_text'], kw['match_type'])
        if composed not in alloc_names and kw.get("avg_cpc") and display_key not in seen_zero:
            seen_zero.add(display_key)
            cid = str(kw.get("campaign_id", ""))
            zero_kws.append({
                "keyword":       f"{kw['keyword_text']} ({kw['match_type']})",
                "campaign_id":   cid,
                "campaign_name": (camp_meta.get(cid, {}).get("name") or
                                  camp_meta.get(cid, {}).get("campaign_name") or cid)[:40],
                "acos_pct":      kw.get("acos"),
            })
    # Build kw_perf lookup for ACOS on maxed keywords
    _kw_acos: Dict[tuple, Optional[float]] = {
        (kw["keyword_text"], kw["match_type"], str(kw.get("campaign_id", ""))): kw.get("acos")
        for kw in kw_perf
    }
    seen_maxed: set = set()
    maxed_kws: List[Dict] = []
    for a in alloc:
        cap = kw_map.get(a["keyword"], {}).get("max_daily_clicks", 0)
        parts = a["keyword"].split("|")
        display_key = (parts[0], parts[1]) if len(parts) > 1 else (parts[0],)
        if cap and a["optimized_clicks"] >= cap * 0.95 and display_key not in seen_maxed:
            seen_maxed.add(display_key)
            cid = parts[2] if len(parts) > 2 else ""
            acos_pct = _kw_acos.get((parts[0], parts[1] if len(parts) > 1 else "", cid))
            maxed_kws.append({
                "keyword":       f"{parts[0]} ({parts[1]})" if len(parts) > 1 else parts[0],
                "campaign_id":   cid,
                "campaign_name": (camp_meta.get(cid, {}).get("name") or
                                  camp_meta.get(cid, {}).get("campaign_name") or cid)[:40],
                "acos_pct":      acos_pct,
            })
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

        budget_util  = round(actual_spend / camp_budget * 100, 1)
        lp_saturated = lp_spend >= camp_budget * 0.90
        in_lp_scope  = bool(_lp_scoped) and (cid in _lp_scoped)

        camp_perf         = [r for r in performance_records if str(r.get("campaign_id")) == cid]
        camp_sales        = sum(float(r.get("sales", 0) or 0) for r in camp_perf)
        camp_spend_total  = sum(float(r.get("spend", 0) or 0) for r in camp_perf)
        camp_acos         = round(camp_spend_total / camp_sales * 100, 1) if camp_sales > 0 else None
        camp_orders_total = sum(float(r.get("orders", 0) or 0) for r in camp_perf)
        camp_daily_orders = camp_orders_total / days if days > 0 else 0.0
        camp_cpo          = round(camp_spend_total / camp_orders_total, 2) if camp_orders_total > 0 else None

        target_acos_pct = (target_acos or 0.30) * 100
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
            elif budget_util >= 90:
                suggested = round(camp_budget * 1.2, 0)
                action, priority = "increase_budget", "P0"
                rationale = (
                    f"ACOS {camp_acos}% ≤ target, budget util {budget_util:.1f}% — "
                    f"safe to scale (auto/PT, outside LP scope)"
                )
            else:
                action, priority = "maintain", "P2"
                rationale = (
                    f"ACOS {camp_acos}% ≤ target, {budget_util:.1f}% utilisation — "
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
                    f"LP hits per-campaign budget cap: allocates ${lp_spend:.0f}/day = current cap ${camp_budget:.0f} "
                    f"(zero headroom — LP cannot spend more without a higher cap); "
                    f"ACOS {camp_acos}% ≤ target {target_acos_pct:.0f}% — "
                    f"raise cap to ${suggested:.0f} (+15% headroom) to let LP scale further"
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
            rationale = f"Budget util {budget_util:.1f}%, ACOS {camp_acos}% — within healthy range"

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
                    "catchable_shipped":    inv_gate["catchable_shipped"],
                    "inbound_lead_days":    inv_gate["inbound_lead_days"],
                    "inbound_lead_source":  inv_gate["inbound_lead_source"],
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

    actions.sort(key=lambda x: PRIORITY_SORT.index(x["priority"]))
    return actions


def _build_keyword_actions(
    lp_input: List[Dict],
    alloc: List[Dict],
    kw_id_map: Dict[tuple, Dict],
    brand_kws: set,
    avg_price: Optional[float],
    inv_gate: Optional[Dict] = None,
    paused_campaign_ids: Optional[set] = None,
    spend_ceiling_bound: bool = False,
    target_acos: Optional[float] = None,
    raw_cvr_map: Optional[Dict[str, float]] = None,
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
        # Use pre-deflation CVR for human-facing metrics (kw_acos_pct, expected deltas).
        # lp_kw["estimated_cvr"] was mutated in-place by CVR deflation before LP solve;
        # raw_cvr_map holds the original values captured before that mutation.
        raw_cvr     = (raw_cvr_map or {}).get(kw_name, lp_kw["estimated_cvr"])
        avg_cpc     = lp_kw["avg_cpc"]
        kw_acos_pct = (
            round(avg_cpc / (raw_cvr * avg_price) * 100, 1)
            if avg_price and raw_cvr > 0 else None
        )

        if a is None:
            camp_is_paused = paused_campaign_ids and str(cid) in paused_campaign_ids
            if not is_brand and not camp_is_paused:
                cur_clicks = lp_kw["daily_clicks"]
                target_acos_pct = (target_acos or 0.30) * 100
                if kw_acos_pct is None:
                    # Insufficient data — low-priority pause, don't imply inefficiency
                    _action   = "pause_keyword"
                    _priority = "P2"
                    _rationale = (
                        f"LP assigned 0 clicks — insufficient CVR data (CVR {raw_cvr:.3f}) "
                        f"to evaluate efficiency; keyword excluded from LP allocation"
                    )
                elif kw_acos_pct < target_acos_pct:
                    # Efficient keyword squeezed out by budget — hold, do NOT actively pause
                    _action   = "hold_keyword"
                    _priority = "P2"
                    _rationale = (
                        f"LP assigned 0 clicks — keyword ACOS {kw_acos_pct}% is below target "
                        f"{target_acos_pct:.0f}% but outcompeted by higher-CVR keywords within "
                        f"the budget constraint; do NOT pause — hold and revisit after budget increase"
                    )
                else:
                    # Genuinely inefficient — active pause warranted
                    _action   = "pause_keyword"
                    _priority = "P1"
                    _rationale = (
                        f"LP assigned 0 clicks — CVR {raw_cvr:.3f} × CPC ${avg_cpc:.2f} → "
                        f"keyword ACOS {kw_acos_pct}% exceeds target {target_acos_pct:.0f}% "
                        f"(budget efficiency threshold)"
                    )
                actions.append({
                    "action":               _action,
                    "priority":             _priority,
                    "keyword_text":         kw_text,
                    "match_type":           match_type,
                    "campaign_id":          cid,
                    "keyword_id":           kw_id,
                    "current_bid":          cur_bid,
                    "keyword_acos_pct":     kw_acos_pct,
                    "expected_order_delta": -round(cur_clicks * raw_cvr, 2),
                    "expected_spend_delta": -round(cur_clicks * avg_cpc, 2),
                    "rationale": _rationale,
                })
        else:
            opt_clicks = a["optimized_clicks"]
            cur_clicks = lp_kw["daily_clicks"]
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
                            "catchable_shipped":    inv_gate["catchable_shipped"],
                            "inbound_lead_days":    inv_gate["inbound_lead_days"],
                            "inbound_lead_source":  inv_gate["inbound_lead_source"],
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

    actions.sort(key=lambda x: PRIORITY_SORT.index(x["priority"]))
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
      pess_cvr_i = (μ_i·s_i + orders_i) / (s_i + clicks_i)   [Beta-Binomial shrinkage]
      s_i = k / μ_i,   k = _K_CVR_PRIOR ≈ 1.0 expected conversion to trust data
      μ_i = click-weighted mean CVR for this keyword's match type (falls back to
            account-level mean when the match-type stratum has < 3 qualifying keywords)

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

        perf_records_all = item.get("performance_records") or []
        historical_spend_total = sum(
            float(r.get("spend", 0) or 0)
            for r in perf_records_all
            if str(r.get("campaign_id")) in campaign_ids
        )
        historical_daily_spend = round(historical_spend_total / days, 2) if days > 0 else 0.0

        if not kw_perf or daily_budget <= 0:
            item["lp_summary"] = {"skipped": True, "reason": "no keyword data or zero budget",
                                  "lp_scoped_cids": []}
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
        total_orders    = item.get("total_orders") or 0
        total_sales     = item.get("total_sales")  or 0
        avg_price       = round(total_sales / total_orders, 2) if total_orders > 0 else None
        can_sell_days   = item.get("can_sell_days")
        total_available = item.get("total_available") or 0

        asin       = (item.get("asin") or "").upper()
        asin_daily = ctx.cache.get(f"{_KEY_DAILY_PERF}:{asin}", [])
        if not asin_daily:
            # Fallback: filter the all-campaigns daily cache by this ASIN's campaign_ids.
            # This handles test fixtures and edge cases where the per-ASIN L1 key was
            # not populated (e.g., _ensure_daily_performance ran from L2 without writing L1).
            _cid_strs  = {str(cid) for cid in (item.get("campaign_ids") or [])}
            asin_daily = [
                r for r in ctx.cache.get(_KEY_DAILY_PERF, [])
                if str(r.get("campaign_id", "")) in _cid_strs
            ]

        # Per-campaign intraday coverage + account-level per-day cap dict.
        # cap_by_date is used as the per-day denominator for utilization metrics,
        # replacing the single-scalar budget cap and the budget_overage_ratio heuristic.
        coverage, cap_by_date = _compute_campaign_budget_coverage(
            camp_meta, asin_daily,
            item.get("change_events") or [],
            days,
        )
        item["campaign_budget_coverage"] = coverage

        # Day-level budget pressure metrics with per-day effective cap denominators.
        budget_metrics = _compute_daily_budget_metrics(asin_daily, cap_by_date, days)
        item.update(budget_metrics)

        mu_by_mt, global_mu = _compute_cvr_prior(kw_perf)
        lp_input = _build_lp_input(
            kw_perf, camp_meta, brand_kws, headroom, placement_multiplier,
            daily_perf=asin_daily,
            mu_by_match_type=mu_by_mt,
            global_mu=global_mu,
        )
        lp_scope_cids_pre = {str(kw.get("campaign_id", "")) for kw in lp_input if kw.get("campaign_id")}
        if not lp_input:
            item["lp_summary"] = {"skipped": True, "reason": "all keywords filtered (insufficient clicks)",
                                  "lp_scoped_cids": sorted(lp_scope_cids_pre)}
            continue

        # ── LP-scope budget: use only LP-scope campaign budgets as global cap ──
        # Non-LP campaigns (auto/PT) consume their own separate campaign budgets.
        # Giving LP the total account budget ($239) inflates the constraint beyond
        # what LP-scope campaigns can actually spend; per-campaign Constraint 2
        # becomes the real binding cap anyway.  Using LP-scope budget makes the
        # global constraint match the actual LP operating budget and prevents the
        # LLM from summing lp_optimal_spend + non_lp_scope_daily_spend against
        # the total account budget (which would always look contradictory).
        lp_scope_campaign_budget_raw = sum(
            float(camp_meta[cid].get("daily_budget") or 0)
            for cid in lp_scope_cids_pre
            if cid in camp_meta and (camp_meta[cid].get("state") or "").upper() == "ENABLED"
        )
        lp_scope_hist_spend_total = sum(
            float(r.get("spend", 0) or 0)
            for r in perf_records_all
            if str(r.get("campaign_id", "")) in lp_scope_cids_pre
        )
        lp_scope_hist_daily = round(lp_scope_hist_spend_total / days, 2) if days > 0 else 0.0
        # LP budget = current campaign snapshot (forward-looking; per-day cap reconstruction
        # makes the old budget_overage_ratio heuristic unnecessary here too).
        lp_budget = lp_scope_campaign_budget_raw if lp_scope_campaign_budget_raw > 0 else daily_budget

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
        # Capture raw CVR BEFORE deflation mutates lp_input in-place.
        # lp_raw_orders uses this map so that order_gap compares:
        #   LP side  : optimized_clicks × un-deflated raw CVR (same basis as actual)
        #   Actual   : kw_attributed_orders / days           (un-deflated)
        # Without this, lp_raw_orders uses deflated CVR while actual is un-deflated,
        # creating a systematic downward bias in order_gap when cvr_deflation < 1.
        raw_cvr_map = {kw["name"]: kw["estimated_cvr"] for kw in lp_input}
        if cvr_deflation < 1.0:
            for kw in lp_input:
                kw["estimated_cvr"] *= cvr_deflation
                kw["prior_mu"]      *= cvr_deflation
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
            item["lp_summary"] = {"skipped": True, "reason": result.get("message"),
                                  "lp_scoped_cids": sorted(lp_scope_cids_pre)}
            continue

        summary    = result["summary"]
        alloc      = result["allocation"]
        camp_spend = result.get("camp_spend", {})
        kw_map     = {lp["name"]: lp for lp in lp_input}

        # raw_cvr_map was captured before deflation — see comment above.
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
        zero_kws, maxed_kws = _classify_lp_keywords(kw_perf, alloc, kw_map, camp_meta)
        kw_id_map           = _build_lp_kw_id_map(ctx, campaign_ids)

        # ── Inventory gate ────────────────────────────────────────────────
        stock_gate_days   = ctx.config.get("stock_gate_days", 21)
        # inbound_lead_days priority: (1) Lingxing sea_transit.p75, (2) SP-API plans p75
        # (biased longer by plan-creation lag — conservative upper bound), (3) config default.
        _lt      = item.get("shipment_lead_time") or {}
        _lt_sea  = _lt.get("sea_transit", {}).get("overall", {})
        _lt_src  = _lt.get("data_source", "none")
        inbound_lead_days = int(
            _lt_sea.get("p75") or ctx.config.get("inbound_lead_days", 30)
        )
        daily_sales_val   = item.get("daily_sales") or 0
        inbound_receiving = item.get("inbound_receiving") or 0
        inbound_shipped   = item.get("inbound_shipped") or 0
        effective_stock_days: Optional[int] = None
        catchable_shipped = 0
        if can_sell_days and daily_sales_val > 0:
            # inbound_shipped only "catches" the stockout if it arrives before current stock runs out
            catchable_shipped = inbound_shipped if inbound_lead_days < can_sell_days else 0
            eff_units = total_available + inbound_receiving + catchable_shipped
            effective_stock_days = round(eff_units / daily_sales_val)
        inv_gate: Optional[Dict] = (
            {
                "stock_gate_days":      stock_gate_days,
                "effective_stock_days": effective_stock_days,
                "can_sell_days":        can_sell_days,
                "inbound_receiving":    inbound_receiving,
                "inbound_shipped":      inbound_shipped,
                "catchable_shipped":    catchable_shipped,
                "inbound_lead_days":    inbound_lead_days,
                "inbound_lead_source":  _lt_src,
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
            lp_input, alloc, kw_id_map, brand_kws, avg_price,
            inv_gate=inv_gate,
            paused_campaign_ids=paused_cids,
            spend_ceiling_bound=spend_ceiling_bound,
            target_acos=target_acos,
            raw_cvr_map=raw_cvr_map,
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
            "lp_orders_bbs_estimate":  summary["total_expected_orders"],
            "lp_orders_cvr_matched":   lp_raw_orders,
            "actual_daily_ad_orders":        round(actual_daily_ad_orders, 2),
            "auto_pt_daily_orders":          round(auto_pt_daily_orders, 2),
            "order_gap":                     round(order_gap, 2),
            "spend_ceiling_bound":           spend_ceiling_bound,
            "budget_binding":                budget_binding,
            "click_headroom":               _p90_headroom(asin_daily, headroom),
            "avg_effective_cpc":             summary["avg_effective_cpc"],
            "placement_multiplier":          round(placement_multiplier, 3),
            "placement_data_unknown":        placement_data_unknown,
            "target_acos_applied":           round(target_acos * 100, 1) if target_acos is not None else None,
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
            # Historical spend (informational; utilization metrics use per-day caps)
            "historical_daily_spend":       historical_daily_spend,
            # LP scope vs non-LP spend split:
            # lp_optimal_spend covers only lp_scope keywords; non_lp_scope_daily_spend
            # explains why total_historical_daily_spend >> lp_optimal_spend.
            "lp_scope_daily_spend":         lp_scope_hist_spend,
            "non_lp_scope_daily_spend":     non_lp_scope_hist_spend,
            # Campaign IDs in LP scope — used by _mine_auto_campaigns to derive auto/PT campaigns.
            "lp_scoped_cids":               sorted(lp_scoped_cids),
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

        # ── LP reallocation table: per-campaign current vs optimal (spend + orders) ──
        # LP is a global optimiser — it does not "take from A and give to B".
        # Each campaign receives an independently computed optimal allocation.
        # delta_spend = lp_optimal_spend − historical_actual_spend (NOT budget_cap − budget_cap).
        # Σ delta_spend ≠ 0 whenever campaigns historically underspent their budget caps:
        #   sum(lp_optimal_spend) ≤ lp_budget (cap constraint)
        #   sum(cur_spend) ≤ lp_budget (but typically < cap when utilisation < 100%)
        #   Net > 0 means LP plans to deploy budget that was previously unspent.
        # delta_orders uses raw_cvr_map (un-deflated) — same basis as order_gap.
        if days > 0:
            _camp_cur_spend: Dict[str, float] = {}
            _camp_cur_orders: Dict[str, float] = {}
            _camp_cur_sales: Dict[str, float] = {}
            for r in perf_records_all:
                cid = str(r.get("campaign_id", ""))
                if cid in lp_scoped_cids:
                    _camp_cur_spend[cid]  = _camp_cur_spend.get(cid, 0.0)  + float(r.get("spend",  0) or 0)
                    _camp_cur_orders[cid] = _camp_cur_orders.get(cid, 0.0) + float(r.get("orders", 0) or 0)
                    _camp_cur_sales[cid]  = _camp_cur_sales.get(cid, 0.0)  + float(r.get("sales",  0) or 0)
            _camp_cur_spend_d  = {k: round(v / days, 2) for k, v in _camp_cur_spend.items()}
            _camp_cur_orders_d = {k: round(v / days, 2) for k, v in _camp_cur_orders.items()}

            _camp_lp_orders: Dict[str, float] = {}
            for a in alloc:
                cid = str(a.get("campaign_id", ""))
                _camp_lp_orders[cid] = (
                    _camp_lp_orders.get(cid, 0.0)
                    + a["optimized_clicks"] * raw_cvr_map.get(a["keyword"], 0.0)
                )

            _all_cids = lp_scoped_cids | set(camp_spend.keys())
            _realloc: List[Dict] = []
            for cid in _all_cids:
                cur_spend   = _camp_cur_spend_d.get(cid, 0.0)
                lp_spend_v  = round(camp_spend.get(cid, 0.0), 2)
                cur_orders  = _camp_cur_orders_d.get(cid, 0.0)
                lp_orders_v = round(_camp_lp_orders.get(cid, 0.0), 2)
                cur_sales   = _camp_cur_sales.get(cid, 0.0)
                camp_acos   = round((_camp_cur_spend.get(cid, 0.0) / cur_sales * 100), 1) \
                              if cur_sales > 0 else None
                name = (camp_meta.get(cid) or {}).get("name") or cid
                _realloc.append({
                    "campaign_name":  name,
                    "acos_pct":       camp_acos,
                    "current_spend":  cur_spend,
                    "lp_spend":       lp_spend_v,
                    "delta_spend":    round(lp_spend_v - cur_spend, 2),
                    "current_orders": cur_orders,
                    "lp_orders":      lp_orders_v,
                    "delta_orders":   round(lp_orders_v - cur_orders, 2),
                })
            # Gainers first (delta_orders desc), then losers (delta_orders asc within negatives)
            _realloc.sort(key=lambda x: (-x["delta_orders"], -abs(x["delta_spend"])))

            # Net across ALL LP-scoped campaigns.
            # delta_spend = lp_optimal_spend − cur_spend; > 0 when campaigns underspent budgets.
            item["lp_reallocation_net"] = {
                "delta_spend":  round(sum(r["delta_spend"]  for r in _realloc), 2),
                "delta_orders": round(sum(r["delta_orders"] for r in _realloc), 2),
                "n_total":      len(_realloc),
            }

            # Show all significant movers: |delta_spend| ≥ $1 or |delta_orders| ≥ 0.1
            item["lp_reallocation_table"] = [
                r for r in _realloc
                if abs(r["delta_spend"]) >= 1.0 or abs(r["delta_orders"]) >= 0.1
            ]

            # Back-fill lp_delta_orders onto campaign_actions so the action group table
            # uses the same LP-projected delta as lp_reallocation_table, not the
            # (suggested_budget − camp_budget) / camp_cpo estimate which only captures
            # the marginal budget-increment effect and diverges from the LP reallocation gain.
            _lp_delta_by_cid: Dict[str, float] = {
                cid: round(_camp_lp_orders.get(cid, 0.0) - _camp_cur_orders_d.get(cid, 0.0), 2)
                for cid in _all_cids
            }
            for _ca in item.get("campaign_actions", []):
                _ca_cid = str(_ca.get("campaign_id", ""))
                if _ca_cid in _lp_delta_by_cid:
                    _ca["lp_delta_orders"] = _lp_delta_by_cid[_ca_cid]

        # Conflict suppression: a campaign-level increase_budget directly contradicts a
        # keyword-level decrease_bid on the same campaign — the extra budget would flow
        # into over-allocated keywords, worsening the inefficiency the bid cut was meant
        # to fix.  Downgrade such campaign actions to review_bids and record the conflict.
        decrease_bid_cids = {
            str(ka["campaign_id"])
            for ka in keyword_actions
            if ka.get("action") == "decrease_bid" and ka.get("campaign_id")
        }
        for ca in campaign_actions:
            if ca.get("action") in {"increase_budget", "enable_and_increase_budget"} and \
                    str(ca.get("campaign_id", "")) in decrease_bid_cids:
                original_action = ca["action"]
                ca["action"] = "review_bids" if original_action == "increase_budget" \
                    else "enable_and_review_bids"
                ca["priority"] = "P1"
                ca["rationale"] = (
                    f"[Conflict resolved] {ca['rationale']} — "
                    f"however, keyword_actions include decrease_bid for this campaign's keywords; "
                    f"increasing budget would amplify over-allocated spend. "
                    f"Downgraded from {original_action}: resolve keyword bid efficiency first, "
                    f"then re-evaluate budget."
                )

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

        sorted_dates = sorted(competitor_price_summary.keys())
        avg_daily_count = (
            round(sum(v["count"] for v in competitor_price_summary.values())
                  / len(competitor_price_summary), 1)
            if competitor_price_summary else 0
        )
        competitor_price_meta = {
            "n_competitors":   len(comp_price_by_asin),  # ASINs with actual data (not raw input count)
            "date_from":       sorted_dates[0]  if sorted_dates else None,
            "date_to":         sorted_dates[-1] if sorted_dates else None,
            "avg_daily_count": avg_daily_count,
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
            "competitor_price_meta":    competitor_price_meta,
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
    today = datetime.now(tz=_store_tz(ctx)).date()
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
    today    = datetime.now(tz=_store_tz(ctx)).date()
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


# Auto campaign search-term mining
# ---------------------------------------------------------------------------

def _mine_auto_campaigns(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    ProcessStep: mine auto/PT campaign search terms for negative keyword candidates
    and harvest-to-manual candidates using Empirical Bayes thresholds.

    Reads raw spSearchTerm records from the L1 cache (_KEY_KW_PERFORMANCE),
    filters to auto/PT campaign IDs (all campaign_ids minus lp_scoped_cids),
    and writes item["auto_mining"] with negatives, harvest, beta_prior, summary.
    """
    from src.intelligence.processors.auto_mining import build_auto_mining_actions

    raw_st_records: List[Dict] = ctx.cache.get(_KEY_KW_PERFORMANCE, [])
    target_acos = ctx.config.get("target_acos", 0.30)
    days        = ctx.config.get("days", 30)

    for item in items:
        campaign_ids  = {str(c) for c in (item.get("campaign_ids") or [])}
        lp_scoped_cids = {str(c) for c in (item.get("lp_summary") or {}).get("lp_scoped_cids", [])}
        auto_pt_cids  = campaign_ids - lp_scoped_cids

        # Existing manual keywords for harvest deduplication
        existing_manual = {
            (kw.get("keyword_text") or "").strip().lower()
            for kw in (item.get("keyword_performance") or [])
            if kw.get("keyword_text")
        }

        total_orders = item.get("total_orders") or 0
        total_sales  = item.get("total_sales")  or 0
        avg_price    = round(total_sales / total_orders, 2) if total_orders > 0 else 0.0

        # Filter raw records to this ASIN's campaigns (avoids passing full account data)
        asin_records = [r for r in raw_st_records
                        if str(r.get("campaign_id", "")) in campaign_ids]

        item["auto_mining"] = build_auto_mining_actions(
            search_term_records = asin_records,
            auto_pt_cids        = auto_pt_cids,
            existing_manual_kws = existing_manual,
            avg_price           = avg_price,
            target_acos         = target_acos,
            days                = days,
        )
        logger.debug(
            f"[auto_mining] {item.get('asin')}: "
            f"{len(item['auto_mining'].get('negatives', []))} negatives, "
            f"{len(item['auto_mining'].get('harvest', []))} harvest candidates"
        )

    return items


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
        asin = item.get("asin", "?").upper()
        try:
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
        except (ValueError, ImportError) as e:
            # Expected operational failures: insufficient data, date-range issues,
            # or optional dependency (rpy2 / causalimpact) not installed.
            logger.warning(f"[causal_analysis] Skipped {asin}: {type(e).__name__}: {e}")
            item["causal_analysis_error"] = f"{type(e).__name__}: {e}"
        except Exception as e:
            # Unexpected error — likely a code bug; log with full traceback so it
            # surfaces in monitoring rather than being silently swallowed.
            logger.error(
                f"[causal_analysis] Unexpected error for {asin}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            item["causal_analysis_error"] = f"{type(e).__name__}: {e}"
    return items


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------

_C = _CHART_PALETTE  # shorthand


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
    ax.legend(fontsize=8, loc="upper center",
              bbox_to_anchor=(0.5, -0.12), ncol=3,
              framealpha=0.6, borderpad=0.5)
    for i, row in enumerate(rows):
        delta = (row.get("lp_optimal_spend") or 0) - (row.get("actual_daily_spend") or 0)
        if abs(delta) >= 0.5:
            ax.text(max(lp_opt[i], current[i]) + 0.5, y[i] + 0.22,
                    f"{delta:+.0f}", fontsize=7, va="center", color=_C["blue"])
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    return _fig_to_png(fig)


def _chart_campaign_budget_coverage(item: Dict) -> Optional[bytes]:
    """
    Per-campaign intraday budget coverage stacked bar chart.
    Each bar = active_days split into: all-day (green) vs exhausted (red/orange).
    Right-hand side: spend vs. budget cap comparison bars.
    """
    if not (item.get("budget_starved_campaigns") or 0):
        return None  # all campaigns had full-day coverage — chart adds no signal

    coverage: List[Dict] = item.get("campaign_budget_coverage") or []
    coverage = [c for c in coverage if c.get("active_days", 0) > 0]
    if not coverage:
        return None

    rows = coverage[:8]  # max 8 campaigns
    names        = [r["campaign_name"][:28] for r in rows]
    active       = [r["active_days"]        for r in rows]
    exhausted    = [r["exhausted_days"]     for r in rows]
    all_day      = [a - e for a, e in zip(active, exhausted)]
    exh_pct      = [r["exhausted_pct"]      for r in rows]
    avg_spend    = [r["avg_daily_spend"]    for r in rows]
    cap          = [r["current_budget"]     for r in rows]

    def _bar_color(pct: float) -> str:
        if pct >= 75:   return _C["red"]
        if pct >= 30:   return _C["orange"]
        return _C["green"]

    y = np.arange(len(rows))
    fig, (ax_days, ax_spend) = plt.subplots(
        1, 2, figsize=(12, max(3.5, len(rows) * 0.65)),
        gridspec_kw={"width_ratios": [1.4, 1.0]},
        facecolor=_C["bg"],
    )

    # ── Left: stacked day bars ─────────────────────────────────────────────
    ax_days.set_facecolor(_C["bg"])
    ax_days.barh(y, all_day,   left=0,       height=0.55,
                 color=_C["green"], alpha=0.85, label="Budget lasted all day")
    for i, (e, a, pct) in enumerate(zip(exhausted, all_day, exh_pct)):
        color = _bar_color(pct)
        ax_days.barh(i, e, left=a, height=0.55,
                     color=color, alpha=0.85,
                     label=("Mid-day exhausted" if i == 0 else "_nolegend_"))
        # Label inside exhausted segment if wide enough
        if e >= 1:
            ax_days.text(a + e / 2, i, f"{pct:.0f}%",
                         ha="center", va="center", fontsize=7.5,
                         color="white", fontweight="bold")

    ax_days.set_yticks(y)
    ax_days.set_yticklabels(names, fontsize=8)
    ax_days.set_xlabel("Active days", fontsize=9)
    ax_days.set_title("Budget Coverage\n(green = full day | colored = ran dry)",
                      fontsize=9, pad=6)
    ax_days.legend(fontsize=7.5, loc="upper center",
                   bbox_to_anchor=(0.5, -0.12), ncol=2, framealpha=0.5)

    # ── Right: spend vs cap comparison ────────────────────────────────────
    ax_spend.set_facecolor(_C["bg"])
    ax_spend.barh(y + 0.18, cap,       0.30, color=_C["grey"],
                  alpha=0.45, label="Daily budget cap")
    for i, (s, c_val, pct) in enumerate(zip(avg_spend, cap, exh_pct)):
        color = _bar_color(pct)
        ax_spend.barh(i - 0.18, s, 0.30,
                      color=color, alpha=0.85,
                      label=("Avg daily spend" if i == 0 else "_nolegend_"))
        gap = s - c_val
        gap_str = f"${gap:+.0f}" if abs(gap) >= 0.5 else ""
        ax_spend.text(max(s, c_val) + 0.5, i,
                      gap_str, fontsize=7, va="center",
                      color=_C["red"] if gap > 0 else _C["grey"])

    ax_spend.set_yticks(y)
    ax_spend.set_yticklabels([])
    ax_spend.set_xlabel("Daily Spend ($)", fontsize=9)
    ax_spend.set_title("Avg Spend vs Cap\n(grey = cap | colored = avg spend)",
                       fontsize=9, pad=6)
    ax_spend.legend(fontsize=7.5, loc="upper center",
                    bbox_to_anchor=(0.5, -0.12), ncol=2, framealpha=0.5)

    fig.suptitle(f"{item.get('asin','?')} — Campaign Intraday Budget Coverage",
                 fontsize=10, y=1.01)
    fig.tight_layout(rect=[0, 0.1, 1, 1.0])
    return _fig_to_png(fig)


def _chart_budget_utilization(item: Dict, daily_perf: List[Dict]) -> Optional[bytes]:
    """
    Daily spend vs budget cap bar chart, bars colour-coded by utilization tier:
      🔴 red    ≥ 85 % of cap  (exhausted — budget is binding)
      🟡 orange 60–84 %        (high utilization)
      🔵 blue   < 60 %         (healthy headroom)
      grey      inactive day   (zero spend)
    Horizontal lines at 85 % (exhaustion threshold) and 100 % (cap).
    """
    if not daily_perf:
        return None

    budget_cap = float(item.get("total_daily_budget") or 0)
    if budget_cap <= 0:
        return None

    # Aggregate spend across all campaigns per date
    by_date: Dict[str, float] = {}
    for r in daily_perf:
        d = r.get("date") or r.get("report_date", "")
        if d:
            by_date[d] = by_date.get(d, 0.0) + float(r.get("spend") or 0)

    dates = sorted(by_date)
    if len(dates) < 3:
        return None

    dt_objs = [_date_cls.fromisoformat(d) for d in dates]
    spends   = [by_date[d] for d in dates]

    # Colour each bar by utilization tier
    bar_colors = []
    for s in spends:
        util = s / budget_cap
        if s == 0:
            bar_colors.append(_C["grey"])
        elif util >= 0.85:
            bar_colors.append(_C["red"])
        elif util >= 0.60:
            bar_colors.append(_C["orange"])
        else:
            bar_colors.append(_C["light_blue"])

    pressure     = item.get("budget_pressure") or "unknown"
    exh_days     = item.get("budget_exhausted_days")
    act_days     = item.get("budget_active_days")
    avg_util     = item.get("avg_daily_utilization_pct")
    p90_util     = item.get("p90_daily_utilization_pct")

    exh_label = (f"{exh_days}/{act_days}d hit cap" if exh_days is not None and act_days
                 else "")
    avg_label = (f"avg {avg_util:.0f}%  p90 {p90_util:.0f}%"
                 if avg_util is not None and p90_util is not None else "")

    fig, ax = plt.subplots(figsize=(10, 4), facecolor=_C["bg"])
    ax.set_facecolor(_C["bg"])

    bar_w = max(0.6, 0.8 * (dt_objs[-1] - dt_objs[0]).days / len(dt_objs))
    ax.bar(dt_objs, spends, width=bar_w, color=bar_colors, zorder=2)

    # Budget cap line and 85 % threshold line
    ax.axhline(budget_cap,          color=_C["grey"],   lw=1.2, linestyle="-",
               label=f"Budget cap ${budget_cap:.0f}/day", zorder=3)
    ax.axhline(budget_cap * 0.85,   color=_C["red"],    lw=1.0, linestyle="--",
               label="85 % cap (exhaustion threshold)", zorder=3)

    # Change-event vertical markers
    change_dates = {a.get("changed_at") for a in (item.get("change_attributions") or [])
                    if a.get("changed_at")}
    for cd in change_dates:
        try:
            ax.axvline(_date_cls.fromisoformat(cd), color=_C["orange"],
                       lw=1.0, linestyle="--", alpha=0.7, zorder=4)
        except Exception:
            pass

    # Legend patches for colour tiers
    from matplotlib.patches import Patch
    legend_patches = [
        Patch(color=_C["red"],        label="Exhausted ≥ 85 %"),
        Patch(color=_C["orange"],     label="High 60–84 %"),
        Patch(color=_C["light_blue"], label="Healthy < 60 %"),
        Patch(color=_C["grey"],       label="Inactive"),
    ]
    ax.legend(handles=legend_patches, fontsize=7, loc="upper left",
              ncol=2, framealpha=0.7)

    ax.set_ylabel("Daily Spend ($)", fontsize=9)
    title_parts = [f"{item.get('asin','?')} — Budget Utilization",
                   f"pressure={pressure}"]
    if exh_label:
        title_parts.append(exh_label)
    if avg_label:
        title_parts.append(avg_label)
    ax.set_title("  |  ".join(title_parts), fontsize=10, pad=6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    fig.autofmt_xdate(rotation=30)
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
    n_kw = min(len(rank_series), 5)
    ax.legend(fontsize=7, loc="upper center",
              bbox_to_anchor=(0.5, -0.18), ncol=n_kw,
              framealpha=0.6, borderpad=0.5)
    fig.tight_layout(rect=[0, 0.08, 1, 1])
    return _fig_to_png(fig)


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


def _chart_its_causal(item: Dict, daily_perf: List[Dict],
                      causal_metric: str = "orders") -> Optional[bytes]:
    attributions = item.get("change_attributions") or []
    if not attributions or not daily_perf:
        return None
    attr = next((a for a in attributions if not (a.get("its") or {}).get("skipped")), None)
    if not attr:
        return None
    change_date_str = attr.get("changed_at", "")
    if not change_date_str:
        return None

    _DIRECT_M  = {"orders", "spend", "clicks", "sales"}
    _DERIVED_M = {"acos", "cvr", "cpc"}
    metric = causal_metric if causal_metric in _DIRECT_M | _DERIVED_M else "orders"

    # Accumulate raw daily totals; derive ratio metrics after summing.
    _acc: Dict[str, Dict[str, float]] = {}
    for r in daily_perf:
        d = r.get("date") or r.get("report_date", "")
        if not d:
            continue
        if d not in _acc:
            _acc[d] = {"orders": 0.0, "spend": 0.0, "clicks": 0.0, "sales": 0.0}
        for k in ("orders", "spend", "clicks", "sales"):
            _acc[d][k] += float(r.get(k, 0) or 0)

    def _derive(day: Dict[str, float]) -> float:
        if metric == "acos":
            return round(day["spend"] / day["sales"] * 100, 4) if day["sales"] > 0 else 0.0
        if metric == "cvr":
            return round(day["orders"] / day["clicks"], 6) if day["clicks"] > 0 else 0.0
        if metric == "cpc":
            return round(day["spend"] / day["clicks"], 4) if day["clicks"] > 0 else 0.0
        return day.get(metric, 0.0)

    by_date: Dict[str, float] = {d: _derive(v) for d, v in _acc.items()}

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
    metric_lbl = metric.upper() if metric in ("acos", "cvr", "cpc") else metric.capitalize()
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
    labels_v = [kw_perf[i].get("keyword_text", "")            for i in valid]

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


def _chart_placement_donut(item: Dict) -> Optional[bytes]:
    placement  = item.get("placement_performance") or {}
    configured = item.get("placement_configured_pcts") or {}
    keys = [k for k in placement if placement[k].get("acos") is not None]
    if not keys:
        return None

    label_map = {
        "PLACEMENT_TOP_OF_SEARCH":  "Top of\nSearch",
        "PLACEMENT_REST_OF_SEARCH": "Rest of\nSearch",
        "PLACEMENT_PRODUCT_PAGE":   "Product\nPage",
    }
    # Fixed colors keyed by placement name — ensures each slot is always the
    # same color regardless of how many placements are present.
    _COLOR_BY_PLACEMENT = {
        "top of search on-amazon":  "#2563EB",  # blue
        "detail page on-amazon":    "#F59E0B",  # amber
        "other on-amazon":          "#8B5CF6",  # purple
        "off amazon":               "#10B981",  # teal
        "placement_top_of_search":  "#2563EB",
        "placement_rest_of_search": "#8B5CF6",
        "placement_product_page":   "#F59E0B",
    }
    _FALLBACK_COLORS = ["#2563EB", "#F59E0B", "#8B5CF6", "#10B981", "#EF4444"]

    warn_pct = (item.get("acos_warn_threshold") or 0.30) * 100
    display  = [label_map.get(k, k)                for k in keys]
    act_acos = [placement[k]["acos"]               for k in keys]
    cfg_pct  = [configured.get(k) or 0             for k in keys]
    raw_sh   = [placement[k].get("spend_share", 0) for k in keys]
    spend_sh = [max(s, 1.0) for s in raw_sh]
    seg_colors = [
        _COLOR_BY_PLACEMENT.get(k.lower(), _FALLBACK_COLORS[i % len(_FALLBACK_COLORS)])
        for i, k in enumerate(keys)
    ]

    def _health_color(acos: float) -> str:
        if acos <= warn_pct * 0.85:
            return _C["green"]
        elif acos <= warn_pct:
            return _C["orange"]
        return _C["red"]

    def _health_label(acos: float) -> str:
        if acos <= warn_pct * 0.85:
            return "✓"
        elif acos <= warn_pct:
            return "△"
        return "✗"

    fig, (ax_ring, ax_tbl) = plt.subplots(
        1, 2, figsize=(10, 5),
        gridspec_kw={"width_ratios": [1.1, 0.9]},
        facecolor=_C["bg"],
    )

    # ── Donut (segment color = placement identity) ─────────────────────────
    ax_ring.set_facecolor(_C["bg"])
    wedge_labels = [f"{d}\n{raw:.0f}%" for d, raw in zip(display, raw_sh)]
    wedges, texts = ax_ring.pie(
        spend_sh,
        labels=wedge_labels,
        colors=seg_colors,
        startangle=90,
        wedgeprops=dict(width=0.52, edgecolor="white", linewidth=2.5),
        textprops=dict(fontsize=8.5),
    )
    # Add health indicator dot on each wedge label
    for text, acos in zip(texts, act_acos):
        text.set_color(_health_color(acos))
        text.set_fontweight("bold")
    ax_ring.text(0, 0, "Spend\nShare", ha="center", va="center",
                 fontsize=9, fontweight="bold", color="#374151")
    ax_ring.set_title(f"{item.get('asin', '?')} — Placement Performance",
                      fontsize=10, pad=8)

    # ── ACOS vs Bid-Adj table ──────────────────────────────────────────────
    ax_tbl.set_facecolor(_C["bg"])
    ax_tbl.axis("off")

    short_labels = {
        "Top of\nSearch":  "Top of Search",
        "Rest of\nSearch": "Rest of Search",
        "Product\nPage":   "Product Page",
    }
    rows = []
    for d, acos, cfg in zip(display, act_acos, cfg_pct):
        diff = acos - warn_pct
        sign = "+" if diff > 0 else ""
        rows.append([
            short_labels.get(d, d),
            f"{acos:.1f}%",
            f"+{cfg:.0f}%",
            f"{sign}{diff:.0f}%",
            _health_label(acos),
        ])

    tbl = ax_tbl.table(
        cellText=rows,
        colLabels=["Placement", "ACOS", "Bid Adj", "vs Target", ""],
        cellLoc="center",
        loc="center",
        bbox=[0.0, 0.22, 1.0, 0.60],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.5)
    for j in range(5):
        tbl[(0, j)].set_facecolor("#E5E7EB")
        tbl[(0, j)].set_text_props(fontweight="bold")
    for i, (seg_c, acos) in enumerate(zip(seg_colors, act_acos), start=1):
        # Placement name cell uses segment color as left-border tint
        tbl[(i, 0)].set_facecolor(seg_c + "22")  # 13% opacity hex
        diff_val = acos - warn_pct
        hc = _health_color(acos)
        tbl[(i, 3)].set_facecolor(
            "#FEE2E2" if diff_val > 0 else
            "#FEF3C7" if diff_val > -5 else
            "#D1FAE5"
        )
        tbl[(i, 4)].set_facecolor(hc)
        tbl[(i, 4)].set_text_props(color="white", fontweight="bold")

    from matplotlib.patches import Patch as _Patch
    ax_tbl.legend(
        handles=[
            _Patch(facecolor=_C["green"],  label=f"✓  ≤ {warn_pct * 0.85:.0f}%  healthy"),
            _Patch(facecolor=_C["orange"], label=f"△  ≤ {warn_pct:.0f}%  watch"),
            _Patch(facecolor=_C["red"],    label=f"✗  > {warn_pct:.0f}%  over target"),
        ],
        loc="lower center", fontsize=7.5, framealpha=0.4,
        title=f"ACOS health (target {warn_pct:.0f}%)", title_fontsize=7.5,
    )

    fig.tight_layout()
    return _fig_to_png(fig)


def _chart_inventory_burndown(item: Dict, store_today: Optional[str] = None) -> Optional[bytes]:
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
    stockout_date = (_date_cls.fromisoformat(store_today) if store_today else _date_cls.today()) + timedelta(days=can_sell)
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


def _chart_lead_time_dist(item: Dict) -> Optional[bytes]:
    """
    Horizontal range chart — shipment lead-time distribution by quarter.

    Left panel  : sea transit (SHIPPED → RECEIVING), shown only when n ≥ 1.
    Right panel : FBA processing time (RECEIVING → CLOSED), shown when n ≥ 1.
    Falls back to single panel when only one metric has data.

    Each quarter row shows: p25–p75 shaded bar + median tick + p90 marker.
    """
    import matplotlib.patches as mpatches

    lt = item.get("shipment_lead_time") or {}
    sea_by_q   = (lt.get("sea_transit")    or {}).get("by_quarter") or {}
    ovs_by_q   = (lt.get("overseas_to_fba") or {}).get("by_quarter") or {}
    local_by_q = (lt.get("local_to_fba")   or {}).get("by_quarter") or {}

    # Collect quarters that have at least one series with n ≥ 1
    all_quarters = sorted(
        q for q in set(list(sea_by_q) + list(ovs_by_q) + list(local_by_q))
        if (sea_by_q.get(q, {}).get("n", 0) > 0
            or ovs_by_q.get(q, {}).get("n", 0) > 0
            or local_by_q.get(q, {}).get("n", 0) > 0)
    )
    if not all_quarters:
        return None

    has_sea   = any(sea_by_q.get(q, {}).get("n", 0) > 0 for q in all_quarters)
    has_ovs   = any(ovs_by_q.get(q, {}).get("n", 0) > 0 for q in all_quarters)
    has_local = any(local_by_q.get(q, {}).get("n", 0) > 0 for q in all_quarters)
    n_panels = (1 if has_sea else 0) + (1 if has_local else 0) + (1 if has_ovs else 0)
    if n_panels == 0:
        return None

    fig, axes = plt.subplots(1, n_panels, figsize=(5 * n_panels, max(3, len(all_quarters) * 0.65 + 1.2)))
    if n_panels == 1:
        axes = [axes]
    fig.patch.set_facecolor(_C["bg"])

    panel_data = []
    if has_sea:
        panel_data.append(("Sea Transit\n(SHIPPED→RECEIVING, >12d)", sea_by_q, _C["blue"]))
    if has_local:
        panel_data.append(("Local Warehouse→FBA\n(SHIPPED→RECEIVING, ≤12d)", local_by_q, _C["green"]))
    if has_ovs:
        panel_data.append(("FBA Processing\n(RECEIVING→CLOSED)", ovs_by_q, _C["orange"]))

    y_pos = list(range(len(all_quarters)))

    for ax, (title, by_q, color) in zip(axes, panel_data):
        ax.set_facecolor(_C["bg"])
        ax.set_title(title, fontsize=9, fontweight="bold", color="#374151", pad=6)

        x_max = 0
        for yi, q in enumerate(all_quarters):
            stats = by_q.get(q) or {}
            n      = stats.get("n", 0)
            p25    = stats.get("p25")
            median = stats.get("median")
            p75    = stats.get("p75")
            p90    = stats.get("p90")

            if n == 0 or median is None:
                ax.text(0.5, yi, "—", ha="left", va="center", fontsize=8, color="#9CA3AF")
                continue

            x_max = max(x_max, p90 or p75 or median)

            # p25–p75 range bar
            ax.barh(
                yi,
                (p75 or median) - (p25 or median),
                left=(p25 or median),
                height=0.55,
                color=color,
                alpha=0.35,
                zorder=2,
            )
            # Median tick
            ax.plot([median, median], [yi - 0.32, yi + 0.32],
                    color=color, linewidth=2.5, zorder=3)
            # p90 marker
            if p90:
                ax.plot(p90, yi, marker="|", markersize=10,
                        markeredgewidth=1.8, color=color, alpha=0.6, zorder=3)
                ax.annotate(
                    f"p90={p90:.0f}d",
                    xy=(p90, yi),
                    xytext=(4, 0),
                    textcoords="offset points",
                    fontsize=7,
                    color="#6B7280",
                    va="center",
                )
            # Median label
            ax.annotate(
                f"med={median:.0f}d  n={n}",
                xy=(p25 or median, yi),
                xytext=(-4, 0),
                textcoords="offset points",
                fontsize=7,
                color=color,
                ha="right",
                va="center",
            )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(all_quarters, fontsize=8)
        ax.set_xlabel("Days", fontsize=8)
        ax.set_xlim(left=0, right=max(x_max * 1.35, 10))
        ax.invert_yaxis()
        ax.grid(axis="x", color="#E5E7EB", linewidth=0.6, zorder=1)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="both", labelsize=8)

        # Legend
        legend_handles = [
            mpatches.Patch(color=color, alpha=0.35, label="p25–p75"),
            plt.Line2D([0], [0], color=color, linewidth=2.5, label="median"),
            plt.Line2D([0], [0], color=color, alpha=0.6, marker="|",
                       markersize=8, markeredgewidth=1.8, linewidth=0, label="p90"),
        ]
        ax.legend(handles=legend_handles, fontsize=7, loc="lower right",
                  framealpha=0.7, edgecolor="#E5E7EB")

    fig.suptitle(
        f"{item.get('asin', '?')} — Shipment Lead-Time Distribution",
        fontsize=10, fontweight="bold", color="#374151", y=1.01,
    )
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

    date_str    = _dt.datetime.now(tz=_store_tz(ctx)).date().isoformat()
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
                ("daily_trend",          lambda _i=item, _p=daily_perf: _chart_daily_trend(_i, _p)),
                ("its_causal",           lambda _i=item, _p=daily_perf, _m=ctx.config.get("causal_metric", "orders"): _chart_its_causal(_i, _p, _m)),
                ("kw_quadrant",          lambda _i=item: _chart_kw_quadrant(_i)),
                ("placement_donut",       lambda _i=item: _chart_placement_donut(_i)),
                ("inventory_burndown",   lambda _i=item, _d=date_str: _chart_inventory_burndown(_i, _d)),
                ("comp_price_box",       lambda _i=item: _chart_comp_price_box(_i)),
                ("lp_waterfall",         lambda _i=item: _chart_lp_waterfall(_i)),
                ("budget_utilization",   lambda _i=item, _p=daily_perf: _chart_budget_utilization(_i, _p)),
                ("campaign_budget_cov",  lambda _i=item: _chart_campaign_budget_coverage(_i)),
                ("rank_trend",           lambda _i=item: _chart_rank_trend(_i)),
                ("lead_time_dist",       lambda _i=item: _chart_lead_time_dist(_i)),
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
        logger.info(f"[charts] {asin}: {len(urls)}/{len(generators)} charts uploaded")

    return items


# ---------------------------------------------------------------------------
# LLM pre-enrichment (summary injection only — no field stripping)
# ---------------------------------------------------------------------------

_SCALE_UP_ACTIONS   = frozenset({"increase_budget", "enable_and_increase_budget", "increase_bid"})
_SCALE_DOWN_ACTIONS = frozenset({"decrease_budget", "pause_candidate", "archive_candidate",
                                  "decrease_bid", "pause_keyword"})


def _detect_action_conflicts(item: Dict) -> List[Dict]:
    """
    Scans change_attributions for past events where Strong/Moderate causal consensus
    contradicts the direction implied by LP-derived campaign_actions or keyword_actions.
    Only call this when causal_reliability='high' (caller responsibility).
    """
    attributions: List[Dict]     = item.get("change_attributions") or []
    campaign_actions: List[Dict] = item.get("campaign_actions") or []
    keyword_actions: List[Dict]  = item.get("keyword_actions") or []

    if not attributions:
        return []

    def _strong(consensus: str) -> bool:
        return consensus.startswith("Strong evidence") or consensus.startswith("Moderate evidence")

    def _parse_float(v) -> Optional[float]:
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _scale_up(old_v, new_v) -> Optional[bool]:
        o, n = _parse_float(old_v), _parse_float(new_v)
        if o is None or n is None:
            return None
        return n > o if n != o else None

    clean_attrs = [
        a for a in attributions
        if not a.get("attribution_suspect")
        and not a.get("compound")
        and not a.get("had_promotion")
        and _strong(a.get("consensus") or "")
    ]

    conflicts: List[Dict] = []

    def _record(action_entry: Dict, scope: str, attr: Dict, summary: str) -> None:
        conflicts.append({
            "action_scope":          scope,
            "action":                action_entry.get("action"),
            "campaign_id":           str(action_entry.get("campaign_id") or ""),
            "keyword_text":          action_entry.get("keyword_text"),
            "conflict_event_date":   attr.get("changed_at"),
            "conflict_change_type":  attr.get("change_type"),
            "conflict_direction":    attr.get("direction"),
            "conflict_consensus":    (attr.get("consensus") or "")[:80],
            "conflict_delta_orders": (
                attr.get("delta_orders_normalized") or attr.get("delta_orders")
            ),
            "conflict_summary":      summary,
        })

    for ca in campaign_actions:
        action = ca.get("action") or ""
        cid    = str(ca.get("campaign_id") or "")
        attrs  = [a for a in clean_attrs if a.get("campaign_id") == cid]

        if action in _SCALE_UP_ACTIONS:
            for attr in attrs:
                if attr.get("change_type") != "BUDGET_AMOUNT":
                    continue
                if _scale_up(attr.get("old_value"), attr.get("new_value")) is True \
                        and attr.get("direction") == "worsened":
                    d = attr.get("delta_orders_normalized") or attr.get("delta_orders")
                    _record(ca, "campaign", attr,
                            f"Budget increase on {attr['changed_at']} → orders declined "
                            f"({'%.1f' % d if d is not None else 'N/A'}/day); "
                            f"{(attr.get('consensus') or '')[:60]}")
                    break

        elif action in _SCALE_DOWN_ACTIONS:
            for attr in attrs:
                ct = attr.get("change_type") or ""
                if ct not in ("BUDGET_AMOUNT", "STATUS"):
                    continue
                is_down = (ct == "STATUS") or (_scale_up(attr.get("old_value"), attr.get("new_value")) is False)
                if is_down and attr.get("direction") == "worsened":
                    d = attr.get("delta_orders_normalized") or attr.get("delta_orders")
                    _record(ca, "campaign", attr,
                            f"{ct} cut/pause on {attr['changed_at']} → orders declined "
                            f"({'%.1f' % d if d is not None else 'N/A'}/day); "
                            f"{(attr.get('consensus') or '')[:60]}")
                    break

    for ka in keyword_actions:
        action  = ka.get("action") or ""
        cid     = str(ka.get("campaign_id") or "")
        kw_id   = str(ka.get("keyword_id") or "")
        kw_text = (ka.get("keyword_text") or "").lower()

        kw_attrs = [
            a for a in clean_attrs
            if a.get("campaign_id") == cid
            and (
                (a.get("entity_type") == "KEYWORD" and str(a.get("entity_id") or "") == kw_id)
                or (a.get("keyword") or "").lower() == kw_text
            )
        ]

        if action in _SCALE_UP_ACTIONS:
            for attr in kw_attrs:
                if attr.get("change_type") != "BID_AMOUNT":
                    continue
                if _scale_up(attr.get("old_value"), attr.get("new_value")) is True \
                        and attr.get("direction") == "worsened":
                    d = attr.get("delta_orders_normalized") or attr.get("delta_orders")
                    _record(ka, "keyword", attr,
                            f"Bid increase on {attr['changed_at']} → orders declined "
                            f"({'%.1f' % d if d is not None else 'N/A'}/day); "
                            f"{(attr.get('consensus') or '')[:60]}")
                    break

        elif action in _SCALE_DOWN_ACTIONS:
            for attr in kw_attrs:
                ct = attr.get("change_type") or ""
                if ct not in ("BID_AMOUNT", "STATUS"):
                    continue
                is_down = (ct == "STATUS") or (_scale_up(attr.get("old_value"), attr.get("new_value")) is False)
                if is_down and attr.get("direction") == "worsened":
                    d = attr.get("delta_orders_normalized") or attr.get("delta_orders")
                    _record(ka, "keyword", attr,
                            f"Bid/status cut on {attr['changed_at']} → orders declined "
                            f"({'%.1f' % d if d is not None else 'N/A'}/day); "
                            f"{(attr.get('consensus') or '')[:60]}")
                    break

    return conflicts


def _summarise_lead_time(lt: Optional[Dict]) -> Dict:
    """Compact summary of compute_quarterly_lead_times output for the LLM snapshot."""
    if not lt:
        return {}
    sea   = (lt.get("sea_transit")    or {}).get("overall") or {}
    ovs   = (lt.get("overseas_to_fba") or {}).get("overall") or {}
    local = (lt.get("local_to_fba")   or {}).get("overall") or {}
    by_q: Dict = lt.get("by_quarter_summary") or {}
    compact_q = {
        q: {
            "sea_median":   v.get("sea_transit_median"),
            "sea_p75":      v.get("sea_transit_p75"),
            "fba_median":   v.get("overseas_to_fba_median"),
            "fba_p75":      v.get("overseas_to_fba_p75"),
            "local_median": v.get("local_to_fba_median"),
            "local_p75":    v.get("local_to_fba_p75"),
            "n":            (v.get("sea_shipment_count") or 0)
                            + (v.get("local_shipment_count") or 0)
                            + (1 if v.get("overseas_to_fba_median") is not None else 0),
        }
        for q, v in by_q.items()
        if any(v.get(k) is not None for k in (
            "sea_transit_median", "overseas_to_fba_median", "local_to_fba_median"))
    }
    result: Dict = {
        "sea_transit_overall": {
            "n": sea.get("n", 0), "p25": sea.get("p25"),
            "median": sea.get("median"), "p75": sea.get("p75"), "p90": sea.get("p90"),
        } if sea.get("n", 0) > 0 else None,
        "fba_processing_overall": {
            "n": ovs.get("n", 0), "p25": ovs.get("p25"),
            "median": ovs.get("median"), "p75": ovs.get("p75"), "p90": ovs.get("p90"),
        } if ovs.get("n", 0) > 0 else None,
        "by_quarter": compact_q,
        "total_shipments_analysed": lt.get("total_input", 0),
    }
    if local.get("n", 0) > 0:
        result["local_to_fba_overall"] = {
            "n": local.get("n", 0), "p25": local.get("p25"),
            "median": local.get("median"), "p75": local.get("p75"), "p90": local.get("p90"),
        }
    result["data_source"] = lt.get("data_source", "lingxing_erp")
    return result


def _causal_reliability_tier(
    backtest_hit_rate: Optional[float],
    events_significant_pct: Optional[float],
) -> str:
    """
    AND-gate: 'high' requires BOTH historical calibration (hit_rate ≥70)
    AND at least one event being statistically significant in this run.
    'low' sub-cases:
      A: hit_rate not None and < 70 (calibrated but near-random)
      B: hit_rate None and events_significant_pct == 0 (no calibration, no significance)
      C: hit_rate None and events_significant_pct > 0 (significant results, no backtest)
    'none': no backtest data and no significant events at all.
    """
    has_calibration  = (backtest_hit_rate or 0) >= 70
    has_significance = (events_significant_pct is not None) and events_significant_pct > 0

    if has_calibration and has_significance:
        return "high"
    if (backtest_hit_rate or 0) > 0 or has_significance:
        return "low"
    return "none"


# ---------------------------------------------------------------------------
# Quick Metrics Snapshot — pre-rendered table with fixed Source column
# ---------------------------------------------------------------------------

# Maps summary field name → the fixed "Source / How derived" string shown in the
# report table.  Mirrors the legend in ad_diagnosis_report.yaml.  Fields absent
# from this dict are not included in the pre-rendered table.
_SNAPSHOT_FIELD_SOURCE: Dict[str, str] = {
    "title":                    "SP-API Catalog",
    "brand":                    "SP-API Catalog",
    "lookback_days":            "days config (default 30); period: data_start_date → data_end_date",
    "data_start_date":          "reporting window start (today − lookback_days)",
    "data_end_date":            "reporting window end (yesterday)",
    "total_available":          "FBA Inventory API — fulfillable units (point-in-time)",
    "inbound_receiving":        "FBA inbound — at FC being checked in (1-2d, certain)",
    "inbound_shipped":          "FBA inbound — in transit from seller (10-30d ETA)",
    "inbound_working":          "FBA inbound — shipment plan not yet shipped (uncertain ETA)",
    "total_inbound":            "inbound_receiving + inbound_shipped (confirmed in-transit only)",
    "can_sell_days":            "total_available ÷ daily_sales (null if daily_sales unavailable)",
    "inventory_risk":           "can_sell_days < inventory_risk_days (default 30d)",
    "campaign_count":           "count of campaigns matched to this ASIN",
    "campaign_match_strategy":  "matching method: explicit_config / spAdvertisedProduct / name_substring / none",
    "active_campaign_count":    "campaigns with state=ENABLED",
    "paused_campaign_count":    "campaigns with state=PAUSED",
    "total_daily_budget":       "sum of ENABLED campaign daily budgets (Ads API, current config)",
    "bidding_strategies":       "Ads API campaigns — {strategy: campaign_count}; e.g. AUTO_FOR_SALES ×2, LEGACY_FOR_SALES ×1",
    "total_spend":              "Ads API spCampaigns — ad-attributed spend, data_start_date → data_end_date",
    "total_sales":              "Ads API spCampaigns — ad-attributed sales, data_start_date → data_end_date",
    "total_orders":             "Ads API spCampaigns — AD-ATTRIBUTED orders only (organic excluded)",
    "total_clicks":             "Ads API spCampaigns — ad-attributed clicks, data_start_date → data_end_date",
    "account_acos":             "total_spend ÷ total_sales × 100",
    "budget_active_days":       "days with non-zero spend in lookback window",
    "budget_exhausted_days":    "active days where daily spend ≥ 85% of per-day effective cap (proxy — Amazon does not expose intraday OUT_OF_BUDGET events via the Ads History API); denominator uses cap reconstructed from BUDGET_AMOUNT change history, not current snapshot",
    "budget_exhausted_days_pct":"budget_exhausted_days ÷ budget_active_days × 100",
    "avg_daily_utilization_pct":"mean(daily_spend ÷ per-day effective cap) × 100 across active days",
    "p90_daily_utilization_pct":"90th-percentile daily utilization across active days; with per-day caps, >100% reflects genuine Amazon pacing (≤25%) only — mid-period budget changes no longer inflate this figure",
    "max_daily_utilization_pct":"peak single-day utilization",
    "overdelivery_days":        "days where spend exceeded the per-day effective cap; caused solely by Amazon pacing (≤25% allowance) — mid-period budget-change artifact eliminated by per-day cap reconstruction",
    "budget_pressure":          "chronic ≥75% / moderate 30-74% / light 10-29% / none <10% of active days at cap",
    "budget_starved_campaigns": "campaigns with exhausted_pct ≥ 30% (daily spend ≥ 85% of cap on ≥ 30% of active days — budget likely runs out mid-day); see campaign_budget_coverage in _summary_json for per-campaign breakdown",
    "keyword_count":            "Ads API keyword list — matched campaigns (current config)",
    "avg_bid":                  "Ads API keyword list — matched campaigns (current config)",
    "min_bid":                  "Ads API keyword list — matched campaigns (current config)",
    "max_bid":                  "Ads API keyword list — matched campaigns (current config)",
    "match_type_dist":          "Ads API keyword list — matched campaigns (current config)",
    "kw_performance_count":     "spSearchTerm report rows with ≥ min_clicks_for_cvr clicks",
    "lp_zero_keywords_count":   "count of keywords LP assigned 0 clicks (detail list in LP Budget Redistribution section)",
    "lp_maxed_keywords_count":  "count of keywords LP hit click ceiling (detail list in LP Budget Redistribution section)",
    "ad_traffic_ratio":         "Xiyouzhaoci traffic score API (latest snapshot)",
    "organic_traffic_ratio":    "Xiyouzhaoci traffic score API (latest snapshot)",
    "traffic_growth_7d":        "Xiyouzhaoci traffic score API (latest snapshot)",
    "rank_tracked_keywords":    "Xiyouzhaoci daily organic rank — tracked keyword list",
    "rank_series_days":         "Xiyouzhaoci rank data days within reporting window",
    "rank_series_history_days": "Xiyouzhaoci rank data days in full historical baseline (rank_lookback_months)",
    "market_trends_keywords":   "Xiyouzhaoci SFR weekly trend data",
    "change_attributions_count":"filtered attributable change events (noise filter); top 20 used for analysis",
    "attribution_suspect_count":"attribution entries where |Δorders| > 1.5× pre-window mean (ASIN fallback)",
    "causal_consensus_sample":  "ITS + CausalImpact + DML model agreement for highest-priority event",
    "orders_reliability":       "high ≥100 orders / medium 30-99 / low <30 in lookback window",
    "acos_ci_lo":               "95% ACOS CI lower bound (Wilson score on CVR)",
    "acos_ci_hi":               "95% ACOS CI upper bound (Wilson score on CVR)",
    "backtest_hit_rate":        "historical model accuracy — fraction of past events where direction matched",
    "backtest_strong_hit_rate": "backtest hit rate restricted to 'Strong evidence' events only",
    "backtest_total":           "total change events evaluated in backtest history",
    "events_significant_count": "change events with p<0.05 AND CI not crossing zero in this run",
    "events_significant_pct":   "events_significant_count ÷ runnable events × 100",
    "causal_reliability":       "AND-gate: backtest_hit_rate ≥70% AND ≥1 significant event → 'high'",
    "shipment_lead_time":       "Lingxing ERP shipment records — sea transit (SHIPPED→RECEIVING) + FBA processing (RECEIVING→CLOSED); full quarterly breakdown in _summary_json",
}

# Fields that are dicts/lists with dedicated report sections — omit from the table.
_SNAPSHOT_SKIP_FIELDS: frozenset = frozenset({
    "asin",
    "lp_summary", "lp_top_allocations", "lp_reallocation_table", "lp_reallocation_net",
    "lp_zero_keywords", "lp_maxed_keywords",  # lists — rendered in LP Budget Redistribution section
    "campaign_actions", "keyword_actions",
    "auto_mining_summary", "auto_mining_beta_prior",
    "auto_mining_negatives", "auto_mining_harvest",
    "action_conflicts", "competitor_price_meta",
    "campaign_budget_coverage",   # rendered as derived scalar below; full list in _summary_json
    "shipment_lead_time",         # nested dict — full data in _summary_json; snapshot uses scalar below
})


def _render_snapshot_table(summary: Dict) -> str:
    """
    Pre-render the Quick Metrics Snapshot as a fixed three-column markdown table.

    Source / How derived values are taken from _SNAPSHOT_FIELD_SOURCE — they
    are identical across every run, eliminating LLM-driven variation.
    Fields absent from the source dict, or in _SNAPSHOT_SKIP_FIELDS, are omitted.
    """
    rows = ["| Field | Value | Source / How derived |", "|---|---|---|"]
    for field, value in summary.items():
        if field in _SNAPSHOT_SKIP_FIELDS:
            continue
        source = _SNAPSHOT_FIELD_SOURCE.get(field)
        if source is None:
            continue
        if isinstance(value, dict):
            if field == "bidding_strategies" and value:
                val_str = ", ".join(f"{s} ×{n}" for s, n in sorted(value.items()))
            else:
                continue  # safety net for other unexpected nested dicts
        elif value is None:
            val_str = "—"
        elif isinstance(value, list):
            val_str = str(value)[:300]
        elif isinstance(value, bool):
            val_str = str(value)
        elif isinstance(value, float):
            val_str = f"{value:.2f}" if abs(value) < 10_000 else f"{value:.0f}"
        else:
            val_str = str(value)
        rows.append(f"| {field} | {val_str} | {source} |")
    return "\n".join(rows)


def _build_item_summary(item: Dict, ctx: WorkflowContext) -> Dict:
    """
    Pre-compute a flat highlights dict from a fully enriched item.
    Python-side extraction: 100% accurate, zero LLM token cost.
    Mirrors the highlights dict in the live test's _print_result.
    """
    rank_series: Dict = item.get("natural_rank_series") or {}
    market_trends: Dict = item.get("market_trends") or {}
    attributions: List = item.get("change_attributions") or []
    campaigns: List = item.get("campaigns") or []
    days = ctx.config.get("days", 30)
    today = datetime.now(tz=_store_tz(ctx)).date()
    data_end_date   = (today - timedelta(days=1)).isoformat()
    data_start_date = (today - timedelta(days=days)).isoformat()
    active_campaign_count = sum(1 for c in campaigns if c.get("state") == "ENABLED")
    paused_campaign_count = sum(1 for c in campaigns if c.get("state") == "PAUSED")
    summary = {
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
        "budget_active_days":          item.get("budget_active_days"),
        "budget_exhausted_days":       item.get("budget_exhausted_days"),
        "budget_exhausted_days_pct":   item.get("budget_exhausted_days_pct"),
        "avg_daily_utilization_pct":   item.get("avg_daily_utilization_pct"),
        "p90_daily_utilization_pct":   item.get("p90_daily_utilization_pct"),
        "max_daily_utilization_pct":   item.get("max_daily_utilization_pct"),
        "overdelivery_days":           item.get("overdelivery_days"),
        "budget_pressure":             item.get("budget_pressure"),
        "keyword_count":             item.get("keyword_count"),
        "avg_bid":                   item.get("avg_bid"),
        "match_type_dist":           item.get("match_type_dist"),
        "kw_performance_count":      len(item.get("keyword_performance", [])),
        "lp_summary":                item.get("lp_summary"),
        "lp_top_allocations":        (item.get("lp_top_allocations") or [])[:3],
        "lp_zero_keywords":          (item.get("lp_zero_keywords") or [])[:10],
        "lp_maxed_keywords":         (item.get("lp_maxed_keywords") or [])[:10],
        "lp_zero_keywords_count":    len(item.get("lp_zero_keywords") or []),
        "lp_maxed_keywords_count":   len(item.get("lp_maxed_keywords") or []),
        "lp_reallocation_table":     item.get("lp_reallocation_table") or [],
        "lp_reallocation_net":       item.get("lp_reallocation_net"),
        "campaign_actions":          (item.get("campaign_actions") or [])[:5],
        "keyword_actions":           (item.get("keyword_actions") or [])[:10],
        "competitor_price_meta":      item.get("competitor_price_meta"),
        "ad_traffic_ratio":          item.get("ad_traffic_ratio"),
        "organic_traffic_ratio":     item.get("organic_traffic_ratio"),
        "rank_tracked_keywords":     item.get("rank_tracked_keywords"),
        "rank_series_days":          sum(
            1 for d in next(iter(rank_series.values()), {})
            if data_start_date <= d <= data_end_date
        ),
        "rank_series_history_days":  len(next(iter(rank_series.values()), {})),
        "market_trends_keywords":    list(market_trends.keys()),
        "change_attributions_count":  item.get("change_attributions_total_count", len(attributions)),
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
        #   "high"   backtest_hit_rate ≥70 AND ≥1 event statistically significant
        #   "low"    any calibration data OR some significant events, but not both conditions
        #   "none"   no backtest data and no significant events
        "causal_reliability": _causal_reliability_tier(
            backtest_hit_rate      = item.get("backtest_hit_rate"),
            events_significant_pct = item.get("events_significant_pct"),
        ),
        # LP-vs-causal conflicts: non-empty only when causal_reliability='high'.
        # Each entry describes one LP action contradicted by Strong/Moderate causal evidence.
        "action_conflicts": (
            _detect_action_conflicts(item)
            if _causal_reliability_tier(
                backtest_hit_rate      = item.get("backtest_hit_rate"),
                events_significant_pct = item.get("events_significant_pct"),
            ) == "high"
            else []
        ),
        # Auto/PT campaign search-term mining results
        "auto_mining_summary":  (item.get("auto_mining") or {}).get("summary"),
        "auto_mining_beta_prior": (item.get("auto_mining") or {}).get("beta_prior"),
        "auto_mining_negatives": (item.get("auto_mining") or {}).get("negatives", [])[:30],
        "auto_mining_harvest":   (item.get("auto_mining") or {}).get("harvest", [])[:20],
        # Per-campaign intraday budget coverage (full list in _summary_json)
        "campaign_budget_coverage": item.get("campaign_budget_coverage") or [],
        "budget_starved_campaigns": sum(
            1 for c in (item.get("campaign_budget_coverage") or [])
            if c.get("exhausted_pct", 0) >= 30
        ),
        # Shipment lead-time distribution (store-wide, from Lingxing ERP)
        "shipment_lead_time": _summarise_lead_time(item.get("shipment_lead_time")),
    }
    # Suppress model-infrastructure metrics from the snapshot when every event is
    # Conflicting — high backtest numbers alongside a Conflicting verdict imply a
    # confidence that does not exist and confuse readers.
    if (summary.get("causal_consensus_sample") or "").startswith("Conflicting"):
        for _f in ("causal_reliability", "backtest_hit_rate",
                   "backtest_strong_hit_rate", "backtest_total"):
            summary.pop(_f, None)
    return summary


def _trim_keyword_performance(item: Dict) -> None:
    """
    Trim keyword_performance in-place before sending to LLM.

    N is determined dynamically by three rules applied in order:

    1. Floor  = max(keywords_in_lp, len(keyword_actions), _KW_PERF_FLOOR)
       LP-analysed keywords and action-listed keywords must always be present.

    2. Pareto = keep top-spend keywords until cumulative spend >= _KW_SPEND_COVERAGE
       of total spend.  Accounts with concentrated spend truncate much shorter
       than accounts with spread spend.

    3. Ceiling = _KW_PERF_CEIL  — hard cap regardless of spread.

    Final N = min(max(floor, pareto_n), ceiling).
    The original count is preserved in keyword_performance_original_count so
    the LLM knows the list may be truncated.
    """
    _KW_PERF_FLOOR    = 20    # always keep at least this many
    _KW_SPEND_COVERAGE = 0.95  # Pareto threshold: cover 95% of spend
    _KW_PERF_CEIL     = 300   # hard ceiling

    kw_perf: List[Dict] = item.get("keyword_performance") or []
    if not kw_perf:
        return

    original_count = len(kw_perf)

    # Must-keep: keywords referenced in change_attributions (may have low spend
    # if the bid/state change happened late in the window, but the LLM needs their
    # historical CVR/ACOS to evaluate causal impact quality).
    must_keep: set = {
        a["keyword"].lower()
        for a in (item.get("change_attributions") or [])
        if a.get("keyword")
    }

    # Floor: LP-analysed + action keywords must be represented
    lp_n   = (item.get("lp_summary") or {}).get("keywords_in_lp", 0) or 0
    act_n  = len(item.get("keyword_actions") or [])
    floor  = max(_KW_PERF_FLOOR, lp_n, act_n)

    # Sort by spend descending (stable — preserves relative order on ties)
    sorted_kw   = sorted(kw_perf, key=lambda x: x.get("total_spend", 0) or 0, reverse=True)
    total_spend = sum(k.get("total_spend", 0) or 0 for k in sorted_kw)

    # Pareto: accumulate until coverage threshold
    if total_spend > 0:
        cumulative = 0.0
        pareto_n   = 0
        for kw in sorted_kw:
            cumulative += kw.get("total_spend", 0) or 0
            pareto_n   += 1
            if cumulative / total_spend >= _KW_SPEND_COVERAGE:
                break
    else:
        pareto_n = floor

    n = min(max(floor, pareto_n), _KW_PERF_CEIL)

    # Partition: top-N by spend + any must-keep stragglers outside that window
    top_n     = sorted_kw[:n]
    top_texts = {kw.get("keyword_text", "").lower() for kw in top_n}
    stragglers = [
        kw for kw in sorted_kw[n:]
        if kw.get("keyword_text", "").lower() in must_keep
           and kw.get("keyword_text", "").lower() not in top_texts
    ]

    item["keyword_performance"] = top_n + stragglers
    if len(item["keyword_performance"]) < original_count:
        item["keyword_performance_original_count"] = original_count


def _prepare_for_llm(items: List[Dict], ctx: WorkflowContext) -> List[Dict]:
    """
    PURE_PYTHON step immediately before ad_diagnosis_llm.

    Injects _summary_json (Python-exact highlights) as a scalar field so
    ProcessStep auto-substitutes it into {_summary_json} in the prompt.

    Fields stripped / trimmed from items_json to avoid C=A∪B redundancy:
      - performance_records      : raw per-campaign rows; all useful aggregates
                                   already stored as scalar fields.
      - auto_mining              : moved into _summary_json at full depth;
                                   raw dict here would be a duplicate subset.
      - keyword_performance      : trimmed to dynamic N via _trim_keyword_performance
                                   (Pareto 95%-spend coverage, floored by LP keyword
                                   count, hard-capped at 300).
    """
    import json as _json
    _STRIP_FIELDS = ("performance_records", "auto_mining")
    for item in items:
        summary = _build_item_summary(item, ctx)
        item["_summary_json"]    = _json.dumps(summary, ensure_ascii=False, default=str)
        item["_snapshot_table"]  = _render_snapshot_table(summary)
        for f in _STRIP_FIELDS:
            item.pop(f, None)
        _trim_keyword_performance(item)
    return items


_CHART_META: Dict[str, str] = {
    "daily_trend":          "Daily Performance Trend",
    "its_causal":           "ITS Causal Analysis",
    "kw_quadrant":          "Keyword ACOS × Orders",
    "placement_donut":      "Placement Performance",
    "inventory_burndown":   "Inventory Burn-down",
    "comp_price_box":       "Competitor Price Distribution",
    "lp_waterfall":         "LP Budget Allocation",
    "budget_utilization":   "Daily Budget Utilization",
    "campaign_budget_cov":  "Campaign Budget Coverage",
    "rank_trend":           "Organic Rank Trend",
    "lead_time_dist":       "Shipment Lead-Time Distribution",
}

# Matches [CHART:chart_name] anywhere in the text (LLM-inserted placeholder)
_CHART_PLACEHOLDER_RE = re.compile(r'\[CHART:(\w+)\]')


def _chart_interpretation(item: Dict, name: str) -> str:
    """One-sentence business interpretation for each chart type."""
    acos_warn    = (item.get("acos_warn_threshold") or 0.30) * 100
    account_acos = item.get("account_acos")

    if name == "daily_trend":
        pressure = item.get("budget_pressure")
        exh_days = item.get("budget_exhausted_days")
        act_days = item.get("budget_active_days")
        avg_util = item.get("avg_daily_utilization_pct")
        if pressure and pressure != "none" and exh_days is not None and act_days:
            exh_s = f"Budget {pressure} pressure ({exh_days}/{act_days}d hit cap)"
        elif avg_util is not None:
            exh_s = f"Budget utilisation avg {avg_util:.1f}% of daily cap"
        else:
            exh_s = f"Budget utilisation {(item.get('budget_exhaustion_pct') or 0):.1f}%"
        acos_s = f"ACOS {account_acos:.0f}%" if account_acos else "ACOS N/A"
        above  = account_acos and account_acos > acos_warn
        return (f"{exh_s}; {acos_s} — "
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

    if name == "placement_donut":
        tos      = (item.get("placement_performance") or {}).get("PLACEMENT_TOP_OF_SEARCH") or {}
        tos_acos = tos.get("acos")
        if tos_acos:
            rec = "reduce TOS bid adjustment" if tos_acos > acos_warn else "TOS within target"
            return (f"Donut slice size = spend share. TOS ACOS {tos_acos:.0f}% vs target {acos_warn:.0f}% → {rec}. "
                    f"Green = healthy, orange = watch, red = over target.")
        return "Donut slice size = spend share per placement. Color = ACOS health vs target. Table shows ACOS vs configured bid adjustment."

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
        if rec and gap > 0:
            rec_note = (
                f" LP plan requires {rec} units ({lp.get('stock_gate_days', 21)}d buffer); "
                f"shortfall {gap} units — procure before activating gated actions."
            )
        elif rec and risk:
            # Pipeline meets LP target but near-term on-hand is below the gate threshold.
            # Do NOT say "sufficient" here — it would contradict the risk warning above.
            rec_note = (
                f" LP plan requires {rec} units — pipeline meets target "
                f"but near-term on-hand stock is short (see can_sell_days / effective_stock_days)."
            )
        elif rec:
            rec_note = f" LP plan requires {rec} units — stock sufficient."
        else:
            rec_note = ""
        if risk:
            return (
                f"⚠ Current stock ~{can_sell:.0f} days — budget/bid increases are gated until "
                f"effective stock ≥ 21 days.{inb_note}{rec_note}"
            )
        return f"Inventory covers ~{can_sell:.0f} days — sufficient runway for current scaling plans.{inb_note}{rec_note}"

    if name == "comp_price_box":
        own_price  = item.get("price") or item.get("sale_price")
        meta       = item.get("competitor_price_meta") or {}
        n_comp     = meta.get("n_competitors", 0)
        date_from  = meta.get("date_from", "")
        date_to    = meta.get("date_to", "")
        sample_note = (
            f" (sample: {n_comp} competitor ASINs, {date_from} – {date_to})"
            if n_comp and date_from and date_to else ""
        )
        comp_flat  = [v for prices in (item.get("competitor_price_by_asin") or {}).values()
                      for v in prices.values() if v is not None]
        if own_price and comp_flat:
            pct = sum(1 for p in comp_flat if p < float(own_price)) / len(comp_flat) * 100
            pos = "above" if float(own_price) > float(np.median(comp_flat)) else "below"
            return (f"Own price ${float(own_price):.2f} is {pos} competitor median; "
                    f"higher than {pct:.0f}% of sampled prices.{sample_note}")
        return f"Competitor price distribution vs own price (red dashed line).{sample_note}"

    if name == "lp_waterfall":
        lp      = item.get("lp_summary") or {}
        gap     = lp.get("order_gap") or 0
        ceiling = lp.get("spend_ceiling_bound", False)
        if ceiling:
            return (f"LP is ceiling-bound (spend ${lp.get('lp_optimal_spend',0):.0f} "
                    f"vs budget ${lp.get('lp_scope_campaign_daily_budget',0):.0f}) — "
                    f"expand keyword coverage to unlock remaining budget.")
        if gap >= 0:
            return (f"Ad order gap +{gap:.1f}/day — rebalancing spend could gain {gap:.1f} ad orders/day. "
                    f"Blue bar = LP target; grey = budget cap. Organic orders not included.")
        binding = lp.get("budget_binding", False)
        lp_raw  = lp.get("lp_orders_cvr_matched", 0)
        actual  = lp.get("actual_daily_ad_orders", 0)
        if binding:
            return (f"Ad order gap {gap:+.1f}/day (LP-projected {lp_raw:.1f} vs actual {actual:.1f} orders/day). "
                    f"Negative gap expected under budget constraint — LP click ceiling limits optimised clicks "
                    f"below actual (Amazon 125% pacing + seasonal CVR uplift not captured). "
                    f"Blue bar = LP target; grey = budget cap.")
        return (f"Ad order gap {gap:+.1f}/day (LP-projected {lp_raw:.1f} vs actual {actual:.1f} orders/day) — "
                f"LP order estimate below actual; review CVR data quality or keyword mix. "
                f"Blue bar = LP target; grey = budget cap. Organic orders not included.")

    if name == "budget_utilization":
        pressure       = item.get("budget_pressure") or "unknown"
        exh_days       = item.get("budget_exhausted_days")
        act_days       = item.get("budget_active_days")
        avg_util       = item.get("avg_daily_utilization_pct")
        p90_util       = item.get("p90_daily_utilization_pct")
        max_util       = item.get("max_daily_utilization_pct")
        overdeliv_days = item.get("overdelivery_days") or 0
        cap            = item.get("total_daily_budget") or 0
        exh_s = (f"{exh_days}/{act_days}d hit the 85% cap"
                 if exh_days is not None and act_days else "")
        util_s = (f"avg {avg_util:.0f}%, p90 {p90_util:.0f}%, max {max_util:.0f}%"
                  if avg_util is not None and p90_util is not None and max_util is not None else "")
        overdeliv_s = ""
        if overdeliv_days > 0:
            # With per-day cap reconstruction, overdelivery_days reflects genuine Amazon
            # pacing (≤25%) only — mid-period budget changes no longer inflate this count.
            overdeliv_s = f"{overdeliv_days}d spend exceeded per-day cap — Amazon ±25% pacing allowance."
        parts = [p for p in [exh_s, util_s, overdeliv_s] if p]
        return (f"Budget pressure = {pressure} (cap ${cap:.0f}/day). "
                + ("; ".join(parts) + ". " if parts else "")
                + "Red bars = exhausted days (≥ 85 % of cap); "
                "orange = high utilization (60–84 %); blue = healthy. "
                "Orange dashed lines = change events.")

    if name == "campaign_budget_cov":
        starved = item.get("budget_starved_campaigns") or 0
        cov     = item.get("campaign_budget_coverage") or []
        if not starved or not cov:
            return ""
        worst = cov[0]
        others = [c for c in cov[1:4] if c.get("exhausted_pct", 0) >= 30]
        other_str = (
            "; ".join(f"'{c['campaign_name']}' {c['exhausted_pct']:.0f}%" for c in others)
        )
        return (
            f"{starved} campaign(s) ran dry before midnight. "
            f"Worst: '{worst['campaign_name']}' — {worst['exhausted_days']}/{worst['active_days']} days "
            f"({worst['exhausted_pct']:.0f}% exhausted)."
            + (f" Also starved: {other_str}." if other_str else "")
        )

    if name == "rank_trend":
        n = len(item.get("natural_rank_series") or {})
        return (f"Organic rank for {n} keyword(s). Downward slope = improving position. "
                f"Orange lines = ad change events. Correlate rank drops with bid/budget cuts.")

    if name == "lead_time_dist":
        lt = item.get("shipment_lead_time") or {}
        sea   = (lt.get("sea_transit")    or {}).get("overall") or {}
        ovs   = (lt.get("overseas_to_fba") or {}).get("overall") or {}
        local = (lt.get("local_to_fba")   or {}).get("overall") or {}
        parts = []
        if sea.get("n", 0):
            parts.append(
                f"sea freight (SHIPPED→RECEIVING >12d) median {sea.get('median', 0):.0f}d "
                f"(p25={sea.get('p25', 0):.0f}d, p75={sea.get('p75', 0):.0f}d, n={sea['n']})"
            )
        if local.get("n", 0):
            parts.append(
                f"local warehouse→FBA (SHIPPED→RECEIVING ≤12d) median {local.get('median', 0):.0f}d "
                f"(p25={local.get('p25', 0):.0f}d, p75={local.get('p75', 0):.0f}d, n={local['n']})"
            )
        if ovs.get("n", 0):
            parts.append(
                f"FBA processing (RECEIVING→CLOSED) median {ovs.get('median', 0):.0f}d "
                f"(p25={ovs.get('p25', 0):.0f}d, p75={ovs.get('p75', 0):.0f}d, n={ovs['n']})"
            )
        if parts:
            return (
                "Historical lead-time by quarter: "
                + "; ".join(parts)
                + ". Use local warehouse→FBA p75 as the replenishment dispatch buffer."
            )
        return "Shipment lead-time distribution chart. No data available."

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
    import json as _json

    from src.core.errors.exceptions import FatalError

    report_dir = os.path.abspath("data/reports")
    os.makedirs(report_dir, exist_ok=True)
    date_str = _dt.datetime.now(tz=_store_tz(ctx)).date().isoformat()
    missing_report_asins: List[str] = []

    for item in items:
        report_data = item.get("ad_diagnosis_llm")
        if hasattr(report_data, "text"):
            text = report_data.text or ""
        elif isinstance(report_data, dict):
            text = report_data.get("text") or report_data.get("response") or ""
            if not text and report_data:
                text = _json.dumps(report_data, ensure_ascii=False, default=str)
        elif report_data:
            text = str(report_data)
        else:
            text = ""

        if not text:
            asin = (item.get("asin") or "unknown").upper()
            missing_report_asins.append(asin)
            item["response"] = (
                f"广告诊断未生成报告正文，无法导出附件。ASIN: {asin}。"
                "请检查 ad_diagnosis_llm Batch 结果是否成功回填。"
            )
            logger.error(
                "[export_report] %s: missing ad_diagnosis_llm; available keys=%s",
                asin,
                sorted(item.keys()),
            )
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

    if missing_report_asins:
        raise FatalError(
            "ad_diagnosis export_report missing LLM report text for ASIN(s): "
            + ", ".join(missing_report_asins)
        )
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
    from src.intelligence.prompts.manager import prompt_manager

    ad_spec = prompt_manager.get_spec("ad_diagnosis_report")
    ctx_vars = {
        name: f"{{{name}}}"
        for name in (ad_spec.required_vars if ad_spec else [])
    }

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
        # Shipment lead-time: store-wide FBA transit + FBA processing distributions.
        # Cached after first ASIN; subsequent ASINs read from ctx.cache[_KEY_LEAD_TIME].
        # Disabled when enable_lingxing=False or LINGXING_ACCOUNT env var is absent.
        EnrichStep(
            name="fetch_shipment_lead_time",
            extractor_fn=_enrich_shipment_lead_time,
            parallel=True,
            concurrency=1,
            enabled=config.get("enable_lingxing", bool(os.getenv("LINGXING_ACCOUNT"))),
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

        # ── Stage 5b2: auto campaign search-term mining ───────────────────────
        # Depends on optimize_budget (needs lp_scoped_cids from lp_summary).
        # Identifies negative keyword candidates and harvest-to-manual candidates
        # from auto/PT campaign search terms using Empirical Bayes thresholds.
        ProcessStep(
            name="mine_auto_campaigns",
            fn=_mine_auto_campaigns,
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
            prompt_template=prompt_manager.render_spec("ad_diagnosis_report", ctx_vars),
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
