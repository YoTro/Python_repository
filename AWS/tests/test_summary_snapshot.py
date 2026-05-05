"""
Build Quick Metrics Snapshot (_build_item_summary) from real Redis data.

Replicates the enrichment chain (campaigns → performance → kw_perf → lp)
without calling any external API. Inventory is injected synthetically.

Usage:
    venv311/bin/python3 tests/test_summary_snapshot.py [ASIN]
"""
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

ASIN = sys.argv[1].upper() if len(sys.argv) > 1 else "B0FXFGMD7Z"
DAYS = 30

# ── Redis ────────────────────────────────────────────────────────────────────

import redis as _redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
r = _redis.from_url(REDIS_URL, decode_responses=True)

PREFIX = "aws:cache:ad_diag:default:US"

def rget(*parts):
    raw = r.get(f"{PREFIX}:{':'.join(str(p) for p in parts)}")
    if not raw:
        return None
    env = json.loads(raw)
    return env.get("data", env)

# ── Load raw Redis data ───────────────────────────────────────────────────────

daily_perf_raw = rget("daily_perf", ASIN, DAYS) or []
campaigns_all  = rget("campaigns") or []
kw_perf_raw    = rget("kw_performance", DAYS) or []
placement_raw  = rget("placement", DAYS) or []
change_hist    = rget("change_history", DAYS) or []
sp_perf_raw    = rget("sp_performance", DAYS) or []

# ── Campaign matching (mirror _enrich_campaigns) ──────────────────────────────

campaign_ids = set(str(r["campaign_id"]) for r in daily_perf_raw if r.get("campaign_id"))
campaigns = [c for c in campaigns_all if str(c.get("campaign_id")) in campaign_ids]
total_daily_budget = sum(
    float(c.get("daily_budget") or 0) for c in campaigns if c.get("state") == "ENABLED"
)
bidding_strategies = list({c.get("bidding_strategy") for c in campaigns if c.get("bidding_strategy")})

# ── Performance enrichment (mirror _enrich_performance) ──────────────────────

perf_matched = [r for r in daily_perf_raw if str(r.get("campaign_id")) in campaign_ids]

total_spend  = sum(float(r.get("spend",  0) or 0) for r in perf_matched)
total_sales  = sum(float(r.get("sales",  0) or 0) for r in perf_matched)
total_orders = sum(float(r.get("orders", 0) or 0) for r in perf_matched)
total_clicks = sum(float(r.get("clicks", 0) or 0) for r in perf_matched)
account_acos = round(total_spend / total_sales * 100, 2) if total_sales > 0 else None

orders_reliability = (
    "high"   if total_orders >= 100 else
    "medium" if total_orders >= 30  else "low"
)

def _wilson_ci(k, n, z=1.96):
    if n <= 0 or k < 0:
        return None, None
    p = k / n
    denom  = 1.0 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, center - margin), min(1.0, center + margin)

cvr_point = round(total_orders / total_clicks, 6) if total_clicks > 0 else None
_cvr_lo, _cvr_hi = _wilson_ci(int(total_orders), int(total_clicks))
acos_ci_lo = acos_ci_hi = None
if account_acos and cvr_point and _cvr_hi and _cvr_lo:
    frac = account_acos / 100
    if _cvr_hi > 0: acos_ci_lo = round(frac * cvr_point / _cvr_hi * 100, 2)
    if _cvr_lo > 0: acos_ci_hi = round(frac * cvr_point / _cvr_lo * 100, 2)

total_budget_capacity = total_daily_budget * DAYS
budget_exhaustion_pct = (
    round(total_spend / total_budget_capacity, 4) if total_budget_capacity > 0 else None
)

# ── Keyword performance (mirror _enrich_kw_performance) ──────────────────────

MIN_CLICKS = 5
kw_agg = {}
for rec in kw_perf_raw:
    if str(rec.get("campaign_id", "")) not in campaign_ids:
        continue
    key = (rec.get("keyword_text", ""), rec.get("match_type", ""))
    if key not in kw_agg:
        kw_agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0, "sales": 0}
    for f in ("spend", "clicks", "orders", "impressions", "sales"):
        kw_agg[key][f] += rec.get(f, 0) or 0

kw_performance = []
for (kw_text, match_type), v in kw_agg.items():
    clicks = v["clicks"]
    if clicks < MIN_CLICKS:
        continue
    kw_performance.append({
        "keyword_text":  kw_text,
        "match_type":    match_type,
        "total_spend":   round(v["spend"], 2),
        "total_sales":   round(v["sales"], 2),
        "total_clicks":  clicks,
        "total_orders":  v["orders"],
        "avg_cpc":       round(v["spend"] / clicks, 4),
        "cvr":           round(v["orders"] / clicks, 4),
        "daily_clicks":  round(clicks / DAYS, 2),
        "acos":          round(v["spend"] / v["sales"] * 100, 2) if v["sales"] > 0 else None,
    })
kw_performance.sort(key=lambda x: x["total_spend"], reverse=True)

# ── Keywords metadata (mirror _enrich_keywords — bids from Ads API, not spSearchTerm) ──

# Ads API keyword data is cached under "keywords:{sorted_campaign_ids}" keys.
# Scan all matching keys and merge (campaign set may span multiple cache entries).
kw_ads_all = []
for key in r.scan_iter(f"{PREFIX}:keywords:*", count=100):
    raw = r.get(key)
    if not raw:
        continue
    env = json.loads(raw)
    data = env.get("data", env)
    if isinstance(data, list):
        kw_ads_all.extend(data)

# Filter to this ASIN's campaigns
kw_ads = [k for k in kw_ads_all if str(k.get("campaign_id", "")) in campaign_ids]
bids = [float(k["bid"]) for k in kw_ads if k.get("bid") is not None]
match_types = {}
for k in kw_ads:
    mt = k.get("match_type", "UNKNOWN")
    match_types[mt] = match_types.get(mt, 0) + 1

# ── LP optimization (mirror _optimize_budget) ─────────────────────────────────

from src.workflows.definitions.ad_diagnosis import (
    _build_kw_to_campaign_map,
    _build_lp_input,
    _classify_lp_keywords,
    _build_campaign_actions,
    _build_keyword_actions,
)
from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer

HEADROOM    = 1.3
BRAND_KWS   = set()
TARGET_ACOS = 0.35
daily_budget = total_daily_budget

camp_meta = {str(c["campaign_id"]): c for c in campaigns if c.get("campaign_id")}
campaign_budgets = {cid: float(c.get("daily_budget") or 0) for cid, c in camp_meta.items() if c.get("daily_budget")}
avg_price = round(total_sales / total_orders, 2) if total_orders > 0 else None

kw_to_campaign  = _build_kw_to_campaign_map(kw_perf_raw, campaign_ids)
lp_input        = _build_lp_input(kw_performance, kw_to_campaign, camp_meta, BRAND_KWS, HEADROOM, 1.0)

# NOTE: _enrich_inventory calls SP-API directly — inventory is NOT in Redis.
# We inject synthetic inventory here to demonstrate the gate logic.
# Pass --real-inv to skip gate test and show can_sell_days from ad orders only.
# Real FBA Available (from SP-API) must be fetched separately.
daily_sales_val     = round(total_orders / DAYS, 2)
# Synthetic: 12-day stock scenario (override with real FBA available to see true runway)
total_available_syn = round(daily_sales_val * 12)   # simulate low stock
inbound_shipped_syn = round(daily_sales_val * 15)   # simulate sea-freight inbound
can_sell_days_syn   = 12
daily_sales_source  = "ad_orders_only"  # organic orders excluded → real stockout is sooner

lp_result = AdBudgetOptimizer().optimize(
    keywords         = lp_input,
    total_budget     = daily_budget or 50.0,
    campaign_budgets = campaign_budgets or None,
    target_acos      = TARGET_ACOS,
    avg_price        = avg_price,
)

lp_summary = None
lp_top_allocations = []
lp_zero_keywords   = []
lp_maxed_keywords  = []
campaign_actions   = []
keyword_actions    = []

if lp_result.get("status") == "OPTIMAL":
    summary    = lp_result["summary"]
    alloc      = lp_result["allocation"]
    camp_spend = lp_result.get("camp_spend", {})
    kw_map     = {kw["name"]: kw for kw in lp_input}

    raw_cvr_map   = {kw["name"]: kw["estimated_cvr"] for kw in lp_input}
    lp_raw_orders = round(
        sum(a["optimized_clicks"] * raw_cvr_map.get(a["keyword"], 0) for a in alloc), 2
    )
    actual_daily_ad_orders = round(total_orders / DAYS, 2)
    lp_spend_total         = summary["actual_spend"]
    spend_ceiling_bound    = lp_spend_total < daily_budget * 0.6

    zero_kws, maxed_kws = _classify_lp_keywords(kw_performance, alloc, kw_map)

    # Inventory gate (sea freight scenario)
    stock_gate_days   = 21
    inbound_lead_days = 30
    catchable = inbound_shipped_syn if inbound_lead_days < can_sell_days_syn else 0
    eff_units = total_available_syn + 0 + catchable  # inbound_receiving=0
    effective_stock_days = round(eff_units / daily_sales_val) if daily_sales_val > 0 else None
    inv_gate = {
        "stock_gate_days":    stock_gate_days,
        "effective_stock_days": effective_stock_days,
        "can_sell_days":      can_sell_days_syn,
        "inbound_receiving":  0,
        "inbound_shipped":    inbound_shipped_syn,
        "inbound_lead_days":  inbound_lead_days,
    } if effective_stock_days is not None else None

    campaign_actions = _build_campaign_actions(
        camp_meta, camp_spend, perf_matched, DAYS, TARGET_ACOS, inv_gate=inv_gate
    )
    keyword_actions = _build_keyword_actions(
        lp_input, alloc, {}, BRAND_KWS, HEADROOM, avg_price, inv_gate=inv_gate
    )

    # Stock recommendation (inventory is NOT a LP constraint)
    daily_consumption_src = daily_sales_source
    organic_daily         = 0.0  # unknown without order_metrics; ad_orders_only fallback
    lp_total_daily        = organic_daily + lp_raw_orders
    recommended_stock     = round(lp_total_daily * stock_gate_days)
    inbound_recv_syn      = 0    # synthetic: no inbound_receiving
    inbound_ship_syn      = inbound_shipped_syn
    inbound_work_syn      = 0
    confirmed_inbound_syn = inbound_recv_syn + inbound_ship_syn
    stock_shortfall       = max(0, recommended_stock - total_available_syn - confirmed_inbound_syn)

    lp_summary = {
        "daily_budget":                  daily_budget,
        "lp_optimal_spend":              lp_spend_total,
        "lp_optimal_orders_pessimistic": summary["total_expected_orders"],
        "lp_optimal_orders_raw":         lp_raw_orders,
        "actual_daily_ad_orders":        actual_daily_ad_orders,
        "order_gap":                     round(lp_raw_orders - actual_daily_ad_orders, 2),
        "spend_ceiling_bound":           spend_ceiling_bound,
        "avg_effective_cpc":             summary["avg_effective_cpc"],
        "placement_data_unknown":        True,
        "target_acos_applied":           TARGET_ACOS,
        "recommended_stock_units":       recommended_stock,
        "stock_shortfall":               stock_shortfall,
        "stock_gate_days":               stock_gate_days,
        "daily_consumption":             round(lp_total_daily, 2),
        "daily_consumption_source":      daily_consumption_src,
        "keywords_in_lp":               len(lp_input),
        "keywords_allocated":           len(alloc),
        "keywords_zeroed":              len(zero_kws),
        "keywords_maxed":               len(maxed_kws),
    }
    lp_top_allocations = [
        {**a, "keyword": a["keyword"].split("|")[0],
         "match_type": a["keyword"].split("|")[1] if "|" in a["keyword"] else ""}
        for a in alloc[:3]
    ]
    lp_zero_keywords  = zero_kws[:5]
    lp_maxed_keywords = maxed_kws[:5]

# ── Build item dict ───────────────────────────────────────────────────────────

item = {
    "asin":                   ASIN,
    "title":                  None,
    "brand":                  None,
    "campaigns":              campaigns,
    "campaign_ids":           list(campaign_ids),
    "total_daily_budget":     total_daily_budget,
    "bidding_strategies":     bidding_strategies,
    "campaign_match_strategy": "spAdvertisedProduct",
    "performance_records":    perf_matched,
    "total_spend":            total_spend,
    "total_sales":            total_sales,
    "total_orders":           total_orders,
    "total_clicks":           total_clicks,
    "account_acos":           account_acos,
    "orders_reliability":     orders_reliability,
    "acos_ci_lo":             acos_ci_lo,
    "acos_ci_hi":             acos_ci_hi,
    "budget_exhaustion_pct":  budget_exhaustion_pct,
    "budget_likely_exhausted": (
        budget_exhaustion_pct is not None and budget_exhaustion_pct > 0.90
    ),
    "keyword_count":          len(kw_ads),
    "avg_bid":                round(sum(bids) / len(bids), 2) if bids else None,
    "min_bid":                round(min(bids), 2) if bids else None,
    "max_bid":                round(max(bids), 2) if bids else None,
    "match_type_dist":        match_types,
    "keyword_performance":    kw_performance,
    "kw_performance_count":   len(kw_performance),
    # Synthetic inventory (sea freight, 12d stock)
    "total_available":        total_available_syn,
    "inbound_receiving":      0,
    "inbound_shipped":        inbound_shipped_syn,
    "inbound_working":        0,
    "total_inbound":          inbound_shipped_syn,
    "daily_sales":            daily_sales_val,
    "daily_sales_source":     daily_sales_source,
    "can_sell_days":          can_sell_days_syn,
    "can_sell_days_note":     (
        "upper_bound — derived from ad-attributed orders only (organic excluded); "
        "true daily unit consumption (ad + organic) is higher, "
        "so actual stockout will occur SOONER than can_sell_days suggests"
    ),
    "inventory_risk":         can_sell_days_syn < 30,
    # LP outputs
    "lp_summary":             lp_summary,
    "lp_top_allocations":     lp_top_allocations,
    "lp_zero_keywords":       lp_zero_keywords,
    "lp_maxed_keywords":      lp_maxed_keywords,
    "campaign_actions":       campaign_actions,
    "keyword_actions":        keyword_actions[:30],
    # Causal (not run here)
    "natural_rank_series":    {},
    "market_trends":          {},
    "change_attributions":    [],
    "backtest_hit_rate":      None,
    "backtest_strong_hit_rate": None,
    "backtest_total":         None,
    "ad_traffic_ratio":       None,
    "organic_traffic_ratio":  None,
    "rank_tracked_keywords":  None,
}

# ── Build summary and print ───────────────────────────────────────────────────

from src.workflows.definitions.ad_diagnosis import _build_item_summary

class _MockConfig:
    def __init__(self, d): self._d = d
    def get(self, k, default=None): return self._d.get(k, default)

class _MockCtx:
    def __init__(self): self.config = _MockConfig({"days": DAYS})

snapshot = _build_item_summary(item, _MockCtx())

print("\n" + "=" * 70)
print(f"Quick Metrics Snapshot — {ASIN}")
print("=" * 70)
print(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str))
