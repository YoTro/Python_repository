"""
Test inventory gate logic (effective_stock_days < stock_gate_days → downgrade to P2)
using real Redis data for keyword performance + campaigns.

Inventory data is injected synthetically (not in Redis) — two scenarios:
  A) Sea freight  : inbound_lead_days=30, can_sell_days=12 → shipped inbound does NOT arrive in time → gate triggers
  B) Domestic US  : inbound_lead_days=10, can_sell_days=12 → shipped inbound arrives in time → effective days may clear gate

Usage:
    venv311/bin/python3 tests/test_inventory_gate.py [ASIN]
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

ASIN = sys.argv[1].upper() if len(sys.argv) > 1 else "B0FXFGMD7Z"
DAYS = 30

# ── 1. Redis ─────────────────────────────────────────────────────────────────

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

# ── 2. Load raw data ──────────────────────────────────────────────────────────

daily_perf_raw = rget("daily_perf", ASIN, DAYS) or []
campaigns      = rget("campaigns") or []
kw_perf_raw    = rget("kw_performance", DAYS) or []
placement_raw  = rget("placement", DAYS) or []

logger.info(f"Redis: daily_perf={len(daily_perf_raw)}, campaigns={len(campaigns)}, "
            f"kw_perf={len(kw_perf_raw)}, placement={len(placement_raw)}")

# ── 3. campaign_ids for this ASIN ─────────────────────────────────────────────

campaign_ids = set(str(r["campaign_id"]) for r in daily_perf_raw if r.get("campaign_id"))
logger.info(f"campaign_ids: {len(campaign_ids)}")

# ── 4. Build kw_perf (mirror _enrich_kw_performance aggregation) ─────────────

MIN_CLICKS = 5
agg = {}
for rec in kw_perf_raw:
    if str(rec.get("campaign_id", "")) not in campaign_ids:
        continue
    key = (rec.get("keyword_text", ""), rec.get("match_type", ""))
    if key not in agg:
        agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0, "sales": 0}
    agg[key]["spend"]       += rec.get("spend", 0) or 0
    agg[key]["clicks"]      += rec.get("clicks", 0) or 0
    agg[key]["orders"]      += rec.get("orders", 0) or 0
    agg[key]["impressions"] += rec.get("impressions", 0) or 0
    agg[key]["sales"]       += rec.get("sales", 0) or 0

kw_perf = []
for (kw_text, match_type), v in agg.items():
    clicks = v["clicks"]
    if clicks < MIN_CLICKS:
        continue
    kw_perf.append({
        "keyword_text":  kw_text,
        "match_type":    match_type,
        "total_spend":   round(v["spend"], 2),
        "total_sales":   round(v["sales"], 2),
        "total_clicks":  clicks,
        "total_orders":  v["orders"],
        "impressions":   v["impressions"],
        "avg_cpc":       round(v["spend"] / clicks, 4),
        "cvr":           round(v["orders"] / clicks, 4),
        "daily_clicks":  round(clicks / DAYS, 2),
        "acos":          round(v["spend"] / v["sales"] * 100, 2) if v["sales"] > 0 else None,
    })
kw_perf.sort(key=lambda x: x["total_spend"], reverse=True)
logger.info(f"kw_perf (≥{MIN_CLICKS} clicks): {len(kw_perf)}")

# ── 5. camp_meta + budgets ────────────────────────────────────────────────────

camp_meta = {str(c["campaign_id"]): c for c in campaigns if c.get("campaign_id")}

# ── 6. Totals from daily_perf ─────────────────────────────────────────────────

total_orders = sum(float(r.get("orders", 0) or 0) for r in daily_perf_raw)
total_sales  = sum(float(r.get("sales",  0) or 0) for r in daily_perf_raw)
daily_sales  = round(total_orders / DAYS, 2) if DAYS > 0 else 0
avg_price    = round(total_sales / total_orders, 2) if total_orders > 0 else None
logger.info(f"total_orders={total_orders:.1f}, daily_sales={daily_sales:.2f}/day, avg_price={avg_price}")

# ── 7. Build LP inputs ────────────────────────────────────────────────────────

from src.workflows.definitions.ad_diagnosis import (
    _build_kw_to_campaign_map,
    _build_lp_input,
    _classify_lp_keywords,
    _build_campaign_actions,
    _build_keyword_actions,
)
from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer

HEADROOM      = 1.3
BRAND_KWS     = set()
TARGET_ACOS   = 0.35
DAILY_BUDGET  = sum(float(c.get("daily_budget", 0) or 0) for c in campaigns
                    if str(c.get("campaign_id")) in campaign_ids)

kw_to_campaign = _build_kw_to_campaign_map(kw_perf_raw, campaign_ids)
lp_input       = _build_lp_input(kw_perf, kw_to_campaign, camp_meta, BRAND_KWS, HEADROOM, 1.0)
logger.info(f"LP keywords: {len(lp_input)}, daily_budget: ${DAILY_BUDGET:.2f}")

campaign_budgets = {
    cid: float(c.get("daily_budget") or 0)
    for cid, c in camp_meta.items() if c.get("daily_budget")
}

optimizer = AdBudgetOptimizer()
result    = optimizer.optimize(
    keywords         = lp_input,
    total_budget     = DAILY_BUDGET or 50.0,
    campaign_budgets = campaign_budgets or None,
    target_acos      = TARGET_ACOS,
    avg_price        = avg_price,
)

if result.get("status") != "OPTIMAL":
    print(f"\nLP failed: {result.get('message')}")
    sys.exit(1)

alloc      = result["allocation"]
camp_spend = result.get("camp_spend", {})
kw_map     = {kw["name"]: kw for kw in lp_input}
logger.info(f"LP OPTIMAL: {len(alloc)} keywords allocated, spend=${result['summary']['actual_spend']:.2f}")

# ── 8. Two inventory scenarios ────────────────────────────────────────────────
# Both: can_sell_days=12, available=200 units, inbound_shipped=500 units
# Scenario A: sea freight (lead=30d) → 500 units arrive AFTER stock runs out → NOT catchable
# Scenario B: domestic US (lead=10d) → 500 units arrive BEFORE stock runs out → catchable

STOCK_GATE_DAYS  = 21
CAN_SELL_DAYS    = 12
TOTAL_AVAILABLE  = round(daily_sales * CAN_SELL_DAYS) if daily_sales > 0 else 200
INBOUND_RECEIVING = 0     # nothing at FC yet
INBOUND_SHIPPED   = round(daily_sales * 15) if daily_sales > 0 else 500  # 15 days worth in transit

scenarios = [
    ("Sea freight (lead=30d)",  30),
    ("Domestic US (lead=10d)",  10),
]

print("\n" + "=" * 70)
print(f"ASIN: {ASIN}   can_sell_days={CAN_SELL_DAYS}   stock_gate={STOCK_GATE_DAYS}d")
print(f"  total_available={TOTAL_AVAILABLE}u  inbound_receiving={INBOUND_RECEIVING}u  "
      f"inbound_shipped={INBOUND_SHIPPED}u")
print("=" * 70)

for label, lead_days in scenarios:
    catchable = INBOUND_SHIPPED if lead_days < CAN_SELL_DAYS else 0
    eff_units = TOTAL_AVAILABLE + INBOUND_RECEIVING + catchable
    eff_days  = round(eff_units / daily_sales) if daily_sales > 0 else None

    inv_gate = {
        "stock_gate_days":    STOCK_GATE_DAYS,
        "effective_stock_days": eff_days,
        "can_sell_days":      CAN_SELL_DAYS,
        "inbound_receiving":  INBOUND_RECEIVING,
        "inbound_shipped":    INBOUND_SHIPPED,
        "inbound_lead_days":  lead_days,
    } if eff_days is not None else None

    camp_actions = _build_campaign_actions(
        camp_meta, camp_spend,
        daily_perf_raw, DAYS, TARGET_ACOS,
        inv_gate=inv_gate,
    )
    kw_actions = _build_keyword_actions(
        lp_input, alloc, {}, BRAND_KWS, HEADROOM, avg_price,
        inv_gate=inv_gate,
    )

    gated_camp = [a for a in camp_actions if a.get("prerequisite")]
    gated_kw   = [a for a in kw_actions   if a.get("prerequisite")]
    spend_up_camp = [a for a in camp_actions
                     if a["action"] in ("increase_budget", "enable_and_increase_budget", "enable_and_review_bids")]
    spend_up_kw   = [a for a in kw_actions if a["action"] == "increase_bid"]

    gate_ok = eff_days is not None and eff_days < STOCK_GATE_DAYS

    print(f"\n── {label} ──")
    print(f"   effective_stock_days = {eff_days}  "
          f"({'GATE TRIGGERED ✅' if gate_ok else 'gate clear'})")
    print(f"   spend-up campaign actions : {len(spend_up_camp)} total, "
          f"{len(gated_camp)} gated (P2)")
    print(f"   spend-up keyword actions  : {len(spend_up_kw)} total, "
          f"{len(gated_kw)} gated (P2)")

    for a in gated_camp[:3]:
        p = a.get("prerequisite", {})
        print(f"     [CAMP {a['campaign_id'][:12]}..] action={a['action']} priority={a['priority']} "
              f"| {p.get('note', '')[:80]}")
    for a in gated_kw[:3]:
        p = a.get("prerequisite", {})
        print(f"     [KW {a['keyword_text'][:20]}] action={a['action']} priority={a['priority']} "
              f"| {p.get('note', '')[:80]}")

print()
