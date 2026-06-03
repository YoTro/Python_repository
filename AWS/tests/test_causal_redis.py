"""
Test run_causal_analysis against real Redis data for B0FXFGMD7Z.

Bypasses the full workflow: reads L2 cache directly from Redis,
reconstructs the item fields that causal analysis needs,
then calls run_causal_analysis and reports results.

Usage:
    venv311/bin/python3 tests/test_causal_redis.py [ASIN]
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

ASIN = sys.argv[1].upper() if len(sys.argv) > 1 else "B0FXFGMD7Z"
DAYS = 30

# ── 1. Connect to Redis ──────────────────────────────────────────────────────

import redis as _redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
r = _redis.from_url(REDIS_URL, decode_responses=True)


def redis_get(key: str):
    raw = r.get(key)
    if not raw:
        return None
    env = json.loads(raw)
    return env.get("data", env)


PREFIX = "aws:cache:ad_diag:default:US"


def rget(*parts):
    return redis_get(f"{PREFIX}:{':'.join(str(p) for p in parts)}")


# ── 2. Load raw data from Redis ──────────────────────────────────────────────

daily_perf_raw: list[dict] = rget("daily_perf", ASIN, DAYS) or []
campaigns: list[dict] = rget("campaigns") or []
change_hist: list[dict] = rget("change_history", DAYS) or []
sp_perf: list[dict] = rget("sp_performance", DAYS) or []
placement_raw: list[dict] = rget("placement", DAYS) or []
kw_perf_raw: list[dict] = rget("kw_performance", DAYS) or []

logger.info(
    f"Redis data: daily_perf={len(daily_perf_raw)}, campaigns={len(campaigns)}, "
    f"change_history={len(change_hist)}, sp_perf={len(sp_perf)}, "
    f"kw_perf={len(kw_perf_raw)}, placement={len(placement_raw)}"
)

# ── 3. Find campaign_ids for this ASIN (via daily_perf) ─────────────────────

campaign_ids = {str(r["campaign_id"]) for r in daily_perf_raw if r.get("campaign_id")}
logger.info(f"campaign_ids for {ASIN}: {len(campaign_ids)}")

# ── 4. Build change_events from change_history ───────────────────────────────
# Mirror the logic in _enrich_change_history (timestamp ms → ISO date)

_CHANGE_PRIORITY = {
    "BID_AMOUNT": 3,
    "STATUS": 3,
    "BUDGET_AMOUNT": 2,
    "TARGETING": 2,
    "PLACEMENT": 1,
}
_NOISE_BID_PCT = 0.03
_NOISE_BUDGET_PCT = 0.05

change_events: list[dict] = []
for ev in change_hist:
    meta = ev.get("metadata") or {}
    cid = str(meta.get("campaignId") or ev.get("entityId") or "")
    if campaign_ids and cid not in campaign_ids:
        continue
    change_type = ev.get("changeType", "")
    if change_type == "CREATED":
        continue
    old_val = ev.get("previousValue")
    new_val = ev.get("newValue")
    is_low = change_type == "IN_BUDGET"
    if not is_low and change_type in ("BID_AMOUNT", "BUDGET_AMOUNT") and old_val and new_val:
        try:
            old_f, new_f = float(old_val), float(new_val)
            thr = _NOISE_BID_PCT if change_type == "BID_AMOUNT" else _NOISE_BUDGET_PCT
            if old_f > 0 and abs(new_f - old_f) / old_f < thr:
                is_low = True
        except (TypeError, ValueError):
            pass
    # Keep raw ms timestamp — _build_attributions parses it as int(ts)/1000
    ts_raw = ev.get("changedAt") or ev.get("timestamp")

    change_events.append(
        {
            "campaign_id": cid,
            "entity_type": ev.get("entityType"),
            "change_type": change_type,
            "old_value": old_val,
            "new_value": new_val,
            "changed_at": ts_raw,
            "priority": _CHANGE_PRIORITY.get(change_type, 0),
            "low_weight": is_low,
            "keyword": meta.get("keyword"),
            "compound_change": False,
        }
    )

logger.info(
    f"change_events for {ASIN}: {len(change_events)} "
    f"({sum(1 for e in change_events if not e['low_weight'])} notable)"
)

# ── 5. Build minimal item ────────────────────────────────────────────────────

# Build a minimal covariate_series covering the lookback window.
# All values are None/missing — this triggers the zero-variance column drop
# fix in _causal_impact_analyze (the exact scenario that caused "exog inf/nan").
today = datetime.utcnow().date()
cov_dates = [(today - timedelta(days=i)).isoformat() for i in range(DAYS + 15, -1, -1)]
covariate_series = {d: {} for d in cov_dates}  # empty dicts → all columns will be 0 after fill

item = {
    "asin": ASIN,
    "campaign_ids": list(campaign_ids),
    "change_events": change_events,
    "covariate_series": covariate_series,
    "competitor_price_summary": {},
    "natural_rank_series": {},
    "market_trends": {},
}

config = {
    "days": DAYS,
    "store_id": "default",
    "region": "US",
    "timezone": "America/Los_Angeles",
}

# ── 6. Run causal analysis ───────────────────────────────────────────────────

import warnings

from src.intelligence.processors.causal_analysis import run_causal_analysis

warnings.filterwarnings("ignore")  # suppress statsmodels convergence noise

logger.info("Running run_causal_analysis ...")
result = run_causal_analysis(item, config, daily_perf=daily_perf_raw)

attrs = result.get("change_attributions", [])
logger.info(f"change_attributions: {len(attrs)}")

# ── 7. Report results ────────────────────────────────────────────────────────

ci_fails = 0
conflicts = 0
strong = 0

for a in attrs:
    ci = a.get("causal_impact") or {}
    con = a.get("consensus", "")
    if ci.get("skipped") and "inf or nan" in (ci.get("reason") or ""):
        ci_fails += 1
    if "Conflicting" in con:
        conflicts += 1
    if "Strong evidence" in con:
        strong += 1

print("\n" + "=" * 60)
print(f"ASIN: {ASIN}   change_attributions: {len(attrs)}")
print(
    f"  CausalImpact inf/nan failures : {ci_fails}  {'✅ fixed' if ci_fails == 0 else '❌ still failing'}"
)
print(f"  Conflicting model consensus   : {conflicts}")
print(f"  Strong evidence               : {strong}")
print(
    f"  Backtest: {result.get('backtest_hit_rate', 'N/A')}  (n={result.get('backtest_total', '?')})"
)
print()

for a in attrs[:10]:
    ci = a.get("causal_impact") or {}
    its = a.get("its") or {}
    print(
        f"  [{a.get('changed_at', '?')}] {a.get('change_type', '?'):20s} "
        f"delta_orders={a.get('delta_orders', 0):+.2f}  "
        f"dir={a.get('direction', '?'):10s}  "
        f"CI={'skip' if ci.get('skipped') else str(round(ci.get('point_effect') or 0, 2))}  "
        f"| {a.get('consensus', '')[:70]}"
    )
