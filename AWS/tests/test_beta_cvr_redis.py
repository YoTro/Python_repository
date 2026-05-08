"""
Test Beta-Binomial CVR shrinkage against real data.

Data source priority:
  1. Redis (aws:cache:ad_diag:default:US:*)
  2. Checkpoint JSON snapshot (ad-diag-<ASIN>-dev.json next to the repo root)

Validates _compute_cvr_prior (match-type-stratified μ) and _beta_cvr by comparing
old Wilson pess_cvr vs new Beta-Binomial pess_cvr on live keyword data, then runs
the LP and reports allocation statistics.

Usage:
    venv311/bin/python3 tests/test_beta_cvr_redis.py [ASIN]
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

# ── 1. Data source: Redis → snapshot fallback ─────────────────────────────────

import redis as _redis
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
_r = _redis.from_url(REDIS_URL, decode_responses=True)
PREFIX = "aws:cache:ad_diag:default:US"

def _rget(*parts):
    raw = _r.get(f"{PREFIX}:{':'.join(str(p) for p in parts)}")
    if not raw:
        return None
    env = json.loads(raw)
    return env.get("data", env)

daily_perf_raw = _rget("daily_perf", ASIN, DAYS) or []
campaigns      = _rget("campaigns")               or []
kw_perf_raw    = _rget("kw_performance", DAYS)    or []

_snap_item = {}
if not campaigns:
    # Fall back to checkpoint snapshot
    _repo_root = os.path.join(os.path.dirname(__file__), "..", "..")
    _snap_path = os.path.join(_repo_root, f"ad-diag-{ASIN}-dev.json")
    if not os.path.exists(_snap_path):
        print(f"Redis empty and no snapshot found at {_snap_path}")
        sys.exit(1)
    logger.info(f"Redis empty — loading from snapshot: {_snap_path}")
    with open(_snap_path) as _f:
        _snap = json.load(_f)
    _cache     = _snap.get("ctx_cache", {})
    _snap_item = (_snap.get("items") or [{}])[0]
    campaigns      = _cache.get("ad_diag:campaigns",          [])
    kw_perf_raw    = _cache.get("ad_diag:kw_performance",     [])
    daily_perf_raw = _cache.get("ad_diag:daily_performance",  [])
    _source = "snapshot"
else:
    _source = "Redis"

logger.info(f"[{_source}] daily_perf={len(daily_perf_raw)}, "
            f"campaigns={len(campaigns)}, kw_perf={len(kw_perf_raw)}")

# ── 3. campaign_ids for this ASIN ─────────────────────────────────────────────

if _snap_item.get("campaign_ids"):
    # Snapshot: campaign_ids already resolved per-ASIN in the item
    campaign_ids = set(str(c) for c in _snap_item["campaign_ids"])
else:
    # Redis: derive from daily_perf (all records belong to this ASIN's campaigns)
    campaign_ids = set(str(rec["campaign_id"]) for rec in daily_perf_raw if rec.get("campaign_id"))

logger.info(f"campaign_ids: {len(campaign_ids)}")

if not campaign_ids:
    print(f"No campaign_ids found for {ASIN} — check data source.")
    sys.exit(1)

# ── 4. Build kw_perf (mirror _enrich_kw_performance) ─────────────────────────

MIN_CLICKS = 5
agg = {}
for rec in kw_perf_raw:
    if str(rec.get("campaign_id", "")) not in campaign_ids:
        continue
    key = (rec.get("keyword_text", ""), rec.get("match_type", ""))
    if key not in agg:
        agg[key] = {"spend": 0, "clicks": 0, "orders": 0, "impressions": 0, "sales": 0}
    agg[key]["spend"]       += rec.get("spend", 0)       or 0
    agg[key]["clicks"]      += rec.get("clicks", 0)      or 0
    agg[key]["orders"]      += rec.get("orders", 0)      or 0
    agg[key]["impressions"] += rec.get("impressions", 0) or 0
    agg[key]["sales"]       += rec.get("sales", 0)       or 0

kw_perf = []
for (kw_text, match_type), v in agg.items():
    clicks = v["clicks"]
    if clicks < MIN_CLICKS:
        continue
    cvr         = round(v["orders"] / clicks, 6) if clicks > 0 else 0.0
    daily_clicks = round(clicks / DAYS, 2)
    acos        = round(v["spend"] / v["sales"] * 100, 2) if v["sales"] > 0 else None
    kw_perf.append({
        "keyword_text":  kw_text,
        "match_type":    match_type,
        "total_spend":   round(v["spend"], 2),
        "total_sales":   round(v["sales"], 2),
        "total_clicks":  clicks,
        "total_orders":  v["orders"],
        "impressions":   v["impressions"],
        "avg_cpc":       round(v["spend"] / clicks, 4),
        "cvr":           cvr,
        "daily_clicks":  daily_clicks,
        "acos":          acos,
    })

kw_perf.sort(key=lambda x: x["total_spend"], reverse=True)
logger.info(f"kw_perf built: {len(kw_perf)} keywords (>= {MIN_CLICKS} clicks)")

if not kw_perf:
    print("No keyword data — exiting.")
    sys.exit(1)

# ── 5. Compute μ by match type ────────────────────────────────────────────────

from src.workflows.definitions.ad_diagnosis import (
    _compute_cvr_prior,
    _build_kw_to_campaign_map,
    _build_lp_input,
)

mu_by_mt, global_mu = _compute_cvr_prior(kw_perf)

print("\n" + "=" * 65)
print(f"ASIN: {ASIN}   keywords: {len(kw_perf)}   DAYS: {DAYS}")
print("=" * 65)

print(f"\n── Prior mean CVR (μ) by match type ──")
print(f"  Global μ = {global_mu:.4f}  (s = {1.0/global_mu:.0f} if k=1)")
for mt in sorted(mu_by_mt):
    mu  = mu_by_mt[mt]
    s   = min(max(1.0 / mu, 5), 500) if mu > 0 else 500
    cnt = sum(1 for k in kw_perf if k["match_type"].upper() == mt)
    print(f"  {mt:10s}  μ={mu:.4f}  s={s:6.0f}  ({cnt} keywords)")

# ── 6. Compare old Wilson vs new Beta-Binomial per keyword ────────────────────

import math

def _wilson_pess(raw_cvr, clicks, prior=30):
    if clicks <= 0:
        return raw_cvr * 0.5
    return raw_cvr * math.sqrt(clicks / (clicks + prior))

_K = 1.0
_S_MIN, _S_MAX = 5.0, 500.0

def _beta_pess(raw_cvr, clicks, orders, prior_mu):
    mu = prior_mu if prior_mu > 0 else (raw_cvr or 0.02)
    s  = max(_S_MIN, min(_K / mu, _S_MAX))
    return (mu * s + orders) / (s + clicks)

print(f"\n── Per-keyword pess_cvr comparison (top 20 by spend) ──")
print(f"  {'Keyword':<28} {'MT':7} {'Clicks':>6} {'Orders':>6} {'raw_cvr':>8} "
      f"{'Wilson':>8} {'Beta':>8} {'Δ%':>7} {'μ':>7}")
print(f"  {'-'*28} {'-'*7} {'-'*6} {'-'*6} {'-'*8} {'-'*8} {'-'*8} {'-'*7} {'-'*7}")

zero_cvr_old = 0
zero_cvr_new = 0

for kw in kw_perf[:20]:
    mt       = kw["match_type"].upper()
    mu       = mu_by_mt.get(mt, global_mu)
    raw      = kw["cvr"]
    clicks   = kw["total_clicks"]
    orders   = kw["total_orders"]
    old_p    = _wilson_pess(raw, clicks)
    new_p    = _beta_pess(raw, clicks, orders, mu)
    delta_pct = (new_p - old_p) / old_p * 100 if old_p > 0 else float("inf")
    if old_p == 0:
        zero_cvr_old += 1
    if new_p == 0:
        zero_cvr_new += 1
    flag = " ⬆" if new_p > old_p * 1.05 else (" ⬇" if new_p < old_p * 0.95 else "")
    print(f"  {kw['keyword_text'][:28]:<28} {mt:7} {clicks:>6} {orders:>6} {raw:>8.4f} "
          f"{old_p:>8.4f} {new_p:>8.4f} {delta_pct:>+7.1f}%{flag}  μ={mu:.4f}")

# All keywords: zero-order count
all_zero_order_kws = [k for k in kw_perf if k["total_orders"] == 0]
print(f"\n── Zero-order keywords: {len(all_zero_order_kws)}/{len(kw_perf)} ──")
for kw in all_zero_order_kws[:8]:
    mt     = kw["match_type"].upper()
    mu     = mu_by_mt.get(mt, global_mu)
    old_p  = _wilson_pess(kw["cvr"], kw["total_clicks"])  # = 0 (raw_cvr=0)
    new_p  = _beta_pess(kw["cvr"], kw["total_clicks"], 0, mu)
    print(f"  {kw['keyword_text'][:28]:<28} {mt:7} clicks={kw['total_clicks']:>4}  "
          f"Wilson={old_p:.4f}  Beta={new_p:.4f}  (μ={mu:.4f})")

# ── 7. Run LP with new shrinkage ──────────────────────────────────────────────

camp_meta = {str(c["campaign_id"]): c for c in campaigns if c.get("campaign_id")}

total_orders = sum(k["total_orders"] for k in kw_perf)
total_sales  = sum(k["total_sales"]  for k in kw_perf)
avg_price    = round(total_sales / total_orders, 2) if total_orders > 0 else None
DAILY_BUDGET = sum(
    float(c.get("daily_budget", 0) or 0) for c in campaigns
    if str(c.get("campaign_id")) in campaign_ids
      and (c.get("state") or "").upper() == "ENABLED"
)

kw_to_campaign = _build_kw_to_campaign_map(kw_perf_raw, campaign_ids)
lp_input = _build_lp_input(
    kw_perf, kw_to_campaign, camp_meta,
    brand_kws=set(), headroom=3.0, placement_multiplier=1.0,
    mu_by_match_type=mu_by_mt, global_mu=global_mu,
)
campaign_budgets = {
    cid: float(c.get("daily_budget") or 0)
    for cid, c in camp_meta.items() if c.get("daily_budget")
}
TARGET_ACOS = 0.35

logger.info(f"LP input: {len(lp_input)} keywords, budget=${DAILY_BUDGET:.2f}, avg_price=${avg_price}")

from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer
optimizer = AdBudgetOptimizer()
result = optimizer.optimize(
    keywords         = lp_input,
    total_budget     = DAILY_BUDGET or 50.0,
    campaign_budgets = campaign_budgets or None,
    target_acos      = TARGET_ACOS,
    avg_price        = avg_price,
)

print(f"\n── LP result ──")
if result.get("status") != "OPTIMAL":
    print(f"  FAILED: {result.get('message')}")
    sys.exit(1)

summary = result["summary"]
alloc   = result["allocation"]
print(f"  Status        : OPTIMAL")
print(f"  Budget        : ${summary['total_budget']:.2f}")
print(f"  Actual spend  : ${summary['actual_spend']:.2f}")
print(f"  Expected orders: {summary['total_expected_orders']:.2f}/day")
print(f"  Avg eff CPC   : ${summary['avg_effective_cpc']:.3f}")
print(f"  Keywords allocated: {len(alloc)}/{len(lp_input)}")

print(f"\n── Top 15 allocated keywords ──")
print(f"  {'Keyword':<30} {'MT':7} {'Clicks':>7} {'Spend':>8} {'Orders':>7} {'pess_cvr':>9} {'prior_mu':>9}")
print(f"  {'-'*30} {'-'*7} {'-'*7} {'-'*8} {'-'*7} {'-'*9} {'-'*9}")
for a in alloc[:15]:
    name_parts = a["keyword"].split("|")
    kw_text = "|".join(name_parts[:-1]) if len(name_parts) > 1 else a["keyword"]
    mt      = name_parts[-1] if len(name_parts) > 1 else ""
    # find prior_mu from lp_input
    lp_kw   = next((k for k in lp_input if k["name"] == a["keyword"]), {})
    prior   = lp_kw.get("prior_mu", 0)
    s_val   = min(max(_K / prior, _S_MIN), _S_MAX) if prior > 0 else 0
    print(f"  {kw_text[:30]:<30} {mt:7} {a['optimized_clicks']:>7.1f} "
          f"${a['estimated_spend']:>7.2f} {a['contribution_to_orders']:>7.3f} "
          f"{a['pessimistic_cvr']:>9.4f}  μ={prior:.4f} s={s_val:.0f}")

# ── 8. Verify prior_mu / sample_orders fields are in lp_input ────────────────

missing_orders   = sum(1 for k in lp_input if "sample_orders" not in k)
missing_prior_mu = sum(1 for k in lp_input if "prior_mu" not in k)
print(f"\n── Field presence check ──")
print(f"  sample_orders missing  : {missing_orders}/{len(lp_input)}  "
      f"{'✅' if missing_orders == 0 else '❌'}")
print(f"  prior_mu missing       : {missing_prior_mu}/{len(lp_input)}  "
      f"{'✅' if missing_prior_mu == 0 else '❌'}")

# s distribution
s_vals = []
for k in lp_input:
    mu = k.get("prior_mu", global_mu)
    if mu > 0:
        s_vals.append(min(max(_K / mu, _S_MIN), _S_MAX))
if s_vals:
    s_vals.sort()
    n = len(s_vals)
    print(f"\n── Prior strength s distribution (n={n}) ──")
    print(f"  min={s_vals[0]:.0f}  p25={s_vals[n//4]:.0f}  "
          f"median={s_vals[n//2]:.0f}  p75={s_vals[3*n//4]:.0f}  max={s_vals[-1]:.0f}")

print()
