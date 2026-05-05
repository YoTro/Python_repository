"""
Calibration check: Amazon Ads API (Redis daily_perf) vs Lingxing ERP sp_campaign_ad_report.

For each ASIN in Redis, fetch the same date range from ERP and compare
orders / spend / clicks day-by-day at ASIN level (aggregated across campaigns).

Run:
    PYTHONPATH=. venv311/bin/python3 tests/test_erp_ads_calibration.py
"""
import json
import os
import sys
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv("/Users/jin/Documents/GitHub/Python_repository/AWS/.env")
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import redis
from src.mcp.servers.erp.lingxing.client import LingxingClient

PROFILE_ID = os.getenv("AMAZON_ADS_PROFILE_ID_US", "3134479135518484")
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379")
METRICS     = ["orders", "spend", "clicks"]

# ── helpers ───────────────────────────────────────────────────────────────────

def _load_redis_daily(asin: str) -> dict[str, dict]:
    """date → {orders, spend, clicks} aggregated across all campaigns."""
    r = redis.from_url(REDIS_URL)
    key = f"aws:cache:ad_diag:default:US:daily_perf:{asin}:30"
    raw = r.get(key)
    if not raw:
        return {}
    rows = json.loads(raw).get("data", [])
    by_date: dict[str, dict] = defaultdict(lambda: {m: 0.0 for m in METRICS})
    for row in rows:
        d = row.get("date")
        if not d:
            continue
        for m in METRICS:
            # Ads API uses 'spend', ERP uses 'spends' — normalised here
            val = row.get(m) or 0.0
            by_date[d][m] += float(val)
    return dict(by_date)


def _load_erp_daily(client: LingxingClient, asin: str, date_range: str) -> dict[str, dict]:
    """date → {orders, spend, clicks} from ERP, aggregated across campaigns."""
    resp = client.get_sp_campaign_ad_report(
        profile_id=PROFILE_ID,
        report_date=date_range,
        asin=[asin],
        is_daily=1,
        length=500,
        fetch_all=True,
    )
    if not resp.get("success"):
        print(f"  ERP call failed: {resp}")
        return {}

    by_date: dict[str, dict] = defaultdict(lambda: {m: 0.0 for m in METRICS})
    for row in resp.get("data", []):
        d = row.get("date_day")
        if not d:
            continue  # skip aggregate row (date_day=null)
        by_date[d]["orders"] += float(row.get("orders") or 0)
        by_date[d]["spend"]  += float(row.get("spends") or 0)   # ERP field = spends
        by_date[d]["clicks"] += float(row.get("clicks") or 0)
    return dict(by_date)


def _compare(ads_day: dict, erp_day: dict, metric: str) -> tuple[float, float, float]:
    """Returns (ads_val, erp_val, abs_diff)."""
    a = ads_day.get(metric, 0.0)
    e = erp_day.get(metric, 0.0)
    return a, e, abs(a - e)


def _report_asin(asin: str, ads: dict, erp: dict):
    common_dates = sorted(set(ads) & set(erp))
    ads_only = sorted(set(ads) - set(erp))
    erp_only = sorted(set(erp) - set(ads))

    print(f"\n{'─'*70}")
    print(f"ASIN: {asin}")
    print(f"  Ads API dates : {len(ads)} days")
    print(f"  ERP dates     : {len(erp)} days")
    print(f"  Common dates  : {len(common_dates)}")
    if ads_only:
        print(f"  Ads-only dates: {ads_only[:5]}{'...' if len(ads_only)>5 else ''}")
    if erp_only:
        print(f"  ERP-only dates: {erp_only[:5]}{'...' if len(erp_only)>5 else ''}")

    if not common_dates:
        print("  ⚠  No overlapping dates — cannot compare.")
        return

    # Per-metric summary
    for metric in METRICS:
        diffs, ads_vals, erp_vals = [], [], []
        for d in common_dates:
            a, e, diff = _compare(ads[d], erp[d], metric)
            ads_vals.append(a)
            erp_vals.append(e)
            diffs.append(diff)

        total_ads = sum(ads_vals)
        total_erp = sum(erp_vals)
        mean_abs_err = sum(diffs) / len(diffs)
        # MAPE relative to Ads API (skip days where ads=0)
        mape_vals = [abs(a - e) / a for a, e in zip(ads_vals, erp_vals) if a > 0]
        mape = (sum(mape_vals) / len(mape_vals) * 100) if mape_vals else float("nan")

        symbol = "✅" if mape < 5 else ("⚠️ " if mape < 15 else "❌")
        print(f"  {symbol} {metric:<8}  Ads={total_ads:>10.2f}  ERP={total_erp:>10.2f}"
              f"  MAE/day={mean_abs_err:>7.2f}  MAPE={mape:>5.1f}%")

    # Show worst 5 days by orders diff
    order_diffs = []
    for d in common_dates:
        a, e, diff = _compare(ads[d], erp[d], "orders")
        order_diffs.append((diff, d, a, e))
    order_diffs.sort(reverse=True)
    if order_diffs:
        print(f"\n  Top-5 divergent days (orders):")
        print(f"  {'date':<12} {'ads':>8} {'erp':>8} {'diff':>8}")
        for diff, d, a, e in order_diffs[:5]:
            flag = " ⚠️" if diff > 1 else ""
            print(f"  {d:<12} {a:>8.1f} {e:>8.1f} {diff:>8.1f}{flag}")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("=== Ads API vs ERP Calibration Check ===\n")

    client = LingxingClient()
    if not client.token:
        print("ERROR: no Lingxing token")
        sys.exit(1)

    r = redis.from_url(REDIS_URL)
    dp_keys = r.keys("aws:cache:ad_diag:default:US:daily_perf:*")
    asins = [k.decode().split(":")[6] for k in dp_keys]
    print(f"ASINs in Redis: {asins}")

    for asin in asins:
        ads = _load_redis_daily(asin)
        if not ads:
            print(f"\n{asin}: no Ads API data in Redis")
            continue

        sorted_dates = sorted(ads)
        date_range = f"{sorted_dates[0]} - {sorted_dates[-1]}"
        print(f"\nFetching ERP data for {asin}: {date_range}")
        erp = _load_erp_daily(client, asin, date_range)

        _report_asin(asin, ads, erp)

    print(f"\n{'═'*70}")
    print("Calibration legend: MAPE < 5% ✅  5-15% ⚠️  >15% ❌")
    print("If ✅ across both ASINs → ERP is safe to use as YoY baseline source.")


if __name__ == "__main__":
    main()
