"""
Live smoke test for XiyouZhaociAPI.get_asin_keywords.

Tests:
  1. Standard monthly window  (2026-01-01 → 2026-01-31)
  2. Cross-month window       (2026-01-01 → 2026-04-21)
  3. Daily sub-month window   (2026-04-01 → 2026-04-21)  ← key: does daily work?
  4. Single-day window        (2026-04-21 → 2026-04-21)

Usage:
    venv311/bin/python tests/test_xiyou_asin_keywords_live.py
    venv311/bin/python tests/test_xiyou_asin_keywords_live.py --asin B0FVLPXRNY
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("xiyou_kw_test")

PASS = "✓"
FAIL = "✗"
WARN = "⚠"


def _parse(args=None):
    p = argparse.ArgumentParser()
    p.add_argument("--asin",    default="B0FVLPXRNY")
    p.add_argument("--country", default="US")
    return p.parse_args(args)


def _summarise(result: dict, label: str) -> bool:
    kw_list = result.get("list") or []
    avail   = result.get("availableDate") or {}
    if not kw_list:
        logger.warning(f"  {WARN} [{label}] returned 0 keywords — empty window or auth issue")
        logger.warning(f"       availableDate: {avail}")
        return False

    logger.info(f"  {PASS} [{label}] {len(kw_list)} keywords returned")
    logger.info(f"       availableDate: {avail}")

    sample = kw_list[0]
    top_asins = (sample.get("topAsins") or {}).get("list") or []
    logger.info(
        f"       Sample kw: '{sample.get('searchTerm')}' "
        f"vol={( sample.get('searchTermReport') or {}).get('weeklySearchVolume')} "
        f"topAsins={[a.get('asin') for a in top_asins]}"
    )
    return True


def main():
    args = _parse()
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    api = XiyouZhaociAPI()

    if not api.auth_token:
        print("No token — run auth first")
        sys.exit(1)

    cases = [
        ("monthly_standard",   "2026-01-01", "2026-01-31"),
        ("cross_month",        "2026-01-01", "2026-04-21"),
        ("daily_sub_month",    "2026-04-01", "2026-04-21"),  # KEY: is daily granularity accepted?
        ("single_day",         "2026-04-21", "2026-04-21"),
    ]

    results = {}
    for label, start, end in cases:
        logger.info(f"\n── {label}: {start} → {end}")
        try:
            raw = api.get_asin_keywords(
                country=args.country,
                asin=args.asin,
                start_date=start,
                end_date=end,
                page_size=10,
            )
            ok = _summarise(raw, label)
            results[label] = "PASS" if ok else "EMPTY"
        except Exception as e:
            logger.error(f"  {FAIL} [{label}] Exception: {e}")
            results[label] = f"ERROR: {e}"

    print(f"\n{'='*55}")
    print("  xiyou_get_asin_keywords date-granularity test results")
    print(f"{'='*55}")
    for label, status in results.items():
        icon = PASS if status == "PASS" else (WARN if status == "EMPTY" else FAIL)
        print(f"  {icon}  {label:<25} {status}")
    print(f"{'='*55}\n")

    # Verdict on daily granularity
    daily_status = results.get("daily_sub_month", "")
    if daily_status == "PASS":
        print("✅ Daily date ranges ARE supported — no need to force month boundaries.")
    elif daily_status == "EMPTY":
        print("⚠️  Daily range returned 0 results — API may require full month boundaries.")
        print("   Recommendation: align start_date to first-of-month, end_date to last-of-month.")
    else:
        print(f"❌ Daily range failed: {daily_status}")


if __name__ == "__main__":
    main()
