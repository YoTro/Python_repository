"""
Live fetch of the crossProgramBenchmarks report for the last 30 days.

Writes results to benchmark_report_<store_id>_<date>.csv in the project root.

Usage:
    venv311/bin/python3 tests/test_benchmark_report_live.py
    venv311/bin/python3 tests/test_benchmark_report_live.py --store-id JP
    venv311/bin/python3 tests/test_benchmark_report_live.py --days 14
    venv311/bin/python3 tests/test_benchmark_report_live.py --time-unit WEEKLY
    venv311/bin/python3 tests/test_benchmark_report_live.py --brand "Acme Corp"
    venv311/bin/python3 tests/test_benchmark_report_live.py --out /tmp/benchmarks.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("benchmark_report")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch crossProgramBenchmarks report to CSV.")
    p.add_argument("--store-id", default="US", help="Store / profile ID (default: US)")
    p.add_argument("--region", default="NA", choices=["NA", "EU", "FE"])
    p.add_argument("--days", type=int, default=30, help="Lookback days (default: 30)")
    p.add_argument(
        "--time-unit",
        default="DAILY",
        choices=["DAILY", "WEEKLY", "MONTHLY"],
        help="Report granularity (default: DAILY)",
    )
    p.add_argument(
        "--brand",
        default=None,
        help="Filter to a specific brand name (case-insensitive, omit for all brands)",
    )
    p.add_argument("--out", default=None, help="Output CSV path (auto-named if omitted)")
    return p.parse_args()


async def main() -> None:
    args = _parse_args()

    from src.mcp.servers.amazon.ads.client import AmazonAdsClient

    client = AmazonAdsClient(store_id=args.store_id, region=args.region)

    logger.info(
        f"Fetching crossProgramBenchmarks — store={args.store_id} "
        f"days={args.days} time_unit={args.time_unit}"
    )
    records = await client.get_benchmark_report(days=args.days, time_unit=args.time_unit)

    if not records:
        logger.warning("No records returned.")
        return

    # The API only accepts adProduct="ALL"; filter SP rows on the client side.
    sp_records = [
        r for r in records if (r.get("adProduct") or "").upper() in ("SPONSORED_PRODUCTS", "SP")
    ]
    logger.info(f"Filtered {len(records)} → {len(sp_records)} SP rows")
    records = sp_records

    if not records:
        logger.warning("No SP records after filtering.")
        return

    if args.brand:
        brand_lower = args.brand.lower()
        brand_records = [r for r in records if (r.get("brand") or "").lower() == brand_lower]
        logger.info(f"Filtered {len(records)} → {len(brand_records)} rows for brand '{args.brand}'")
        records = brand_records
        if not records:
            logger.warning(f"No records found for brand '{args.brand}'.")
            return

    out_path = args.out or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        f"benchmark_report_{args.store_id}_{datetime.now().strftime('%Y%m%d')}.csv",
    )

    fieldnames = list(records[0].keys())
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    logger.info(f"Wrote {len(records)} rows → {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
