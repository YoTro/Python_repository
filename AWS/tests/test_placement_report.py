"""
Test spCampaignsPlacement report with groupBy=["campaignPlacement"].

Creates a real report request, polls until done, downloads raw rows, and
prints the first 20 raw records plus a placement-value summary so we can
tell whether Amazon populates campaignPlacement in the response.

Usage:
    venv311/bin/python3 tests/test_placement_report.py [DAYS]
"""

import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

try:
    DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 30
except ValueError:
    DAYS = 30


async def main() -> None:
    from src.mcp.servers.amazon.ads.client import AmazonAdsClient

    store_id = os.getenv("AMAZON_ADS_DEFAULT_STORE", "US").upper()
    client = AmazonAdsClient(store_id=store_id, region="NA")

    logger.info(
        f"Creating spCampaignsPlacement report (groupBy=[campaignPlacement], last {DAYS} days) …"
    )
    records = await client.get_performance_report(
        report_type="spCampaignsPlacement",
        days=DAYS,
    )

    print(f"\n{'=' * 65}")
    print("spCampaignsPlacement  groupBy=['campaignPlacement']")
    print(f"Total rows returned: {len(records)}")
    print(f"{'=' * 65}")

    # Placement-value summary
    placements = [r.get("placement") for r in records]
    unique_placements = sorted({str(p) for p in placements})
    non_null = sum(1 for p in placements if p is not None)
    print("\n── Placement field stats ──")
    print(f"  Rows with placement != None : {non_null}/{len(records)}")
    print(f"  Unique placement values     : {unique_placements}")

    # campaign_id coverage
    with_cid = sum(1 for r in records if r.get("campaign_id"))
    print("\n── campaign_id coverage ──")
    print(f"  Rows with campaign_id       : {with_cid}/{len(records)}")

    # Per-placement totals
    by_placement: dict = {}
    for r in records:
        p = str(r.get("placement") or "NULL")
        if p not in by_placement:
            by_placement[p] = {"rows": 0, "spend": 0.0, "clicks": 0, "orders": 0}
        by_placement[p]["rows"] += 1
        by_placement[p]["spend"] += r.get("spend") or 0
        by_placement[p]["clicks"] += r.get("clicks") or 0
        by_placement[p]["orders"] += r.get("orders") or 0

    print("\n── Aggregated by placement ──")
    print(f"  {'Placement':<35} {'Rows':>5} {'Spend':>9} {'Clicks':>7} {'Orders':>7}")
    print(f"  {'-' * 35} {'-' * 5} {'-' * 9} {'-' * 7} {'-' * 7}")
    for p, v in sorted(by_placement.items(), key=lambda x: -x[1]["spend"]):
        print(f"  {p:<35} {v['rows']:>5} ${v['spend']:>8.2f} {v['clicks']:>7} {v['orders']:>7}")

    print("\n── First 20 raw parsed records ──")
    for i, r in enumerate(records[:20], 1):
        print(f"  [{i:02d}] {json.dumps(r, default=str)}")

    print()


asyncio.run(main())
