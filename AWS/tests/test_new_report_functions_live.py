"""
Live smoke test for AmazonAdsClient.get_change_history.

Usage:
    venv311/bin/python3 tests/test_new_report_functions_live.py
    venv311/bin/python3 tests/test_new_report_functions_live.py --days 7
"""
from __future__ import annotations

import argparse
import asyncio
import json
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
logger = logging.getLogger("report_smoke_test")

PASS = "✓"
FAIL = "✗"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--store-id", default="US")
    p.add_argument("--region",   default="NA")
    p.add_argument("--days",     type=int, default=7)
    return p.parse_args()


# ── test: Ads change history ───────────────────────────────────────────────────

async def test_change_history(store_id: str, region: str, days: int) -> bool:
    # Narrow query: single day Apr 17 + BID_AMOUNT only → ~20 records, avoids rate-limit
    # Fetch campaign IDs first so parents[] can be injected (required by the API).
    logger.info(f"[Ads] get_change_history  2026-04-17  AD_GROUP/BID_AMOUNT  campaign=A01848952MBUG5UJOJLG1")
    try:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient

        client  = AmazonAdsClient(store_id=store_id, region=region)

        # Profile-wide fetch (useProfileIdAdvertiser:true); campaign filter is client-side
        logger.info(f"  Profile-wide fetch with pagination")

        from_ms = int(datetime(2026, 4, 16,  0,  0,  0).timestamp() * 1000)
        to_ms   = int(datetime(2026, 4, 18, 23, 59, 59).timestamp() * 1000)

        result = await client.get_change_history(
            from_date = from_ms,
            to_date   = to_ms,
            count     = 200,
        )

        if not isinstance(result, dict):
            logger.error(f"  Expected dict, got {type(result)}")
            return False

        events    = result.get("events", [])
        total     = result.get("total", len(events))
        logger.info(f"  {PASS} returned {len(events)} events (total reported: {total})")

        if events:
            sample = events[0]
            logger.info(f"  Sample event: {json.dumps(sample, ensure_ascii=False, default=str)}")
        else:
            logger.warning("  No change events in this window — may be a quiet period")

        return True

    except Exception as e:
        logger.error(f"  {FAIL} {type(e).__name__}: {e}")
        return False


# ── main ───────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    ok = await test_change_history(args.store_id, args.region, args.days)

    print(f"\n{'='*50}")
    print("  Smoke Test Summary")
    print(f"{'='*50}")
    print(f"  {PASS if ok else FAIL}  change_history")
    print(f"{'='*50}\n")

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
