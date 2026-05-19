"""
Live smoke test for AmazonAdsClient.get_change_history.

Usage:
    venv311/bin/python3 tests/test_new_report_functions_live.py
    venv311/bin/python3 tests/test_new_report_functions_live.py --days 7
    venv311/bin/python3 tests/test_new_report_functions_live.py --probe-budget-status
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
    p.add_argument("--store-id",           default="US")
    p.add_argument("--region",             default="NA")
    p.add_argument("--days",               type=int, default=7)
    p.add_argument("--probe-budget-status", action="store_true",
                   help="Probe for OUT_OF_BUDGET events instead of running normal test")
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


# ── probe: BUDGET_STATUS / OUT_OF_BUDGET ──────────────────────────────────────

async def probe_budget_status_events(store_id: str, region: str, days: int) -> None:
    """
    Probe whether Amazon Ads change-history returns OUT_OF_BUDGET events.

    Three attempts:
      A) Add BUDGET_STATUS to the CAMPAIGN filter — the documented way.
      B) Request everything (no entity filter) — catch any event type Amazon returns.
      C) Check raw changeType values from attempt A/B to see what actually came back.
    """
    from src.mcp.servers.amazon.ads.client import AmazonAdsClient
    from datetime import timezone, timedelta

    client = AmazonAdsClient(store_id=store_id, region=region)
    now_ms    = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    from_ms   = now_ms - days * 24 * 3600 * 1000

    # ── Attempt A: explicit BUDGET_STATUS filter ───────────────────────────
    logger.info("=== Attempt A: CAMPAIGN filter includes BUDGET_STATUS ===")
    try:
        result_a = await client.get_change_history(
            from_date   = from_ms,
            to_date     = now_ms,
            event_types = {
                "CAMPAIGN": {"filters": ["BUDGET_STATUS"]},
            },
            count = 200,
        )
        events_a = result_a.get("events", [])
        logger.info(f"  total reported: {result_a.get('total', '?')}, returned: {len(events_a)}")
        types_a = sorted({e.get("changeType") or e.get("change_type", "?") for e in events_a})
        logger.info(f"  changeType values seen: {types_a}")
        for e in events_a[:3]:
            logger.info(f"  sample: {json.dumps(e, ensure_ascii=False, default=str)}")
        if not events_a:
            logger.warning("  No events returned — either BUDGET_STATUS is not a valid filter "
                           "or no budget-status events occurred in the past %d days", days)
    except Exception as e:
        logger.error(f"  Attempt A failed: {type(e).__name__}: {e}")

    # ── Attempt B: no entity-type filter (profile-wide, all event types) ──
    logger.info("=== Attempt B: no entity filter — collect ALL changeType values ===")
    try:
        result_b = await client.get_change_history(
            from_date   = from_ms,
            to_date     = now_ms,
            event_types = {},   # empty → client falls back to default; override below
            count       = 200,
        )
        # Override: re-call with default filter to get raw everything
        result_b = await client.get_change_history(
            from_date = from_ms,
            to_date   = now_ms,
            count     = 200,
        )
        events_b = result_b.get("events", [])
        types_b  = sorted({e.get("changeType") or e.get("change_type", "?") for e in events_b})
        logger.info(f"  total reported: {result_b.get('total', '?')}, returned: {len(events_b)}")
        logger.info(f"  ALL changeType values seen: {types_b}")
        ob = [e for e in events_b
              if (e.get("changeType") or e.get("change_type", "")).upper() in
                 ("OUT_OF_BUDGET", "BUDGET_STATUS", "BUDGET")]
        if ob:
            logger.info(f"  OUT_OF_BUDGET / BUDGET_STATUS candidates: {len(ob)}")
            for e in ob[:3]:
                logger.info(f"  {json.dumps(e, ensure_ascii=False, default=str)}")
        else:
            logger.warning("  No OUT_OF_BUDGET / BUDGET_STATUS events found in default fetch")
    except Exception as e:
        logger.error(f"  Attempt B failed: {type(e).__name__}: {e}")

    logger.info("=== Probe complete ===")


# ── main ───────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    if args.probe_budget_status:
        await probe_budget_status_events(args.store_id, args.region, args.days)
        return

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
