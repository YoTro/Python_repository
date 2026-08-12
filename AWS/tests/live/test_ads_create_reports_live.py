"""
Live smoke test for AmazonAdsClient.get_ad_type_summary() (offline performance
reports: create + poll + download + cross-type aggregation).

Usage:
    venv311/bin/python3 tests/live/test_ads_create_reports_live.py
    venv311/bin/python3 tests/live/test_ads_create_reports_live.py --days 14
    venv311/bin/python3 tests/live/test_ads_create_reports_live.py --ad-types SP SB
    venv311/bin/python3 tests/live/test_ads_create_reports_live.py --raw-response

Notes:
    store_id resolves credentials via AmazonAdsAuth (AMAZON_ADS_REFRESH_TOKEN_{STORE_ID}
    etc. in .env) — no separate advertiserAccountId is needed on this client.

This file intentionally defines no `test_*` functions: pytest would try to
collect them, but they take positional args rather than fixtures, so this
lives purely as an `if __name__ == "__main__":` script (see TESTING.md §4.2).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv

load_dotenv(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"
    )
)

from src.mcp.servers.amazon.ads.client import AmazonAdsClient  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ads_create_reports_test")

PASS = "✓"
FAIL = "✗"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--store-id", default="US")
    p.add_argument("--region", default="NA")
    p.add_argument("--days", type=int, default=7)
    p.add_argument(
        "--ad-types",
        nargs="+",
        default=["SP", "SB", "SD"],
        metavar="TYPE",
        help="Ad types to test (default: SP SB SD)",
    )
    p.add_argument(
        "--raw-response",
        action="store_true",
        help="Dump the raw get_ad_type_summary() JSON result before formatting",
    )
    return p.parse_args()


async def _check_summary(
    client: AmazonAdsClient,
    start_date: str,
    end_date: str,
    ad_types: list[str],
    dump_raw: bool = False,
) -> bool:
    logger.info(f"[Summary] ad_types={ad_types}  {start_date} → {end_date}")
    try:
        result = await client.get_ad_type_summary(
            start_date=start_date,
            end_date=end_date,
            ad_types=ad_types,
        )
        if dump_raw:
            logger.info(f"  Raw response:\n{json.dumps(result, indent=2, default=str)}")

        period = result["period"]
        logger.info(f"  Period: {period['start_date']} → {period['end_date']}")

        if result["errors"]:
            logger.warning(f"  Partial errors: {result['errors']}")

        total = result["total"]
        logger.info(
            f"  Total — spend ${total['spend']:.2f} | clicks {total['clicks']} | "
            f"orders {total['orders']} | sales ${total['sales']:.2f} | "
            f"ACOS {total['acos_pct']}% | CTR {total['ctr_pct']}%"
        )

        print(
            f"\n{'Type':<6} {'Spend':>10} {'Share%':>8} {'Clicks':>8} {'Clk%':>7} "
            f"{'Orders':>8} {'Sales':>10} {'ACOS%':>7} {'CTR%':>7} {'CPC':>6} {'Campaigns':>10}"
        )
        print("-" * 95)
        for ad_type, m in sorted(result["by_type"].items()):
            acos = f"{m['acos_pct']}%" if m["acos_pct"] is not None else "—"
            ctr = f"{m['ctr_pct']}%" if m["ctr_pct"] is not None else "—"
            cpc = f"${m['cpc']}" if m["cpc"] is not None else "—"
            print(
                f"{ad_type:<6} "
                f"${m['spend']:>9.2f} "
                f"{m['spend_share_pct']:>7.1f}% "
                f"{m['clicks']:>8} "
                f"{m['clicks_share_pct']:>6.1f}% "
                f"{m['orders']:>8} "
                f"${m['sales']:>9.2f} "
                f"{acos:>7} "
                f"{ctr:>7} "
                f"{cpc:>6} "
                f"{m['campaign_count']:>10}"
            )
        print("-" * 95)
        print(
            f"{'TOTAL':<6} "
            f"${total['spend']:>9.2f} "
            f"{'100.0%':>8} "
            f"{total['clicks']:>8} "
            f"{'100.0%':>7} "
            f"{total['orders']:>8} "
            f"${total['sales']:>9.2f} "
            f"{str(total['acos_pct']) + '%' if total['acos_pct'] is not None else '—':>7}"
        )

        return not result["errors"]

    except Exception as e:
        logger.error(f"  {FAIL} {type(e).__name__}: {e}", exc_info=True)
        return False


# ── main ───────────────────────────────────────────────────────────────────────


async def main(args: argparse.Namespace) -> None:
    store_id = args.store_id.upper()
    client = AmazonAdsClient(store_id=store_id, region=args.region)

    end_dt = datetime.now(tz=UTC).date()
    start_dt = end_dt - timedelta(days=args.days - 1)
    start_date = start_dt.isoformat()
    end_date = end_dt.isoformat()

    ok = await _check_summary(
        client,
        start_date=start_date,
        end_date=end_date,
        ad_types=args.ad_types,
        dump_raw=args.raw_response,
    )

    print(f"\n{'=' * 55}")
    print("  Smoke Test Summary  (AmazonAdsClient.get_ad_type_summary)")
    print(f"{'=' * 55}")
    print(f"  {PASS if ok else FAIL}  summary")
    print(f"{'=' * 55}\n")

    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
