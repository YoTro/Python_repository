"""
Live smoke tests for AdsCreateReportsClient (/adsApi/v1/create/reports).

Usage:
    venv311/bin/python3 tests/test_ads_create_reports_live.py
    venv311/bin/python3 tests/test_ads_create_reports_live.py --days 14
    venv311/bin/python3 tests/test_ads_create_reports_live.py --ad-types SP SB
    venv311/bin/python3 tests/test_ads_create_reports_live.py --advertiser-account-id 3134479135518484
    venv311/bin/python3 tests/test_ads_create_reports_live.py --raw-response

Notes:
    advertiserAccountId defaults to AMAZON_ADS_PROFILE_ID_{STORE_ID} from .env.
    Override with --advertiser-account-id or env AMAZON_ADS_ADVERTISER_ACCOUNT_IDS
    (comma-separated list).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import UTC, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

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
        default=["SP", "SB", "SD", "STV", "DSP"],
        metavar="TYPE",
        help="Ad types to test (default: SP SB SD STV DSP)",
    )
    p.add_argument(
        "--advertiser-account-id",
        dest="advertiser_account_id",
        default=None,
        help="Override advertiserAccountId (defaults to profile ID from .env)",
    )
    p.add_argument(
        "--raw-response",
        action="store_true",
        help="Dump raw create_reports JSON response before downloading",
    )
    return p.parse_args()


def _resolve_account_ids(store_id: str, override: str | None) -> list[str]:
    """
    Resolve advertiserAccountId list.
    Priority:
      1. --advertiser-account-id CLI arg
      2. AMAZON_ADS_ADVERTISER_ACCOUNT_IDS env var (comma-separated)
      3. AMAZON_ADS_PROFILE_ID_{STORE_ID} env var (profile ID as fallback)
    """
    if override:
        return [override]
    env_ids = os.getenv("AMAZON_ADS_ADVERTISER_ACCOUNT_IDS", "").strip()
    if env_ids:
        return [x.strip() for x in env_ids.split(",") if x.strip()]
    profile_id = os.getenv(f"AMAZON_ADS_PROFILE_ID_{store_id.upper()}")
    if profile_id:
        logger.info(
            f"Using profile ID {profile_id} as advertiserAccountId "
            f"(set AMAZON_ADS_ADVERTISER_ACCOUNT_IDS to override)"
        )
        return [profile_id]
    raise ValueError(
        f"No advertiserAccountId found. Pass --advertiser-account-id or set "
        f"AMAZON_ADS_ADVERTISER_ACCOUNT_IDS or AMAZON_ADS_PROFILE_ID_{store_id.upper()} in .env"
    )


# ── test: create raw reports (single ad type, minimal fields) ─────────────────


async def test_create_raw(
    account_ids: list[str],
    store_id: str,
    region: str,
    start_date: str,
    end_date: str,
    ad_type: str = "SP",
    dump_raw: bool = False,
) -> bool:
    logger.info(f"[RawCreate] ad_type={ad_type}  {start_date} → {end_date}")
    try:
        from src.mcp.servers.amazon.ads.report_client import AdsCreateReportsClient

        client = AdsCreateReportsClient(store_id=store_id, region=region)
        spec = AdsCreateReportsClient.build_report_spec(
            start_date=start_date,
            end_date=end_date,
            fields=["campaignId", "campaignName", "impressions", "clicks", "cost"],
            ad_type=ad_type,
            fmt="JSON",
        )
        logger.info(f"  Request spec: {json.dumps(spec, indent=2)}")

        raw = await client.create_reports(account_ids, [spec])
        if dump_raw:
            logger.info(f"  Raw response:\n{json.dumps(raw, indent=2, default=str)}")

        errors = raw.get("error") or []
        success = raw.get("success") or []
        if errors:
            logger.error(f"  API errors: {json.dumps(errors, default=str)}")
            return False

        logger.info(f"  {PASS} success entries: {len(success)}")
        for s in success:
            report = s.get("report", {})
            logger.info(
                f"    index={s.get('index')} status={report.get('status')} "
                f"reportId={report.get('reportId')}"
            )
        return True

    except Exception as e:
        logger.error(f"  {FAIL} {type(e).__name__}: {e}", exc_info=True)
        return False


# ── test: full summary (create + poll + download + aggregate) ─────────────────


async def test_summary(
    account_ids: list[str],
    store_id: str,
    region: str,
    start_date: str,
    end_date: str,
    ad_types: list[str],
) -> bool:
    logger.info(f"[Summary] ad_types={ad_types}  {start_date} → {end_date}")
    try:
        from src.mcp.servers.amazon.ads.report_client import AdsCreateReportsClient

        client = AdsCreateReportsClient(store_id=store_id, region=region)
        result = await client.get_all_ad_type_summary(
            advertiser_account_ids=account_ids,
            start_date=start_date,
            end_date=end_date,
            ad_types=ad_types,
        )

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

        return True

    except Exception as e:
        logger.error(f"  {FAIL} {type(e).__name__}: {e}", exc_info=True)
        return False


# ── main ───────────────────────────────────────────────────────────────────────


async def main(args: argparse.Namespace) -> None:
    store_id = args.store_id.upper()
    account_ids = _resolve_account_ids(store_id, args.advertiser_account_id)
    logger.info(f"advertiserAccountId(s): {account_ids}")

    end_dt = datetime.now(tz=UTC).date()
    start_dt = end_dt - timedelta(days=args.days - 1)
    start_date = start_dt.isoformat()
    end_date = end_dt.isoformat()

    results: dict[str, bool] = {}

    # Step 1: raw create test (SP only, minimal fields) to confirm API access
    results["raw_create_SP"] = await test_create_raw(
        account_ids=account_ids,
        store_id=store_id,
        region=args.region,
        start_date=start_date,
        end_date=end_date,
        ad_type=args.ad_types[0] if args.ad_types else "SP",
        dump_raw=args.raw_response,
    )

    # Step 2: full summary across all requested ad types
    if results["raw_create_SP"]:
        results["summary"] = await test_summary(
            account_ids=account_ids,
            store_id=store_id,
            region=args.region,
            start_date=start_date,
            end_date=end_date,
            ad_types=args.ad_types,
        )
    else:
        logger.warning("Skipping summary test — raw create test failed")
        results["summary"] = False

    print(f"\n{'=' * 55}")
    print("  Smoke Test Summary  (AdsCreateReportsClient)")
    print(f"{'=' * 55}")
    for name, ok in results.items():
        print(f"  {PASS if ok else FAIL}  {name}")
    print(f"{'=' * 55}\n")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main(_parse_args()))
