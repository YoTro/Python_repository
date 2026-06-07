"""
Test whether adding placementClassification / campaignPlacement to columns
returns per-placement data in the spCampaignsPlacement report.

Tries two groupBy variants back-to-back:
  A) groupBy=["campaign"]            + extra columns
  B) groupBy=["campaignPlacement"]   + extra columns

Usage:
    venv311/bin/python3 tests/test_placement_columns.py [DAYS]
"""

import asyncio
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

try:
    DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 30
except ValueError:
    DAYS = 30

EXTRA_COLUMNS = ["campaignPlacement", "placementClassification"]

BASE_METRICS = [
    "impressions",
    "clicks",
    "cost",
    "spend",
    "purchases7d",
    "sales7d",
    "clickThroughRate",
    "costPerClick",
    "campaignId",
    "campaignName",
    "campaignBiddingStrategy",
    "campaignBudgetAmount",
]


def _summarise(label: str, records: list) -> None:
    print(f"\n{'=' * 65}")
    print(f"{label}")
    print(f"Total rows: {len(records)}")
    print(f"{'=' * 65}")

    for col in EXTRA_COLUMNS + ["placement"]:
        vals = [r.get(col) for r in records]
        non_null = sum(1 for v in vals if v is not None)
        unique = sorted({str(v) for v in vals})
        print(f"  {col:<28}  non-null={non_null}/{len(records)}  unique={unique[:8]}")

    # First 5 raw rows (full)
    print("\n── First 5 raw records ──")
    for i, r in enumerate(records[:5], 1):
        print(f"  [{i}] {json.dumps(r, default=str)}")


async def _fetch(client, group_by: list, metrics: list, days: int) -> list:
    """Call _create_report / _poll_report / _download_report directly so we
    can override groupBy without touching the production code path."""

    from src.mcp.servers.amazon.ads.client import _parse_report_record

    today = datetime.utcnow().date()
    end = str(today - timedelta(days=1))
    start = str(today - timedelta(days=days))

    report_id = await client._create_report(
        report_type="spCampaignsPlacement",
        start_date=start,
        end_date=end,
        metrics=metrics,
        filters=None,
        time_unit="SUMMARY",
        _override_group_by=group_by,  # added below
    )
    url = await client._poll_report(report_id)
    records = await client._download_report(url)
    return [_parse_report_record(r, "spCampaignsPlacement") for r in records], records


async def main() -> None:
    # Patch _create_report to accept _override_group_by
    from src.mcp.servers.amazon.ads import client as _client_mod

    _orig_create = _client_mod.AmazonAdsClient._create_report

    async def _patched_create(
        self,
        report_type,
        start_date,
        end_date,
        metrics,
        filters=None,
        time_unit="SUMMARY",
        _override_group_by=None,
    ):
        if _override_group_by is not None:
            import time as _time

            import requests as _req

            report_type_id = "spCampaigns"
            headers = {
                "Authorization": f"Bearer {self.auth.get_access_token()}",
                "Amazon-Advertising-API-ClientId": self.auth.client_id,
                "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
            ts = int(_time.time())
            body = {
                "name": f"placement_col_test_{ts}",
                "startDate": start_date,
                "endDate": end_date,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "reportTypeId": report_type_id,
                    "groupBy": _override_group_by,
                    "columns": metrics,
                    "timeUnit": time_unit,
                    "format": "GZIP_JSON",
                },
            }
            url = f"{self.base_url}/reporting/reports"
            resp = await asyncio.to_thread(_req.post, url, json=body, headers=headers)
            if resp.status_code not in (200, 202):
                raise RuntimeError(f"_create_report failed {resp.status_code}: {resp.text[:400]}")
            data = resp.json()
            report_id = data.get("reportId") or data.get("reportid") or data.get("id")
            if not report_id:
                raise ValueError(f"No reportId in response: {data}")
            logger.info(f"Created report {report_id} (groupBy={_override_group_by})")
            return report_id

        return await _orig_create(
            self, report_type, start_date, end_date, metrics, filters=filters, time_unit=time_unit
        )

    _client_mod.AmazonAdsClient._create_report = _patched_create

    from src.mcp.servers.amazon.ads.client import AmazonAdsClient, _parse_report_record

    store_id = os.getenv("AMAZON_ADS_DEFAULT_STORE", "US").upper()
    client = AmazonAdsClient(store_id=store_id, region="NA")

    metrics = BASE_METRICS + EXTRA_COLUMNS
    today = datetime.utcnow().date()
    end = str(today - timedelta(days=1))
    start = str(today - timedelta(days=DAYS))

    results = {}
    for label, group_by in [
        ("A: groupBy=['campaign']", ["campaign"]),
        ("B: groupBy=['campaignPlacement']", ["campaignPlacement"]),
    ]:
        logger.info(f"Requesting {label} with extra columns {EXTRA_COLUMNS} …")
        try:
            report_id = await client._create_report(
                "spCampaignsPlacement",
                start,
                end,
                metrics,
                _override_group_by=group_by,
            )
            url = await client._poll_report(report_id)
            raw = await client._download_report(url)
            parsed = [_parse_report_record(r, "spCampaignsPlacement") for r in raw]
            # attach raw fields for inspection
            for p, r in zip(parsed, raw, strict=False):
                for col in EXTRA_COLUMNS:
                    if col not in p:
                        p[col] = r.get(col)
            results[label] = parsed
        except Exception as exc:
            print(f"\n{label} → ERROR: {exc}")
            results[label] = []

    out_path = os.path.join(os.path.dirname(__file__), "placement_columns_result.txt")
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        for label, records in results.items():
            _summarise(label, records)
    output = buf.getvalue()
    print(output)
    with open(out_path, "w") as f:
        f.write(output)
    logger.info(f"Saved to {out_path}")


asyncio.run(main())
