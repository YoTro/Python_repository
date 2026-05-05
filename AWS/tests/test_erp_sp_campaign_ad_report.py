"""
Quick integration test for LingxingClient.get_sp_campaign_ad_report.
Requires LINGXING_ACCOUNT / LINGXING_PASSWORD (and optionally the four
x-ak-* identity env vars) to be set in .env.

Run:
    PYTHONPATH=. python3 tests/test_erp_sp_campaign_ad_report.py
"""
import json
import os
import sys

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.mcp.servers.erp.lingxing.client import LingxingClient

PROFILE_ID  = "3134479135518484"
REPORT_DATE = "2025-04-02 - 2025-05-01"
ASINS       = ["B0DRWPNT6Z", "B0DRWN72VB"]


def _print_row(label: str, row: dict):
    fields = ["date_day", "clicks", "impressions", "orders", "spends",
              "sales", "acos", "roas", "ctr", "cvr", "cpc", "cpa"]
    vals = {k: row.get(k, "--") for k in fields}
    print(f"  [{label}] {json.dumps(vals, ensure_ascii=False)}")


def main():
    print("=== Lingxing SP Campaign Ad Report Test ===\n")

    client = LingxingClient()
    if not client.token:
        print("ERROR: no auth token — check LINGXING_ACCOUNT / LINGXING_PASSWORD")
        sys.exit(1)
    print(f"Auth token OK (first 20 chars): {client.token[:20]}...")

    print(f"\n--- length clamp test: length=10 → should clamp to 25 ---")
    resp = client.get_sp_campaign_ad_report(
        profile_id=PROFILE_ID,
        report_date=REPORT_DATE,
        asin=ASINS,
        is_daily=0,
        length=10,
    )
    print(f"  success={resp.get('success')}  rows={len(resp.get('data', []))}")

    print(f"\n--- single page, aggregate + daily (length=50) ---")
    resp = client.get_sp_campaign_ad_report(
        profile_id=PROFILE_ID,
        report_date=REPORT_DATE,
        asin=ASINS,
        is_daily=1,
        length=50,
    )
    data = resp.get("data", [])
    print(f"  success={resp.get('success')}  total_rows={len(data)}")
    if data:
        _print_row("aggregate", data[0])
    for row in data[1:4]:
        _print_row("daily", row)
    if len(data) > 4:
        print(f"  ... ({len(data) - 4} more daily rows)")

    print(f"\n--- fetch_all=True (auto-paginate, length=500) ---")
    resp_all = client.get_sp_campaign_ad_report(
        profile_id=PROFILE_ID,
        report_date=REPORT_DATE,
        asin=ASINS,
        is_daily=1,
        length=500,
        fetch_all=True,
    )
    data_all = resp_all.get("data", [])
    print(f"  success={resp_all.get('success')}  total_rows={len(data_all)}")
    if data_all:
        _print_row("aggregate", data_all[0])

    print("\n=== DONE ===")


if __name__ == "__main__":
    main()
