"""
Integration test for LingxingClient.get_fba_shipment_tracking and
the shipment_lead_time analysis pipeline.

Run:
    PYTHONPATH=. python3 tests/test_erp_shipment_lead_time.py
"""

import importlib.util
import json
import os
import sys
from pprint import pformat

from dotenv import load_dotenv

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)


def _load(rel_path):
    """Load a single module file without triggering package __init__ imports."""
    abs_path = os.path.join(ROOT, rel_path)
    spec = importlib.util.spec_from_file_location(rel_path.replace("/", "."), abs_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Load the two modules we actually need, bypassing the heavy __init__ chain
_slt = _load("src/intelligence/processors/shipment_lead_time.py")
adapt_lingxing_shipments = _slt.adapt_lingxing_shipments
compute_quarterly_lead_times = _slt.compute_quarterly_lead_times

# Lingxing client has no problematic transitive imports
from src.mcp.servers.erp.lingxing.client import LingxingClient

# ── probe a range that is likely to have closed shipments ──────────────────
START_DATE = "2024-01-01"
END_DATE = "2025-03-31"


def _dump(label: str, obj) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print("─" * 60)
    if isinstance(obj, (dict, list)):
        print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
    else:
        print(pformat(obj))


def main():
    print("=== Lingxing FBA Shipment Lead-Time Test ===\n")

    client = LingxingClient()
    if not client.token:
        print("ERROR: no auth token — set LINGXING_ACCOUNT / LINGXING_PASSWORD in .env")
        sys.exit(1)
    print(f"Auth OK  token[:20]={client.token[:20]}...")

    # ── Step 1: raw API call, single page, inspect structure ──────────────
    print(f"\n[1] Raw API — offset=0, length=5  ({START_DATE} → {END_DATE})")
    raw = client.get_fba_shipment_tracking(
        start_date=START_DATE,
        end_date=END_DATE,
        fetch_all=False,
        length=5,
    )

    if not isinstance(raw, list):
        _dump("Unexpected raw response type", raw)
        sys.exit(1)

    print(f"    Records returned: {len(raw)}")
    if not raw:
        print("    No records — check LINGXING_SIDS env var and date range.")
        sys.exit(0)

    # Print keys of the first record so we can see the real field names
    first = raw[0]
    _dump("First record (raw keys + values)", first)
    print(f"\n    All keys: {list(first.keys())}")

    # ── Step 2: fetch all pages and normalise ──────────────────────────────
    print("\n[2] Fetching all pages (fetch_all=True) ...")
    raw_all = client.get_fba_shipment_tracking(
        start_date=START_DATE,
        end_date=END_DATE,
        fetch_all=True,
    )
    print(f"    Total records fetched: {len(raw_all)}")

    normalised = adapt_lingxing_shipments(raw_all)
    print(f"    After normalisation  : {len(normalised)} records")

    # Show a sample normalised record
    if normalised:
        _dump("Sample normalised record [0]", normalised[0])

    # Count how many have each date field populated
    date_fields = [
        "domestic_ship_date",
        "overseas_arrival_date",
        "overseas_ship_date",
        "fba_received_date",
    ]
    print("\n    Date field coverage:")
    for f in date_fields:
        n = sum(1 for r in normalised if r.get(f))
        print(f"      {f:<26} : {n}/{len(normalised)}")

    # ── Step 3a: sea transit (SHIPPED → RECEIVING), binned by SHIPPED date ───
    print("\n[3a] Sea transit: SHIPPED → RECEIVING (35 records expected) ...")
    result_sea = compute_quarterly_lead_times(
        normalised,
        sea_start_field="domestic_ship_date",  # SHIPPED date_info
        sea_end_field="overseas_arrival_date",  # RECEIVING date_info
        ovs_start_field="overseas_ship_date",
        ovs_end_field="fba_received_date",
        quarter_field="domestic_ship_date",
        sea_min_days=0,
        sea_max_days=180,
    )
    print(f"    total_input : {result_sea['total_input']}")
    print(f"    skipped     : {result_sea['skipped']}")
    _dump("sea_transit.overall (SHIPPED→RECEIVING)", result_sea["sea_transit"]["overall"])
    _dump("by_quarter_summary", result_sea["by_quarter_summary"])

    # ── Step 3b: FBA processing time (RECEIVING → CLOSED) ─────────────────
    print("\n[3b] FBA processing time: RECEIVING → CLOSED ...")
    result_fba = compute_quarterly_lead_times(
        normalised,
        sea_start_field="overseas_arrival_date",  # RECEIVING date_info
        sea_end_field="fba_received_date",  # CLOSED date_info
        ovs_start_field="overseas_arrival_date",
        ovs_end_field="fba_received_date",
        quarter_field="overseas_arrival_date",  # bin by RECEIVING date (177/224)
        sea_min_days=0,
        sea_max_days=60,
        ovs_min_days=0,
        ovs_max_days=60,
    )
    print(f"    total_input : {result_fba['total_input']}")
    print(f"    skipped     : {result_fba['skipped']}")
    _dump("sea_transit.overall (RECEIVING→CLOSED proxy)", result_fba["sea_transit"]["overall"])
    _dump("overseas_to_fba.overall", result_fba["overseas_to_fba"]["overall"])

    print("\n    By quarter (RECEIVING→CLOSED):")
    for q, stats in sorted(result_fba["sea_transit"].get("by_quarter", {}).items()):
        if stats.get("n", 0) > 0:
            print(
                f"      {q}: n={stats['n']}  "
                f"p25={stats['p25']}  median={stats['median']}  p75={stats['p75']} days"
            )

    _dump("by_quarter_summary", result_fba["by_quarter_summary"])

    print("\n=== Test complete ===")


if __name__ == "__main__":
    main()
