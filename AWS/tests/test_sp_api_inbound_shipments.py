"""
Integration test for SPAPIClient.get_inbound_shipments and get_inbound_plans.

Run:
    PYTHONPATH=. python3 tests/test_sp_api_inbound_shipments.py

Requires a valid SP-API refresh token for the target store in .env:
    AMAZON_SP_API_CLIENT_ID
    AMAZON_SP_API_CLIENT_SECRET
    AMAZON_SP_API_REFRESH_TOKEN_US   (or whichever store)
"""

import asyncio
import importlib.util
import json
import os
import sys
from collections import Counter
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from src.mcp.servers.amazon.sp_api.client import SPAPIClient


# Load shipment_lead_time directly to avoid __init__.py → zoneinfo import on Python 3.8
def _load_mod(rel_path):
    abs_path = os.path.join(ROOT, rel_path)
    spec = importlib.util.spec_from_file_location(rel_path.replace("/", "."), abs_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_slt = _load_mod("src/intelligence/processors/shipment_lead_time.py")
adapt_sp_api_plans = _slt.adapt_sp_api_plans
compute_quarterly_lead_times = _slt.compute_quarterly_lead_times

STORE_ID = os.getenv("TEST_STORE_ID", "US")
LAST_UPDATED_AFTER = "2024-01-01T00:00:00Z"


def _dump(label: str, obj) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {label}")
    print("─" * 60)
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _parse_iso(s: str) -> datetime:
    """Parse ISO-8601 UTC string, tolerating fractional seconds."""
    s = s.rstrip("Z").split("+")[0]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s!r}")


async def test_inbound_shipments(client: SPAPIClient) -> None:
    """Section A — FBA Inbound Shipments v0 (no date fields, for reference)."""
    print(f"\n{'═' * 60}")
    print("  SECTION A: FBA Inbound Shipments v0")
    print(f"{'═' * 60}")

    print(f"\n[A1] Fetching CLOSED shipments since {LAST_UPDATED_AFTER} ...")
    shipments = await client.get_inbound_shipments(
        shipment_status_list=["CLOSED"],
        last_updated_after=LAST_UPDATED_AFTER,
        max_pages=3,
    )
    print(f"    Total records: {len(shipments)}")

    if not shipments:
        print("    No CLOSED shipments found. Trying RECEIVING ...")
        shipments = await client.get_inbound_shipments(
            shipment_status_list=["RECEIVING"],
            last_updated_after=LAST_UPDATED_AFTER,
            max_pages=2,
        )
        print(f"    RECEIVING records: {len(shipments)}")

    if not shipments:
        print("\n[!] No shipments returned — skipping Section A detail.")
        return

    _dump("First shipment (raw fields)", shipments[0])

    date_fields = ["CreatedDate", "LastUpdatedDate", "ConfirmedNeedByDate"]
    print("\n    Date field coverage (v0 — expected 0):")
    for f in date_fields:
        n = sum(1 for s in shipments if s.get(f))
        print(f"      {f:<26} : {n}/{len(shipments)}")

    status_counts = Counter(s.get("ShipmentStatus") for s in shipments)
    print(f"\n    Status distribution : {dict(status_counts)}")
    fc_counts = Counter(s.get("DestinationFulfillmentCenterId") for s in shipments)
    print(f"    Top FCs             : {dict(fc_counts.most_common(5))}")


async def test_inbound_plans(client: SPAPIClient) -> None:
    """Section B — FBA Inbound Plans 2024-03-20 (has createdAt / lastUpdatedAt)."""
    print(f"\n{'═' * 60}")
    print("  SECTION B: FBA Inbound Plans 2024-03-20")
    print(f"{'═' * 60}")

    # ── B1: fetch all statuses ─────────────────────────────────────────────
    print("\n[B1] Fetching all inbound plans (SHIPPED + ACTIVE + VOIDED) ...")
    plans = await client.get_inbound_plans(status=None, max_pages=10)
    print(f"    Total plans fetched: {len(plans)}")

    if not plans:
        print("\n[!] No plans returned.")
        print("    Possible causes:")
        print("      - Token lacks FBA Inbound Plans permission (2024-03-20 scope)")
        print("      - No plans exist for this marketplace")
        return

    # ── B2: inspect structure ──────────────────────────────────────────────
    _dump("First plan (raw fields)", plans[0])
    print(f"\n    All keys: {list(plans[0].keys())}")

    # ── B3: status breakdown ───────────────────────────────────────────────
    status_counts = Counter(p.get("status") for p in plans)
    print(f"\n[B3] Status distribution: {dict(status_counts)}")

    # ── B4: date field coverage ────────────────────────────────────────────
    date_fields = ["createdAt", "lastUpdatedAt"]
    print("\n[B4] Date field coverage:")
    for f in date_fields:
        n = sum(1 for p in plans if p.get(f))
        print(f"      {f:<20} : {n}/{len(plans)}")

    # ── B5: source country distribution ───────────────────────────────────
    def _country(p):
        src = p.get("sourceAddress") or {}
        return src.get("countryCode", "??") if isinstance(src, dict) else "??"

    country_counts = Counter(_country(p) for p in plans)
    print(f"\n[B5] Source country distribution: {dict(country_counts.most_common(10))}")

    cn_plans = [p for p in plans if _country(p) == "CN"]
    print(f"    Plans from CN (sea freight candidates): {len(cn_plans)}/{len(plans)}")

    # ── B6: transit time proxy — createdAt → lastUpdatedAt for SHIPPED/CN ──
    print("\n[B6] Plan-level transit proxy (createdAt → lastUpdatedAt) for SHIPPED+CN plans:")
    shipped_cn = [
        p
        for p in cn_plans
        if p.get("status") == "SHIPPED" and p.get("createdAt") and p.get("lastUpdatedAt")
    ]
    print(f"    SHIPPED + CN + both dates: {len(shipped_cn)} plans")

    if shipped_cn:
        durations = []
        for p in shipped_cn:
            try:
                d_create = _parse_iso(p["createdAt"])
                d_update = _parse_iso(p["lastUpdatedAt"])
                days = (d_update - d_create).days
                if 0 <= days <= 180:
                    durations.append(days)
            except Exception:
                pass

        if durations:
            durations.sort()
            n = len(durations)

            def _pct(k):
                return durations[int(n * k / 100)]

            print(
                f"    n={n}  min={min(durations)}  "
                f"p25={_pct(25)}  median={_pct(50)}  "
                f"p75={_pct(75)}  p90={_pct(90)}  max={max(durations)} days"
            )
        else:
            print("    [!] All durations out of range or unparseable.")

    # ── B7: quarterly breakdown ────────────────────────────────────────────
    print("\n[B7] Quarterly plan count by createdAt quarter:")
    quarter_counts: Counter = Counter()
    for p in plans:
        raw = p.get("createdAt")
        if not raw:
            continue
        try:
            dt = _parse_iso(raw)
            q = f"{dt.year}Q{(dt.month - 1) // 3 + 1}"
            quarter_counts[q] += 1
        except Exception:
            pass
    for q, cnt in sorted(quarter_counts.items()):
        print(f"      {q}: {cnt} plans")

    # ── B8: sample 3 plans with full date info ─────────────────────────────
    dated_plans = [p for p in plans if p.get("createdAt") and p.get("lastUpdatedAt")]
    print(f"\n[B8] Sample plans with both dates ({len(dated_plans)} total):")
    for p in dated_plans[:3]:
        src = p.get("sourceAddress") or {}
        country = src.get("countryCode", "??") if isinstance(src, dict) else "??"
        print(
            f"      status={p.get('status'):<8}  country={country}  "
            f"createdAt={p.get('createdAt', '—')[:10]}  "
            f"lastUpdatedAt={p.get('lastUpdatedAt', '—')[:10]}"
        )


async def test_lead_time_pipeline(client: SPAPIClient) -> None:
    """Section C — end-to-end lead-time analysis via adapt_sp_api_plans + compute_quarterly_lead_times."""
    print(f"\n{'═' * 60}")
    print("  SECTION C: Lead-Time Pipeline (SP-API Plans → quarterly stats)")
    print(f"{'═' * 60}")

    print("\n[C1] Fetching SHIPPED plans (CN source only) ...")
    plans = await client.get_inbound_plans(status="SHIPPED", max_pages=20)
    print(f"    Raw SHIPPED plans fetched: {len(plans)}")

    normalised = adapt_sp_api_plans(plans, cn_only=True, shipped_only=True)
    print(f"    After adapt (CN-only)    : {len(normalised)} records")

    if not normalised:
        print("\n[!] No CN-source SHIPPED plans — cannot compute lead times.")
        print("    Try running with cn_only=False to include domestic plans.")
        return

    _dump("Sample normalised plan [0]", normalised[0])

    print("\n[C2] Computing quarterly lead-time distributions ...")
    result = compute_quarterly_lead_times(
        normalised,
        sea_start_field="domestic_ship_date",
        sea_end_field="fba_received_date",  # plan creation → FBA receive proxy
        ovs_start_field="overseas_ship_date",  # None → will produce 0 overseas results
        ovs_end_field="fba_received_date",
    )

    print(f"    total_input : {result['total_input']}")
    print(f"    skipped     : {result['skipped']}")

    overall = result["sea_transit"]["overall"]
    print("\n    Sea transit proxy (plan creation → FBA receive) overall:")
    if overall.get("n", 0) > 0:
        print(
            f"      n={overall['n']}  min={overall['min']}  "
            f"p25={overall['p25']}  median={overall['median']}  "
            f"p75={overall['p75']}  p90={overall['p90']}  "
            f"max={overall['max']}  mean={overall['mean']:.1f} days"
        )
    else:
        print("      (no valid records)")

    print("\n    By quarter:")
    for q, stats in sorted(result["sea_transit"].get("by_quarter", {}).items()):
        if stats.get("n", 0) > 0:
            print(
                f"      {q}: n={stats['n']}  "
                f"p25={stats['p25']}  median={stats['median']}  p75={stats['p75']} days"
            )

    _dump("Full quarterly summary", result["by_quarter_summary"])


async def main():
    print(f"=== SP-API Inbound Shipments / Plans Test  (store={STORE_ID}) ===\n")

    client = SPAPIClient(store_id=STORE_ID)
    print(f"Endpoint   : {client.auth.endpoint}")
    print(f"Marketplace: {client.auth.marketplace_id}")

    await test_inbound_shipments(client)
    await test_inbound_plans(client)
    await test_lead_time_pipeline(client)

    print("\n=== Test complete ===")


if __name__ == "__main__":
    asyncio.run(main())
