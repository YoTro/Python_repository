"""
Shipment lead-time analysis — quarterly distribution of:
  1. Sea freight transit time  (origin departure → overseas warehouse arrival)
  2. Overseas-to-FBA lead time (overseas warehouse departure → FBA receive complete)

Data sources
------------
Primary : Lingxing ERP  — provides all four stage dates per shipment
Fallback : SP-API Inbound Plans 2024-03-20 — provides createdAt + lastUpdatedAt;
           maps to plan-creation → FBA-receive proxy (CN-source SHIPPED plans only).
           Sea transit and overseas warehouse breakdown are not available.
Last    : SP-API FBA Inbound Shipments v0 — no date fields; not usable for
          lead-time analysis.

Quarterly binning is by the domestic ship-out date so that seasonality effects
(e.g. pre-CNY rush) are attributed to the correct dispatch period.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime

logger = logging.getLogger(__name__)

# ── date helpers ──────────────────────────────────────────────────────────────


def _to_date(val: str | None) -> date | None:
    """Parse date string or epoch-second int → date. Returns None on failure.

    Handles formats:
      YYYY-MM-DD, YYYY-MM-DDTHH:MM:SSZ, YYYY-MM-DDTHH:MM:SS,
      YYYY-MM-DD HH:MM:SS (Lingxing), YYYY-MM-DD HH:MM (Lingxing short)
    """
    if val is None:
        return None
    if isinstance(val, int | float):
        try:
            return datetime.utcfromtimestamp(val).date()
        except (OSError, ValueError, OverflowError):
            return None
    s = str(val).strip()
    # Try ISO T-separator formats first (slice to 19 chars to drop timezone/fraction)
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except ValueError:
            continue
    # Space-separated formats (Lingxing ERP: "2025-04-09 14:46" or "2025-04-09 14:46:00")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except ValueError:
            continue
    return None


def _quarter(d: date) -> str:
    """Return quarter label e.g. '2024-Q2'."""
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


# ── distribution helper ───────────────────────────────────────────────────────


def _distribution(values: list[float]) -> dict:
    """Compute summary statistics for a list of day-counts."""
    if not values:
        return {"n": 0}
    s = sorted(values)
    n = len(s)

    def _pct(p: float) -> float:
        idx = (n - 1) * p
        lo, hi = int(idx), min(int(idx) + 1, n - 1)
        return round(s[lo] + (s[hi] - s[lo]) * (idx - lo), 1)

    return {
        "n": n,
        "min": round(s[0], 1),
        "p25": _pct(0.25),
        "median": _pct(0.50),
        "p75": _pct(0.75),
        "p90": _pct(0.90),
        "max": round(s[-1], 1),
        "mean": round(sum(s) / n, 1),
    }


# ── core analysis ─────────────────────────────────────────────────────────────


def _shipment_days(d_start: str | None, d_end: str | None) -> float | None:
    """Return calendar days between two date strings, or None if either is missing."""
    s, e = _to_date(d_start), _to_date(d_end)
    if s is None or e is None:
        return None
    delta = (e - s).days
    return float(delta) if delta >= 0 else None


def compute_quarterly_lead_times(
    shipments: list[dict],
    *,
    sea_start_field: str = "domestic_ship_date",
    sea_end_field: str = "overseas_arrival_date",
    ovs_start_field: str = "overseas_ship_date",
    ovs_end_field: str = "fba_received_date",
    local_start_field: str | None = None,
    local_end_field: str | None = None,
    quarter_field: str = "domestic_ship_date",
    transport_field: str = "transport_type",
    sea_transport_values: tuple[str, ...] = ("SEA", "ocean", "OCEAN", "sea"),
    sea_min_days: float = 5.0,
    sea_max_days: float = 120.0,
    ovs_min_days: float = 0.0,
    ovs_max_days: float = 60.0,
    local_min_days: float = 0.0,
    local_max_days: float = 12.0,
) -> dict:
    """
    Compute quarterly lead-time distributions from a list of shipment records.

    Three optional phases (each independently computed):
      sea_transit   : sea_start_field → sea_end_field  (long-haul freight)
      overseas_to_fba: ovs_start_field → ovs_end_field (FBA processing)
      local_to_fba  : local_start_field → local_end_field, only when
                      local_start_field is set; transit filtered to
                      [local_min_days, local_max_days] to isolate short
                      domestic dispatches from sea-freight outliers.

    The local_to_fba metric is designed for stores that ship from a domestic
    (destination-country) 3PL/overseas warehouse to FBA — e.g. SHIPPED status
    = local warehouse dispatch date, RECEIVING = FBA first scan.
    Using local_max_days=12 keeps only domestic transits (1–10 d) while
    excluding sea-freight records that share the same date fields (25–40 d).

    Parameters
    ----------
    sea_start_field / sea_end_field : field names for sea-transit start/end.
    ovs_start_field / ovs_end_field : field names for overseas→FBA start/end.
    local_start_field / local_end_field : field names for local-warehouse→FBA
        start/end.  When None (default), local_to_fba is not computed.
    quarter_field : field used to assign the shipment to a quarter.
    transport_field : field that identifies transport mode.
    sea_transport_values : transport values treated as sea freight.
    sea_min_days / sea_max_days : valid range filter for sea transit.
    ovs_min_days / ovs_max_days : valid range filter for overseas→FBA.
    local_min_days / local_max_days : valid range filter for local→FBA.

    Returns
    -------
    {
      "sea_transit":    {"overall": {n,min,p25,median,p75,p90,max,mean}, "by_quarter": {...}},
      "overseas_to_fba":{"overall": {...}, "by_quarter": {...}},
      "local_to_fba":   {"overall": {...}, "by_quarter": {...}},  # only when local_start_field set
      "by_quarter_summary": {"2024-Q1": {sea_transit_median, ..., local_to_fba_median, ...}},
      "skipped": N,
      "total_input": N,
    }
    """
    sea_all: list[float] = []
    ovs_all: list[float] = []
    local_all: list[float] = []
    sea_by_q: dict[str, list[float]] = defaultdict(list)
    ovs_by_q: dict[str, list[float]] = defaultdict(list)
    local_by_q: dict[str, list[float]] = defaultdict(list)
    total_by_q: dict[str, int] = defaultdict(int)
    skipped = 0

    compute_local = local_start_field is not None and local_end_field is not None

    for s in shipments:
        transport = str(s.get(transport_field) or "").upper()
        is_sea = (not transport) or any(t.upper() in transport for t in sea_transport_values)

        # Quarter bin key
        q_date = _to_date(s.get(quarter_field))
        q_key = _quarter(q_date) if q_date else None
        if q_key:
            total_by_q[q_key] += 1

        # Sea transit
        if is_sea:
            sea_days = _shipment_days(s.get(sea_start_field), s.get(sea_end_field))
            if sea_days is not None and sea_min_days <= sea_days <= sea_max_days:
                sea_all.append(sea_days)
                if q_key:
                    sea_by_q[q_key].append(sea_days)
            elif sea_days is None:
                skipped += 1

        # Overseas → FBA
        ovs_days = _shipment_days(s.get(ovs_start_field), s.get(ovs_end_field))
        if ovs_days is not None and ovs_min_days <= ovs_days <= ovs_max_days:
            ovs_all.append(ovs_days)
            if q_key:
                ovs_by_q[q_key].append(ovs_days)

        # Local warehouse → FBA (short-transit domestic dispatches only)
        if compute_local:
            loc_days = _shipment_days(s.get(local_start_field), s.get(local_end_field))
            if loc_days is not None and local_min_days <= loc_days <= local_max_days:
                local_all.append(loc_days)
                if q_key:
                    local_by_q[q_key].append(loc_days)

    # Build by_quarter_summary
    all_quarters = sorted(set(sea_by_q) | set(ovs_by_q) | set(local_by_q) | set(total_by_q))
    by_q_summary = {}
    for q in all_quarters:
        sea_vals = sea_by_q[q]
        ovs_vals = ovs_by_q[q]
        local_vals = local_by_q[q]
        entry: dict = {
            "sea_transit_median": _distribution(sea_vals).get("median"),
            "sea_transit_p25": _distribution(sea_vals).get("p25"),
            "sea_transit_p75": _distribution(sea_vals).get("p75"),
            "overseas_to_fba_median": _distribution(ovs_vals).get("median"),
            "overseas_to_fba_p25": _distribution(ovs_vals).get("p25"),
            "overseas_to_fba_p75": _distribution(ovs_vals).get("p75"),
            "sea_shipment_count": len(sea_vals),
            "total_shipment_count": total_by_q[q],
        }
        if compute_local:
            entry["local_to_fba_median"] = _distribution(local_vals).get("median")
            entry["local_to_fba_p25"] = _distribution(local_vals).get("p25")
            entry["local_to_fba_p75"] = _distribution(local_vals).get("p75")
            entry["local_shipment_count"] = len(local_vals)
        by_q_summary[q] = entry

    result: dict = {
        "sea_transit": {
            "overall": _distribution(sea_all),
            "by_quarter": {q: _distribution(sea_by_q[q]) for q in sorted(sea_by_q)},
        },
        "overseas_to_fba": {
            "overall": _distribution(ovs_all),
            "by_quarter": {q: _distribution(ovs_by_q[q]) for q in sorted(ovs_by_q)},
        },
        "by_quarter_summary": by_q_summary,
        "skipped": skipped,
        "total_input": len(shipments),
    }
    if compute_local:
        result["local_to_fba"] = {
            "overall": _distribution(local_all),
            "by_quarter": {q: _distribution(local_by_q[q]) for q in sorted(local_by_q)},
        }
    return result


# ── SP-API adapter ────────────────────────────────────────────────────────────


def adapt_sp_api_shipments(
    sp_shipments: list[dict],
    shipment_items_by_id: dict[str, list[dict]] | None = None,
) -> list[dict]:
    """
    Normalise SP-API FBA inbound shipment records to the common schema.

    SP-API only provides FBA-side dates (LastUpdatedDate ≈ receive complete when
    status==CLOSED; no sea-origin or overseas-warehouse dates).  Use this adapter
    when Lingxing data is unavailable — sea transit times will be missing.

    shipment_items_by_id: optional {shipmentId: [items]} from GetShipmentItems,
    used to extract PrepDetails or first/last PrepDate if present.
    """
    out = []
    for s in sp_shipments:
        sid = s.get("ShipmentId") or s.get("shipmentId", "")
        status = s.get("ShipmentStatus") or s.get("shipmentStatus", "")
        last_updated = s.get("LastUpdatedDate") or s.get("lastUpdatedDate")
        created = s.get("CreatedDate") or s.get("createdDate")

        fba_received_date = last_updated if status in ("CLOSED", "RECEIVING") else None

        out.append(
            {
                "shipment_id": sid,
                "shipment_name": s.get("ShipmentName") or s.get("shipmentName", ""),
                "transport_type": "SEA",  # unknown from SP-API; default sea for transit analysis
                "domestic_ship_date": created,  # creation ≈ earliest known date
                "overseas_arrival_date": None,  # not available from SP-API
                "overseas_ship_date": None,  # not available from SP-API
                "fba_received_date": fba_received_date,
                "status": status,
                "destination_fc": s.get("DestinationFulfillmentCenterId"),
            }
        )
    return out


# ── SP-API Inbound Plans adapter (2024-03-20) ─────────────────────────────────


def adapt_sp_api_plans(
    plans: list[dict],
    cn_only: bool = True,
    shipped_only: bool = True,
) -> list[dict]:
    """
    Normalise SP-API Inbound Plans (2024-03-20) records to the common schema.

    Each plan provides:
      createdAt      → domestic_ship_date  (plan creation ≈ earliest dispatch date)
      lastUpdatedAt  → fba_received_date   (status-change date; for SHIPPED plans
                       this approximates when FBA started receiving the shipment)
      sourceAddress.countryCode — used to filter sea-freight candidates (CN)

    Limitations:
      - overseas_arrival_date and overseas_ship_date are not available; the
        sea_transit metric will capture plan-creation → FBA-receive, not the
        precise factory-departure → overseas-warehouse window.
      - Plans may be created weeks before actual shipping, so durations will
        skew longer than true sea transit times.

    Parameters
    ----------
    plans        : raw list from SPAPIClient.get_inbound_plans()
    cn_only      : if True (default), keep only plans with CN source address
    shipped_only : if True (default), keep only SHIPPED plans (have FBA dates)
    """
    out = []
    for p in plans:
        status = p.get("status", "")
        src = p.get("sourceAddress") or {}
        country = src.get("countryCode", "") if isinstance(src, dict) else ""

        if shipped_only and status != "SHIPPED":
            continue
        if cn_only and country != "CN":
            continue

        created_at = p.get("createdAt")
        last_updated = p.get("lastUpdatedAt")

        out.append(
            {
                "shipment_id": p.get("inboundPlanId", ""),
                "shipment_name": p.get("name", ""),
                "transport_type": "SEA",  # CN-source plans assumed sea freight
                "domestic_ship_date": created_at,  # proxy: plan creation date
                "overseas_arrival_date": None,
                "overseas_ship_date": None,
                "fba_received_date": last_updated,  # proxy: last status change
                "status": status,
                "source_country": country,
            }
        )
    return out


# ── Lingxing adapter ──────────────────────────────────────────────────────────

# ship_mode integer encoding used by showShipment_v2
_LINGXING_SHIP_MODE: dict[int, str] = {
    1: "SEA",
    2: "AIR",
    3: "EXPRESS",
}


def adapt_lingxing_shipments(raw: list[dict]) -> list[dict]:
    """
    Normalise Lingxing FBA shipment tracking records (showShipment_v2) to the
    common schema.

    The API returns stage dates inside a `date_info` array:
      [{"status_name": "WORKING",   "status_time": "YYYY-MM-DD HH:MM"},
       {"status_name": "SHIPPED",   "status_time": "..."},   # often empty
       {"status_name": "RECEIVING", "status_time": "..."},
       {"status_name": "CLOSED",    "status_time": "..."}]

    Mapping to the common schema:
      domestic_ship_date    → SHIPPED status_time (left origin/overseas warehouse)
      overseas_arrival_date → RECEIVING status_time (FBA first scan; proxy)
      overseas_ship_date    → None (not tracked in this endpoint)
      fba_received_date     → CLOSED status_time (FBA receive complete)

    Also handles legacy flat-field formats for backward compatibility.

    Transport type: decoded from ship_mode integer (1=SEA, 2=AIR, 3=EXPRESS)
    or read from flat transport_type / transportType fields.

    SKU is extracted from item_list[0].msku when not present at the top level.
    """

    def _field(d, *keys):
        return next((d[k] for k in keys if d.get(k)), None)

    def _date_info(r: dict) -> dict[str, str | None]:
        di = r.get("date_info")
        if not isinstance(di, list):
            return {}
        return {
            entry["status_name"]: (entry.get("status_time") or None)
            for entry in di
            if isinstance(entry, dict) and entry.get("status_name")
        }

    def _first_item(r: dict, *keys):
        for item in (r.get("item_list") or [])[:1]:
            for k in keys:
                v = item.get(k)
                if v:
                    return v
        return None

    out = []
    for r in raw:
        di = _date_info(r)

        domestic_ship = _field(r, "ship_date", "shipDate", "domestic_ship_date") or di.get(
            "SHIPPED"
        )
        overseas_arrive = _field(r, "overseas_arrive_date", "overseasArriveDate") or di.get(
            "RECEIVING"
        )
        overseas_ship = _field(r, "overseas_ship_date", "overseasShipDate")
        fba_received = _field(r, "fba_receive_date", "receiveDate", "fbaReceiveDate") or di.get(
            "CLOSED"
        )

        ship_mode_raw = r.get("ship_mode")
        transport_type = _field(
            r, "transport_type", "transportType", "logistics_type"
        ) or _LINGXING_SHIP_MODE.get(ship_mode_raw)

        sku = _field(r, "msku", "sku", "seller_sku") or _first_item(r, "msku", "sku", "seller_sku")
        quantity = _field(r, "quantity", "qty", "ship_qty") or _first_item(
            r, "quantity_shipped", "quantity", "qty"
        )

        out.append(
            {
                "shipment_id": _field(r, "shipment_id", "amazon_shipment_id", "shipmentId"),
                "shipment_name": _field(r, "shipment_name", "shipmentName", "name"),
                "sku": sku,
                "quantity": quantity,
                "transport_type": transport_type,
                "domestic_ship_date": domestic_ship,
                "overseas_arrival_date": overseas_arrive,
                "overseas_ship_date": overseas_ship,
                "fba_received_date": fba_received,
                "status": _field(r, "shipment_status", "status", "shipmentStatus"),
            }
        )
    return out
