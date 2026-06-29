from __future__ import annotations

import asyncio
import gzip
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from src.core.errors import (
    ErrorCode,
    ExtractorError,
    FatalError,
    RetryableError,
    ScraperError,
    classify_http,
    is_retryable,
)
from src.core.utils.decorators import exponential_backoff

from .auth import SPAPIAuth

logger = logging.getLogger(__name__)


class SPAPIClient:
    """
    Amazon Selling Partner API client.

    Covers two domains needed for ad diagnostics:
      - FBA Inventory (available, reserved, inbound quantities)
      - Catalog (basic item metadata fallback)
    """

    def __init__(self, store_id: str | None = None):
        self.auth = SPAPIAuth(store_id)

    # ── internal ───────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {
            "x-amz-access-token": self.auth.get_access_token(),
            "x-amz-date": _utc_now(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _post(self, path: str, body: dict) -> dict[str, Any]:
        url = f"{self.auth.endpoint}{path}"
        try:
            resp = requests.post(url, headers=self._headers(), json=body, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            logger.error(
                f"SP-API POST {path} → HTTP {status}: "
                f"{e.response.text[:400] if e.response is not None else e}"
            )
            raise ScraperError(
                f"SP-API POST {path} failed: HTTP {status}",
                code=classify_http(status or 0, "sp_api"),
            ) from e
        except Exception as e:
            logger.error(f"SP-API POST {path} failed: {e}")
            raise ScraperError(f"SP-API POST {path} failed: {e}") from e

    def _get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        url = f"{self.auth.endpoint}{path}"
        try:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            logger.error(
                f"SP-API GET {path} → HTTP {status}: "
                f"{e.response.text[:400] if e.response is not None else e}"
            )
            raise ScraperError(
                f"SP-API GET {path} failed: HTTP {status}",
                code=classify_http(status or 0, "sp_api"),
            ) from e
        except Exception as e:
            logger.error(f"SP-API GET {path} failed: {e}")
            raise ScraperError(f"SP-API GET {path} failed: {e}") from e

    # ── Inventory ──────────────────────────────────────────────────────────

    @exponential_backoff(max_retries=5, base_delay=1.0)
    async def _fetch_inventory_page(self, params: dict) -> dict:
        """Internal helper to fetch a single page of inventory with retries."""
        url = f"{self.auth.endpoint}/fba/inventory/v1/summaries"
        resp = await asyncio.to_thread(
            requests.get, url, headers=self._headers(), params=params, timeout=30
        )
        code = classify_http(resp.status_code)
        if not resp.ok and is_retryable(code):
            raise RetryableError(resp.text[:200], code=code)
        resp.raise_for_status()
        return resp.json()

    async def get_inventory(
        self,
        seller_skus: list[str] | None = None,
        include_details: bool = True,
    ) -> list[dict[str, Any]]:
        """
        FBA Inventory Summaries (v1).
        Usage Plan: Rate 2, Burst 2.

        Returns per-SKU dict with:
          sku, asin, fn_sku, condition, total_quantity, available_quantity,
          reserved_quantity, inbound_quantity, last_updated
        """
        sem = asyncio.Semaphore(2)

        async def _fetch_all_pages(base_params: dict) -> list[dict]:
            all_items = []
            params = dict(base_params)
            while True:
                async with sem:
                    data = await self._fetch_inventory_page(params)

                payload = data.get("payload", {})
                summaries = payload.get("inventorySummaries", [])
                all_items.extend([_parse_inventory_summary(s) for s in summaries])

                next_token = data.get("pagination", {}).get("nextToken")
                if not next_token:
                    break
                logger.info(
                    f"Fetching next page of SP-API inventory with token: {next_token[:20]}..."
                )
                params["nextToken"] = next_token
            return all_items

        base_params: dict[str, Any] = {
            "details": str(include_details).lower(),
            "granularityType": "Marketplace",
            "granularityId": self.auth.marketplace_id,
            "marketplaceIds": self.auth.marketplace_id,
        }

        if not seller_skus:
            return await _fetch_all_pages(base_params)

        # Amazon SP-API allows up to 50 SKUs per request.
        sku_list = list(seller_skus)
        batches = [sku_list[i : i + 50] for i in range(0, len(sku_list), 50)]

        results = await asyncio.gather(
            *[_fetch_all_pages({**base_params, "sellerSkus": ",".join(batch)}) for batch in batches]
        )

        return [item for batch_result in results for item in batch_result]

    # ── Sales & Traffic Report ─────────────────────────────────────────────

    _REPORT_POLL_INTERVAL = 15  # seconds between status polls
    _REPORT_POLL_MAX = 120  # max polls → 30 min ceiling

    async def get_sales_and_traffic(
        self,
        asin: str | None = None,
        days: int = 30,
        start_date: str | None = None,
        end_date: str | None = None,
        granularity: str = "DAY",
    ) -> list[dict[str, Any]]:
        """
        Request GET_SALES_AND_TRAFFIC_REPORT, poll until complete, download and
        parse the result.  Returns a list of daily (or summary) traffic+sales
        records filtered to `asin` when provided.

        Args:
            asin:        Optional ASIN to filter results.
            days:        Lookback window in days (ignored when start_date is given).
            start_date:  ISO-8601 date string "YYYY-MM-DD" (inclusive).
            end_date:    ISO-8601 date string "YYYY-MM-DD" (inclusive, defaults to yesterday).
            granularity: "DAY" (default) or "MONTH" — maps to reportOptions byAsin.

        Returns:
            List of dicts with keys:
              date, asin, sessions, session_percentage, page_views,
              page_views_percentage, buy_box_percentage, units_ordered,
              units_ordered_b2b, ordered_product_sales, ordered_product_sales_b2b,
              total_order_items, total_order_items_b2b
        """
        today = datetime.now(UTC).date()
        end_dt = end_date or str(today - timedelta(days=1))
        start_dt = start_date or str(today - timedelta(days=days))

        body: dict[str, Any] = {
            "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
            "dataStartTime": f"{start_dt}T00:00:00.000Z",
            "dataEndTime": f"{end_dt}T23:59:59.000Z",
            "marketplaceIds": [self.auth.marketplace_id],
            "reportOptions": {
                "dateGranularity": granularity,
                "asinGranularity": "CHILD" if asin else "PARENT",
            },
        }

        data = await asyncio.to_thread(self._post, "/reports/2021-06-30/reports", body)
        report_id = data.get("reportId")
        if not report_id:
            raise ExtractorError(f"No reportId in createReport response: {data}")
        logger.info(
            f"Created SP-API report {report_id} (GET_SALES_AND_TRAFFIC_REPORT {start_dt}→{end_dt})"
        )

        document_id = await self._poll_sp_report(report_id)
        raw_records = await self._download_sp_report(document_id)

        return _parse_sales_traffic_records(raw_records, asin)

    async def _poll_sp_report(self, report_id: str) -> str:
        """Poll until DONE, return reportDocumentId."""
        path = f"/reports/2021-06-30/reports/{report_id}"
        for attempt in range(self._REPORT_POLL_MAX):
            data = await asyncio.to_thread(self._get, path)
            status = data.get("processingStatus")
            logger.debug(f"SP report {report_id} status: {status} (attempt {attempt + 1})")
            if status == "DONE":
                doc_id = data.get("reportDocumentId")
                if not doc_id:
                    raise ExtractorError(f"SP report DONE but no reportDocumentId: {data}")
                return doc_id
            if status in ("CANCELLED", "FATAL"):
                raise FatalError(f"SP report {report_id} ended with status {status}")
            await asyncio.sleep(self._REPORT_POLL_INTERVAL)
        raise RetryableError(
            f"SP report {report_id} did not complete after {self._REPORT_POLL_MAX} polls.",
            code=ErrorCode.TIMEOUT,
        )

    async def _download_sp_report(self, document_id: str) -> list[dict]:
        """Fetch document metadata, download the file, decompress if needed."""
        meta = await asyncio.to_thread(self._get, f"/reports/2021-06-30/documents/{document_id}")
        url = meta.get("url")
        compression = meta.get("compressionAlgorithm", "")
        if not url:
            raise ExtractorError(f"No URL in SP report document metadata: {meta}")

        resp = await asyncio.to_thread(requests.get, url, timeout=120)
        resp.raise_for_status()

        raw = resp.content
        if compression.upper() == "GZIP":
            raw = gzip.decompress(raw)

        return json.loads(raw.decode("utf-8"))

    # ── Order Metrics ─────────────────────────────────────────────────────

    async def get_order_metrics(
        self,
        asin: str,
        start_date: str,
        end_date: str,
        granularity: str = "Total",
        granularity_timezone: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Sales Order Metrics API v1 — getOrderMetrics.

        Returns unit/order counts for a single ASIN over [start_date, end_date].
        Each element contains: unitCount, orderItemCount, orderCount,
        averageUnitPrice, totalSales.

        granularity: Hour | Day | Week | Month | Year | Total
          - "Total" aggregates the entire interval into one record (default).
          - "Day" / "Week" etc. require granularity_timezone so day boundaries
            align with the store's local calendar (e.g. "America/Los_Angeles").

        Interval format: two ISO-8601 datetimes separated by "--" (double hyphen).
        The end boundary is EXCLUSIVE — we add 1 day so end_date is fully included.
        """
        from datetime import date as _date
        from datetime import timedelta as _td

        _end_excl = (_date.fromisoformat(end_date) + _td(days=1)).isoformat()
        interval = f"{start_date}T00:00:00Z--{_end_excl}T00:00:00Z"
        params: dict[str, Any] = {
            "marketplaceIds": self.auth.marketplace_id,
            "interval": interval,
            "granularity": granularity,
            "asin": asin,
        }
        if granularity_timezone:
            params["granularityTimeZone"] = granularity_timezone
        data = await asyncio.to_thread(self._get, "/sales/v1/orderMetrics", params)
        return data.get("payload", [])

    # ── FBA Inbound Shipments ──────────────────────────────────────────────

    async def get_inbound_shipments(
        self,
        shipment_status_list: list[str] | None = None,
        last_updated_after: str | None = None,
        last_updated_before: str | None = None,
        query_type: str = "DATE_RANGE",
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        """
        FBA Inbound Shipments API v0 — list historical shipments.

        Rate limit: 2 req/s, burst 30.  Paginated via NextToken.

        Parameters
        ----------
        shipment_status_list : filter by status, e.g. ["CLOSED", "RECEIVING"].
                               Defaults to ["CLOSED"] when query_type="DATE_RANGE".
        last_updated_after   : ISO-8601 UTC string, e.g. "2023-01-01T00:00:00Z".
        last_updated_before  : ISO-8601 UTC string.
        query_type           : "DATE_RANGE" (requires last_updated_after) or
                               "SHIPMENT" (requires ShipmentIdList).
        max_pages            : safety cap on pagination (default 20).

        Returns
        -------
        List of raw shipment dicts from the API payload.
        Each dict includes: ShipmentId, ShipmentName, ShipmentStatus,
        DestinationFulfillmentCenterId, LastUpdatedDate, CreatedDate,
        ShipFromAddress, LabelPrepType, AreCasesRequired, ConfirmedNeedByDate.
        """
        statuses = shipment_status_list or ["CLOSED"]
        # DATE_RANGE requires both bounds; default LastUpdatedBefore to now.
        if not last_updated_before:
            last_updated_before = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        params: dict[str, Any] = {
            "MarketplaceId": self.auth.marketplace_id,
            "QueryType": query_type,
            "ShipmentStatusList": ",".join(statuses),
            "LastUpdatedAfter": last_updated_after or "2020-01-01T00:00:00Z",
            "LastUpdatedBefore": last_updated_before,
        }

        loop = asyncio.get_event_loop()
        all_shipments: list[dict] = []
        page = 0
        while page < max_pages:
            data = await loop.run_in_executor(
                None, lambda p=params: self._get("/fba/inbound/v0/shipments", p)
            )
            payload = data.get("payload", {})
            shipments = payload.get("ShipmentData", [])
            all_shipments.extend(shipments)
            next_token = payload.get("NextToken")
            if not next_token or not shipments:
                break
            params = {
                "MarketplaceId": self.auth.marketplace_id,
                "QueryType": "NEXT_TOKEN",
                "NextToken": next_token,
            }
            page += 1

        logger.info(
            f"get_inbound_shipments: fetched {len(all_shipments)} shipments "
            f"(status={statuses}, pages={page + 1})"
        )
        return all_shipments

    async def get_inbound_plans(
        self,
        status: str | None = None,
        max_pages: int = 50,
    ) -> list[dict[str, Any]]:
        """
        FBA Inbound Plans API 2024-03-20 — list inbound plans with date fields.

        Unlike v0 shipments, each plan record includes:
          createdAt       — plan creation date (ISO-8601 UTC)
          lastUpdatedAt   — last status change date; when status=SHIPPED this
                            approximates the overseas warehouse departure date
          sourceAddress   — countryCode identifies sea (CN) vs domestic shipments
          status          — ACTIVE | SHIPPED | VOIDED

        Rate limit: 2 req/s.  Paginated via paginationToken.

        Parameters
        ----------
        status    : filter by plan status ("ACTIVE", "SHIPPED", or "VOIDED").
                    Pass None to fetch all statuses across separate calls.
        max_pages : pagination safety cap.

        Returns
        -------
        List of inbound plan dicts.
        """
        loop = asyncio.get_event_loop()

        async def _fetch_status(s: str | None) -> list[dict]:
            params: dict[str, Any] = {"pageSize": 30}  # API max is 30
            if s:
                params["status"] = s
            results = []
            pages = 0
            while pages < max_pages:
                data = await loop.run_in_executor(
                    None, lambda p=params: self._get("/inbound/fba/2024-03-20/inboundPlans", p)
                )
                results.extend(data.get("inboundPlans", []))
                token = (
                    data.get("pagination", {}).get("nextToken")
                    if isinstance(data.get("pagination"), dict)
                    else data.get("paginationToken")
                )
                if not token:
                    break
                params = {"pageSize": 30, "paginationToken": token}
                if s:
                    params["status"] = s
                pages += 1
            return results

        if status:
            plans = await _fetch_status(status)
        else:
            # Fetch all three status groups
            results = await asyncio.gather(
                _fetch_status("SHIPPED"),
                _fetch_status("ACTIVE"),
                _fetch_status("VOIDED"),
            )
            plans = [p for group in results for p in group]

        logger.info(f"get_inbound_plans: fetched {len(plans)} plans (status={status})")
        return plans

    # ── Catalog ────────────────────────────────────────────────────────────

    async def get_catalog_item(self, asin: str) -> dict[str, Any]:
        """
        Catalog Items API 2022-04-01.

        Returns: asin, title, brand, product_type, color, size, bullet_point_count
        """
        params = {
            "marketplaceIds": self.auth.marketplace_id,
            "includedData": "attributes,summaries,identifiers",
        }
        data = await asyncio.to_thread(self._get, f"/catalog/2022-04-01/items/{asin}", params)
        return _parse_catalog_item(asin, data)


# ── parsers ────────────────────────────────────────────────────────────────


def _utc_now() -> str:
    from datetime import datetime

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _parse_inventory_summary(s: dict) -> dict[str, Any]:
    inv_details = s.get("inventoryDetails", {})
    fulfillable = inv_details.get("fulfillableQuantity", 0)
    reserved = inv_details.get("reservedQuantity", {})
    inbound_receiving = inv_details.get("inboundReceivingQuantity", 0)
    inbound_shipped = inv_details.get("inboundShippedQuantity", 0)
    inbound_working = inv_details.get("inboundWorkingQuantity", 0)
    return {
        "sku": s.get("sellerSku"),
        "asin": s.get("asin"),
        "fn_sku": s.get("fnSku"),
        "condition": s.get("condition"),
        "total_quantity": s.get("totalQuantity", 0),
        "available_quantity": fulfillable,
        "total_available": fulfillable,
        "reserved_quantity": reserved.get("totalReservedQuantity", 0)
        if isinstance(reserved, dict)
        else reserved,
        "fc_transfer": reserved.get("pendingTransshipmentQuantity", 0)
        if isinstance(reserved, dict)
        else 0,
        # Split inbound tiers — reliability differs significantly:
        #   receiving  : already at FC, available in 1-2 days (certain)
        #   shipped    : in transit from seller, ETA 10-30 days (certain but timing varies)
        #   working    : shipment plan only, not yet handed to carrier (uncertain ETA)
        "inbound_receiving": inbound_receiving,
        "inbound_shipped": inbound_shipped,
        "inbound_working": inbound_working,
        # inbound_quantity = confirmed in-transit only (receiving + shipped); working excluded
        "inbound_quantity": inbound_receiving + inbound_shipped,
        "last_updated": s.get("lastUpdatedTime"),
    }


def _parse_catalog_item(asin: str, data: dict) -> dict[str, Any]:
    summaries = data.get("summaries", [{}])
    summary = summaries[0] if summaries else {}
    attributes = data.get("attributes", {})
    return {
        "asin": asin,
        "title": summary.get("itemName"),
        "brand": summary.get("brand"),
        "product_type": summary.get("productType"),
        "color": _first_attr(attributes, "color"),
        "size": _first_attr(attributes, "size"),
        "bullet_point_count": len(attributes.get("bullet_point", [])),
    }


def _first_attr(attributes: dict, key: str) -> str | None:
    vals = attributes.get(key, [])
    if vals and isinstance(vals, list):
        return vals[0].get("value")
    return None


def _parse_sales_traffic_records(
    data: Any,
    asin_filter: str | None,
) -> list[dict[str, Any]]:
    """
    Parse GET_SALES_AND_TRAFFIC_REPORT JSON payload.

    The report returns two arrays simultaneously:
      salesAndTrafficByAsin  — one entry per ASIN (totals over the period)
        keys: parentAsin, childAsin, salesByAsin, trafficByAsin
      salesAndTrafficByDate  — one entry per date (account-level daily totals)
        keys: date, salesByDate, trafficByDate

    We parse salesAndTrafficByAsin for per-ASIN metrics and join in the
    date dimension from salesAndTrafficByDate when available.
    """
    if not isinstance(data, dict):
        return []

    filter_upper = asin_filter.upper() if asin_filter else None

    def _money(val: Any) -> float | None:
        if val is None:
            return None
        if isinstance(val, dict):
            return val.get("amount")
        return float(val)

    results = []

    # ── ASIN-level records (summary over period) ──────────────────────────
    for entry in data.get("salesAndTrafficByAsin", []):
        parent = (entry.get("parentAsin") or "").upper()
        child = (entry.get("childAsin") or "").upper()
        if filter_upper and filter_upper not in (parent, child):
            continue

        traffic = entry.get("trafficByAsin") or {}
        sales = entry.get("salesByAsin") or {}
        results.append(
            {
                "date": None,  # period summary, no single date
                "asin": entry.get("childAsin") or entry.get("parentAsin"),
                "parent_asin": entry.get("parentAsin"),
                "sessions": traffic.get("sessions"),
                "session_percentage": traffic.get("sessionPercentage"),
                "page_views": traffic.get("pageViews"),
                "page_views_percentage": traffic.get("pageViewsPercentage"),
                "buy_box_percentage": traffic.get("buyBoxPercentage"),
                "units_ordered": sales.get("unitsOrdered"),
                "units_ordered_b2b": sales.get("unitsOrderedB2B"),
                "ordered_product_sales": _money(sales.get("orderedProductSales")),
                "ordered_product_sales_b2b": _money(sales.get("orderedProductSalesB2B")),
                "total_order_items": sales.get("totalOrderItems"),
                "total_order_items_b2b": sales.get("totalOrderItemsB2B"),
            }
        )

    # ── Date-level records (account totals per day, no per-ASIN split) ───
    # Only include if no ASIN filter is active (these are account-wide rows)
    if not filter_upper:
        for entry in data.get("salesAndTrafficByDate", []):
            traffic = entry.get("trafficByDate") or {}
            sales = entry.get("salesByDate") or {}
            results.append(
                {
                    "date": entry.get("date"),
                    "asin": None,
                    "parent_asin": None,
                    "sessions": traffic.get("sessions"),
                    "session_percentage": traffic.get("sessionPercentage"),
                    "page_views": traffic.get("pageViews"),
                    "page_views_percentage": traffic.get("pageViewsPercentage"),
                    "buy_box_percentage": traffic.get("buyBoxPercentage"),
                    "units_ordered": sales.get("unitsOrdered"),
                    "units_ordered_b2b": sales.get("unitsOrderedB2B"),
                    "ordered_product_sales": _money(sales.get("orderedProductSales")),
                    "ordered_product_sales_b2b": _money(sales.get("orderedProductSalesB2B")),
                    "total_order_items": sales.get("totalOrderItems"),
                    "total_order_items_b2b": sales.get("totalOrderItemsB2B"),
                }
            )

    return results
