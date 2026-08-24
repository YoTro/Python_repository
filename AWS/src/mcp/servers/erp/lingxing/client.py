from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from curl_cffi import requests

from src.core.errors import (
    ErrorCode,
    ScraperError,
    classify_api_code,
    classify_response_message,
    is_auth_error,
)

from ..base import ERPClient
from .auth import LingxingAuth

logger = logging.getLogger(__name__)


def _is_token_expired(data: dict) -> bool:
    """True when the Lingxing response body signals an expired/missing token."""
    api_code = classify_api_code(data.get("code", ""), "lingxing")
    if is_auth_error(api_code):
        return True
    msg_code = classify_response_message(str(data.get("msg", "")), "lingxing")
    return is_auth_error(msg_code)


class LingxingClient(ERPClient):
    """
    Lingxing ERP client.
    Implements ERPClient against the Lingxing gateway API.
    """

    BASE_URL = "https://gw.lingxingerp.com"
    ERP_DIRECT_URL = "https://erp.lingxing.com"  # for web-app endpoints not on the gateway

    def __init__(self, account: str = None, password: str = None):
        self.auth = LingxingAuth()
        self.session = requests.Session(impersonate="chrome")
        self.token = self.auth.load_token()
        if not self.token:
            self.token = self.auth.login(account, password)
        if not self.token:
            logger.warning("LingxingClient: no valid token. Call auth.login() to authenticate.")

        # Ad API identity headers: env vars take priority; fall back to saved token meta.
        _meta = self.auth.load_meta()
        self._company_id = os.getenv("LINGXING_COMPANY_ID") or _meta.get("company_id", "")
        self._env_key = os.getenv("LINGXING_ENV_KEY") or _meta.get("env_key", "")
        self._uid = os.getenv("LINGXING_UID") or _meta.get("uid", "")
        self._zid = os.getenv("LINGXING_ZID") or _meta.get("zid", "")
        # sids: optional store filter ("4443,4441"). Empty = all stores (API default).
        self._sids = os.getenv("LINGXING_SIDS") or _meta.get("sids", "")

    # ── Internal request helper ───────────────────────────────────────────────

    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.BASE_URL}{path}"
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "ak-origin": "https://erp.lingxing.com",
            "x-ak-request-source": "erp",
            "x-ak-version": "AKVERSIONNUM",
            "x-ak-request-id": str(uuid.uuid4()),
            "auth-token": self.token or "",
        }
        headers.update(kwargs.pop("headers", {}))
        try:
            resp = self.session.request(method, url, headers=headers, **kwargs)
            data = resp.json()
            if _is_token_expired(data):
                logger.warning("Token expired, re-authenticating...")
                self.token = self.auth.login()
                if self.token:
                    headers["auth-token"] = self.token
                    resp = self.session.request(method, url, headers=headers, **kwargs)
                    data = resp.json()
            return data
        except Exception as e:
            logger.error(f"Lingxing API request failed [{method} {path}]: {e}")
            raise ScraperError(
                f"Lingxing API request failed [{method} {path}]: {e}",
                code=ErrorCode.SERVER_ERROR,
            ) from e

    # ── ERP direct request helper (erp.lingxing.com web-app endpoints) ─────────

    def _erp_request(self, path: str, payload: dict) -> dict:
        """POST to erp.lingxing.com (web-app endpoints not exposed on the gateway)."""
        erp_headers = {
            "AK-Client-Type": "web",
            "AK-Origin": "https://erp.lingxing.com",
            "Origin": "https://erp.lingxing.com",
            "Referer": "https://erp.lingxing.com/",
            "x-ak-request-source": "erp",
            "x-ak-platform": "1",
            "x-ak-language": "zh",
            "x-ak-version": "3.8.4.2.0.007",
            "x-ak-company-id": self._company_id,
            "x-ak-env-key": self._env_key,
            "x-ak-uid": self._uid,
            "x-ak-zid": self._zid,
        }
        url = f"{self.ERP_DIRECT_URL}{path}"
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "auth-token": self.token or "",
        }
        headers.update(erp_headers)
        try:
            resp = self.session.request("POST", url, headers=headers, json=payload)
            data = resp.json()
            if _is_token_expired(data):
                logger.warning("Token expired, re-authenticating...")
                self.token = self.auth.login()
                if self.token:
                    headers["auth-token"] = self.token
                    resp = self.session.request("POST", url, headers=headers, json=payload)
                    data = resp.json()
            return data
        except Exception as e:
            logger.error(f"Lingxing ERP direct request failed [POST {path}]: {e}")
            raise ScraperError(
                f"Lingxing ERP direct request failed [POST {path}]: {e}",
                code=ErrorCode.SERVER_ERROR,
            ) from e

    # ── Ad API request helper (ads.lingxing.com origin) ──────────────────────

    def _ad_request(self, path: str, payload: dict) -> dict:
        """POST to the Lingxing ad-report gateway (/pb-newad-web/...)."""
        ad_headers = {
            "ak-origin": "https://ads.lingxing.com",
            "Origin": "https://ads.lingxing.com",
            "Referer": "https://ads.lingxing.com/",
            "x-ak-request-source": "erp",
            "x-ak-version": "1.1.4.0.000",
            "x-ak-company-id": self._company_id,
            "x-ak-env-key": self._env_key,
            "x-ak-uid": self._uid,
            "x-ak-zid": self._zid,
        }
        return self._request("POST", path, json=payload, headers=ad_headers)

    # ── ERPClient interface ───────────────────────────────────────────────────

    def get_inventory(self, sku: str) -> dict[str, Any]:
        # TODO: map to real Lingxing inventory endpoint
        data = self._request("POST", "/newadmin/api/inventory/list", json={"msku": sku})
        return data

    def get_purchase_orders(
        self,
        sku: str | None = None,
        status: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        # TODO: map to real Lingxing purchase order endpoint
        payload = {}
        if sku:
            payload["msku"] = sku
        if status:
            payload["status"] = status
        data = self._request("POST", "/newadmin/api/purchase/list", json=payload)
        return data.get("data", [])

    def get_sales_orders(
        self,
        sku: str | None = None,
        days: int = 30,
        **kwargs,
    ) -> list[dict[str, Any]]:
        # TODO: map to real Lingxing sales order endpoint
        payload = {"days": days}
        if sku:
            payload["msku"] = sku
        data = self._request("POST", "/newadmin/api/orders/list", json=payload)
        return data.get("data", [])

    # ── FBA shipment tracking ─────────────────────────────────────────────────

    def get_fba_shipment_tracking(
        self,
        sku: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        transport_type: str | None = None,
        shipment_status: list[str] | None = None,
        sids: str | None = None,
        search_field_time: str = "create_date",
        search_field: str = "shipment_id",
        search_value: str | None = None,
        length: int = 100,
        fetch_all: bool = True,
        **kwargs,
    ) -> list[dict]:
        """
        Query Lingxing FBA shipment tracking records.

        Endpoint: POST /api/fba_shipment/showShipment_v2 (erp.lingxing.com)

        Parameters
        ----------
        sku              : filter by seller MSKU (optional).
        start_date       : range start date, "YYYY-MM-DD".
        end_date         : range end date, "YYYY-MM-DD".
        transport_type   : shipping mode integer or label — mapped to ship_mode field.
        shipment_status  : list of status strings (default: all statuses).
        sids             : comma-separated store IDs (defaults to LINGXING_SIDS env var).
        search_field_time: date field for start/end range — "create_date" or "ship_date".
        search_field     : text-search dimension. Valid values:
                             "shipment_id"                    (default)
                             "destination_fulfillment_center_id"
                             "product_sku"
                             "fnsku"
                             "asin"
                             "parent_asin"
                             "product_name"
        search_value     : text value to match against search_field (optional).
        fetch_all        : auto-paginate to collect all records (default True).
        """
        # sids: omit entirely when empty → API returns all stores for this account.
        # Pass when filtering to specific stores: "4443,4441"
        _sids = sids or os.getenv("LINGXING_SIDS") or self._sids or ""
        _seq_counter = [0]

        def _build_payload(offset: int) -> dict:
            _seq_counter[0] += 1
            payload: dict[str, Any] = {
                "search_field_time": search_field_time,
                "is_sta": "",
                "is_awd": "",
                "ship_mode": transport_type or "",
                "step": [],
                "is_closed": "",
                "application_diff": "",
                "received_diff": "",
                "application_received_diff": "",
                "is_relate_packing_task_sn": "",
                "is_add_tracking": "",
                "delivery_order_status": [],
                "box_type": "",
                "is_uploaded_box": "",
                "sta_transportation_mode": "",
                "delivery_mode": "",
                "carrier_type": "",
                "create_uids": [],
                "principal_uids": [],
                "is_store_diff": "",
                "search_field": search_field,
                "shipment_status": shipment_status or [],
                "is_relate_shipment": "",
                "seniorSearchList": [],
                "shipment_type": [],
                "offset": offset,
                "length": length,
                "req_time_sequence": f"/api/fba_shipment/showShipment_v2$${_seq_counter[0]}",
            }
            if _sids:
                payload["sids"] = _sids
            if sku:
                payload["msku"] = sku
            if start_date:
                payload["start_date"] = start_date
            if end_date:
                payload["end_date"] = end_date
            if search_value:
                payload["search_value"] = search_value
            return payload

        path = "/api/fba_shipment/showShipment_v2"
        resp = self._erp_request(path, _build_payload(0))
        if not resp:
            return []

        # Response structure: {"code": 0, "data": {"list": [...], "total": N}}
        # or {"code": 200, "data": [...], "total": N}
        raw_data = resp.get("data", {})
        if isinstance(raw_data, dict):
            records = raw_data.get("list", raw_data.get("data", []))
            total = raw_data.get("total", 0)
        elif isinstance(raw_data, list):
            records = raw_data
            total = resp.get("total", len(records))
        else:
            records = []
            total = 0

        if not isinstance(records, list):
            records = []

        if not fetch_all:
            logger.info(
                f"get_fba_shipment_tracking: {len(records)}/{total} records "
                f"(sku={sku}, {start_date}→{end_date})"
            )
            return records

        fetched = len(records)
        while fetched < total:
            next_resp = self._erp_request(path, _build_payload(fetched))
            raw_next = next_resp.get("data", {})
            if isinstance(raw_next, dict):
                page_data = raw_next.get("list", raw_next.get("data", []))
            elif isinstance(raw_next, list):
                page_data = raw_next
            else:
                page_data = []
            if not page_data:
                break
            records.extend(page_data)
            fetched += len(page_data)

        logger.info(
            f"get_fba_shipment_tracking: fetched {len(records)}/{total} records "
            f"(sku={sku}, {start_date}→{end_date})"
        )
        return records

    # ── Product management search ─────────────────────────────────────────────

    def search_product(
        self,
        search_value: str,
        search_field: str = "sku",
        search_field_time: str = "create_time",
        sort_field: str = "create_time",
        sort_type: str = "desc",
        status: list[str] | None = None,
        product_type: list[int] | None = None,
        length: int = 20,
        fetch_all: bool = True,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Search local products in Lingxing Product Management.

        Endpoint: POST /api/product/lists (erp.lingxing.com)

        Parameters
        ----------
        search_value     : text value to match against ``search_field``.
        search_field     : dimension to search. Valid values:
                             "sku"                            (default)
                             "sku_identifier"
                             "msku"
                             "product_name"
                             "model"
                             "description"
                             "customs_clearance_internal_code"
        search_field_time: date field for time-based filtering (default "create_time").
        sort_field       : field to sort by (default "create_time").
        sort_type        : "desc" (default) or "asc".
        status           : list of product status filters (default: all).
        product_type     : list of product type ids (default [1, 2]).
        length           : rows per page (default 20).
        fetch_all        : auto-paginate to collect all records (default True).

        Returns
        -------
        List of product record dicts. Each record contains the full product
        detail from Product Management, including (non-exhaustive):

            id                        : Lingxing internal product id
            sku                       : local SKU
            sku_identifier            : SKU identifier
            product_name              : product name
            model                     : model / spec
            spu, spu_name             : SPU code and name
            brand_name                : brand
            category_name             : category
            pic_url                   : main image URL
            status, status_text       : status code + label (e.g. 1 / "在售")
            open_status               : listing-open status
            product_type              : product type id
            is_combo                  : 1 = bundle/combo product
            unit                      : product unit (e.g. "kg")
            cg_price                  : purchase/cost price
            cg_delivery               : purchase lead time (days)
            cg_transport_costs        : transport cost
            cg_product_gross_weight   : gross weight (+ *_unit fields)
            cg_package_length/width/height, cg_box_length/width/height, cg_box_pcs,
            cg_box_weight             : packaging & carton dimensions/weight
            supplier_name, primary_supplier_id : supplier info
            quote_step_prices         : tiered quote prices
            product_creator_realname, product_developer, cg_opt_username : owner names
            create_time, update_time  : timestamps ("YYYY-MM-DD HH:MM")
            custom_fields             : dict of custom attributes {id: {name, val, val_text, ...}}
            is_matched_listing, is_matched_alibaba(_text) : matching flags
            sonProducts, comboProducts, global_tags, logistics, clearance_price : related data

        Returns an empty list when nothing matches.

        Note: the API signals success with ``code == 1`` (not 0) and returns
        ``list``/``total`` at the top level of the response body.
        """
        _seq_counter = [0]

        def _extract(resp: dict) -> tuple[list, int]:
            """Pull (records, total) from a /api/product/lists response.

            The endpoint returns ``list``/``total`` at the top level, but fall
            back to a nested ``data`` container for robustness.
            """
            records = resp.get("list")
            total = resp.get("total")
            if records is None:
                raw = resp.get("data", {})
                if isinstance(raw, dict):
                    records = raw.get("list", raw.get("data", []))
                    if total is None:
                        total = raw.get("total", 0)
                elif isinstance(raw, list):
                    records = raw
            if not isinstance(records, list):
                records = []
            if not isinstance(total, int):
                total = len(records)
            return records, total

        def _build_payload(offset: int) -> dict:
            _seq_counter[0] += 1
            return {
                "search_field_time": search_field_time,
                "product_creator_uid": [],
                "product_developer_uid": [],
                "permission_uid": [],
                "cg_opt_uid": [],
                "supplier_id": [],
                "sort_field": sort_field,
                "sort_type": sort_type,
                "search_field": search_field,
                "search_value": search_value,
                "attribute": [],
                "status": status or [],
                "open_status": "",
                "gtag_ids": "",
                "senior_search_list": "[]",
                "single_product_id": [],
                "is_matched_listing": "",
                "is_matched_alibaba": "",
                "relation_aux": "",
                "is_have_pic": "",
                "cg_package": "",
                "cg_product_gross_weight": {"left": "", "right": "", "symbol": "gt"},
                "cg_price": {"left": "", "right": "", "symbol": "gt"},
                "cg_transport_costs": {
                    "left": "",
                    "right": "",
                    "symbol": "gt",
                    "country_code": "US",
                },
                "offset": offset,
                "is_combo": "",
                "length": length,
                "is_aux": 0,
                "product_type": product_type or [1, 2],
                "selected_product_ids": "",
                "req_time_sequence": f"/api/product/lists$${_seq_counter[0]}",
            }

        path = "/api/product/lists"
        resp = self._erp_request(path, _build_payload(0))
        if not resp:
            return []

        # Response: {"code": 1, "msg": "操作成功", "total": N, "list": [...]}
        records, total = _extract(resp)

        if not fetch_all:
            logger.info(
                f"search_product: {len(records)}/{total} records ({search_field}={search_value})"
            )
            return records

        fetched = len(records)
        while fetched < total:
            next_resp = self._erp_request(path, _build_payload(fetched))
            page_data, _ = _extract(next_resp)
            if not page_data:
                break
            records.extend(page_data)
            fetched += len(page_data)

        logger.info(
            f"search_product: fetched {len(records)}/{total} records "
            f"({search_field}={search_value})"
        )
        return records

    # ── Ad report ─────────────────────────────────────────────────────────────

    def get_sp_campaign_ad_report(
        self,
        profile_id: str,
        report_date: str,
        asin: list[str] | None = None,
        search_type: str = "campaign_name",
        date_key: str = "day",
        is_daily: int = 1,
        record_key: str = "total",
        page: int = 1,
        length: int = 50,
        fetch_all: bool = False,
    ) -> dict[str, Any]:
        """
        Query Lingxing Sponsored Products campaign-level ad report.

        Endpoint: /pb-newad-web/v2/ad_report/campaign/index/detail
        Ad type:  Sponsored Products only.

        Args:
            profile_id:  Amazon Advertising profile ID.
            report_date: Date range string, e.g. "2025-04-02 - 2025-05-01".
                         No restriction on range length.
            asin:        Filter to specific ASINs (optional).
            search_type: Grouping dimension — "campaign_name" (default), "ad_group", etc.
            date_key:    Time granularity — "day" (default) or "month".
            is_daily:    1 = include per-day rows, 0 = aggregate only.
            record_key:  "total" (default) or other record scope.
            page:        Page number (1-based).
            length:      Rows per page. Clamped to [25, 500].
            fetch_all:   If True, auto-paginate and return all pages merged.
                         ``data[0]`` is the aggregate row (key=null);
                         ``data[1:]`` are daily rows.

        Returns:
            Raw API response dict {success, data, total, ...}.
        """
        length = max(25, min(500, length))

        def _build_payload(p: int) -> dict:
            payload: dict[str, Any] = {
                "length": length,
                "page": p,
                "profile_id": profile_id,
                "report_date": report_date,
                "search_type": search_type,
                "date_key": date_key,
                "is_daily": is_daily,
                "record_key": record_key,
            }
            if asin:
                payload["asin"] = asin
            return payload

        path = "/pb-newad-web/v2/ad_report/campaign/index/detail"
        resp = self._ad_request(path, _build_payload(page))

        if not fetch_all or not resp.get("success"):
            return resp

        all_data = list(resp.get("data", []))
        aggregate = all_data[:1] if all_data else []
        daily = all_data[1:]
        total = resp.get("total", len(all_data))
        fetched = len(all_data)

        while fetched < total:
            page += 1
            next_resp = self._ad_request(path, _build_payload(page))
            if not next_resp.get("success"):
                logger.warning(
                    f"get_sp_campaign_ad_report pagination stopped at page {page}: {next_resp}"
                )
                break
            page_data = next_resp.get("data", [])
            daily.extend(page_data)
            fetched += len(page_data)
            if not page_data:
                break

        resp["data"] = aggregate + daily
        return resp
