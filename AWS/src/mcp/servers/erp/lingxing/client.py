from __future__ import annotations
import logging
import os
import uuid
from typing import Any, Dict, List, Optional
from curl_cffi import requests
from ..base import ERPClient
from .auth import LingxingAuth

logger = logging.getLogger(__name__)


class LingxingClient(ERPClient):
    """
    Lingxing ERP (领星ERP) client.
    Implements ERPClient against the Lingxing gateway API.
    """

    BASE_URL = "https://gw.lingxingerp.com"

    def __init__(self, account: str = None, password: str = None):
        self.auth       = LingxingAuth()
        self.session    = requests.Session(impersonate="chrome")
        self.token      = self.auth.load_token()
        if not self.token:
            self.token = self.auth.login(account, password)
        if not self.token:
            logger.warning("LingxingClient: no valid token. Call auth.login() to authenticate.")

        # Ad API identity headers: env vars take priority; fall back to saved token meta.
        _meta = self.auth.load_meta()
        self._company_id = os.getenv("LINGXING_COMPANY_ID") or _meta.get("company_id", "")
        self._env_key    = os.getenv("LINGXING_ENV_KEY")    or _meta.get("env_key", "")
        self._uid        = os.getenv("LINGXING_UID")        or _meta.get("uid", "")
        self._zid        = os.getenv("LINGXING_ZID")        or _meta.get("zid", "")

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
            if data.get("code") in (401, "401", -1) or "token" in str(data.get("msg", "")).lower():
                logger.warning("Token expired, re-authenticating...")
                self.token = self.auth.login()
                if self.token:
                    headers["auth-token"] = self.token
                    resp = self.session.request(method, url, headers=headers, **kwargs)
                    data = resp.json()
            return data
        except Exception as e:
            logger.error(f"Lingxing API request failed [{method} {path}]: {e}")
            return {}

    # ── Ad API request helper (ads.lingxing.com origin) ──────────────────────

    def _ad_request(self, path: str, payload: dict) -> dict:
        """POST to the Lingxing ad-report gateway (/pb-newad-web/...)."""
        ad_headers = {
            "ak-origin":        "https://ads.lingxing.com",
            "Origin":           "https://ads.lingxing.com",
            "Referer":          "https://ads.lingxing.com/",
            "x-ak-request-source": "erp",
            "x-ak-version":     "1.1.4.0.000",
            "x-ak-company-id":  self._company_id,
            "x-ak-env-key":     self._env_key,
            "x-ak-uid":         self._uid,
            "x-ak-zid":         self._zid,
        }
        return self._request("POST", path, json=payload, headers=ad_headers)

    # ── ERPClient interface ───────────────────────────────────────────────────

    def get_inventory(self, sku: str) -> Dict[str, Any]:
        # TODO: map to real Lingxing inventory endpoint
        data = self._request("POST", "/newadmin/api/inventory/list", json={"msku": sku})
        return data

    def get_purchase_orders(
        self,
        sku: Optional[str] = None,
        status: Optional[str] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        # TODO: map to real Lingxing purchase order endpoint
        payload = {}
        if sku:    payload["msku"]   = sku
        if status: payload["status"] = status
        data = self._request("POST", "/newadmin/api/purchase/list", json=payload)
        return data.get("data", [])

    def get_sales_orders(
        self,
        sku: Optional[str] = None,
        days: int = 30,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        # TODO: map to real Lingxing sales order endpoint
        payload = {"days": days}
        if sku: payload["msku"] = sku
        data = self._request("POST", "/newadmin/api/orders/list", json=payload)
        return data.get("data", [])

    # ── Ad report ─────────────────────────────────────────────────────────────

    def get_sp_campaign_ad_report(
        self,
        profile_id: str,
        report_date: str,
        asin: Optional[List[str]] = None,
        search_type: str = "campaign_name",
        date_key: str = "day",
        is_daily: int = 1,
        record_key: str = "total",
        page: int = 1,
        length: int = 50,
        fetch_all: bool = False,
    ) -> Dict[str, Any]:
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
            payload: Dict[str, Any] = {
                "length":      length,
                "page":        p,
                "profile_id":  profile_id,
                "report_date": report_date,
                "search_type": search_type,
                "date_key":    date_key,
                "is_daily":    is_daily,
                "record_key":  record_key,
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
        daily     = all_data[1:]
        total     = resp.get("total", len(all_data))
        fetched   = len(all_data)

        while fetched < total:
            page += 1
            next_resp = self._ad_request(path, _build_payload(page))
            if not next_resp.get("success"):
                logger.warning(f"get_sp_campaign_ad_report pagination stopped at page {page}: {next_resp}")
                break
            page_data = next_resp.get("data", [])
            daily.extend(page_data)
            fetched += len(page_data)
            if not page_data:
                break

        resp["data"] = aggregate + daily
        return resp
