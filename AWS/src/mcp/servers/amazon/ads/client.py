import asyncio
import gzip
import json
import logging
import os
import re as _re
import time
from datetime import datetime, timedelta
from typing import Any

import requests

from src.core.errors import (
    ErrorCode,
    ExtractorError,
    FatalError,
    RetryableError,
    classify_api_code,
    classify_http,
    is_retryable,
)
from src.core.utils.decorators import exponential_backoff

from .auth import AmazonAdsAuth

logger = logging.getLogger(__name__)


def _is_report_425(resp: requests.Response) -> bool:
    """True for HTTP 425 OR HTTP 200 with body {"code":"425"} (Amazon Ads quirk)."""
    if resp.status_code == 425:
        return True
    if resp.ok:
        try:
            return (
                classify_api_code(str(resp.json().get("code", "")), "amazon_ads")
                == ErrorCode.DUPLICATE_REQUEST
            )
        except Exception:
            pass
    return False


def _extract_dup_report_id(resp: requests.Response) -> str | None:
    """Extract the existing reportId from a 425 duplicate response body, or None."""
    try:
        detail = resp.json().get("detail", "")
        m = _re.search(r"duplicate of\s*:\s*([0-9a-f\-]{30,})", detail, _re.I)
        return m.group(1).strip() if m else None
    except Exception:
        return None


class AmazonAdsClient:
    """
    Client for Amazon Advertising API (Sponsored Products v3/v5).
    Fully async-compatible with robust 422 fallback.
    """

    ENDPOINTS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com",
    }

    _REPORT_POLL_INTERVAL = 10  # seconds between status checks
    _REPORT_POLL_MAX = 360  # max attempts → 60 min ceiling (large reports need > 30 min)
    _REPORT_POLL_TIMEOUT = 30  # per-request timeout for poll GET (seconds)

    def __init__(self, store_id: str | None = None, region: str = "NA"):
        self.auth = AmazonAdsAuth(store_id)
        self.base_url = self.ENDPOINTS.get(region.upper(), self.ENDPOINTS["NA"])
        self._owned_asin_cache = None

    async def _get_owned_asin_fallback(self) -> str | None:
        """
        Attempts to find a valid owned ASIN from the account.
        """
        env_fallback = os.getenv(f"AMAZON_ADS_FALLBACK_ASIN_{self.auth.store_id}") or os.getenv(
            "AMAZON_ADS_FALLBACK_ASIN"
        )
        if env_fallback:
            return env_fallback

        if self._owned_asin_cache:
            return self._owned_asin_cache

        logger.info("422 detected. Attempting automated discovery of an owned ASIN...")

        url = f"{self.base_url}/sp/ads/list"
        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": "application/vnd.spad.v3+json",
            "Accept": "application/vnd.spad.v3+json",
        }

        try:
            # Wrap request in to_thread since requests is synchronous
            resp = await asyncio.to_thread(
                requests.post, url, json={"maxResults": 10}, headers=headers
            )
            if resp.status_code == 200:
                data = resp.json()
                ads = data.get("ads", [])
                if ads:
                    discovered_asin = ads[0].get("asin")
                    if discovered_asin:
                        logger.info(f"Discovered owned ASIN: {discovered_asin}")
                        self._owned_asin_cache = discovered_asin
                        return discovered_asin
        except Exception as e:
            logger.error(f"Owned ASIN discovery failed: {e}")

        return None

    async def get_keyword_bid_recommendations(
        self,
        keywords: list[dict[str, str]],
        asins: list[str] | None = None,
        include_analysis: bool = False,
        strategy: str = "AUTO_FOR_SALES",
        adjustments: list[dict[str, Any]] | None = None,
        max_retries: int = 3,
    ) -> dict[str, Any]:
        """
        Fetch bid recommendations (Asynchronous).
        """
        endpoint = f"{self.base_url}/sp/targets/bid/recommendations"
        v5_media_type = "application/vnd.spthemebasedbidrecommendation.v5+json"

        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": v5_media_type,
            "Accept": v5_media_type,
        }

        match_map = {
            "EXACT": "KEYWORD_EXACT_MATCH",
            "PHRASE": "KEYWORD_PHRASE_MATCH",
            "BROAD": "KEYWORD_BROAD_MATCH",
        }

        targeting_expressions = []
        for kw in keywords:
            m_type = kw.get("matchType", "EXACT").upper()
            targeting_expressions.append(
                {
                    "type": match_map.get(m_type, "KEYWORD_EXACT_MATCH"),
                    "value": kw.get("keyword", kw.get("keywordText")),
                }
            )

        current_asins = asins or []

        for attempt in range(max_retries):
            try:
                # Fallback if no ASINs
                if not current_asins:
                    fallback = await self._get_owned_asin_fallback()
                    if fallback:
                        current_asins = [fallback]
                    else:
                        raise FatalError(
                            "No owned ASIN available for bid recommendations.",
                            code=ErrorCode.INVALID_PARAMS,
                        )

                payload = {
                    "recommendationType": "BIDS_FOR_NEW_AD_GROUP",
                    "asins": current_asins,
                    "targetingExpressions": targeting_expressions,
                    "bidding": {"strategy": strategy, "adjustments": adjustments},
                    "includeAnalysis": include_analysis,
                }

                response = await asyncio.to_thread(
                    requests.post, endpoint, json=payload, headers=headers
                )

                resp_code = classify_http(response.status_code, "amazon_ads")
                # Handle 422: any 422 while ASINs are in the payload may indicate an
                # ownership mismatch (Amazon's message varies — don't rely on message text).
                if resp_code == ErrorCode.INVALID_PARAMS:
                    error_details = response.text
                    if current_asins:
                        logger.warning(
                            f"422 with ASINs {current_asins} — retrying with owned-ASIN fallback. "
                            f"Detail: {error_details}"
                        )
                        fallback = await self._get_owned_asin_fallback()
                        if fallback and fallback not in current_asins:
                            current_asins = [fallback]
                            continue

                    logger.error(f"API 422 Error: {error_details}")
                    response.raise_for_status()

                if resp_code == ErrorCode.RATE_LIMITED:
                    wait_time = (attempt + 1) * 10
                    await asyncio.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response.json()

            except Exception:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)

        return {}

    # ── helpers ────────────────────────────────────────────────────────────

    def _v3_headers(self, media_type: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": media_type,
            "Accept": media_type,
        }

    async def _post_list(self, path: str, media_type: str, body: dict) -> dict:
        url = f"{self.base_url}{path}"

        @exponential_backoff(max_retries=2, base_delay=10.0, max_delay=30.0, jitter=False)
        async def _do() -> dict:
            resp = await asyncio.to_thread(
                requests.post, url, json=body, headers=self._v3_headers(media_type)
            )
            code = classify_http(resp.status_code, "amazon_ads")
            if not resp.ok and is_retryable(code):
                raise RetryableError(resp.text[:100], code=code)
            resp.raise_for_status()
            return resp.json()

        return await _do()

    # ── Campaigns ──────────────────────────────────────────────────────────

    async def list_campaigns(
        self,
        states: list[str] | None = None,
        max_results: int = 100,
    ) -> list[dict[str, Any]]:
        """
        List Sponsored Products campaigns (v3) with auto-pagination.

        Returns per-campaign dict:
          campaign_id, name, state, daily_budget, start_date, end_date,
          bidding_strategy, placement_adjustments
        """
        all_campaigns: list[dict] = []
        next_token: str | None = None

        while True:
            body: dict[str, Any] = {"maxResults": min(max_results, 100)}
            if states:
                body["stateFilter"] = {"include": [s.upper() for s in states]}
            if next_token:
                body["nextToken"] = next_token

            data = await self._post_list(
                "/sp/campaigns/list",
                "application/vnd.spCampaign.v3+json",
                body,
            )
            page = data.get("campaigns", [])
            all_campaigns.extend([_parse_campaign(c) for c in page])
            next_token = data.get("nextToken")

            if not next_token or len(all_campaigns) >= max_results:
                break

        return all_campaigns[:max_results]

    # ── Ad Groups ──────────────────────────────────────────────────────────

    async def list_ad_groups(
        self,
        campaign_ids: list[str] | None = None,
        states: list[str] | None = None,
        max_results: int = 100,
    ) -> list[dict[str, Any]]:
        """
        List Sponsored Products ad groups (v3).

        Returns per-ad-group dict:
          ad_group_id, campaign_id, name, state, default_bid
        """
        body: dict[str, Any] = {"maxResults": max_results}
        if campaign_ids:
            body["campaignIdFilter"] = {"include": campaign_ids}
        if states:
            body["stateFilter"] = {"include": [s.upper() for s in states]}

        data = await self._post_list(
            "/sp/adGroups/list",
            "application/vnd.spAdGroup.v3+json",
            body,
        )
        return [_parse_ad_group(g) for g in data.get("adGroups", [])]

    # ── Keywords ───────────────────────────────────────────────────────────

    async def list_keywords(
        self,
        campaign_ids: list[str] | None = None,
        ad_group_ids: list[str] | None = None,
        states: list[str] | None = None,
        max_results: int = 200,
    ) -> list[dict[str, Any]]:
        """
        List Sponsored Products keywords (v3).

        Returns per-keyword dict:
          keyword_id, ad_group_id, campaign_id, keyword_text,
          match_type, state, bid
        """
        all_keywords: list[dict] = []
        next_token: str | None = None

        while True:
            body: dict[str, Any] = {"maxResults": min(max_results, 100)}
            if campaign_ids:
                body["campaignIdFilter"] = {"include": campaign_ids}
            if ad_group_ids:
                body["adGroupIdFilter"] = {"include": ad_group_ids}
            if states:
                body["stateFilter"] = {"include": [s.upper() for s in states]}
            if next_token:
                body["nextToken"] = next_token

            data = await self._post_list(
                "/sp/keywords/list",
                "application/vnd.spKeyword.v3+json",
                body,
            )
            page = data.get("keywords", [])
            all_keywords.extend([_parse_keyword(k) for k in page])
            next_token = data.get("nextToken")

            if not next_token or len(all_keywords) >= max_results:
                break

        return all_keywords[:max_results]

    # ── Performance Report ─────────────────────────────────────────────────

    async def get_performance_report(
        self,
        report_type: str = "spKeywords",
        start_date: str | None = None,
        end_date: str | None = None,
        days: int = 30,
        time_unit: str = "SUMMARY",
        filters: list[dict] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Request an async SP performance report and return parsed records.

        report_type: "spKeywords", "spCampaigns", or "spAdvertisedProduct"
        start_date / end_date: "YYYY-MM-DD"; if omitted, last `days` days are used.
        filters: optional list of {"field": ..., "values": [...]} API filters;
                 caller-supplied filters are merged with any report-type defaults.

        Keyword report metrics:
          keyword_text, match_type, campaign_id, ad_group_id,
          impressions, clicks, spend, orders, sales, acos, ctr

        Campaign report metrics:
          campaign_id, campaign_name, impressions, clicks, spend,
          orders, sales, acos, ctr

        Advertised-product report metrics:
          advertised_asin, impressions, clicks, spend, orders, sales, acos, ctr
        """
        today = datetime.utcnow().date()
        end = end_date or str(today - timedelta(days=1))
        start = start_date or str(today - timedelta(days=days))

        type_filters: list[dict] | None = None
        if report_type == "spSearchTerm":
            # spSearchTerm uses "cost" for spend and "keyword" for keyword text.
            # Filter to manual keyword types to exclude auto-targeting noise.
            metrics = [
                "impressions",
                "clicks",
                "cost",
                "purchases7d",
                "sales7d",
                "keyword",
                "matchType",
                "keywordBid",
                "campaignId",
                "adGroupId",
            ]
            if time_unit == "DAILY":
                metrics.append("date")
            type_filters = [{"field": "keywordType", "values": ["BROAD", "EXACT", "PHRASE"]}]
        elif report_type == "spCampaignsPlacement":
            # groupBy=["campaign","campaignPlacement"] + placementClassification column
            # returns one row per (campaign × placement bucket).
            metrics = [
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
                "placementClassification",
            ]
        elif report_type == "spAdvertisedProduct":
            # SponsoredProductsAdvertisedProductDailyReport: per-ASIN daily.
            # groupBy=advertiser returns one row per (ASIN, campaignId, date).
            # advertisedAsin filter is not supported; we filter client-side.
            # campaignStatus filter is required to include all campaign states.
            metrics = [
                "impressions",
                "clicks",
                "cost",
                "purchases7d",
                "sales7d",
                "advertisedAsin",
                "campaignId",
                "campaignName",
            ]
            if time_unit == "DAILY":
                metrics.append("date")
            type_filters = [
                {"field": "adCreativeStatus", "values": ["ENABLED", "PAUSED", "ARCHIVED"]},
            ]
        else:
            metrics = [
                "impressions",
                "clicks",
                "spend",
                "purchases7d",
                "sales7d",
                "campaignName",
                "campaignId",
            ]
            if time_unit == "DAILY":
                metrics.append("date")

        # Merge report-type default filters with any caller-supplied filters.
        combined_filters: list[dict] | None = (type_filters or []) + (filters or []) or None

        report_id = await self._create_report(
            report_type, start, end, metrics, filters=combined_filters, time_unit=time_unit
        )
        download_url = await self._poll_report(report_id)
        records = await self._download_report(download_url)
        return [_parse_report_record(r, report_type) for r in records]

    async def _create_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        metrics: list[str],
        filters: list[dict] | None = None,
        time_unit: str = "SUMMARY",
    ) -> str:
        url = f"{self.base_url}/reporting/reports"
        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        # spCampaignsPlacement uses the same reportTypeId as spCampaigns.
        # Confirmed working config: groupBy=["campaign","campaignPlacement"] +
        # column "placementClassification" → returns 4 placement buckets per campaign:
        #   "Top of Search on-Amazon", "Detail Page on-Amazon",
        #   "Other on-Amazon", "Off Amazon"
        # Note: "campaignPlacement" as a column is invalid (400); use
        # "placementClassification" instead. The campaignPlacement field in the
        # response payload is always null — placementClassification is the real key.
        report_type_id = "spCampaigns" if report_type == "spCampaignsPlacement" else report_type
        group_by_map = {
            "spCampaigns": ["campaign"],
            "spCampaignsPlacement": ["campaign", "campaignPlacement"],
            "spSearchTerm": ["searchTerm"],
            "spAdGroups": ["adGroup"],
            "spAdvertisedProduct": ["advertiser"],
        }
        ts = int(time.time())
        configuration: dict[str, Any] = {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": report_type_id,
            "groupBy": group_by_map.get(report_type, ["campaign"]),
            "columns": metrics,
            "timeUnit": time_unit,
            "format": "GZIP_JSON",
        }
        if filters:
            configuration["filters"] = filters
        body = {
            "name": f"{report_type}_{start_date}_{end_date}_{ts}",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": configuration,
        }

        @exponential_backoff(max_retries=5, base_delay=30.0, max_delay=180.0, jitter=False)
        async def _post() -> requests.Response | str:
            nonlocal body
            body["name"] = f"{report_type}_{start_date}_{end_date}_{int(time.time())}"
            resp = await asyncio.to_thread(requests.post, url, json=body, headers=headers)
            if _is_report_425(resp):
                dup_id = _extract_dup_report_id(resp)
                if dup_id:
                    logger.info(
                        f"[{report_type}] 425 duplicate — reusing report {dup_id}"
                        f" ({start_date}→{end_date})"
                    )
                    return dup_id
                raise RetryableError(
                    "425 duplicate without extractable reportId",
                    code=ErrorCode.DUPLICATE_REQUEST,
                )
            code = classify_http(resp.status_code, "amazon_ads")
            if not resp.ok:
                if is_retryable(code):
                    raise RetryableError(resp.text[:200], code=code)
                logger.error(f"Report creation failed {resp.status_code}: {resp.text[:500]}")
                resp.raise_for_status()
            return resp

        result = await _post()
        if isinstance(result, str):
            return result
        report_id = result.json().get("reportId")
        if not report_id:
            raise ExtractorError(f"No reportId in response: {result.text[:200]}")
        logger.info(f"Created report {report_id} ({report_type} {start_date}→{end_date})")
        return report_id

    def _build_poll_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
        }

    async def _poll_report(self, report_id: str) -> str:
        url = f"{self.base_url}/reporting/reports/{report_id}"
        _net_errors = (
            requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
        for attempt in range(self._REPORT_POLL_MAX):
            headers = self._build_poll_headers()
            try:
                resp = await asyncio.to_thread(
                    requests.get, url, headers=headers, timeout=self._REPORT_POLL_TIMEOUT
                )
            except _net_errors as net_err:
                # Transient network error — log and continue polling without
                # consuming a separate attempt slot so the ceiling isn't wasted.
                logger.warning(
                    f"Poll attempt {attempt + 1} for {report_id} hit network error "
                    f"({type(net_err).__name__}), retrying after {self._REPORT_POLL_INTERVAL}s"
                )
                await asyncio.sleep(self._REPORT_POLL_INTERVAL)
                continue

            poll_code = classify_http(resp.status_code, "amazon_ads")
            if poll_code == ErrorCode.AUTH_TOKEN_EXPIRED:
                logger.info(f"Poll got 401 on attempt {attempt + 1}, refreshing token and retrying")
                self.auth._token_cache.pop(self.auth.store_id, None)
                headers = self._build_poll_headers()
                try:
                    resp = await asyncio.to_thread(
                        requests.get, url, headers=headers, timeout=self._REPORT_POLL_TIMEOUT
                    )
                except _net_errors as net_err:
                    logger.warning(f"Token-refresh poll also failed: {net_err}, retrying")
                    await asyncio.sleep(self._REPORT_POLL_INTERVAL)
                    continue
                poll_code = classify_http(resp.status_code, "amazon_ads")

            if poll_code == ErrorCode.SERVER_ERROR:
                logger.warning(
                    f"Poll attempt {attempt + 1} for {report_id} got {resp.status_code}, "
                    f"retrying after {self._REPORT_POLL_INTERVAL}s"
                )
                await asyncio.sleep(self._REPORT_POLL_INTERVAL)
                continue

            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            logger.debug(f"Report {report_id} status: {status} (attempt {attempt + 1})")

            if status == "COMPLETED":
                download_url = data.get("url")
                if not download_url:
                    raise ExtractorError("Report COMPLETED but no download URL in response")
                return download_url

            if status == "FAILED":
                raise FatalError(f"Report {report_id} failed: {data.get('statusDetails')}")

            await asyncio.sleep(self._REPORT_POLL_INTERVAL)

        raise RetryableError(
            f"Report {report_id} did not complete after {self._REPORT_POLL_MAX} polls.",
            code=ErrorCode.TIMEOUT,
        )

    async def _download_report(self, url: str) -> list[dict]:
        resp = await asyncio.to_thread(requests.get, url, timeout=60)
        resp.raise_for_status()
        raw = gzip.decompress(resp.content)
        return json.loads(raw.decode("utf-8"))

    # ── Ad-type config ────────────────────────────────────────────────────
    # Each entry describes how to construct a /reporting/reports request for
    # one sponsored-product type.  spend/orders/sales field names differ per type.
    _AD_TYPE_CONFIG: dict[str, dict] = {
        "SP": {
            "adProduct": "SPONSORED_PRODUCTS",
            "reportTypeId": "spCampaigns",
            "groupBy": ["campaign"],
            "metrics": ["spend", "clicks", "impressions", "purchases7d", "sales7d"],
            "spend_field": "spend",
            "orders_field": "purchases7d",
            "sales_field": "sales7d",
        },
        "SB": {
            "adProduct": "SPONSORED_BRANDS",
            "reportTypeId": "sbCampaigns",
            "groupBy": ["campaign"],
            "metrics": ["cost", "clicks", "impressions", "purchases14d", "sales14d"],
            "spend_field": "cost",
            "orders_field": "purchases14d",
            "sales_field": "sales14d",
        },
        "SD": {
            "adProduct": "SPONSORED_DISPLAY",
            "reportTypeId": "sdCampaigns",
            "groupBy": ["campaign"],
            "metrics": ["cost", "clicks", "impressions", "purchases14d", "sales14d"],
            "spend_field": "cost",
            "orders_field": "purchases14d",
            "sales_field": "sales14d",
        },
        "STV": {
            "adProduct": "SPONSORED_TELEVISION",
            "reportTypeId": "stCampaigns",  # API uses "stCampaigns", not "stvCampaigns"
            "groupBy": ["campaign"],
            # STV reports no direct purchase attribution; omit orders/sales fields.
            "metrics": ["cost", "impressions"],
            "spend_field": "cost",
            "orders_field": None,
            "sales_field": None,
        },
        # Product-level report types — used for per-ASIN order attribution (SB + SD).
        # SP is intentionally excluded: spPurchasedProduct groups by purchasedAsin
        # (the product actually bought), where advertisedAsin is a dimension column.
        # Filtering by advertisedAsin == target gives cross-ASIN purchases (other
        # products bought after clicking target's ad), NOT purchases of target itself.
        # Purchases of the advertised ASIN are in spCampaigns (sp_ad_orders field).
        #
        # SB: sbPurchasedProduct groups by purchasedAsin (product actually bought).
        # orders14d = 14-day attribution window (click + view-through).
        # No spend column in this report type.
        "SB_PRODUCT": {
            "adProduct": "SPONSORED_BRANDS",
            "reportTypeId": "sbPurchasedProduct",
            "groupBy": ["purchasedAsin"],
            "metrics": ["purchasedAsin", "campaignId", "orders14d", "sales14d"],
            "spend_field": None,
            "orders_field": "orders14d",
            "sales_field": "sales14d",
            "asin_field": "purchasedAsin",
        },
        # SD advertised product: reportTypeId=sdAdvertisedProduct, groupBy=["advertiser"].
        # ASIN field is "promotedAsin"; purchases are not broken out by attribution window
        # (no purchases14d) — "purchases" covers all attribution (click + view, ~14d).
        # Use "purchasesClicks" for click-only attribution comparable to SP's purchases7d.
        "SD_PRODUCT": {
            "adProduct": "SPONSORED_DISPLAY",
            "reportTypeId": "sdAdvertisedProduct",
            "groupBy": ["advertiser"],
            "metrics": [
                "promotedAsin",
                "cost",
                "clicks",
                "impressions",
                "purchases",
                "purchasesClicks",
                "sales",
                "salesClicks",
            ],
            "spend_field": "cost",
            "orders_field": "purchasesClicks",
            "sales_field": "salesClicks",
            "asin_field": "promotedAsin",
        },
    }

    # ── Per-type summary report ────────────────────────────────────────────

    async def _fetch_ad_type_records(
        self,
        ad_type: str,
        start_date: str,
        end_date: str,
    ) -> list[dict]:
        """
        Create, poll, and download a SUMMARY campaign-level report for one ad type.
        Returns raw parsed records from the gzip-JSON payload.
        Raises on API failure; caller should catch and record the error per type.
        """
        cfg = self._AD_TYPE_CONFIG[ad_type]
        url = f"{self.base_url}/reporting/reports"
        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        report_cfg: dict[str, Any] = {
            "adProduct": cfg["adProduct"],
            "reportTypeId": cfg["reportTypeId"],
            "groupBy": cfg["groupBy"],
            "columns": cfg["metrics"],
            "timeUnit": "SUMMARY",
            "format": "GZIP_JSON",
        }
        if cfg.get("filters"):
            report_cfg["filters"] = cfg["filters"]

        @exponential_backoff(max_retries=4, base_delay=5.0, max_delay=60.0)
        async def _post() -> requests.Response | str:
            body = {
                "name": f"{ad_type}_summary_{start_date}_{end_date}_{int(time.time())}",
                "startDate": start_date,
                "endDate": end_date,
                "configuration": report_cfg,
            }
            resp = await asyncio.to_thread(requests.post, url, json=body, headers=headers)
            if _is_report_425(resp):
                dup_id = _extract_dup_report_id(resp)
                if dup_id:
                    logger.info(
                        f"[{ad_type}] 425 duplicate — reusing report {dup_id}"
                        f" ({start_date}→{end_date})"
                    )
                    return dup_id
                raise RetryableError(
                    "425 duplicate without extractable reportId",
                    code=ErrorCode.DUPLICATE_REQUEST,
                )
            code = classify_http(resp.status_code, "amazon_ads")
            if not resp.ok:
                if is_retryable(code):
                    raise RetryableError(resp.text[:200], code=code)
                logger.warning(
                    f"[{ad_type}] report creation failed {resp.status_code}: {resp.text[:300]}"
                )
                resp.raise_for_status()
            return resp

        result = await _post()
        if isinstance(result, str):
            report_id = result
        else:
            report_id = result.json().get("reportId")
            if not report_id:
                raise ExtractorError(f"[{ad_type}] no reportId in response: {result.text[:200]}")
        logger.info(f"[{ad_type}] created report {report_id} ({start_date}→{end_date})")
        download_url = await self._poll_report(report_id)
        return await self._download_report(download_url)

    async def get_ad_type_summary(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        days: int = 30,
        ad_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Fetch campaign-level SUMMARY reports for each ad type and compute
        cross-type spend share, clicks, ACOS, CTR, orders, sales.

        Args:
            start_date: "YYYY-MM-DD"; defaults to today − days.
            end_date:   "YYYY-MM-DD"; defaults to yesterday.
            days:       Lookback window when start/end not supplied (default 30).
            ad_types:   Subset of ["SP","SB","SD","STV"] to fetch.
                        Defaults to SP + SB + SD (STV optional, often not enabled).

        Returns:
        {
          "period": {"start_date": ..., "end_date": ...},
          "by_type": {
            "SP": {
              "spend": float,
              "clicks": int,
              "impressions": int,
              "orders": int,
              "sales": float,
              "acos_pct": float | None,
              "ctr_pct":  float | None,
              "cpc":      float | None,
              "spend_share_pct": float,
              "clicks_share_pct": float,
              "campaign_count": int,
            },
            ...
          },
          "total": {
            "spend": float, "clicks": int, "impressions": int,
            "orders": int, "sales": float,
            "acos_pct": float | None, "ctr_pct": float | None,
          },
          "errors": {"STV": "report creation failed 403: ...", ...},
        }
        """
        today = datetime.utcnow().date()
        end = end_date or str(today - timedelta(days=1))
        start = start_date or str(today - timedelta(days=days))

        types = [t.upper() for t in (ad_types or ["SP", "SB", "SD"])]
        unknown = [t for t in types if t not in self._AD_TYPE_CONFIG]
        if unknown:
            raise FatalError(
                f"Unknown ad_types: {unknown}. Valid: {list(self._AD_TYPE_CONFIG)}",
                code=ErrorCode.INVALID_PARAMS,
            )

        # Fetch all types in parallel; capture per-type errors so one failure
        # doesn't abort the whole call.
        async def _fetch_safe(ad_type: str):
            try:
                records = await self._fetch_ad_type_records(ad_type, start, end)
                return ad_type, records, None
            except Exception as exc:
                logger.warning(f"[{ad_type}] fetch failed: {exc}")
                return ad_type, [], str(exc)

        results = await asyncio.gather(*(_fetch_safe(t) for t in types))

        by_type: dict[str, dict] = {}
        errors: dict[str, str] = {}

        for ad_type, records, error in results:
            if error:
                errors[ad_type] = error
                continue
            cfg = self._AD_TYPE_CONFIG[ad_type]
            sf = cfg["spend_field"]
            of = cfg["orders_field"]
            vf = cfg["sales_field"]

            spend = sum(float(r.get(sf) or 0) for r in records) if sf else 0.0
            clicks = sum(int(r.get("clicks") or 0) for r in records)
            impr = sum(int(r.get("impressions") or 0) for r in records)
            orders = sum(int(r.get(of) or 0) for r in records) if of else 0
            sales = sum(float(r.get(vf) or 0) for r in records) if vf else 0.0

            by_type[ad_type] = {
                "spend": round(spend, 2),
                "clicks": clicks,
                "impressions": impr,
                "orders": orders,
                "sales": round(sales, 2),
                "acos_pct": round(spend / sales * 100, 2) if sales > 0 else None,
                "ctr_pct": round(clicks / impr * 100, 4) if impr > 0 else None,
                "cpc": round(spend / clicks, 2) if clicks > 0 else None,
                # share fields filled in below after totals are known
                "spend_share_pct": 0.0,
                "clicks_share_pct": 0.0,
                "campaign_count": len(records),
            }

        # Totals
        total_spend = sum(v["spend"] for v in by_type.values())
        total_clicks = sum(v["clicks"] for v in by_type.values())
        total_impr = sum(v["impressions"] for v in by_type.values())
        total_orders = sum(v["orders"] for v in by_type.values())
        total_sales = sum(v["sales"] for v in by_type.values())

        # Back-fill share percentages
        for v in by_type.values():
            v["spend_share_pct"] = (
                round(v["spend"] / total_spend * 100, 2) if total_spend > 0 else 0.0
            )
            v["clicks_share_pct"] = (
                round(v["clicks"] / total_clicks * 100, 2) if total_clicks > 0 else 0.0
            )

        total: dict[str, Any] = {
            "spend": round(total_spend, 2),
            "clicks": total_clicks,
            "impressions": total_impr,
            "orders": total_orders,
            "sales": round(total_sales, 2),
            "acos_pct": round(total_spend / total_sales * 100, 2) if total_sales > 0 else None,
            "ctr_pct": round(total_clicks / total_impr * 100, 4) if total_impr > 0 else None,
            "cpc": round(total_spend / total_clicks, 2) if total_clicks > 0 else None,
        }

        return {
            "period": {"start_date": start, "end_date": end},
            "by_type": by_type,
            "total": total,
            "errors": errors,
        }

    async def get_non_sp_orders_by_asin(
        self,
        asin: str,
        start_date: str,
        end_date: str,
        ad_types: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Fetch SB and SD order counts for a specific ASIN using product-level reports.

        SP orders are sourced from spCampaigns (sp_ad_orders) — spPurchasedProduct
        groups by purchasedAsin and filtering by advertisedAsin gives cross-ASIN
        purchases (other products bought after clicking the ad), not same-ASIN SP orders.
        SB: sbPurchasedProduct (orders14d, 14-day attribution, purchasedAsin filter).
            No spend column.
        SD: sdAdvertisedProduct (purchasesClicks, click-attributed, promotedAsin filter).
        DSP: separate DSP seat API, not integrated.

        Returns:
        {
          "sb_ad_orders": int,    # sbPurchasedProduct orders14d for purchasedAsin
          "sb_ad_spend":  None,   # not available in sbPurchasedProduct
          "sd_ad_orders": int,    # sdAdvertisedProduct purchasesClicks for promotedAsin
          "sd_ad_spend":  float,
          "errors":       {"SB_PRODUCT": "...", ...},
        }
        """
        types = [t.upper() for t in (ad_types or ["SB_PRODUCT", "SD_PRODUCT"])]

        async def _fetch_safe(ad_type: str):
            try:
                records = await self._fetch_ad_type_records(ad_type, start_date, end_date)
                return ad_type, records, None
            except Exception as exc:
                logger.warning(f"[{ad_type}] non-SP orders fetch failed: {exc}")
                return ad_type, [], str(exc)

        results = await asyncio.gather(*(_fetch_safe(t) for t in types))

        out: dict[str, Any] = {
            "sb_ad_orders": None,
            "sb_ad_spend": None,
            "sd_ad_orders": None,
            "sd_ad_spend": None,
            "errors": {},
        }
        for ad_type, records, error in results:
            if error:
                out["errors"][ad_type] = error
                continue
            cfg = self._AD_TYPE_CONFIG[ad_type]
            asin_field = cfg["asin_field"]
            orders_field = cfg["orders_field"]
            spend_field = cfg["spend_field"]
            asin_upper = asin.upper()
            matched = [r for r in records if (r.get(asin_field) or "").upper() == asin_upper]
            orders = sum(int(r.get(orders_field) or 0) for r in matched) if orders_field else 0
            spend = (
                round(sum(float(r.get(spend_field) or 0) for r in matched), 2)
                if spend_field
                else None
            )
            if ad_type == "SB_PRODUCT":
                out["sb_ad_orders"] = orders
                out["sb_ad_spend"] = spend
            elif ad_type == "SD_PRODUCT":
                out["sd_ad_orders"] = orders
                out["sd_ad_spend"] = spend

        logger.info(
            f"[non_sp_orders] {asin} ({start_date}→{end_date}): "
            f"SB={out['sb_ad_orders']} SD={out['sd_ad_orders']} orders"
        )
        return out

    # ── Change History ─────────────────────────────────────────────────────

    _CH_BATCH_SIZE = 10  # max campaign IDs per history request (API limit)
    _CH_CONCURRENCY = 1  # sequential batches — /history rate-limit is strict

    async def get_change_history(
        self,
        from_date: int,
        to_date: int,
        campaign_ids: list[str] | None = None,
        event_types: dict[str, Any] | None = None,
        count: int = 200,
        sort_direction: str = "DESC",
        next_token: str | None = None,
    ) -> dict[str, Any]:
        """
        Return change history for SP campaigns.

        When campaign_ids is provided the API is called in batches of
        _CH_BATCH_SIZE with parents=[{id, type:CAMPAIGN}] — avoiding the
        profile-wide fetch that triggers 429 on large accounts.

        When campaign_ids is empty, falls back to useProfileIdAdvertiser:true
        (profile-wide), which is the only option when no scope is known.

        Args:
            from_date:      Start of range in UTC epoch milliseconds.
            to_date:        End of range in UTC epoch milliseconds.
            campaign_ids:   Campaign IDs to scope the query (batched, max 10/req).
            event_types:    Override default CAMPAIGN/AD_GROUP/KEYWORD event filters.
            count:          Max records per page (50–200).
            sort_direction: "DESC" (newest first) or "ASC".
            next_token:     Resume token — only used in profile-wide (no campaign_ids) mode.

        Returns:
            {"events": [...], "total": int}
        """
        _90_days_ms = 90 * 24 * 3600 * 1000
        if int(to_date) - int(from_date) > _90_days_ms:
            raise FatalError(
                f"Change history window exceeds 90 days: "
                f"span={(int(to_date) - int(from_date)) // (24 * 3600 * 1000)} days",
                code=ErrorCode.INVALID_PARAMS,
            )

        url = f"{self.base_url}/history"
        needs_v11 = event_types and "THEME" in event_types
        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": "application/json",
            "Accept": "application/vnd.historyresponse.v1.1+json"
            if needs_v11
            else "application/json",
        }

        # Default event types. IN_BUDGET excluded — Amazon fires it automatically at midnight US
        # time on every budget-reset, making it equivalent to spend≈100%-of-cap; it carries no
        # intraday exhaustion timestamp and adds no signal beyond the 85%-threshold proxy already
        # used in _compute_campaign_budget_coverage. OUT_OF_BUDGET does not exist in this API.
        default_et: dict[str, Any] = {
            "CAMPAIGN": {
                "filters": ["SMART_BIDDING_STRATEGY", "PLACEMENT_GROUP", "BUDGET_AMOUNT", "STATUS"]
            },
            "AD_GROUP": {"filters": ["BID_AMOUNT", "STATUS"]},
            "KEYWORD": {"filters": ["STATUS"]},
        }
        resolved_base = {**default_et, **(event_types or {})}

        count_clamped = min(max(50, count), 200)
        base_fields: dict[str, Any] = {
            "count": count_clamped,
            "fromDate": int(from_date),
            "toDate": int(to_date),
            "sort": {"direction": sort_direction.upper(), "key": "DATE"},
        }

        @exponential_backoff(max_retries=5, base_delay=60.0, max_delay=300.0, jitter=False)
        async def _post_with_retry(body_dict: dict) -> dict:
            body_bytes = json.dumps(body_dict).encode("utf-8")
            resp = await asyncio.to_thread(requests.post, url, data=body_bytes, headers=headers)
            code = classify_http(resp.status_code, "amazon_ads")
            if code == ErrorCode.AUTH_TOKEN_EXPIRED:
                # Refresh token before the decorator retries
                logger.info("change_history got 401 — refreshing access token before retry")
                self.auth._token_cache.pop(self.auth.store_id, None)
                headers["Authorization"] = f"Bearer {self.auth.get_access_token()}"
                raise RetryableError("Token expired (401), refreshed", code=code)
            if not resp.ok and is_retryable(code):
                raise RetryableError(resp.text[:200], code=code)
            resp.raise_for_status()
            return resp.json()

        async def _paginate(initial_body: dict, label: str = "") -> list[dict]:
            """Exhaust all pages for a given request body."""
            events: list[dict] = []
            token: str | None = None
            for page in range(20):
                body = dict(initial_body)
                if token:
                    body["nextToken"] = token
                data = await _post_with_retry(body)
                page_events = data.get("events", [])
                events.extend(page_events)
                token = data.get("nextToken")
                total = data.get("totalRecords", len(events))
                if label:
                    logger.debug(
                        f"Change history {label} page {page + 1}: "
                        f"{len(page_events)} events (cum {len(events)}/{total})"
                    )
                if not token or len(events) >= total:
                    break
            return events

        ids = [str(c) for c in (campaign_ids or [])]

        if ids:
            # ── Campaign-batched mode ─────────────────────────────────────
            # Each batch of _CH_BATCH_SIZE campaigns becomes one API request
            # using parents=[{id, type:CAMPAIGN}].  Max _CH_CONCURRENCY in
            # flight at once to stay well below rate-limit thresholds.
            batches = [
                ids[i : i + self._CH_BATCH_SIZE] for i in range(0, len(ids), self._CH_BATCH_SIZE)
            ]
            sem = asyncio.Semaphore(self._CH_CONCURRENCY)

            async def _fetch_batch(batch: list[str]) -> list[dict]:
                parents = [{"campaignId": cid} for cid in batch]
                et_cfg = {k: {**v, "parents": parents} for k, v in resolved_base.items()}
                body = {**base_fields, "eventTypes": et_cfg}
                async with sem:
                    return await _paginate(body)

            results = await asyncio.gather(*(_fetch_batch(b) for b in batches))
            all_events: list[dict] = [ev for batch_evs in results for ev in batch_evs]

            # Re-sort globally (each batch is ordered; merge needs a global sort)
            desc = sort_direction.upper() == "DESC"
            all_events.sort(
                key=lambda e: int(e.get("changedAt") or e.get("timestamp") or 0),
                reverse=desc,
            )
            logger.info(
                f"Change history (batched): {len(all_events)} events "
                f"from {len(batches)} batches ({len(ids)} campaigns)"
            )
        else:
            # ── Profile-wide fallback ─────────────────────────────────────
            parents = [{"useProfileIdAdvertiser": True}]
            et_cfg = {k: {**v, "parents": parents} for k, v in resolved_base.items()}
            body = {**base_fields, "eventTypes": et_cfg}
            if next_token:
                body["nextToken"] = next_token
            all_events = await _paginate(body, label="profile-wide")
            logger.info(f"Change history (profile-wide): {len(all_events)} events")

        return {"events": all_events, "total": len(all_events)}


# ── module-level parsers ────────────────────────────────────────────────────


def _parse_campaign(c: dict) -> dict[str, Any]:
    # v3 uses dynamicBidding instead of bidding
    db = c.get("dynamicBidding", {})
    adjustments = db.get("placementBidding", [])
    placement_map = {a["placement"]: a["percentage"] for a in adjustments if "placement" in a}

    # Map internal strategy codes to human-readable UI names
    # MANUAL -> Fixed bids
    # AUTO_FOR_SALES -> Dynamic bids - down only
    # LEGACY_FOR_SALES -> Dynamic bids - up and down (usually)
    raw_strategy = db.get("strategy")
    strategy_map = {
        "MANUAL": "Fixed bids",
        "AUTO_FOR_SALES": "Dynamic bids - down only",
        "LEGACY_FOR_SALES": "Dynamic bids - up and down",
    }
    bidding_strategy = strategy_map.get(raw_strategy, raw_strategy)

    return {
        "campaign_id": c.get("campaignId"),
        "name": c.get("name"),
        "state": c.get("state"),
        "targeting_type": c.get("targetingType"),  # "AUTO" | "MANUAL"
        "daily_budget": c.get("budget", {}).get("budget"),
        "budget_type": c.get("budget", {}).get("budgetType"),
        "start_date": c.get("startDate"),
        "end_date": c.get("endDate"),
        "bidding_strategy": bidding_strategy,
        "placement_top_of_search_pct": placement_map.get("PLACEMENT_TOP"),
        "placement_product_page_pct": placement_map.get("PLACEMENT_PRODUCT_PAGE"),
    }


def _parse_ad_group(g: dict) -> dict[str, Any]:
    return {
        "ad_group_id": g.get("adGroupId"),
        "campaign_id": g.get("campaignId"),
        "name": g.get("name"),
        "state": g.get("state"),
        "default_bid": g.get("defaultBid"),
    }


def _parse_keyword(k: dict) -> dict[str, Any]:
    return {
        "keyword_id": k.get("keywordId"),
        "ad_group_id": k.get("adGroupId"),
        "campaign_id": k.get("campaignId"),
        "keyword_text": k.get("keywordText"),
        "match_type": k.get("matchType"),
        "state": k.get("state"),
        "bid": k.get("bid"),
    }


def _parse_report_record(r: dict, report_type: str) -> dict[str, Any]:
    # spSearchTerm, spCampaignsPlacement, spAdvertisedProduct use "cost"; spCampaigns uses "spend"
    spend = r.get("spend") or r.get("cost") or 0
    sales = r.get("sales7d", 0) or 0
    clicks = r.get("clicks", 0) or 0
    impressions = r.get("impressions", 0) or 0
    orders = r.get("purchases7d", 0) or 0
    acos = round(spend / sales * 100, 2) if sales > 0 else None
    ctr = round(clicks / impressions * 100, 4) if impressions > 0 else None

    base = {
        "campaign_id": r.get("campaignId"),
        "date": r.get("date"),
        "impressions": impressions,
        "clicks": clicks,
        "spend": spend,
        "orders": orders,
        "sales": sales,
        "acos": acos,
        "ctr": ctr,
    }
    if report_type == "spSearchTerm":
        base.update(
            {
                "keyword_text": r.get("keyword"),
                "match_type": r.get("matchType"),
                "keyword_bid": r.get("keywordBid"),
                "ad_group_id": r.get("adGroupId"),
                "search_term": r.get("searchTerm"),
            }
        )
    elif report_type == "spCampaignsPlacement":
        base.update(
            {
                "campaign_name": r.get("campaignName"),
                "placement": r.get("placementClassification"),
                "bidding_strategy": r.get("campaignBiddingStrategy"),
                "daily_budget": r.get("campaignBudgetAmount"),
                "cpc": r.get("costPerClick"),
            }
        )
    elif report_type == "spAdvertisedProduct":
        base.update(
            {
                "advertised_asin": r.get("advertisedAsin"),
                "campaign_id": r.get("campaignId"),
                "campaign_name": r.get("campaignName"),
            }
        )
    else:
        base["campaign_name"] = r.get("campaignName")
    return base
