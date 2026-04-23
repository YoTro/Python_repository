import requests
import logging
import time
import json
import os
import gzip
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from .auth import AmazonAdsAuth

logger = logging.getLogger(__name__)

class AmazonAdsClient:
    """
    Client for Amazon Advertising API (Sponsored Products v3/v5).
    Fully async-compatible with robust 422 fallback.
    """

    ENDPOINTS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com"
    }

    _REPORT_POLL_INTERVAL = 10  # seconds between status checks
    _REPORT_POLL_MAX = 180      # max attempts → 30 min ceiling

    def __init__(self, store_id: Optional[str] = None, region: str = "NA"):
        self.auth = AmazonAdsAuth(store_id)
        self.base_url = self.ENDPOINTS.get(region.upper(), self.ENDPOINTS["NA"])
        self._owned_asin_cache = None

    async def _get_owned_asin_fallback(self) -> Optional[str]:
        """
        Attempts to find a valid owned ASIN from the account.
        """
        env_fallback = os.getenv(f"AMAZON_ADS_FALLBACK_ASIN_{self.auth.store_id}") or os.getenv("AMAZON_ADS_FALLBACK_ASIN")
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
            "Accept": "application/vnd.spad.v3+json"
        }
        
        try:
            # Wrap request in to_thread since requests is synchronous
            resp = await asyncio.to_thread(requests.post, url, json={"maxResults": 10}, headers=headers)
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
        keywords: List[Dict[str, str]], 
        asins: Optional[List[str]] = None,
        include_analysis: bool = False,
        strategy: str = "AUTO_FOR_SALES",
        adjustments: Optional[List[Dict[str, Any]]] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
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
            "Accept": v5_media_type
        }

        match_map = {
            "EXACT": "KEYWORD_EXACT_MATCH",
            "PHRASE": "KEYWORD_PHRASE_MATCH",
            "BROAD": "KEYWORD_BROAD_MATCH"
        }

        targeting_expressions = []
        for kw in keywords:
            m_type = kw.get("matchType", "EXACT").upper()
            targeting_expressions.append({
                "type": match_map.get(m_type, "KEYWORD_EXACT_MATCH"),
                "value": kw.get("keyword", kw.get("keywordText"))
            })

        current_asins = asins or []

        for attempt in range(max_retries):
            try:
                # Fallback if no ASINs
                if not current_asins:
                    fallback = await self._get_owned_asin_fallback()
                    if fallback:
                        current_asins = [fallback]
                    else:
                        raise ValueError("No owned ASIN available for recommendation context.")

                payload = {
                    "recommendationType": "BIDS_FOR_NEW_AD_GROUP",
                    "asins": current_asins,
                    "targetingExpressions": targeting_expressions,
                    "bidding": {"strategy": strategy, "adjustments": adjustments},
                    "includeAnalysis": include_analysis
                }

                response = await asyncio.to_thread(requests.post, endpoint, json=payload, headers=headers)
                
                # Handle 422 Ownership error
                if response.status_code == 422:
                    error_details = response.text
                    if "not owned by the advertiser" in error_details:
                        logger.warning(f"Ownership mismatch for {current_asins}. Retrying with discovered fallback...")
                        fallback = await self._get_owned_asin_fallback()
                        if fallback and fallback not in current_asins:
                            current_asins = [fallback]
                            continue
                    
                    logger.error(f"API 422 Error: {error_details}")
                    response.raise_for_status() # Trigger exception if not handled

                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    await asyncio.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)

        return {}

    # ── helpers ────────────────────────────────────────────────────────────

    def _v3_headers(self, media_type: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": media_type,
            "Accept": media_type,
        }

    async def _post_list(self, path: str, media_type: str, body: Dict) -> Dict:
        url = f"{self.base_url}{path}"
        resp = await asyncio.to_thread(
            requests.post, url, json=body, headers=self._v3_headers(media_type)
        )
        if resp.status_code == 429:
            await asyncio.sleep(10)
            resp = await asyncio.to_thread(
                requests.post, url, json=body, headers=self._v3_headers(media_type)
            )
        resp.raise_for_status()
        return resp.json()

    # ── Campaigns ──────────────────────────────────────────────────────────

    async def list_campaigns(
        self,
        states: Optional[List[str]] = None,
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List Sponsored Products campaigns (v3) with auto-pagination.

        Returns per-campaign dict:
          campaign_id, name, state, daily_budget, start_date, end_date,
          bidding_strategy, placement_adjustments
        """
        all_campaigns: List[Dict] = []
        next_token: Optional[str] = None

        while True:
            body: Dict[str, Any] = {"maxResults": min(max_results, 100)}
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
        campaign_ids: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        max_results: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List Sponsored Products ad groups (v3).

        Returns per-ad-group dict:
          ad_group_id, campaign_id, name, state, default_bid
        """
        body: Dict[str, Any] = {"maxResults": max_results}
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
        campaign_ids: Optional[List[str]] = None,
        ad_group_ids: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        max_results: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        List Sponsored Products keywords (v3).

        Returns per-keyword dict:
          keyword_id, ad_group_id, campaign_id, keyword_text,
          match_type, state, bid
        """
        all_keywords: List[Dict] = []
        next_token: Optional[str] = None

        while True:
            body: Dict[str, Any] = {"maxResults": min(max_results, 100)}
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
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        days: int = 30,
        time_unit: str = "SUMMARY",
        filters: Optional[List[Dict]] = None,
    ) -> List[Dict[str, Any]]:
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

        type_filters: Optional[List[Dict]] = None
        if report_type == "spSearchTerm":
            # spSearchTerm uses "cost" for spend and "keyword" for keyword text.
            # Filter to manual keyword types to exclude auto-targeting noise.
            metrics = [
                "impressions", "clicks", "cost",
                "purchases7d", "sales7d",
                "keyword", "matchType", "keywordBid",
                "campaignId", "adGroupId",
            ]
            type_filters = [{"field": "keywordType", "values": ["BROAD", "EXACT", "PHRASE"]}]
        elif report_type == "spCampaignsPlacement":
            # Placement report reuses spCampaigns reportTypeId but groups by
            # campaign + campaignPlacement to get per-placement breakdown.
            metrics = [
                "impressions", "clicks", "cost", "spend",
                "purchases7d", "sales7d",
                "clickThroughRate", "costPerClick",
                "campaignId", "campaignName",
                "campaignBiddingStrategy", "campaignBudgetAmount",
            ]
        elif report_type == "spAdvertisedProduct":
            # SponsoredProductsAdvertisedProductDailyReport: per-ASIN daily.
            # groupBy=advertiser returns one row per (ASIN, campaignId, date).
            # advertisedAsin filter is not supported; we filter client-side.
            # campaignStatus filter is required to include all campaign states.
            metrics = [
                "impressions", "clicks", "cost",
                "purchases7d", "sales7d",
                "advertisedAsin", "campaignId", "campaignName",
            ]
            if time_unit == "DAILY":
                metrics.append("date")
            type_filters = [
                {"field": "adCreativeStatus", "values": ["ENABLED", "PAUSED", "ARCHIVED"]},
            ]
        else:
            metrics = [
                "impressions", "clicks", "spend",
                "purchases7d", "sales7d",
                "campaignName", "campaignId",
            ]
            if time_unit == "DAILY":
                metrics.append("date")

        # Merge report-type default filters with any caller-supplied filters.
        combined_filters: Optional[List[Dict]] = (type_filters or []) + (filters or []) or None

        report_id = await self._create_report(report_type, start, end, metrics, filters=combined_filters, time_unit=time_unit)
        download_url = await self._poll_report(report_id)
        records = await self._download_report(download_url)
        return [_parse_report_record(r, report_type) for r in records]

    async def _create_report(
        self,
        report_type: str,
        start_date: str,
        end_date: str,
        metrics: List[str],
        filters: Optional[List[Dict]] = None,
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
        # spCampaignsPlacement is a virtual type: same reportTypeId as spCampaigns
        # but grouped by campaign + campaignPlacement for per-placement breakdown.
        report_type_id = "spCampaigns" if report_type == "spCampaignsPlacement" else report_type
        group_by_map = {
            "spCampaigns":          ["campaign"],
            "spCampaignsPlacement": ["campaign", "campaignPlacement"],
            "spSearchTerm":         ["searchTerm"],
            "spAdGroups":           ["adGroup"],
            "spAdvertisedProduct":  ["advertiser"],
        }
        ts = int(time.time())
        configuration: Dict[str, Any] = {
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
        # 425 means a previous identical report is still processing; wait and retry.
        # Amazon sometimes returns HTTP 200 with {"code":"425"} in the body instead
        # of a proper HTTP 425, so we check both.
        for attempt in range(6):
            resp = await asyncio.to_thread(requests.post, url, json=body, headers=headers)
            is_425 = resp.status_code == 425
            if not is_425 and resp.ok:
                try:
                    is_425 = str(resp.json().get("code", "")) == "425"
                except Exception:
                    pass
            if is_425:
                wait = 30 * (attempt + 1)
                logger.info(f"Report 425 (duplicate), waiting {wait}s before retry {attempt+1}/6")
                await asyncio.sleep(wait)
                body["name"] = f"{report_type}_{start_date}_{end_date}_{int(time.time())}"
                continue
            if not resp.ok:
                logger.error(f"Report creation failed {resp.status_code}: {resp.text[:500]}")
            resp.raise_for_status()
            break

        report_id = resp.json().get("reportId")
        if not report_id:
            raise ValueError(f"No reportId in response: {resp.text[:200]}")
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
        for attempt in range(self._REPORT_POLL_MAX):
            headers = self._build_poll_headers()
            resp = await asyncio.to_thread(requests.get, url, headers=headers)
            if resp.status_code == 401:
                logger.info(f"Poll got 401 on attempt {attempt + 1}, refreshing token and retrying")
                self.auth._token_cache.pop(self.auth.store_id, None)
                headers = self._build_poll_headers()
                resp = await asyncio.to_thread(requests.get, url, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            logger.debug(f"Report {report_id} status: {status} (attempt {attempt + 1})")

            if status == "COMPLETED":
                download_url = data.get("url")
                if not download_url:
                    raise ValueError("Report COMPLETED but no download URL returned.")
                return download_url

            if status == "FAILED":
                raise RuntimeError(f"Report {report_id} failed: {data.get('statusDetails')}")

            await asyncio.sleep(self._REPORT_POLL_INTERVAL)

        raise TimeoutError(f"Report {report_id} did not complete after {self._REPORT_POLL_MAX} polls.")

    async def _download_report(self, url: str) -> List[Dict]:
        resp = await asyncio.to_thread(requests.get, url, timeout=60)
        resp.raise_for_status()
        raw = gzip.decompress(resp.content)
        return json.loads(raw.decode("utf-8"))

    # ── Change History ─────────────────────────────────────────────────────

    _CH_BATCH_SIZE   = 10   # max campaign IDs per history request (API limit)
    _CH_CONCURRENCY  = 1    # sequential batches — /history rate-limit is strict

    async def get_change_history(
        self,
        from_date: int,
        to_date: int,
        campaign_ids: Optional[List[str]] = None,
        event_types: Optional[Dict[str, Any]] = None,
        count: int = 200,
        sort_direction: str = "DESC",
        next_token: Optional[str] = None,
    ) -> Dict[str, Any]:
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
            raise ValueError(
                f"Change history window exceeds 90 days: "
                f"span={(int(to_date) - int(from_date)) // (24*3600*1000)} days"
            )

        url = f"{self.base_url}/history"
        needs_v11 = event_types and "THEME" in event_types
        headers = {
            "Authorization": f"Bearer {self.auth.get_access_token()}",
            "Amazon-Advertising-API-ClientId": self.auth.client_id,
            "Amazon-Advertising-API-Scope": self.auth.get_profile_id(),
            "Content-Type": "application/json",
            "Accept": "application/vnd.historyresponse.v1.1+json" if needs_v11 else "application/json",
        }

        # Default event types. IN_BUDGET excluded — auto-generated, dominates volume.
        default_et: Dict[str, Any] = {
            "CAMPAIGN": {"filters": ["SMART_BIDDING_STRATEGY", "PLACEMENT_GROUP",
                                     "BUDGET_AMOUNT", "STATUS"]},
            "AD_GROUP": {"filters": ["BID_AMOUNT", "STATUS"]},
            "KEYWORD":  {"filters": ["STATUS"]},
        }
        resolved_base = {**default_et, **(event_types or {})}

        count_clamped = min(max(50, count), 200)
        base_fields: Dict[str, Any] = {
            "count":    count_clamped,
            "fromDate": int(from_date),
            "toDate":   int(to_date),
            "sort":     {"direction": sort_direction.upper(), "key": "DATE"},
        }

        async def _post_with_retry(body_dict: Dict) -> Dict:
            body_bytes = json.dumps(body_dict).encode("utf-8")
            for attempt in range(5):
                resp = await asyncio.to_thread(
                    requests.post, url, data=body_bytes, headers=headers
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    wait = max(retry_after, 60 * (attempt + 1))
                    logger.info(
                        f"Change history 429, Retry-After={retry_after}s, "
                        f"waiting {wait}s (attempt {attempt+1}/5)"
                    )
                    await asyncio.sleep(wait)
                    continue
                break
            if not resp.ok:
                logger.error(f"Change history failed {resp.status_code}: {resp.text[:300]}")
                resp.raise_for_status()
            return resp.json()

        async def _paginate(initial_body: Dict, label: str = "") -> List[Dict]:
            """Exhaust all pages for a given request body."""
            events: List[Dict] = []
            token: Optional[str] = None
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
                        f"Change history {label} page {page+1}: "
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
                ids[i: i + self._CH_BATCH_SIZE]
                for i in range(0, len(ids), self._CH_BATCH_SIZE)
            ]
            sem = asyncio.Semaphore(self._CH_CONCURRENCY)

            async def _fetch_batch(batch: List[str]) -> List[Dict]:
                parents = [{"campaignId": cid} for cid in batch]
                et_cfg  = {k: {**v, "parents": parents} for k, v in resolved_base.items()}
                body    = {**base_fields, "eventTypes": et_cfg}
                async with sem:
                    return await _paginate(body)

            results   = await asyncio.gather(*(_fetch_batch(b) for b in batches))
            all_events: List[Dict] = [ev for batch_evs in results for ev in batch_evs]

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
            et_cfg  = {k: {**v, "parents": parents} for k, v in resolved_base.items()}
            body    = {**base_fields, "eventTypes": et_cfg}
            if next_token:
                body["nextToken"] = next_token
            all_events = await _paginate(body, label="profile-wide")
            logger.info(f"Change history (profile-wide): {len(all_events)} events")

        return {"events": all_events, "total": len(all_events)}

# ── module-level parsers ────────────────────────────────────────────────────

def _parse_campaign(c: Dict) -> Dict[str, Any]:
    bidding = c.get("bidding", {})
    adjustments = bidding.get("adjustments", [])
    placement_map = {a["placement"]: a["percentage"] for a in adjustments if "placement" in a}
    return {
        "campaign_id": c.get("campaignId"),
        "name": c.get("name"),
        "state": c.get("state"),
        "daily_budget": c.get("budget", {}).get("budget"),
        "budget_type": c.get("budget", {}).get("budgetType"),
        "start_date": c.get("startDate"),
        "end_date": c.get("endDate"),
        "bidding_strategy": bidding.get("strategy"),
        "placement_top_of_search_pct": placement_map.get("PLACEMENT_TOP_OF_SEARCH"),
        "placement_product_page_pct": placement_map.get("PLACEMENT_PRODUCT_PAGE"),
    }


def _parse_ad_group(g: Dict) -> Dict[str, Any]:
    return {
        "ad_group_id": g.get("adGroupId"),
        "campaign_id": g.get("campaignId"),
        "name": g.get("name"),
        "state": g.get("state"),
        "default_bid": g.get("defaultBid"),
    }


def _parse_keyword(k: Dict) -> Dict[str, Any]:
    return {
        "keyword_id": k.get("keywordId"),
        "ad_group_id": k.get("adGroupId"),
        "campaign_id": k.get("campaignId"),
        "keyword_text": k.get("keywordText"),
        "match_type": k.get("matchType"),
        "state": k.get("state"),
        "bid": k.get("bid"),
    }


def _parse_report_record(r: Dict, report_type: str) -> Dict[str, Any]:
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
        "date":        r.get("date"),
        "impressions": impressions,
        "clicks": clicks,
        "spend": spend,
        "orders": orders,
        "sales": sales,
        "acos": acos,
        "ctr": ctr,
    }
    if report_type == "spSearchTerm":
        base.update({
            "keyword_text": r.get("keyword"),
            "match_type":   r.get("matchType"),
            "keyword_bid":  r.get("keywordBid"),
            "ad_group_id":  r.get("adGroupId"),
            "search_term":  r.get("searchTerm"),
        })
    elif report_type == "spCampaignsPlacement":
        base.update({
            "campaign_name":       r.get("campaignName"),
            "placement":           r.get("campaignPlacement"),
            "bidding_strategy":    r.get("campaignBiddingStrategy"),
            "daily_budget":        r.get("campaignBudgetAmount"),
            "cpc":                 r.get("costPerClick"),
        })
    elif report_type == "spAdvertisedProduct":
        base.update({
            "advertised_asin": r.get("advertisedAsin"),
            "campaign_id":     r.get("campaignId"),
            "campaign_name":   r.get("campaignName"),
        })
    else:
        base["campaign_name"] = r.get("campaignName")
    return base
