import requests
import logging
import time
import json
import os
import asyncio
from typing import List, Dict, Optional, Any
from .auth import AmazonAdsAuth

logger = logging.getLogger(__name__)

class AmazonAdsClient:
    """
    Client for Amazon Advertising API (Sponsored Products v5.0).
    Fully async-compatible with robust 422 fallback.
    """

    ENDPOINTS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com"
    }

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
