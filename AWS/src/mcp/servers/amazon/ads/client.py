import requests
import logging
import time
import json
from typing import List, Dict, Optional, Any, Union
from .auth import AmazonAdsAuth

logger = logging.getLogger(__name__)

class AmazonAdsClient:
    """
    Client for Amazon Advertising API (Sponsored Products v5.0).
    Strictly following the Theme-based Bid Recommendations schema.
    """

    ENDPOINTS = {
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com"
    }

    def __init__(self, store_id: Optional[str] = None, region: str = "NA"):
        self.auth = AmazonAdsAuth(store_id)
        self.base_url = self.ENDPOINTS.get(region.upper(), self.ENDPOINTS["NA"])

    def get_keyword_bid_recommendations(
        self, 
        keywords: List[Dict[str, str]], 
        asins: List[str],
        include_analysis: bool = False,
        strategy: Union[str, List[str]] = "AUTO_FOR_SALES",
        adjustments: Optional[List[Dict[str, Any]]] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Fetch bid recommendations using SP v5.0.
        
        :param strategy: Single strategy string or a list of strategy strings.
                        Options: "AUTO_FOR_SALES" (Up & Down), "LEGACY_FOR_SALES" (Down only), "MANUAL" (Fixed)
        :param adjustments: Optional list of placement adjustments.
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

        # Mapping internal match types to v5 enums
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

        is_single = isinstance(strategy, str)
        strategies = [strategy] if is_single else strategy
        results = {}

        for s in strategies:
            # BUILD THE FULLY COMPLIANT v5 PAYLOAD
            payload = {
                "recommendationType": "BIDS_FOR_NEW_AD_GROUP",
                "asins": asins,
                "targetingExpressions": targeting_expressions,
                "bidding": {
                    "strategy": s,
                    "adjustments": adjustments # Expects [{"percentage": 100, "predicate": "PLACEMENT_TOP"}]
                },
                "includeAnalysis": include_analysis # Schema says boolean
            }

            strategy_result = {}
            for attempt in range(max_retries):
                try:
                    # Use json=payload to ensure correct encoding
                    response = requests.post(endpoint, json=payload, headers=headers)
                    
                    if response.status_code == 429:
                        wait_time = (attempt + 1) * 10 
                        logger.warning(f"Rate limited (429) for strategy {s}. Waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    if response.status_code == 415:
                        # Amazon can be finicky about Content-Type vs schema validation
                        logger.warning(f"415 Error for strategy {s}: Retrying with standard application/json Content-Type...")
                        headers["Content-Type"] = "application/json"
                        continue

                    response.raise_for_status()
                    strategy_result = response.json()
                    break
                    
                except Exception as e:
                    if attempt == max_retries - 1:
                        if hasattr(e, 'response') and e.response is not None:
                            logger.error(f"Final Attempt Failed for strategy {s}: {e.response.status_code} - {e.response.text}")
                        raise
                    time.sleep(2)
            
            results[s] = strategy_result
        
        return results[strategy] if is_single else results
