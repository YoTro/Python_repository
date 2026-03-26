from __future__ import annotations
import logging
import json
from typing import List, Dict, Any
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class ProfitabilitySearchExtractor(AmazonBaseScraper):
    """
    Extractor using Amazon's FBA Profitability Calculator public API.
    This API is designed for sellers to search products for fee estimation.
    It works without cookies or CSRF tokens if headers are properly set.
    """

    async def search_products(self, keywords: str, page_offset: int = 1) -> List[Dict[str, Any]]:
        """
        Search for products and return their full metadata dictionaries.
        
        The returned dictionaries typically contain the following rich metadata:
        - asin: The product ASIN
        - title: Full product title
        - brandName: The brand of the product
        - price & currency: Current price
        - weight & weightUnit: e.g., 0.2910 pounds
        - length, width, height & dimensionUnit: Physical dimensions
        - salesRank & salesRankContextName: e.g., Rank 1 in "Computer Mice"
        - customerReviewsCount & customerReviewsRating: e.g., 41039 reviews, 4.5 rating
        - imageUrl & thumbStringUrl: Product images
        - feeCategoryString: e.g., "Electronic Accessories"
        
        :param keywords: Search query keywords.
        :param page_offset: Page offset (1-indexed).
        :return: A list of dictionaries containing detailed product data.
        """
        url = "https://sellercentral.amazon.com/rcpublic/searchproduct?countryCode=US&locale=en-US"
        
        headers = {
            "sec-ch-ua-platform": '"macOS"',
            "Referer": "https://sellercentral.amazon.com/hz/fba/profitabilitycalculator/index?lang=en_US",
            "Accept": "application/json",
            "content-type": "application/json; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
            "anti-csrftoken-a2z": "", # Explicitly empty as discovered
        }
        
        payload = {
            "keywords": keywords,
            "countryCode": "US",
            "searchType": "GENERAL",
            "pageOffset": page_offset
        }

        try:
            logger.info(f"Searching profitability API for '{keywords}' (page {page_offset})...")
            # Using self.session.post from AmazonBaseScraper (curl_cffi)
            response = await self.session.post(
                url, 
                json=payload, 
                headers=headers,
                timeout=20
            )
            
            if response.status_code != 200:
                logger.warning(f"Profitability API returned status {response.status_code}")
                return []
                
            try:
                data = response.json()
            except Exception:
                logger.error("Failed to parse JSON response from Profitability API")
                return []

            if not data.get("succeed"):
                logger.warning(f"Profitability API reported failure: {data.get('error')}")
                return []
                
            products = data.get("data", {}).get("products", [])
            logger.info(f"Profitability API found {len(products)} products on page {page_offset}.")
            return products

        except Exception as e:
            logger.error(f"Profitability API request failed: {e}")
            return []
