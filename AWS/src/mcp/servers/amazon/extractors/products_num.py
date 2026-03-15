from __future__ import annotations
import asyncio
import logging
import json
import re
import random
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class ProductsNumExtractor(AmazonBaseScraper):
    """
    Extractor to find the number of products a specific Amazon seller has listed.
    """

    async def get_seller_id_from_listing(self, url: str) -> str:
        """
        Visit a product listing URL and extract the Seller ID (merchant ID).
        """
        logger.info(f"Fetching listing to find seller ID: {url}")
        html = await self.fetch(url)
        if not html:
            return None

        # Try using BeautifulSoup to find the seller link
        soup = BeautifulSoup(html, "html.parser")
        merchant_link = soup.find("a", id="sellerProfileTriggerId")

        if merchant_link and merchant_link.get("href"):
            href = merchant_link.get("href")
            # Extract seller ID from href (e.g., &seller=A123456789)
            match = re.search(r"seller=([A-Z0-9]+)", href)
            if match:
                return match.group(1)

        # Fallback to the legacy regex method
        match = re.search(r'seller=([A-Z0-9]+)(?:&|")', html)
        if match:
            return match.group(1)

        logger.warning(f"Could not find seller ID on page: {url}")
        return None

    async def get_products_num_by_seller(self, seller_id: str) -> int:
        """
        Query the Amazon AJAX endpoint to get the total number of products for a seller.
        """
        url = "https://www.amazon.com/sp/ajax/products"

        headers = self._get_default_headers()
        headers.update(
            {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"https://www.amazon.com/sp?seller={seller_id}",
            }
        )

        payload = {
            "marketplaceID": "ATVPDKIKX0DER",
            "seller": seller_id,
            "productSearchRequestData": json.dumps(
                {
                    "marketplace": "ATVPDKIKX0DER",
                    "seller": seller_id,
                    "url": "/sp/ajax/products",
                    "pageSize": 12,
                    "searchKeyword": "",
                    "extraRestrictions": {},
                    "pageNumber": 1,
                }
            ),
        }

        logger.info(f"Fetching total product count for seller: {seller_id}")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await self.session.post(url, headers=headers, data=payload, timeout=15)
                response.raise_for_status()

                try:
                    data = response.json()
                    if "productsTotalCount" in data:
                        return int(data["productsTotalCount"])
                except json.JSONDecodeError:
                    pass

                # Fallback to regex if JSON decoding fails
                match = re.search(r'"productsTotalCount"\s*:\s*(\d+)', response.text)
                if match:
                    return int(match.group(1))

                logger.warning(f"Could not find productsTotalCount in response for seller {seller_id}")
                return 0

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to fetch product count for {seller_id}: {e}")
                await asyncio.sleep(random.uniform(2, 5))
                headers["User-Agent"] = self._get_random_ua()

        logger.error(f"Failed to fetch product count for {seller_id} after {max_retries} attempts.")
        return 0

    async def get_seller_and_products_count(self, url: str) -> dict:
        """
        High-level method returning structured data for CSV saving.
        """
        seller_id = await self.get_seller_id_from_listing(url)
        products_num = 0

        if seller_id:
            products_num = await self.get_products_num_by_seller(seller_id)

        return {
            "URL": url,
            "SellerID": seller_id,
            "ProductsCount": products_num,
        }
