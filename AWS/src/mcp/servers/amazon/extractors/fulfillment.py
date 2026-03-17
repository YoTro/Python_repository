from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class FulfillmentExtractor(AmazonBaseScraper):
    """
    Extractor to determine who fulfills the product (e.g., FBA / Amazon or Merchant).
    """

    async def get_fulfillment_info(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and extract fulfillment information.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host (default: .com).
        :return: A dictionary containing ASIN, URL, and FulfilledBy.
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching fulfillment info for: {url}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for {url}")
            return {"ASIN": asin, "URL": url, "FulfilledBy": None}
            
        soup = BeautifulSoup(html, 'html.parser')
        fulfilled_by = None
        
        # In modern Amazon DOM, "Ships from" and "Sold by" are often in a tabular buybox
        ships_from_div = soup.find('div', class_='tabular-buybox-text', attrs={'merchant-info': True})
        if ships_from_div:
            text = ships_from_div.get_text(strip=True)
            if text:
                fulfilled_by = text
        
        # Another common modern structure
        if not fulfilled_by:
            merchant_info = soup.find('a', id='sellerProfileTriggerId')
            if merchant_info:
                # If there's a merchant link, but we also check if it's FBA
                if "Fulfilled by Amazon" in html or "Ships from Amazon" in html:
                    fulfilled_by = "Amazon"
                else:
                    fulfilled_by = merchant_info.get_text(strip=True)
        
        # Legacy regex fallback
        if not fulfilled_by:
            match = re.search(r'Fulfilled by (.*?)</span></a><span>(.*?).[\s]?</span>', html)
            if match:
                fulfilled_by = f"{match.group(1)}{match.group(2)}".strip()
                
        # Final fallback by just looking at text
        if not fulfilled_by:
            if "Ships from and sold by Amazon.com" in html:
                fulfilled_by = "Amazon"
                
        return {
            "ASIN": asin,
            "URL": url,
            "FulfilledBy": fulfilled_by
        }
