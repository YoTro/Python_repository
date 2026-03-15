from __future__ import annotations
import logging
import re
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class PastMonthSalesExtractor(AmazonBaseScraper):
    """
    Extractor for the "X bought in past month" data point.
    """

    async def get_past_month_sales(self, url_or_asin: str) -> dict:
        """
        Extracts the past month sales figure from a product detail page.
        
        :param url_or_asin: The ASIN or full URL of the product.
        :return: A dictionary containing the ASIN and the extracted sales figure (or None if not found).
        """
        if "http" not in url_or_asin:
            asin = url_or_asin
            url = f"https://www.amazon.com/dp/{asin}"
        else:
            url = url_or_asin
            # Attempt to extract ASIN from URL
            match = re.search(r'/dp/([A-Z0-9]{10})', url)
            asin = match.group(1) if match else "Unknown"

        logger.info(f"Fetching past month sales for ASIN: {asin}")
        html = await self.fetch(url)

        result = {"ASIN": asin, "PastMonthSales": None}

        if not html:
            return result

        # Method 1: Exact match based on the provided HTML structure
        match = re.search(r'<span id="social-proofing-faceout-title-tk_bought"[^>]*>.*?<span class="a-text-bold">([^<]+) bought</span>.*?<span>\s*in past month</span>', html, re.DOTALL | re.IGNORECASE)
        if match:
            result["PastMonthSales"] = match.group(1).strip()
            logger.debug(f"Found sales via primary regex: {result['PastMonthSales']}")
            return result

        # Method 2: Broader regex for various possible layouts
        # Matches e.g., "4K+ bought in past month" or "800+ bought in past month"
        match = re.search(r'>\s*([0-9KkM\+]+)\s+bought in past month\s*<', html, re.IGNORECASE)
        if match:
            result["PastMonthSales"] = match.group(1).strip()
            logger.debug(f"Found sales via fallback regex: {result['PastMonthSales']}")
            return result
            
        # Method 3: Look for the text across tags
        match = re.search(r'([0-9KkM\+]+)\s*bought\s*</span>\s*<span>\s*in past month', html, re.IGNORECASE)
        if match:
            result["PastMonthSales"] = match.group(1).strip()
            logger.debug(f"Found sales via cross-tag regex: {result['PastMonthSales']}")
            return result

        logger.info(f"No 'past month sales' data found for ASIN {asin}.")
        return result
