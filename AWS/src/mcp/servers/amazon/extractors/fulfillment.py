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
        host = host.rstrip("/")
        if not host.startswith(("http://", "https://")):
            host = "https://" + host
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching fulfillment info for: {url}")

        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for {url}")
            return {"ASIN": asin, "URL": url, "FulfilledBy": None}

        soup = BeautifulSoup(html, "html.parser")
        fulfilled_by = None
        sold_by = None

        # New ODF (Offer Display Features) structure.
        # FBA: fulfillerInfoFeature has a "Ships from" span (e.g. "Amazon"),
        #      merchantInfoFeature has a separate "Sold by" seller link.
        # FBM: fulfillerInfoFeature is empty, merchantInfoFeature shows
        #      a combined "Shipper / Seller" label with the seller link.
        fulfiller_div = soup.find("div", id="fulfillerInfoFeature_feature_div")
        merchant_div = soup.find("div", id="merchantInfoFeature_feature_div")
        if fulfiller_div is not None or merchant_div is not None:
            ships_from = None
            seller_name = None
            if fulfiller_div:
                msg = fulfiller_div.find("span", class_="offer-display-feature-text-message")
                if msg:
                    ships_from = msg.get_text(strip=True)
            if merchant_div:
                seller_link = merchant_div.find("a", id="sellerProfileTriggerId")
                if seller_link:
                    seller_name = seller_link.get_text(strip=True)
                else:
                    # Amazon-direct: no seller profile link, plain span (e.g. "Amazon.com")
                    msg = merchant_div.find("span", class_="offer-display-feature-text-message")
                    if msg:
                        seller_name = msg.get_text(strip=True)
            if ships_from:
                fulfilled_by = ships_from  # FBA: "Amazon"
                sold_by = seller_name
            elif seller_name:
                fulfilled_by = seller_name  # FBM: fulfiller == seller
                sold_by = seller_name

        # Older tabular buybox structure
        if not fulfilled_by:
            ships_from_div = soup.find(
                "div", class_="tabular-buybox-text", attrs={"merchant-info": True}
            )
            if ships_from_div:
                text = ships_from_div.get_text(strip=True)
                if text:
                    fulfilled_by = text

        # Seller profile link with FBA keyword check
        if not fulfilled_by:
            merchant_info = soup.find("a", id="sellerProfileTriggerId")
            if merchant_info:
                if "Fulfilled by Amazon" in html or "Ships from Amazon" in html:
                    fulfilled_by = "Amazon"
                else:
                    fulfilled_by = merchant_info.get_text(strip=True)

        # Legacy regex fallback
        if not fulfilled_by:
            match = re.search(r"Fulfilled by (.*?)</span></a><span>(.*?).[\s]?</span>", html)
            if match:
                fulfilled_by = f"{match.group(1)}{match.group(2)}".strip()

        # Final text-scan fallback
        if not fulfilled_by:
            if "Ships from and sold by Amazon.com" in html:
                fulfilled_by = "Amazon"

        return {"ASIN": asin, "URL": url, "FulfilledBy": fulfilled_by, "SoldBy": sold_by}
