from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class BrandExtractor(AmazonBaseScraper):
    """
    Extracts the brand name from a single Amazon product listing.

    Four selectors tried in priority order:
      A) img#brandLogoHiResByline  — premium brand logo (alt attribute, exact name)
      B) a#visitStoreDesktopUrl    — "Visit the X Store" link (premium layout)
      C) a#bylineInfo              — "Visit the X Store" link (legacy layout)
      D) span#bylineInfo           — "Brand: X" plain span (legacy layout)
    """

    async def get_brand(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Returns {"ASIN": asin, "Brand": str | None}.
        Brand is None when none of the four selectors match.
        """
        host = host.rstrip("/")
        if not host.startswith(("http://", "https://")):
            host = "https://" + host
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching brand for ASIN: {asin}")

        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch product page for {asin}")
            return {"ASIN": asin, "Brand": None}

        soup = BeautifulSoup(html, "html.parser")
        brand = None

        # A) Premium layout — brand logo img alt
        logo = soup.find("img", id="brandLogoHiResByline")
        if logo and logo.get("alt", "").strip():
            brand = logo["alt"].strip()

        # B) Premium layout — "Visit the X Store" anchor
        if not brand:
            link = soup.find("a", id="visitStoreDesktopUrl")
            if link:
                m = re.match(r"Visit the (.+?) Store$", link.get_text(strip=True))
                brand = m.group(1) if m else link.get_text(strip=True) or None

        # C) Legacy layout — bylineInfo anchor
        if not brand:
            link = soup.find("a", id="bylineInfo")
            if link:
                m = re.match(r"Visit the (.+?) Store$", link.get_text(strip=True))
                brand = m.group(1) if m else link.get_text(strip=True) or None

        # D) Legacy layout — bylineInfo span ("Brand: X")
        if not brand:
            span = soup.find("span", id="bylineInfo")
            if span:
                text = span.get_text(strip=True)
                brand = re.sub(r"^Brand:\s*", "", text, flags=re.I) or None

        if brand:
            logger.info(f"[Brand] {asin}: {brand!r}")
        else:
            logger.warning(f"[Brand] {asin}: brand not found at {url}")

        return {"ASIN": asin, "Brand": brand}
