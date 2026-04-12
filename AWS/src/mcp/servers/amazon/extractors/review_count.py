from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class ReviewRatioExtractor(AmazonBaseScraper):
    """
    Fetches both global ratings count and written reviews count for a product.

    Uses the dedicated reviews page (/product-reviews/{asin}) which shows both
    numbers in a single request:
      - GlobalRatings : all star ratings (with or without written text)
      - WrittenReviews: only ratings that include written review text

    Natural ratio WrittenReviews / GlobalRatings ≈ 0.10 (1:10).
    A ratio > 0.50 (e.g. 298 written / 470 global) is a strong fake-review signal.
    """

    async def get_review_count(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Returns {"ASIN", "GlobalRatings", "WrittenReviews", "Ratio"}.
        Missing values are None; Ratio is None when GlobalRatings is 0/None.
        """
        # Normalise host: strip trailing slash, add scheme if missing
        host = host.rstrip("/")
        if not host.startswith("http"):
            host = "https://" + host
        url = f"{host}/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
        logger.info(f"Fetching review counts for ASIN: {asin} url={url}")

        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch review page for {asin}")
            return {"ASIN": asin, "GlobalRatings": None, "WrittenReviews": None, "Ratio": None}

        soup = BeautifulSoup(html, "html.parser")
        global_ratings = None
        written_reviews = None

        # ── Primary: data-hook="cr-filter-info-review-rating-count" ──────────
        # Text examples:
        #   "4,567 global ratings | 1,234 with reviews"
        #   "470 global ratings | 298 customer reviews"
        #   "272,876 global ratings"
        info = soup.find(attrs={"data-hook": "cr-filter-info-review-rating-count"})
        if info:
            text = info.get_text(" ", strip=True)
            m = re.search(r"([\d,]+)\s+global\s+ratings?", text, re.IGNORECASE)
            if m:
                global_ratings = int(m.group(1).replace(",", ""))
            m = re.search(r"([\d,]+)\s+(?:with\s+reviews?|customer\s+reviews?)", text, re.IGNORECASE)
            if m:
                written_reviews = int(m.group(1).replace(",", ""))

        # ── Fallback A: ratings histogram header ──────────────────────────────
        # <span data-hook="rating-out-of-text"> or averageStarRatingNumerical area
        if global_ratings is None:
            m = re.search(r"([\d,]+)\s+global\s+ratings?", html, re.IGNORECASE)
            if m:
                global_ratings = int(m.group(1).replace(",", ""))

        # ── Fallback B: "Showing X–Y of Z reviews" in pagination ─────────────
        if written_reviews is None:
            pag = soup.find(attrs={"data-hook": "cr-filter-info-section"})
            if pag:
                m = re.search(r"([\d,]+)\s+reviews?", pag.get_text(), re.IGNORECASE)
                if m:
                    written_reviews = int(m.group(1).replace(",", ""))

        # ── Fallback C: legacy acrCustomerReviewText on /dp/ pages ───────────
        if global_ratings is None:
            el = soup.find("span", id="acrCustomerReviewText")
            if el:
                text = el.get_text(strip=True)
                m = re.search(r"([\d,]+)", text)
                if m:
                    global_ratings = int(m.group(1).replace(",", ""))

        ratio = None
        if global_ratings and written_reviews is not None:
            ratio = round(written_reviews / global_ratings, 3) if global_ratings > 0 else None

        if global_ratings is None:
            logger.warning(
                f"[ReviewCount] {asin}: failed to parse any counts from {url} "
                f"(html_len={len(html)}, may be CAPTCHA/bot-check page)"
            )
        logger.info(
            f"[ReviewCount] {asin}: global={global_ratings}, written={written_reviews}, ratio={ratio}"
        )
        return {
            "ASIN": asin,
            "GlobalRatings": global_ratings,
            "WrittenReviews": written_reviews,
            "Ratio": ratio,
        }
