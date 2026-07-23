from __future__ import annotations

import logging

from bs4 import BeautifulSoup

from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

_PERIODS = [
    ("thirty", "30d"),
    ("ninety", "90d"),
    ("year", "365d"),
    ("lifetime", "lifetime"),
]


class SellerFeedbackExtractor(AmazonBaseScraper):
    """
    Extractor for seller ratings across all time windows from the Amazon storefront profile.
    """

    async def get_seller_feedback_count(
        self, seller_id: str, host: str = "https://www.amazon.com"
    ) -> dict:
        if host and not host.startswith(("http://", "https://")):
            host = f"https://{host}"
        url = f"{host}/sp?seller={seller_id}"
        logger.info(f"Fetching feedback for seller: {seller_id}")

        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for seller {seller_id}")
            return {"SellerID": seller_id, "FeedbackCount": None}

        soup = BeautifulSoup(html, "html.parser")
        feedback_count: dict[str, dict] = {}

        for div_suffix, label in _PERIODS:
            container = soup.find("div", id=f"rating-{div_suffix}")
            if not container:
                continue
            score_span = container.find("span", class_="ratings-reviews")
            count_span = container.find("span", class_="ratings-reviews-count")
            feedback_count[label] = {
                "score": float(score_span.get_text(strip=True)) if score_span else None,
                "count": int(count_span.get_text(strip=True).replace(",", ""))
                if count_span
                else None,
            }

        if feedback_count:
            logger.info(
                f"[feedback] {seller_id} — captured {list(feedback_count)}: "
                + ", ".join(
                    f"{k}={v['count']}({v['score']}★)"
                    for k, v in feedback_count.items()
                    if v.get("count") is not None
                )
            )
        return {"SellerID": seller_id, "FeedbackCount": feedback_count or None}
