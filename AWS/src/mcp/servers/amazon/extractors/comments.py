from __future__ import annotations
import asyncio
import logging
import random
import re
import json
from typing import Optional
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper
from src.core.models.review import Review

logger = logging.getLogger(__name__)


class CommentsExtractor(AmazonBaseScraper):
    """
    Advanced extractor for Amazon reviews.
    Prioritizes the internal AJAX API for speed and stability,
    with a fallback to HTML page scraping.
    """

    async def get_all_comments(self, asin: str, max_pages: int = 2) -> list[Review]:
        all_reviews = []
        for page in range(1, max_pages + 1):
            reviews = await self._fetch_reviews_via_ajax(asin, page)
            if reviews is None:  # API failed, fallback to HTML
                logger.warning(f"AJAX failed on page {page}, falling back to HTML scraping...")
                reviews = await self._fetch_reviews_via_html(asin, page)

            if not reviews:
                break

            all_reviews.extend(reviews)
            if page < max_pages:
                await asyncio.sleep(random.uniform(1.0, 2.5))
        return all_reviews

    async def _acquire_csrf_token(self, asin: str) -> Optional[str]:
        """
        Visit the product reviews page to let Amazon set the anti-csrftoken-a2z cookie via JS.
        Since curl_cffi doesn't execute JS, we parse the token from the HTML meta/script tags instead.
        """
        reviews_url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
        html = await self.fetch(reviews_url)
        if not html:
            return None

        # Try to find the CSRF token embedded in the page HTML
        match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', html)
        if not match:
            match = re.search(r'anti-csrftoken-a2z["\s:]+([A-Za-z0-9%+/=]+)', html)
        if match:
            token = match.group(1)
            logger.info("Acquired CSRF token from page HTML.")
            return token

        # Fallback: check if cookie was set (won't work without JS, but worth trying)
        token = self.session.cookies.get("anti-csrftoken-a2z")
        if token:
            return token

        logger.warning("Could not acquire anti-csrftoken-a2z from page.")
        return None

    async def _fetch_reviews_via_ajax(self, asin: str, page: int) -> Optional[list[Review]]:
        """
        Attempt to fetch reviews using the undocumented AJAX endpoint.
        Returns None on failure to signal a fallback.
        """
        try:
            # Try to get CSRF token from cookies first, then acquire from page
            csrf_token = self.session.cookies.get("anti-csrftoken-a2z")
            if not csrf_token:
                csrf_token = await self._acquire_csrf_token(asin)
            if not csrf_token:
                logger.warning("Missing 'anti-csrftoken-a2z'. AJAX call will likely fail.")
                return None

            url = f"https://www.amazon.com/portal/customer-reviews/ajax/reviews/get/ref=cm_cr_arp_d_paging_btm_next_{page}"
            referrer = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"

            headers = {
                "accept": "text/html,*/*",
                "accept-language": "en-US,en;q=0.9",
                "anti-csrftoken-a2z": csrf_token,
                "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                "device-memory": "8",
                "downlink": "9",
                "dpr": "1",
                "ect": "4g",
                "priority": "u=1, i",
                "rtt": "0",
                "sec-ch-device-memory": "8",
                "sec-ch-dpr": "1",
                "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="124", "Chromium";v="124"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-ch-viewport-width": "1280",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "viewport-width": "1280",
                "x-requested-with": "XMLHttpRequest",
                "referer": referrer,
            }

            reftag = f"cm_cr_arp_d_paging_btm_next_{page}"
            body = (
                f"sortBy=&reviewerType=all_reviews&formatType=&mediaType=&filterByStar="
                f"&filterByAge=&pageNumber={page}&filterByLanguage=&filterByKeyword="
                f"&shouldAppend=undefined&deviceType=desktop&canShowIntHeader=undefined"
                f"&reviewsShown=undefined&reftag={reftag}&pageSize=10&asin={asin}&scope=reviewsAjax1"
            )

            response_text = await self.fetch(url, method="POST", headers=headers, data=body)
            if not response_text:
                return None

            # Parse the &&& delimited response
            html_content = ""
            parts = response_text.split("&&&")
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                try:
                    data = json.loads(part)
                    if isinstance(data, list) and len(data) >= 3:
                        if data[0] == "append" and data[1] == "#cm_cr-review_list":
                            html_content += data[2]
                except (json.JSONDecodeError, IndexError):
                    continue

            if not html_content:
                logger.warning("AJAX responded but returned no review content.")
                return []

            soup = BeautifulSoup(html_content, "html.parser")
            return self._parse_soup(soup, asin)
        except Exception as e:
            logger.error(f"Error in AJAX review fetch: {e}")
            return None

    async def _fetch_reviews_via_html(self, asin: str, page: int) -> list[Review]:
        """
        Fallback method to scrape the full HTML review page.
        """
        try:
            url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&reviewerType=all_reviews&pageNumber={page}"
            html_content = await self.fetch(url)
            if not html_content:
                return []

            # Detect actual login wall (not just nav bar signin links)
            if 'name="password"' in html_content or 'id="ap_password"' in html_content:
                logger.error(f"LOGIN REQUIRED (HTML Fallback): Amazon is requesting login for {asin}.")
                return []

            soup = BeautifulSoup(html_content, "html.parser")
            return self._parse_soup(soup, asin)
        except Exception as e:
            logger.error(f"Error in HTML review fetch: {e}")
            return []

    def _parse_soup(self, soup: BeautifulSoup, asin: str) -> list[Review]:
        """Shared parser for both AJAX and HTML content."""
        review_elements = soup.find_all({"div", "li"}, {"data-hook": "review"})
        reviews = []
        for el in review_elements:
            try:
                author = el.find("span", class_="a-profile-name").get_text(strip=True)
                rating = int(
                    re.search(
                        r"(\d)", el.find("i", {"data-hook": "review-star-rating"}).get_text(strip=True)
                    ).group(1)
                )
                title = el.find("a", {"data-hook": "review-title"}).get_text(strip=True)
                content = el.find("span", {"data-hook": "review-body"}).get_text(strip=True)
                date = el.find("span", {"data-hook": "review-date"}).get_text(strip=True)
                is_verified = el.find("span", {"data-hook": "avp-badge"}) is not None

                helpful_votes = 0
                helpful_span = el.find("span", {"data-hook": "helpful-vote-statement"})
                if helpful_span:
                    match = re.search(r"(\d+)", helpful_span.get_text(strip=True).replace(",", ""))
                    if match:
                        helpful_votes = int(match.group(1))

                reviews.append(
                    Review(
                        asin=asin,
                        author=author,
                        rating=rating,
                        title=title,
                        content=content,
                        date=date,
                        is_verified=is_verified,
                        helpful_votes=helpful_votes,
                    )
                )
            except Exception:
                continue
        return reviews
