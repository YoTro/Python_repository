from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time

from bs4 import BeautifulSoup

from src.core.models.review import Review
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class CommentsExtractor(AmazonBaseScraper):
    """
    Advanced extractor for Amazon reviews.
    Prioritizes the internal AJAX API for speed and stability,
    with a fallback to HTML page scraping.
    """

    async def get_all_comments(self, asin: str, max_pages: int = 3) -> list[Review]:
        all_reviews = []
        next_page_token = None
        seen_review_ids: set[str] = set()
        for page in range(1, max_pages + 1):
            reviews, next_page_token = await self._fetch_reviews_via_ajax(
                asin, page, next_page_token
            )
            if reviews is None:  # API failed, fallback to HTML
                logger.warning(f"AJAX failed on page {page}, falling back to HTML scraping...")
                reviews, next_page_token = await self._fetch_reviews_via_html(
                    asin, page, next_page_token
                )

            if not reviews:
                break

            # Deduplicate: HTML fallback without nextPageToken repeats page 1 silently
            new_reviews = [r for r in reviews if (r.author, r.title) not in seen_review_ids]
            if not new_reviews:
                logger.info(f"Page {page} returned only already-seen reviews — stopping early.")
                break
            seen_review_ids.update((r.author, r.title) for r in new_reviews)
            all_reviews.extend(new_reviews)

            if not next_page_token and page < max_pages:
                logger.info(f"No nextPageToken after page {page} — cannot paginate further.")
                break

            if page < max_pages:
                await asyncio.sleep(random.uniform(1.0, 2.5))
        return all_reviews

    def _extract_next_page_token(self, soup: BeautifulSoup) -> str | None:
        """Extract nextPageToken from pagination section."""
        # Method 1: Look for pagination 'Next' link
        next_li = soup.find("li", class_="a-last")
        if next_li:
            link = next_li.find("a")
            if link and link.get("href"):
                match = re.search(r"nextPageToken=([^&]+)", link.get("href"))
                if match:
                    return match.group(1)

        # Method 2: Look for 'Next' link by data-hook
        next_link = soup.find("a", {"data-hook": "pagination-next"})
        if next_link and next_link.get("href"):
            match = re.search(r"nextPageToken=([^&]+)", next_link.get("href"))
            if match:
                return match.group(1)

        # Method 3: Look for data-next-page-token attribute
        token_el = soup.find(attrs={"data-next-page-token": True})
        if token_el:
            return token_el.get("data-next-page-token")

        return None

    async def _acquire_csrf_token(self, asin: str) -> tuple[str | None, str | None]:
        """
        Visit the product reviews page to let Amazon set the anti-csrftoken-a2z cookie via JS.
        Since curl_cffi doesn't execute JS, we parse the token from the HTML meta/script tags instead.
        Also extracts the initial nextPageToken for pagination.
        """
        # Primary URL; falls back to the HTML-fallback URL if it returns nothing,
        # which happens on some ASINs where the dp-sourced ref tag redirects to 404.
        reviews_url = (
            f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
        )
        html = await self.fetch(reviews_url)
        if not html:
            reviews_url = (
                f"https://www.amazon.com/product-reviews/{asin}"
                f"?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
            )
            html = await self.fetch(reviews_url)
        if not html:
            return None, None

        # Debug: Save HTML to file to inspect manually if needed
        # with open("debug_reviews.html", "w", encoding="utf-8") as f:
        #     f.write(html)

        # Try to find the CSRF token embedded in the page HTML
        csrf_token = None

        # Pattern 1: JSON-like config
        match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', html)
        if match:
            csrf_token = match.group(1)
            # logger.info("Acquired CSRF token from 'csrfToken' JSON.")

        # Pattern 2: anti-csrftoken-a2z — require ≥20 chars to avoid matching HTML attr fragments
        if not csrf_token:
            match = re.search(r'anti-csrftoken-a2z["\s:=]+([A-Za-z0-9%+/]{20,})', html)
            if match:
                csrf_token = match.group(1)

        # Pattern 3: Hidden input
        if not csrf_token:
            soup = BeautifulSoup(html, "html.parser")
            token_input = soup.find("input", {"name": "anti-csrftoken-a2z"})
            if token_input:
                csrf_token = token_input.get("value")
                # logger.info("Acquired CSRF token from hidden input.")

        # If still missing, check session cookies
        if not csrf_token:
            csrf_token = self.session.cookies.get("anti-csrftoken-a2z")
            if csrf_token:
                logger.info("Acquired CSRF token from session cookies.")

        # Fallback: login page always exposes the token in an <input> field
        if not csrf_token:
            login_url = "https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=usflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
            login_html = await self.fetch(login_url)
            if login_html:
                login_soup = BeautifulSoup(login_html, "html.parser")
                token_input = login_soup.find("input", {"name": "anti-csrftoken-a2z"})
                if token_input:
                    csrf_token = token_input.get("value")
                if not csrf_token:
                    match = re.search(
                        r'anti-csrftoken-a2z["\s:=]+([A-Za-z0-9%+/]{20,})', login_html
                    )
                    if match:
                        csrf_token = match.group(1)

        if not csrf_token:
            logger.warning("Failed to find CSRF token in HTML or cookies. AJAX will likely fail.")

        soup = BeautifulSoup(html, "html.parser")
        next_token = self._extract_next_page_token(soup)

        return csrf_token, next_token

    async def _fetch_reviews_via_ajax(
        self, asin: str, page: int, next_page_token: str | None = None
    ) -> tuple[list[Review] | None, str | None]:
        """
        Attempt to fetch reviews using the undocumented AJAX endpoint.
        Returns (None, None) on failure to signal a fallback.
        """
        try:
            # Try to get CSRF token from cookies first, then acquire from page
            csrf_token = self.session.cookies.get("anti-csrftoken-a2z")
            if not csrf_token:
                csrf_token, initial_token = await self._acquire_csrf_token(asin)
                if not next_page_token:
                    next_page_token = initial_token

            if not csrf_token:
                logger.warning("Missing 'anti-csrftoken-a2z'. AJAX call will likely fail.")
                return None, None

            reftag = f"cm_cr_getr_d_paging_btm_{page}"
            url = f"https://www.amazon.com/portal/customer-reviews/ajax/reviews/get/ref={reftag}"
            referrer = (
                f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
            )
            if next_page_token:
                referrer += f"&nextPageToken={next_page_token}"

            should_append = "true" if page > 1 else "false"
            headers = {
                "accept": "text/html,*/*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9",
                "anti-csrftoken-a2z": csrf_token,
                "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                "device-memory": "8",
                "downlink": "9",
                "dpr": "1",
                "ect": "4g",
                "origin": "https://www.amazon.com",
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
                "x-amzn-flow-closure-id": str(int(time.time())),
                "x-requested-with": "XMLHttpRequest",
                "referer": referrer,
            }

            body = (
                f"sortBy=&reviewerType=all_reviews&formatType=&mediaType=&filterByStar="
                f"&filterByAge=&pageNumber={page}&filterByLanguage=&filterByKeyword="
                f"&shouldAppend={should_append}&deviceType=desktop&canShowIntHeader=true"
                f"&reviewsShown=undefined&reftag={reftag}&pageSize=10&asin={asin}&scope=reviewsAjax1"
            )
            if next_page_token:
                body += f"&nextPageToken={next_page_token}"

            response_text = await self.fetch(url, method="POST", headers=headers, data=body)
            if not response_text:
                return None, None

            # Parse the &&& delimited response
            html_content = ""
            next_token = None
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
                        elif (data[0] == "set" or data[0] == "update") and data[
                            1
                        ] == "#cm_cr-pagination_bar":
                            pagination_soup = BeautifulSoup(data[2], "html.parser")
                            next_token = self._extract_next_page_token(pagination_soup)
                except (json.JSONDecodeError, IndexError):
                    continue

            if not html_content:
                logger.warning("AJAX responded but returned no review content.")
                return [], next_token

            soup = BeautifulSoup(html_content, "html.parser")
            reviews = self._parse_soup(soup, asin)

            # If next_token wasn't in pagination_bar, try searching in the main review list HTML
            if not next_token:
                next_token = self._extract_next_page_token(soup)

            return reviews, next_token
        except Exception as e:
            logger.error(f"Error in AJAX review fetch: {e}")
            return None, None

    async def _fetch_reviews_via_html(
        self, asin: str, page: int, next_page_token: str | None = None
    ) -> tuple[list[Review], str | None]:
        """
        Fallback method to scrape the full HTML review page.
        """
        try:
            url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&reviewerType=all_reviews&pageNumber={page}"
            if next_page_token:
                url += f"&nextPageToken={next_page_token}"
            html_content = await self.fetch(url)
            if not html_content:
                return [], None

            # Detect bot-detection redirect: Amazon returns the homepage (HTTP 200)
            # instead of a review page when the session is blocked or rate-limited.
            if 'data-hook="review"' not in html_content and (
                "Spend less. Smile more." in html_content
                or "<title>Amazon.com</title>" in html_content
            ):
                logger.warning(
                    f"Bot detection triggered for {asin} page {page} — "
                    "Amazon returned homepage instead of reviews."
                )
                return [], None

            # Detect actual login wall (not just nav bar signin links)
            if 'name="password"' in html_content or 'id="ap_password"' in html_content:
                logger.error(
                    f"LOGIN REQUIRED (HTML Fallback): Amazon is requesting login for {asin}."
                )
                return [], None

            soup = BeautifulSoup(html_content, "html.parser")
            reviews = self._parse_soup(soup, asin)
            next_token = self._extract_next_page_token(soup)
            return reviews, next_token
        except Exception as e:
            logger.error(f"Error in HTML review fetch: {e}")
            return [], None

    def _parse_soup(self, soup: BeautifulSoup, asin: str) -> list[Review]:
        """Shared parser for both AJAX and HTML content."""
        review_elements = soup.find_all({"div", "li"}, {"data-hook": "review"})
        reviews = []
        for el in review_elements:
            try:
                author = el.find("span", class_="a-profile-name").get_text(strip=True)
                rating = int(
                    re.search(
                        r"(\d)",
                        el.find("i", {"data-hook": "review-star-rating"}).get_text(strip=True),
                    ).group(1)
                )
                title_anchor = el.find("a", {"data-hook": "review-title"})
                # The anchor contains an <i data-hook="review-star-rating"> child whose
                # text ("5.0 out of 5 stars") would be prepended to the title by get_text().
                # Decompose it so only the actual title <span> text is returned.
                star_i = title_anchor.find("i", {"data-hook": "review-star-rating"})
                if star_i:
                    star_i.decompose()
                title = title_anchor.get_text(strip=True)
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
