from __future__ import annotations
import asyncio
import logging
import re
import json
import time
import html as html_lib
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class BestSellersExtractor(AmazonBaseScraper):
    """
    Extractor for Amazon Best Sellers pages.
    Parses pre-rendered DOM items (1-30) then calls Amazon's internal ACP nextPage
    API to fetch lazy-loaded items (31-50) with full product details.
    Supports pagination (?pg=1, ?pg=2) for up to 100 items.
    """

    async def get_bestsellers(self, url: str, max_pages: int = 2) -> list:
        """
        Fetch Best Sellers pages and extract all ranked products.

        :param url: The Amazon Best Sellers URL.
        :param max_pages: Number of pagination pages to scrape (default 2 = 100 items).
        :return: A list of dicts with Rank, ASIN, Title, Image, Stars, Reviews, Price.
        """
        all_results = []
        for page_num in range(1, max_pages + 1):
            page_url = self._build_page_url(url, page_num)
            logger.info(f"Fetching best sellers page {page_num}: {page_url}")
            raw_html = await self.fetch(page_url)
            if not raw_html:
                logger.warning(f"Failed to fetch page {page_num}")
                break

            logger.info(f"Fetched page {page_num} ({len(raw_html)} bytes). Parsing...")

            # Parse pre-rendered DOM items (1-30)
            dom_items = self._parse_dom_cards(raw_html)
            logger.info(f"Page {page_num}: {len(dom_items)} DOM items")
            all_results.extend(dom_items)

            # Fetch lazy-loaded items (31-50) via ACP nextPage API
            lazy_items = await self._fetch_lazy_items(raw_html)
            logger.info(f"Page {page_num}: {len(lazy_items)} lazy-loaded items")
            all_results.extend(lazy_items)

        # Deduplicate by ASIN, sorted by rank
        seen = set()
        deduped = []
        for item in sorted(all_results, key=lambda x: int(x["Rank"] or 999)):
            if item["ASIN"] and item["ASIN"] not in seen:
                seen.add(item["ASIN"])
                deduped.append(item)

        logger.info(f"Total unique BSR items: {len(deduped)}")
        return deduped

    async def _fetch_lazy_items(self, raw_html: str) -> list:
        """
        Call Amazon's internal ACP nextPage API to fetch lazy-loaded items.
        Extracts widget path, ACP token, and item metadata from the initial page HTML,
        then POSTs to the nextPage endpoint to get full product card HTML.
        """
        # Extract ACP path and token from the initial page
        path_match = re.search(r'data-acp-path="(/acp/[^"]+)"', raw_html)
        token_match = re.search(r'data-acp-params="([^"]+)"', raw_html)
        if not path_match or not token_match:
            logger.warning("Could not find ACP path/token for lazy-load API")
            return []

        acp_path = path_match.group(1).rstrip('/')
        acp_token = token_match.group(1)

        # Extract category node for reftag
        cat_match = re.search(r'zg_bs_g_(\d+)', raw_html)
        cat_node = cat_match.group(1) if cat_match else ""

        # Determine which ASINs are already in DOM cards
        dom_asins = set(re.findall(r'id="gridItemRoot[^"]*"', raw_html))
        soup = BeautifulSoup(raw_html, "html.parser")
        dom_asin_set = set()
        for card in soup.find_all("div", id=re.compile(r"^gridItemRoot")):
            link = card.find("a", class_="a-link-normal")
            if link:
                m = re.search(r"/dp/([A-Z0-9]{10})", link.get("href", ""))
                if m:
                    dom_asin_set.add(m.group(1))

        # Extract ALL embedded JSON metadata items
        decoded = html_lib.unescape(raw_html)
        item_pattern = (
            r'(\{"id":"([A-Z0-9]{10})","metadataMap":\{"render\.zg\.rank":"(\d+)"'
            r'[^}]*\},"linkParameters":\{[^}]*\}\})'
        )
        all_raw_items = re.findall(item_pattern, decoded)

        # Filter to items NOT in DOM (the lazy-loaded ones)
        lazy_raw = [
            (full_json, asin, rank)
            for full_json, asin, rank in all_raw_items
            if asin not in dom_asin_set
        ]

        if not lazy_raw:
            return []

        # Find the offset (the lowest rank among lazy items)
        min_rank = min(int(rank) for _, _, rank in lazy_raw)
        offset = min_rank - 1  # 0-based offset

        indexes = list(range(offset, offset + len(lazy_raw)))
        body = {
            "faceoutkataname": "GeneralFaceout",
            "ids": [item[0] for item in lazy_raw],
            "indexes": indexes,
            "linkparameters": "",
            "offset": str(offset),
            "reftagprefix": f"zg_bs_g_{cat_node}" if cat_node else "",
        }

        api_url = (
            f"https://www.amazon.com{acp_path}/nextPage"
            f"?page-type=zeitgeist&stamp={int(time.time() * 1000)}"
        )

        try:
            resp = await self.session.post(
                api_url,
                json=body,
                headers={
                    "accept": "text/html, application/json",
                    "content-type": "application/json",
                    "x-amz-acp-params": acp_token,
                    "x-requested-with": "XMLHttpRequest",
                },
            )
            if resp.status_code != 200:
                logger.warning(f"ACP nextPage returned {resp.status_code}")
                return []

            return self._parse_dom_cards(resp.text)

        except Exception as e:
            logger.error(f"ACP nextPage request failed: {e}")
            return []

    def _parse_dom_cards(self, html: str) -> list:
        """Parse all gridItemRoot product cards from HTML."""
        soup = BeautifulSoup(html, "html.parser")
        results = []
        for card in soup.find_all("div", id=re.compile(r"^gridItemRoot")):
            item = self._parse_card(card)
            if item["ASIN"]:
                results.append(item)
        return results

    def _parse_card(self, card) -> dict:
        """Extract product data from a single gridItemRoot DOM element."""
        item = {
            "Rank": None,
            "ASIN": None,
            "Title": None,
            "Image": None,
            "Stars": None,
            "Reviews": None,
            "Price": None,
        }

        # Rank
        rank_span = card.find("span", class_="zg-bdg-text")
        if rank_span:
            item["Rank"] = rank_span.get_text(strip=True).replace("#", "")

        # ASIN and Title
        link = card.find("a", class_="a-link-normal")
        if link:
            href = link.get("href", "")
            match = re.search(r"/dp/([A-Z0-9]{10})", href)
            if match:
                item["ASIN"] = match.group(1)

            title_div = card.find(
                "div", class_=re.compile(r"p13n-sc-css-line-clamp")
            )
            if title_div:
                item["Title"] = title_div.get_text(strip=True)
            else:
                img = card.find("img")
                if img and img.get("alt"):
                    item["Title"] = img.get("alt")

        # Image
        img = card.find("img")
        if img:
            item["Image"] = img.get("src")

        # Stars and Reviews
        icon_row = card.find("div", class_="a-icon-row")
        if icon_row:
            # Modern: aria-label="4.4 out of 5 stars, 272,876 ratings"
            star_link = icon_row.find("a", attrs={"aria-label": re.compile(r"out of 5 stars")})
            if star_link:
                label = star_link.get("aria-label", "")
                m = re.search(r"([\d.]+)\s+out of 5 stars", label)
                if m:
                    item["Stars"] = m.group(1)
            else:
                # Legacy: <a title="4.4 out of 5 stars">
                star_span = icon_row.find("a", title=re.compile(r"out of 5 stars"))
                if star_span:
                    item["Stars"] = star_span.get("title").replace(" out of 5 stars", "")

            review_span = icon_row.find("span", class_="a-size-small")
            if review_span:
                item["Reviews"] = review_span.get_text(strip=True)

        # Price
        price_span = card.find("span", class_="a-color-price")
        if price_span:
            item["Price"] = price_span.get_text(strip=True)

        return item

    @staticmethod
    def _build_page_url(base_url: str, page_num: int) -> str:
        """Build paginated URL. Amazon BSR uses ?pg=N parameter."""
        if page_num <= 1:
            return base_url
        clean = re.sub(r"[?&]pg=\d+", "", base_url)
        separator = "&" if "?" in clean else "?"
        return f"{clean}{separator}pg={page_num}"
