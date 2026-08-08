from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import urllib.parse
from typing import Any

from bs4 import BeautifulSoup
from curl_cffi import requests

from src.core.errors.codes import ErrorCode, classify_http

logger = logging.getLogger(__name__)


class DealHistoryClient:
    """
    Client to fetch off-Amazon deal history.
    Targets top-level deal sites like Slickdeals, DealNews, and Woot (via its
    forums.woot.com @wootbot post history, since Woot has no product search API).
    """

    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        # Base browser fingerprint (safe for most sites)
        self.base_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="146", "Chromium";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }

    async def _fetch_slickdeals(
        self, search_term: str, max_pages: int, date_range_days: int = 1095
    ) -> list[dict[str, Any]]:
        """
        ``date_range_days`` maps to Slickdeals' ``filters[date][]`` search param — an
        arbitrary lookback window in days (confirmed live: e.g. 7/30/90 return nothing
        for a term whose only matches are ~1yr+ old, while 1095/3650/9999 progressively
        surface older results). Defaults to 3 years.
        """
        encoded_term = urllib.parse.quote(search_term)
        all_deals = []

        # Site-specific headers with dynamic referer
        headers = {
            **self.base_headers,
            "referer": "https://slickdeals.net/",
            "origin": "https://slickdeals.net",
            "cache-control": "max-age=0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }

        for page in range(1, max_pages + 1):
            url = (
                f"https://slickdeals.net/search?q={encoded_term}&searchtype=normal"
                f"&filters%5Bforum%5D%5B%5D=&sort=relevance&filters%5Brating%5D%5B%5D=all"
                f"&filters%5Bdate%5D%5B%5D={date_range_days}&filters%5Bprice%5D%5Bmin%5D="
                f"&filters%5Bprice%5D%5Bmax%5D=&filters%5Bstore%5D%5B%5D=1&page={page}"
            )

            try:
                response = await asyncio.to_thread(
                    self.session.get, url, headers=headers, timeout=15
                )
                if response.status_code != 200:
                    code = classify_http(response.status_code, provider="slickdeals")
                    logger.warning(
                        f"Slickdeals [{code}] status {response.status_code} on page {page}. Stopping."
                    )
                    break

                page_deals = self._parse_slickdeals(response.text)
                if not page_deals:
                    logger.info(f"No more deals found on Slickdeals page {page}.")
                    break
                all_deals.extend(page_deals)

                if page < max_pages:
                    await asyncio.sleep(1.0)  # Politeness delay
            except Exception as e:
                logger.error(f"Slickdeals [{ErrorCode.TIMEOUT}] error on page {page}: {e}")
                break
        return all_deals

    async def _fetch_dealnews(self, search_term: str, max_pages: int) -> list[dict[str, Any]]:
        encoded_term = urllib.parse.quote(search_term[:64])
        all_deals = []

        # Site-specific headers with dynamic referer
        headers = {
            **self.base_headers,
            "referer": "https://www.dealnews.com/",
            "origin": "https://www.dealnews.com",
        }

        # 1. Fetch the initial search page to get deal IDs
        url = f"https://www.dealnews.com/s313/Amazon/?search={encoded_term}&sort=featured"
        try:
            response = await asyncio.to_thread(self.session.get, url, headers=headers, timeout=15)
            if response.status_code != 200:
                code = classify_http(response.status_code, provider="dealnews")
                logger.warning(f"DealNews [{code}] status {response.status_code}")
                return []

            html = response.text
            # Parse deals already rendered on the page
            initial_deals = self._parse_dealnews(html)
            all_deals.extend(initial_deals)

            # 2. Extract remaining deal IDs for async pagination
            ids_match = re.search(r'data-ids="([^"]+)"', html)
            if not ids_match or max_pages <= 1:
                return all_deals

            all_ids = ids_match.group(1).split(",")
            remaining_ids = all_ids[len(initial_deals) :]

            # 3. Fetch remaining pages via async grid API
            chunk_size = 20
            num_extra_pages = min(
                max_pages - 1, (len(remaining_ids) + chunk_size - 1) // chunk_size
            )

            for i in range(num_extra_pages):
                offset = i * chunk_size
                chunk = remaining_ids[offset : offset + chunk_size]
                if not chunk:
                    break

                payload = {
                    "i": ",".join(chunk),
                    "e": 0,
                    "c": len(chunk),
                    "g": "ContentCard",
                    "w": 1,
                    "gutter": False,
                    "x": "eyJmb3JjZV9pbWFnZSI6ZmFsc2UsInRpdGxlX2xpbWl0X3NtYWxsIjoyfQ==",
                }
                h_param = base64.b64encode(json.dumps(payload).replace(" ", "").encode()).decode()

                async_url = f"https://www.dealnews.com/async/grids/?h={h_param}"
                async_headers = {**headers, "accept": "dealnews/json, */*; q=0.1"}

                resp = await asyncio.to_thread(
                    self.session.get, async_url, headers=async_headers, timeout=15
                )
                if resp.status_code == 200 and "html" in resp.json():
                    all_deals.extend(self._parse_dealnews(resp.json()["html"]))

                if i < num_extra_pages - 1:
                    await asyncio.sleep(1.0)

        except Exception as e:
            logger.error(f"DealNews [{ErrorCode.TIMEOUT}] error: {e}")

        return all_deals

    # Offer links live under whichever category subdomain the deal belongs to
    # (tools.woot.com, electronics.woot.com, home.woot.com, sports.woot.com, ...),
    # not just "tools" — match any *.woot.com/offers/<slug>.
    _WOOT_BLURB_LINK_RE = re.compile(r"https://[a-z0-9-]+\.woot\.com/offers/[\w-]+", re.I)

    async def _fetch_woot(
        self,
        search_term: str,
        max_pages: int = 1,
        after: str | None = None,
        before: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search the Woot forums for @wootbot posts mentioning search_term. Woot has no
        product search API of its own — every live promotion is instead announced by the
        wootbot forum account, so we search its posts and pull the unique offer link(s)
        out of each post's blurb (a post's blurb repeats the same link twice).

        ``category:6`` scopes the search to the forum's "Deal Chatter" category, where
        wootbot posts. ``after``/``before`` are optional Discourse date filters
        (YYYY-MM-DD) to narrow the search to a specific window.
        """
        query = f"{search_term} category:6"
        if after:
            query += f" after:{after}"
        if before:
            query += f" before:{before}"
        query += " @wootbot"
        encoded_query = urllib.parse.quote(query)

        headers = {
            **self.base_headers,
            "accept": "application/json, text/javascript, */*; q=0.01",
            "discourse-present": "true",
            "x-requested-with": "XMLHttpRequest",
            "x-csrf-token": "undefined",
            "referer": f"https://forums.woot.com/search?q={encoded_query}",
        }

        seen_links: dict[str, dict[str, str]] = {}
        for page in range(1, max_pages + 1):
            url = f"https://forums.woot.com/search?q={encoded_query}&page={page}"
            try:
                response = await asyncio.to_thread(
                    self.session.get, url, headers=headers, timeout=15
                )
                if response.status_code != 200:
                    code = classify_http(response.status_code, provider="woot")
                    logger.warning(
                        f"Woot forum search [{code}] status {response.status_code} on page {page}."
                    )
                    break

                payload = response.json()
                posts = payload.get("posts", [])
                if not posts:
                    break

                # The search response also carries a "topics" array alongside "posts";
                # fancy_title there is the full, untruncated product title, unlike the
                # blurb (see below) which Discourse caps to a fixed length.
                topic_titles = {
                    t["id"]: t.get("fancy_title", "") for t in payload.get("topics", [])
                }

                for post in posts:
                    created_at = post.get("created_at", "")
                    blurb = post.get("blurb", "")
                    links = self._WOOT_BLURB_LINK_RE.findall(blurb)
                    if not links:
                        continue
                    # A wootbot blurb is "<link> <link> <product title>" — the links
                    # repeat, then the title follows. The Discourse search API
                    # truncates `blurb` to a fixed length, and the cutoff sometimes
                    # lands mid-URL on the second (duplicate) link, e.g. ".../open-box"
                    # -> ".../open-bo". The first link is always the intact one, so
                    # use it rather than risking a truncated fragment.
                    link = links[0]
                    title = (
                        topic_titles.get(post.get("topic_id"))
                        or self._WOOT_BLURB_LINK_RE.sub("", blurb).strip()
                    )
                    seen_links.setdefault(link, {"date": created_at, "title": title})

                if page < max_pages:
                    await asyncio.sleep(1.0)  # Politeness delay
            except Exception as e:
                logger.error(f"Woot forum search [{ErrorCode.TIMEOUT}] error on page {page}: {e}")
                break

        return [
            {
                "date": info["date"],
                "title": info["title"],
                "site": "woot.com",
                "type": "Forum Post",
                "deal_url": link,
            }
            for link, info in seen_links.items()
        ]

    def _parse_slickdeals(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        deals = []
        for card in soup.select(".dealCardListView"):
            try:
                title_el = card.select_one(".dealCardListView__title")
                a_el = (
                    title_el
                    if (title_el and title_el.name == "a")
                    else (title_el.find("a") if title_el else None)
                )
                raw_href = a_el.get("href", "") if a_el else ""
                deal_url = (
                    raw_href
                    if raw_href.startswith("http")
                    else (f"https://slickdeals.net{raw_href}" if raw_href else "")
                )

                deals.append(
                    {
                        "date": card.select_one(".slickdealsTimestamp").get("title", ""),
                        "price": self._extract_price(
                            card.select_one(".dealCardListView__finalPrice").get_text(strip=True)
                        ),
                        "discount_pct": self._extract_percentage(
                            card.select_one(".dealCardListView__savings").get_text(strip=True)
                        ),
                        "title": title_el.get_text(strip=True) if title_el else "",
                        "site": "slickdeals.net",
                        "type": "Search Result",
                        "deal_url": deal_url,
                    }
                )
            except Exception:
                continue
        return deals

    _ASIN_IN_URL = re.compile(r"/(?:dp|gp/product)/([A-Z0-9]{10})", re.I)

    async def _follow_redirect_extract_asin(self, click_url: str, referer: str) -> str | None:
        """Follow a tracking redirect and extract the ASIN from the final Amazon URL."""
        headers = {
            **self.base_headers,
            "referer": referer,
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
        }
        try:
            r = await asyncio.to_thread(
                self.session.get, click_url, headers=headers, timeout=15, allow_redirects=True
            )
            final_url: str = r.url if isinstance(r.url, str) else str(r.url)
            m = self._ASIN_IN_URL.search(final_url)
            if m:
                return m.group(1)
            logger.debug(f"Redirect landed on non-ASIN URL: {final_url[:120]}")
        except Exception as e:
            logger.warning(f"[{ErrorCode.TIMEOUT}] Failed to follow redirect {click_url}: {e}")
        return None

    async def _resolve_asin_from_deal_page(self, deal_url: str) -> str | None:
        """
        Fetch a Slickdeals deal detail page, find the slickdeals.net/click tracking
        link for "Visit Amazon", follow its redirect, and extract the ASIN from the
        final Amazon URL.
        """
        if not deal_url:
            return None

        page_headers = {
            **self.base_headers,
            "referer": "https://slickdeals.net/",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
        }

        try:
            resp = await asyncio.to_thread(
                self.session.get, deal_url, headers=page_headers, timeout=15
            )
            if resp.status_code != 200:
                code = classify_http(resp.status_code, provider="slickdeals")
                logger.warning(f"Deal page [{code}] status {resp.status_code}: {deal_url}")
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            # Collect slickdeals.net/click? tracking links; prefer "Visit Amazon" CTA
            click_links: list[str] = []
            for a in soup.find_all("a", href=True):
                href: str = a["href"]
                if "/click?" not in href:
                    continue
                params = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                trd = params.get("trd", [""])[0]
                if "amazon" in trd.lower():
                    click_links.insert(0, href)
                else:
                    click_links.append(href)

            if not click_links:
                logger.debug(f"No /click? links found on {deal_url}")
                return None

            return await self._follow_redirect_extract_asin(click_links[0], referer=deal_url)

        except Exception as e:
            logger.warning(f"[{ErrorCode.TIMEOUT}] Failed to resolve ASIN from {deal_url}: {e}")

        return None

    # Offer pages don't expose a JSON API — the offer record(s) are embedded as a JS
    # array literal (`var offerItems = [{"Asin": ..., "SalePrice": ..., ...}];`) inside
    # a <script> tag on the rendered HTML page.
    _WOOT_OFFER_ITEMS_RE = re.compile(r"var offerItems\s*=\s*(\[.*?\]);")

    async def _resolve_woot_offers(self, offer_url: str) -> list[dict[str, Any]]:
        """
        Fetch a *.woot.com/offers/... page and extract its offer record(s): asin
        (Asin), price (SalePrice), discount_pct (parsed from FormattedDiscount, e.g.
        "35% off" -> 35.0). One offer page can list more than one ASIN (bundles/variants).
        """
        if not offer_url:
            return []

        headers = {**self.base_headers, "referer": "https://forums.woot.com/"}
        try:
            resp = await asyncio.to_thread(self.session.get, offer_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                code = classify_http(resp.status_code, provider="woot")
                logger.warning(f"Woot offer page [{code}] status {resp.status_code}: {offer_url}")
                return []

            m = self._WOOT_OFFER_ITEMS_RE.search(resp.text)
            if not m:
                logger.debug(f"No offerItems found on {offer_url}")
                return []

            try:
                items = json.loads(m.group(1))
            except json.JSONDecodeError as e:
                logger.warning(
                    f"[{ErrorCode.PARSE_ERROR}] Failed to parse offerItems JSON from {offer_url}: {e}"
                )
                return []

            offers: list[dict[str, Any]] = []
            seen_asins: set[str] = set()
            for item in items:
                asin = item.get("Asin")
                if not asin or asin in seen_asins:
                    continue
                seen_asins.add(asin)
                offers.append(
                    {
                        "asin": asin,
                        "price": item.get("SalePrice") or 0.0,
                        "discount_pct": self._extract_percentage(
                            item.get("FormattedDiscount") or ""
                        ),
                    }
                )
            return offers
        except Exception as e:
            logger.warning(f"[{ErrorCode.TIMEOUT}] Failed to resolve offers from {offer_url}: {e}")
            return []

    async def get_deals_for_asins(
        self, asins: set[str], brand: str, max_pages: int = 2
    ) -> dict[str, list[dict[str, Any]]]:
        """
        2-phase ASIN-confirmed deal lookup across Slickdeals, DealNews, and Woot forums,
        shared across every ASIN of one brand — a single brand search + candidate
        resolution pass whose results are then bucketed per target ASIN, instead of
        repeating both phases once per ASIN (the brand search and per-candidate
        resolution are identical regardless of which of the brand's ASINs is being
        confirmed).

        Phase 1: search all sources by brand in parallel → candidate deals with a
                 site-specific URL to resolve.
        Phase 2: resolve each candidate in parallel → bucket by resolved ASIN(s).
          - Slickdeals: click URL is on the deal detail page (one extra fetch per candidate)
          - DealNews:   click URL (lw/click) is already on the search result card
          - Woot:       deal_url is the tools.woot.com offer page itself, which can
                         resolve to more than one ASIN (bundle listings)
        """
        sd_candidates, dn_candidates, woot_candidates = await asyncio.gather(
            self._fetch_slickdeals(brand, max_pages),
            self._fetch_dealnews(brand, max_pages),
            self._fetch_woot(brand, max_pages),
        )
        candidates = sd_candidates + dn_candidates + woot_candidates
        matched: dict[str, list[dict[str, Any]]] = {asin: [] for asin in asins}
        if not candidates:
            logger.info(f"[get_deals_for_asins] No candidates for brand={brand!r}")
            return matched

        sem = asyncio.Semaphore(5)

        async def resolve(deal: dict) -> list[tuple[dict, str | None]]:
            async with sem:
                if deal["site"] == "slickdeals.net":
                    resolved = await self._resolve_asin_from_deal_page(deal.get("deal_url", ""))
                    return [(deal, resolved)]
                elif deal["site"] == "woot.com":
                    offers = await self._resolve_woot_offers(deal.get("deal_url", ""))
                    return [({**deal, **offer}, offer["asin"]) for offer in offers]
                else:
                    # DealNews: deal_url is already the lw/click tracking link
                    resolved = await self._follow_redirect_extract_asin(
                        deal.get("deal_url", ""), referer="https://www.dealnews.com/"
                    )
                    return [(deal, resolved)]

        results = await asyncio.gather(*(resolve(d) for d in candidates), return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                continue
            for deal, resolved_asin in r:
                if resolved_asin in matched:
                    matched[resolved_asin].append({**deal, "confirmed_asin": resolved_asin})

        logger.info(
            f"[get_deals_for_asins] brand={brand!r} targets={sorted(asins)}: "
            f"{sum(len(v) for v in matched.values())}/{len(candidates)} confirmed "
            f"(SD={len(sd_candidates)} DN={len(dn_candidates)} Woot={len(woot_candidates)})"
        )
        return matched

    def _parse_dealnews(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        deals = []
        for card in soup.select(".content-view, .content-card"):
            try:
                price = self._extract_price(card.select_one(".callout").get_text(strip=True))
                comp_price = self._extract_price(
                    card.select_one(".callout-comparison").get_text(strip=True)
                )
                discount_pct = (
                    round(((comp_price - price) / comp_price) * 100, 1)
                    if comp_price and price
                    else 0.0
                )

                date_text = ""
                script = card.find("script", type="application/ld+json")
                if script:
                    data = json.loads(script.string)
                    offers = data.get("offers", [])
                    date_text = (offers[0] if isinstance(offers, list) else offers).get(
                        "validFrom", ""
                    )

                title_el = card.select_one(".title-link, .title")
                a_el = (
                    title_el
                    if (title_el and title_el.name == "a")
                    else (title_el.find("a") if title_el else None)
                )
                lw_click = a_el.get("href", "") if a_el else ""
                deal_url = (
                    lw_click
                    if lw_click.startswith("http")
                    else (f"https://www.dealnews.com{lw_click}" if lw_click else "")
                )

                deals.append(
                    {
                        "date": date_text,
                        "price": price,
                        "discount_pct": discount_pct,
                        "title": title_el.get_text(strip=True) if title_el else "",
                        "site": "dealnews.com",
                        "type": "Search Result",
                        "deal_url": deal_url,
                    }
                )
            except Exception:
                continue
        return deals

    def _extract_price(self, text: str) -> float:
        match = re.search(r"\$([\d,]+(?:\.\d+)?)", text.replace(",", ""))
        return float(match.group(1)) if match else 0.0

    def _extract_percentage(self, text: str) -> float:
        match = re.search(r"([\d\.]+)%", text)
        return float(match.group(1)) if match else 0.0
