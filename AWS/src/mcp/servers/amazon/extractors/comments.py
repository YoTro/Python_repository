from __future__ import annotations

import asyncio
import html as html_module
import json
import logging
import random
import re
import time
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from src.core.models.review import Review
from src.core.scraper import AmazonBaseScraper
from src.mcp.servers.amazon.cookie_pool import (
    CookieBrowserPool,
    CookieSlot,
    _resolve_chrome_path,
)

logger = logging.getLogger(__name__)


class CommentsExtractor(AmazonBaseScraper):
    """
    Advanced extractor for Amazon reviews.
    Prioritizes the internal AJAX API for speed and stability,
    with an HTML fallback, then a real-Chrome (DrissionPage) fallback.

    Tier 1 — AJAX POST (fastest, needs anti-csrftoken-a2z; 403-blocked when WAF rejects TLS)
    Tier 2 — HTML GET  (slower; soft-blocked when ak_bmsc / bm_sv are absent)
    Tier 3 — Browser   (DrissionPage real Chrome; always works; browser stays open so
                         Akamai cookies are captured and reused by Tier 1/2 on later calls)

    Multi-account concurrency
    -------------------------
    When ``CookieBrowserPool`` is initialised, each call to ``get_all_comments``
    picks a slot via round-robin.  The slot provides an isolated curl_cffi session
    (Tier 1/2) and a dedicated Chrome browser (Tier 3).  ``slot.browser_lock``
    serialises Tier-3 use per slot so two concurrent requests never navigate the
    same tab simultaneously.

    Single-account / legacy mode
    ----------------------------
    When no pool is active, the extractor falls back to ``self.session`` (curl_cffi)
    and the class-level ``_browser_page`` singleton — identical to the original
    single-user behaviour.
    """

    # Legacy browser singleton — kept for direct injection in live tests:
    #   CommentsExtractor._browser_page = bp
    # When CookieBrowserPool is active this attribute is not used.
    _browser_page = None  # type: ignore[assignment]

    async def get_all_comments(
        self,
        asin: str,
        max_pages: int = 3,
        *,
        sort_by: str = "",
        reviewer_type: str = "all_reviews",
        filter_by_star: str = "",
        format_type: str = "",
        media_type: str = "",
        filter_by_keyword: str = "",
    ) -> list[Review]:
        pool = CookieBrowserPool.get_instance()
        slot: CookieSlot | None = pool.next_slot() if pool else None

        all_reviews: list[Review] = []
        next_page_token: str | None = None
        seen_review_ids: set[tuple[str, str]] = set()
        ajax_failed = False
        html_failed = False
        _html_blocked = False  # True when HTML tier returned a definitive block (bot/login)
        _success_recorded = False
        page1_html: str | None = None  # page-1 HTML from AJAX tier, forwarded to HTML fallback
        page = 1

        _filter_kwargs = {
            "sort_by": sort_by,
            "reviewer_type": reviewer_type,
            "filter_by_star": filter_by_star,
            "format_type": format_type,
            "media_type": media_type,
            "filter_by_keyword": filter_by_keyword,
        }

        while page <= max_pages:
            reviews: list[Review] | None = None

            if not ajax_failed:
                reviews, next_page_token, _p1html = await self._fetch_reviews_via_ajax(
                    asin, page, next_page_token, slot=slot, **_filter_kwargs
                )
                if page == 1:
                    page1_html = _p1html
                if reviews is None:
                    logger.warning(f"AJAX failed on page {page}, falling back to HTML scraping...")
                    ajax_failed = True

            if ajax_failed and not html_failed:
                reviews, next_page_token = await self._fetch_reviews_via_html(
                    asin, page, next_page_token, slot=slot, page1_html=page1_html, **_filter_kwargs
                )
                if reviews is None:
                    html_failed = True
                    _html_blocked = True  # bot-detection or login wall — not "no reviews"
                elif not reviews and page == 1:
                    html_failed = True

            # Both curl_cffi tiers blocked — bootstrap via real Chrome, then resume curl_cffi.
            if html_failed and page == 1:
                logger.warning("Both AJAX and HTML failed — bootstrapping via browser...")

                if slot is not None:
                    # Acquire the slot's browser lock for the entire Tier-3 session so
                    # a concurrent request on the same slot cannot navigate the tab.
                    async with slot.browser_lock:
                        browser_reviews, next_page_token = await self._fetch_reviews_via_browser(
                            asin, max_pages, slot=slot, **_filter_kwargs
                        )
                else:
                    browser_reviews, next_page_token = await self._fetch_reviews_via_browser(
                        asin, max_pages, **_filter_kwargs
                    )

                for r in browser_reviews:
                    if (r.author, r.title) not in seen_review_ids:
                        seen_review_ids.add((r.author, r.title))
                        all_reviews.append(r)

                if slot is not None:
                    if not browser_reviews and _html_blocked:
                        # All 3 tiers failed with a confirmed block signal — not just an
                        # empty ASIN.  Trip the circuit so this slot is skipped until cooldown.
                        slot.circuit.record_failure()
                    elif browser_reviews and not _success_recorded:
                        slot.circuit.record_success()
                        _success_recorded = True

                browser_pages_loaded = (len(browser_reviews) + 9) // 10
                if not next_page_token or browser_pages_loaded >= max_pages:
                    return all_reviews

                # Browser warmed up Akamai cookies — retry curl_cffi for remaining pages.
                ajax_failed = False
                html_failed = False
                page = browser_pages_loaded + 1
                logger.info(
                    f"Browser loaded {browser_pages_loaded} pages; "
                    f"resuming curl_cffi from page {page} with fresh cookies."
                )
                await asyncio.sleep(random.uniform(1.0, 2.0))
                continue

            if not reviews:
                break

            new_reviews = [r for r in reviews if (r.author, r.title) not in seen_review_ids]
            if not new_reviews:
                logger.info(f"Page {page} returned only already-seen reviews — stopping early.")
                break
            seen_review_ids.update((r.author, r.title) for r in new_reviews)
            all_reviews.extend(new_reviews)

            if slot is not None and not _success_recorded:
                slot.circuit.record_success()
                _success_recorded = True

            if not next_page_token and page < max_pages:
                logger.info(f"No nextPageToken after page {page} — cannot paginate further.")
                break

            page += 1
            if page <= max_pages:
                await asyncio.sleep(random.uniform(1.0, 2.5))

        return all_reviews

    async def get_negative_reviews(self, asin: str, max_pages: int = 2) -> list[Review]:
        """Fetch verified 1–3 star reviews using Amazon's critical filter directly."""
        return await self.get_all_comments(
            asin,
            max_pages=max_pages,
            sort_by="recent",
            reviewer_type="avp_only_reviews",
            filter_by_star="critical",
            format_type="current_format",
            media_type="all_contents",
        )

    def _extract_next_page_token(self, soup: BeautifulSoup) -> str | None:
        """Extract nextPageToken from pagination section."""
        # Method 1: "Show 10 more reviews" button — token lives in data-reviews-state-param
        # e.g. data-reviews-state-param='{"nextPageToken":"MjAy...","pageNumber":"2",...}'
        show_more = soup.find("a", {"data-hook": "show-more-button"})
        if show_more:
            raw = show_more.get("data-reviews-state-param", "")
            if raw:
                try:
                    state = json.loads(raw)
                    token = state.get("nextPageToken")
                    if token:
                        return token
                except (json.JSONDecodeError, ValueError):
                    pass

        # Method 2: Inline JSON blob (older page format)
        match = re.search(r'"nextPageToken"\s*:\s*"([^"]+)"', str(soup))
        if match:
            return match.group(1)

        # Method 3: Look for pagination 'Next' link
        next_li = soup.find("li", class_="a-last")
        if next_li:
            link = next_li.find("a")
            if link and link.get("href"):
                match = re.search(r"nextPageToken=([^&]+)", link.get("href"))
                if match:
                    return match.group(1)

        # Method 4: Look for 'Next' link by data-hook
        next_link = soup.find("a", {"data-hook": "pagination-next"})
        if next_link and next_link.get("href"):
            match = re.search(r"nextPageToken=([^&]+)", next_link.get("href"))
            if match:
                return match.group(1)

        # Method 5: data-next-page-token attribute
        token_el = soup.find(attrs={"data-next-page-token": True})
        if token_el:
            return token_el.get("data-next-page-token")

        return None

    async def _acquire_csrf_token(
        self, asin: str, *, slot: CookieSlot | None = None
    ) -> tuple[str | None, str | None, str | None]:
        """
        Visit the product reviews page to harvest anti-csrftoken-a2z.
        Returns (csrf_token, next_page_token, page1_html) so callers can
        thread the HTML as a local variable into the HTML fallback tier
        without touching any shared slot state.
        """
        reviews_url = (
            f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
        )
        _sess = slot.session if slot else None
        html = await self.fetch(reviews_url, _session=_sess)
        if not html:
            reviews_url = (
                f"https://www.amazon.com/product-reviews/{asin}"
                f"?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
            )
            html = await self.fetch(reviews_url, _session=_sess)
        if not html:
            return None, None, None

        csrf_token = None

        match = re.search(r'"csrfToken"\s*:\s*"([^"]+)"', html)
        if match:
            csrf_token = match.group(1)

        if not csrf_token:
            match = re.search(r'anti-csrftoken-a2z["\s:=]+([A-Za-z0-9%+/]{20,})', html)
            if match:
                csrf_token = match.group(1)

        if not csrf_token:
            soup = BeautifulSoup(html, "html.parser")
            token_input = soup.find("input", {"name": "anti-csrftoken-a2z"})
            if token_input:
                csrf_token = token_input.get("value")

        if not csrf_token:
            _cookies = slot.session.cookies if slot else self.session.cookies
            csrf_token = _cookies.get("anti-csrftoken-a2z")
            if csrf_token:
                logger.info("Acquired CSRF token from session cookies.")

        if not csrf_token:
            login_url = (
                "https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0"
                "&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_signin"
                "&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
                "&openid.assoc_handle=usflex&openid.mode=checkid_setup"
                "&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
                "&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
            )
            login_html = await self.fetch(login_url, _session=_sess)
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

        return csrf_token, next_token, html

    async def _fetch_reviews_via_ajax(
        self,
        asin: str,
        page: int,
        next_page_token: str | None = None,
        *,
        slot: CookieSlot | None = None,
        sort_by: str = "",
        reviewer_type: str = "all_reviews",
        filter_by_star: str = "",
        format_type: str = "",
        media_type: str = "",
        filter_by_keyword: str = "",
    ) -> tuple[list[Review] | None, str | None, str | None]:
        """
        Attempt to fetch reviews using the undocumented AJAX endpoint.
        Returns (reviews, next_page_token, page1_html) where page1_html is the
        HTML fetched by _acquire_csrf_token (non-None only on page 1 when the
        CSRF token was not already in the session).  Returns (None, None, None)
        on failure to signal a fallback.
        """
        try:
            _sess = slot.session if slot else self.session
            _hdrs = slot.headers if slot else self._headers

            _page1_html: str | None = None
            csrf_token = _sess.cookies.get("anti-csrftoken-a2z")
            if not csrf_token:
                csrf_token, initial_token, _page1_html = await self._acquire_csrf_token(
                    asin, slot=slot
                )
                if not next_page_token:
                    next_page_token = initial_token

            if not csrf_token:
                logger.warning("Missing 'anti-csrftoken-a2z'. AJAX call will likely fail.")
                return None, None, None

            _filtered = bool(
                filter_by_star
                or filter_by_keyword
                or format_type
                or media_type
                or sort_by
                or reviewer_type != "all_reviews"
            )
            reftag = "cm_cr_arp_d_viewopt_sr" if _filtered else f"cm_cr_getr_d_paging_btm_{page}"
            url = f"https://www.amazon.com/portal/customer-reviews/ajax/reviews/get/ref={reftag}"

            if _filtered:
                _ref_base = (
                    f"https://www.amazon.com/product-reviews/{asin}/ref={reftag}"
                    f"?_encoding=UTF8&ie=UTF8&reviewerType={reviewer_type}"
                    f"&sortBy={sort_by}&filterByStar={filter_by_star}"
                    f"&formatType={format_type}&mediaType={media_type}"
                    f"&filterByKeyword={quote_plus(filter_by_keyword)}&pageNumber={max(1, page - 1)}"
                )
                referrer = _ref_base + (
                    f"&nextPageToken={next_page_token}" if next_page_token else ""
                )
            elif page == 1:
                referrer = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8"
                if next_page_token:
                    referrer += f"&nextPageToken={next_page_token}"
            else:
                prev_reftag = f"cm_cr_getr_d_paging_btm_{page - 1}"
                referrer = (
                    f"https://www.amazon.com/product-reviews/{asin}/ref={prev_reftag}"
                    f"?_encoding=UTF8&ie=UTF8&reviewerType=all_reviews&pageNumber={page - 1}"
                )
                if next_page_token:
                    referrer += f"&nextPageToken={next_page_token}"

            should_append = "true" if page > 1 else "false"

            session_ua = _hdrs.get("User-Agent", "")
            chrome_match = re.search(r"Chrome/((\d+)\.[\d.]+)", session_ua)
            chrome_major = chrome_match.group(2) if chrome_match else "124"
            chrome_full = chrome_match.group(1) if chrome_match else "124.0.0.0"
            sec_ch_ua = (
                f'"Chromium";v="{chrome_major}", '
                f'"Google Chrome";v="{chrome_major}", '
                f'"Not/A)Brand";v="99"'
            )
            sec_ch_ua_full = (
                f'"Chromium";v="{chrome_full}", '
                f'"Google Chrome";v="{chrome_full}", '
                f'"Not/A)Brand";v="99.0.0.0"'
            )
            if "Macintosh" in session_ua:
                sec_ch_ua_platform = '"macOS"'
            elif "Linux" in session_ua:
                sec_ch_ua_platform = '"Linux"'
            else:
                sec_ch_ua_platform = '"Windows"'

            headers = {
                "accept": "text/html,*/*",
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9",
                "anti-csrftoken-a2z": csrf_token,
                "cache-control": "no-cache",
                "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                "device-memory": "8",
                "downlink": "10",
                "dpr": "2",
                "ect": "4g",
                "origin": "https://www.amazon.com",
                "priority": "u=1, i",
                "rtt": "250",
                "sec-ch-device-memory": "8",
                "sec-ch-dpr": "2",
                "sec-ch-ua": sec_ch_ua,
                "sec-ch-ua-full-version-list": sec_ch_ua_full,
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": sec_ch_ua_platform,
                "sec-ch-viewport-width": "1280",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "viewport-width": "1280",
                "user-agent": session_ua,
                "x-amzn-flow-closure-id": str(int(time.time())),
                "x-requested-with": "XMLHttpRequest",
                "referer": referrer,
            }

            # scope rotates: page1→reviewsAjax1, page2→reviewsAjax0, page3→reviewsAjax1, page4→reviewsAjax2
            scope = f"reviewsAjax{(page - 2) % 3}" if page > 1 else "reviewsAjax1"

            token_param = f"&nextPageToken={next_page_token}" if next_page_token else ""
            body = (
                f"sortBy={sort_by}&reviewerType={reviewer_type}"
                f"&formatType={format_type}&mediaType={media_type}&filterByStar={filter_by_star}"
                f"&filterByAge=&pageNumber={page}&filterByLanguage="
                f"&filterByKeyword={quote_plus(filter_by_keyword)}"
                f"{token_param}&shouldAppend={should_append}&deviceType=desktop&canShowIntHeader=true"
                f"&reviewsShown=undefined&reftag={reftag}&pageSize=10&asin={asin}&scope={scope}"
            )

            response_text = await self.fetch(
                url, method="POST", headers=headers, data=body, _session=_sess
            )
            if not response_text:
                return None, None, None

            reviews, next_token = self._parse_ajax_response(response_text, asin)
            if not reviews and next_token is None:
                logger.warning("AJAX responded but returned no review content.")
            return reviews if reviews else [], next_token, _page1_html
        except Exception as e:
            logger.error(f"Error in AJAX review fetch: {e}")
            return None, None, None

    def _parse_ajax_response(
        self, response_text: str, asin: str
    ) -> tuple[list[Review], str | None]:
        """Parse the &&& -delimited AJAX response into reviews + next page token."""
        html_content = ""
        next_token: str | None = None
        for part in response_text.split("&&&"):
            part = part.strip()
            if not part:
                continue
            try:
                data = json.loads(part)
                if not (isinstance(data, list) and len(data) >= 3):
                    continue
                if data[0] == "append" and data[1] == "#cm_cr-review_list":
                    html_content += data[2]
                elif data[0] in ("set", "update") and data[1] == "#cm_cr-pagination_bar":
                    pagination_soup = BeautifulSoup(data[2], "html.parser")
                    next_token = self._extract_next_page_token(pagination_soup)
            except (json.JSONDecodeError, IndexError):
                continue

        if not html_content:
            return [], next_token

        soup = BeautifulSoup(html_content, "html.parser")
        reviews = self._parse_soup(soup, asin)
        if not next_token:
            next_token = self._extract_next_page_token(soup)
        return reviews, next_token

    async def _fetch_reviews_via_html(
        self,
        asin: str,
        page: int,
        next_page_token: str | None = None,
        *,
        slot: CookieSlot | None = None,
        page1_html: str | None = None,
        sort_by: str = "",
        reviewer_type: str = "all_reviews",
        filter_by_star: str = "",
        format_type: str = "",
        media_type: str = "",
        filter_by_keyword: str = "",
    ) -> tuple[list[Review] | None, str | None]:
        """Fallback method to scrape the full HTML review page."""
        try:
            _filtered = bool(
                filter_by_star
                or filter_by_keyword
                or format_type
                or media_type
                or sort_by
                or reviewer_type != "all_reviews"
            )

            # Page 1 was already fetched by _acquire_csrf_token — reuse it only when no
            # filters are active (the cached page is always unfiltered).
            # page1_html is a local passed by the caller; no shared slot state is touched.
            if page == 1 and page1_html and not _filtered:
                html_content = page1_html
            else:
                url = (
                    f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_arp_d_viewopt_sr"
                    f"?ie=UTF8&reviewerType={reviewer_type}&sortBy={sort_by}"
                    f"&filterByStar={filter_by_star}&formatType={format_type}"
                    f"&mediaType={media_type}&filterByKeyword={quote_plus(filter_by_keyword)}"
                    f"&pageNumber={page}"
                )
                if next_page_token:
                    url += f"&nextPageToken={next_page_token}"
                _sess = slot.session if slot else None
                html_content = await self.fetch(url, _session=_sess)
            if not html_content:
                return [], None

            if 'data-hook="review"' not in html_content and (
                "Spend less. Smile more." in html_content
                or "<title>Amazon.com</title>" in html_content
            ):
                logger.warning(
                    f"Bot detection triggered for {asin} page {page} — "
                    "Amazon returned homepage instead of reviews."
                )
                return None, None  # definitive block — circuit breaker signal

            if 'name="password"' in html_content or 'id="ap_password"' in html_content:
                logger.error(
                    f"LOGIN REQUIRED (HTML Fallback): Amazon is requesting login for {asin}."
                )
                return None, None  # definitive block — circuit breaker signal

            soup = BeautifulSoup(html_content, "html.parser")
            reviews = self._parse_soup(soup, asin)
            next_token = self._extract_next_page_token(soup)
            return reviews, next_token
        except Exception as e:
            logger.error(f"Error in HTML review fetch: {e}")
            return [], None

    async def _fetch_reviews_via_browser(
        self,
        asin: str,
        max_pages: int = 3,
        *,
        slot: CookieSlot | None = None,
        sort_by: str = "",
        reviewer_type: str = "all_reviews",
        filter_by_star: str = "",
        format_type: str = "",
        media_type: str = "",
        filter_by_keyword: str = "",
    ) -> tuple[list[Review], str | None]:
        """
        Tier 3 fallback: real Chrome (DrissionPage) + AJAX hand-off.

        When a CookieSlot is provided the browser is managed by the slot
        (``slot.get_or_init_browser()`` / ``slot.invalidate_browser()``).
        The caller is responsible for holding ``slot.browser_lock`` before
        invoking this method.

        When no slot is provided the legacy class-level ``_browser_page``
        singleton is used (single-user / dev mode).
        """
        import random as _random
        import time as _time

        try:
            from DrissionPage import ChromiumOptions, ChromiumPage
        except ImportError:
            logger.error("[browser] DrissionPage not installed.")
            return [], None

        # ── helpers ────────────────────────────────────────────────────────

        async def _get_browser() -> ChromiumPage:
            # Chrome launch + initial navigation block for up to ~20 s (subprocess
            # spawn, CDP handshake, page load, cookie injection).  Run entirely in
            # a thread-pool worker so the event loop stays live for all other
            # concurrent requests during the wait.
            loop = asyncio.get_running_loop()

            def _blocking() -> ChromiumPage:
                if slot is not None:
                    return slot.get_or_init_browser()

                # Legacy path: class-level singleton (single-user / dev mode)
                bp = CommentsExtractor._browser_page
                if bp is not None:
                    try:
                        _ = bp.url  # probe — raises if tab/process is dead
                        return bp
                    except Exception:
                        CommentsExtractor._browser_page = None

                co = ChromiumOptions()
                co.set_local_port(_random.randint(10000, 60000))
                chrome_path = _resolve_chrome_path()
                if chrome_path:
                    co.set_browser_path(chrome_path)
                co.incognito()
                co.set_argument("--disable-gpu")
                co.set_argument("--no-sandbox")
                co.set_argument("--disable-dev-shm-usage")
                co.headless(False)

                session_ua = self._headers.get("User-Agent", "")
                if session_ua:
                    co.set_user_agent(session_ua)

                new_bp = ChromiumPage(co)
                new_bp.set.load_mode.normal()

                cookie_data = self.cookie_helper.get_cookie_data()
                saved_cookies = cookie_data.get("cookies", {})
                new_bp.get("https://www.amazon.com/", timeout=30)
                _time.sleep(3)
                for name, value in saved_cookies.items():
                    try:
                        new_bp.set.cookies({"name": name, "value": value, "domain": ".amazon.com"})
                    except Exception:
                        pass

                CommentsExtractor._browser_page = new_bp
                return new_bp

            return await loop.run_in_executor(None, _blocking)

        def _capture_cookies(bp: ChromiumPage) -> dict[str, str]:
            try:
                result = bp.run_cdp("Network.getAllCookies")
                return {c["name"]: c["value"] for c in result.get("cookies", [])}
            except Exception:
                return {c.get("name"): c.get("value") for c in bp.cookies()}

        def _refresh_curl_session(fresh_cookies: dict[str, str]):
            # Update the slot's session (or self.session in legacy mode)
            target_session = slot.session if slot else self.session
            for name, value in fresh_cookies.items():
                try:
                    target_session.cookies.set(name, value)
                except Exception:
                    pass
            # Persist to the slot's own cookie file (or the shared file in legacy mode)
            # so the next cold start benefits without contaminating other slots.
            _cache_path = slot.cache_file if slot is not None else self.cookie_helper.cache_file
            try:
                import json as _json

                with open(_cache_path, encoding="utf-8") as f:
                    cache = _json.load(f)
                cache.setdefault("cookies", {}).update(fresh_cookies)
                with open(_cache_path, "w", encoding="utf-8") as f:
                    _json.dump(cache, f, indent=4)
            except Exception:
                pass

        def _log_ajax_comparison(packet: object, our_headers: dict, our_body: str, page_num: int):
            try:
                br_headers = dict(getattr(getattr(packet, "request", None), "headers", {}) or {})
                br_body = getattr(getattr(packet, "request", None), "body", "") or ""
                br_status = getattr(getattr(packet, "response", None), "status", "?")

                logger.info(f"[ajax-compare] page={page_num} browser_status={br_status}")
                logger.info("[ajax-compare] --- BROWSER request headers ---")
                for k, v in sorted(br_headers.items()):
                    logger.info(f"[ajax-compare]   {k}: {v}")
                logger.info("[ajax-compare] --- curl_cffi request headers ---")
                for k, v in sorted(our_headers.items()):
                    logger.info(f"[ajax-compare]   {k}: {v}")

                br_keys = {k.lower() for k in br_headers}
                our_keys = {k.lower() for k in our_headers}
                missing = br_keys - our_keys
                extra = our_keys - br_keys
                if missing:
                    logger.warning(
                        f"[ajax-compare] Headers in browser but NOT in curl_cffi: {missing}"
                    )
                if extra:
                    logger.info(f"[ajax-compare] Headers in curl_cffi but NOT in browser: {extra}")

                br_cookie_str = br_headers.get("cookie", br_headers.get("Cookie", ""))
                br_cookie_names = {
                    p.split("=")[0].strip() for p in br_cookie_str.split(";") if "=" in p
                }
                our_cookie_str = our_headers.get("cookie", our_headers.get("Cookie", ""))
                our_cookie_names = {
                    p.split("=")[0].strip() for p in our_cookie_str.split(";") if "=" in p
                }
                missing_cookies = br_cookie_names - our_cookie_names
                if missing_cookies:
                    logger.warning(
                        f"[ajax-compare] Cookies browser sent but curl_cffi DID NOT: {missing_cookies}"
                    )

                if br_body != our_body:
                    logger.info(f"[ajax-compare] Browser body: {br_body[:300]}")
                    logger.info(f"[ajax-compare] curl_cffi body: {our_body[:300]}")
            except Exception as exc:
                logger.warning(f"[ajax-compare] Could not parse packet: {exc}")

        # ── main scrape loop ───────────────────────────────────────────────

        try:
            bp = await _get_browser()
            _target_session = slot.session if slot else self.session

            reviews_url = (
                f"https://www.amazon.com/product-reviews/{asin}"
                f"/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&reviewerType={reviewer_type}"
                f"&sortBy={sort_by}&filterByStar={filter_by_star}"
                f"&formatType={format_type}&mediaType={media_type}"
                f"&filterByKeyword={quote_plus(filter_by_keyword)}&pageNumber=1"
            )
            logger.info(f"[browser] Navigating to reviews page for ASIN {asin}...")
            bp.get(reviews_url, timeout=60)

            waf_token_found = False
            for wait_i in range(25):
                _time.sleep(1)
                check = _capture_cookies(bp)
                if "aws-waf-token" in check:
                    logger.info(f"[browser] aws-waf-token appeared after {wait_i + 1}s.")
                    waf_token_found = True
                    break
            if not waf_token_found:
                logger.warning("[browser] aws-waf-token not set after 25s — proceeding without it.")

            all_reviews: list[Review] = []
            seen_ids: set[tuple[str, str]] = set()
            pages_loaded = 1
            next_page_token: str | None = None

            for page_num in range(1, max_pages + 1):
                fresh_cookies = _capture_cookies(bp)
                _refresh_curl_session(fresh_cookies)

                csrf_token: str | None = None
                try:
                    csrf_token = _target_session.cookies.get("anti-csrftoken-a2z")
                except Exception:
                    pass
                if not csrf_token:
                    try:
                        csrf_token = bp.run_js(
                            "var i=document.querySelector('input[name=\"anti-csrftoken-a2z\"]');"
                            "if(i&&i.value)return i.value;"
                            "var m=document.body.innerHTML.match(/[\"']csrfToken[\"']\\s*:\\s*[\"']([A-Za-z0-9%+/]{20,})[\"']/);"
                            "if(m)return m[1];"
                            "var p=document.body.innerHTML.match(/anti-csrftoken-a2z[^{]*\\{[^}]*[\"']token[\"']\\s*:\\s*[\"']([A-Za-z0-9%+/]{20,})[\"']/);"
                            "if(p)return p[1];"
                            'var a=document.body.innerHTML.match(/anti-csrftoken-a2z["\\s:=,]+([A-Za-z0-9%+/]{20,})/);'
                            "if(a)return a[1];"
                            "return null;"
                        )
                    except Exception:
                        pass
                if csrf_token:
                    try:
                        _target_session.cookies.set("anti-csrftoken-a2z", csrf_token)
                    except Exception:
                        pass

                raw_html = bp.html  # single DOM serialisation — reused for both regex and parser
                decoded_html = html_module.unescape(raw_html)
                npt_m = re.search(r'"nextPageToken"\s*:\s*"([^"]+)"', decoded_html)
                next_page_token = npt_m.group(1) if npt_m else None

                slot_label = f"slot={slot.slot_id}" if slot else "legacy"
                logger.info(
                    f"[browser:{slot_label}] Page {page_num}: "
                    f"csrf={'PRESENT' if csrf_token else 'MISSING'} | "
                    f"nextToken={'PRESENT' if next_page_token else 'MISSING'} | "
                    f"ak_bmsc={'PRESENT' if 'ak_bmsc' in fresh_cookies else 'MISSING'} | "
                    f"cookies={len(fresh_cookies)}"
                )

                soup = BeautifulSoup(raw_html, "html.parser")
                page_reviews = self._parse_soup(soup, asin)
                for r in page_reviews:
                    if (r.author, r.title) not in seen_ids:
                        seen_ids.add((r.author, r.title))
                        all_reviews.append(r)

                if page_num >= max_pages or not next_page_token:
                    break

                next_page_num = page_num + 1
                if csrf_token and next_page_token:
                    _filtered = bool(
                        filter_by_star
                        or filter_by_keyword
                        or format_type
                        or media_type
                        or sort_by
                        or reviewer_type != "all_reviews"
                    )
                    reftag = (
                        "cm_cr_arp_d_viewopt_sr"
                        if _filtered
                        else f"cm_cr_getr_d_paging_btm_{next_page_num}"
                    )
                    scope = f"reviewsAjax{(next_page_num - 2) % 3}"
                    ajax_url = f"/portal/customer-reviews/ajax/reviews/get/ref={reftag}"
                    body_str = (
                        f"sortBy={sort_by}&reviewerType={reviewer_type}"
                        f"&formatType={format_type}&mediaType={media_type}"
                        f"&filterByStar={filter_by_star}"
                        f"&filterByAge=&pageNumber={next_page_num}&filterByLanguage="
                        f"&filterByKeyword={quote_plus(filter_by_keyword)}"
                        f"&nextPageToken={next_page_token}&shouldAppend=true"
                        f"&deviceType=desktop&canShowIntHeader=true&reviewsShown=undefined"
                        f"&reftag={reftag}&pageSize=10&asin={asin}&scope={scope}"
                    )
                    csrf_js = csrf_token.replace("'", "\\'")
                    body_js = body_str.replace("'", "\\'")
                    url_js = ajax_url.replace("'", "\\'")
                    logger.info(f"[browser] Firing XHR for page {next_page_num} via run_js...")
                    try:
                        js_response = bp.run_js(
                            f"var xhr=new XMLHttpRequest();"
                            f"xhr.open('POST','{url_js}',false);"
                            f"xhr.setRequestHeader('content-type','application/x-www-form-urlencoded;charset=UTF-8');"
                            f"xhr.setRequestHeader('anti-csrftoken-a2z','{csrf_js}');"
                            f"xhr.setRequestHeader('x-requested-with','XMLHttpRequest');"
                            f"xhr.setRequestHeader('accept','text/html,*/*');"
                            f"xhr.send('{body_js}');"
                            f"return JSON.stringify({{status:xhr.status,body:xhr.responseText}});"
                        )
                        if js_response:
                            parsed = json.loads(js_response)
                            status = parsed.get("status", 0)
                            body_text = parsed.get("body", "")
                            logger.info(f"[browser] XHR status={status}, body_len={len(body_text)}")
                            if status == 200 and "&&&" in body_text:
                                ajax_reviews, ajax_npt = self._parse_ajax_response(body_text, asin)
                                if ajax_reviews:
                                    logger.info(
                                        f"[browser] XHR AJAX succeeded for page {next_page_num}: "
                                        f"{len(ajax_reviews)} reviews."
                                    )
                                    for r in ajax_reviews:
                                        if (r.author, r.title) not in seen_ids:
                                            seen_ids.add((r.author, r.title))
                                            all_reviews.append(r)
                                    next_page_token = ajax_npt
                                    pages_loaded = next_page_num
                                    continue
                            else:
                                logger.warning(
                                    f"[browser] XHR returned status={status} — falling back to button click."
                                )
                    except Exception as xhr_exc:
                        logger.warning(f"[browser] run_js XHR failed: {xhr_exc}")

                show_more = bp.ele("@@data-hook=show-more-button", timeout=5) or bp.ele(
                    'xpath://a[@data-hook="show-more-button"]', timeout=3
                )
                if not show_more:
                    logger.info("[browser] No 'Show 10 more' button — reached last page.")
                    break

                logger.info(f"[browser] Clicking 'Show 10 more' for page {next_page_num}...")
                try:
                    bp.listen.start("portal/customer-reviews/ajax")
                    show_more.click()
                    packet = bp.listen.wait(timeout=15)
                    bp.listen.stop()
                    if packet and not csrf_token:
                        req_headers = getattr(getattr(packet, "request", None), "headers", {}) or {}
                        captured = req_headers.get("anti-csrftoken-a2z") or req_headers.get(
                            "Anti-Csrftoken-A2z"
                        )
                        if captured:
                            csrf_token = captured
                            logger.info(
                                f"[browser] Captured anti-csrftoken-a2z from intercepted request: "
                                f"{csrf_token[:20]}..."
                            )
                            try:
                                _target_session.cookies.set("anti-csrftoken-a2z", csrf_token)
                            except Exception:
                                pass
                            try:
                                import json as _json2

                                _csrf_cache_path = (
                                    slot.cache_file
                                    if slot is not None
                                    else self.cookie_helper.cache_file
                                )
                                with open(_csrf_cache_path, encoding="utf-8") as f:
                                    _cache = _json2.load(f)
                                _cache.setdefault("cookies", {})["anti-csrftoken-a2z"] = csrf_token
                                with open(_csrf_cache_path, "w", encoding="utf-8") as f:
                                    _json2.dump(_cache, f, indent=4)
                            except Exception:
                                pass
                    _log_ajax_comparison(packet, {}, "", next_page_num)
                except Exception as listen_exc:
                    logger.warning(f"[browser] Network listener failed: {listen_exc}")
                    try:
                        bp.listen.stop()
                    except Exception:
                        pass
                    show_more.click()

                _time.sleep(_random.uniform(2.0, 3.5))
                pages_loaded = next_page_num

            logger.info(
                f"[browser] Done: {len(all_reviews)} reviews from {pages_loaded} page(s). "
                "Browser stays open."
            )
            return all_reviews, next_page_token

        except Exception as e:
            logger.error(f"[browser] Scraping failed: {e}")
            if slot is not None:
                slot.invalidate_browser()
            else:
                CommentsExtractor._browser_page = None
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
