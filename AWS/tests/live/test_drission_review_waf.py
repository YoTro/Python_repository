"""
Diagnostic script: Use DrissionPage (real Chrome) to:
1. Log in to Amazon manually
2. Check cookies at each stage for aws-waf-token
3. Navigate to review pages directly via browser (no curl_cffi)
4. Parse and print review data across 10 pages

Run with:
    PYTHONPATH=. venv311/bin/python tests/test_drission_review_waf.py
"""

from __future__ import annotations

import html as html_module
import logging
import os
import random
import re
import sys
import time

sys.path.insert(0, os.getcwd())

from DrissionPage import ChromiumOptions, ChromiumPage

from src.core.utils.cookie_helper import AMAZON_UA, AmazonCookieHelper

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

ASIN = "B0CPJ37XZH"
MAX_PAGES = 10
COOKIE_CACHE = "config/cookies.json"


def _get_all_cookies_cdp(page: ChromiumPage) -> dict[str, str]:
    try:
        result = page.run_cdp("Network.getAllCookies")
        return {c["name"]: c["value"] for c in result.get("cookies", [])}
    except Exception:
        return {c.get("name"): c.get("value") for c in page.cookies()}


def _report_waf(cookies: dict, stage: str):
    waf = cookies.get("aws-waf-token")
    cmc = cookies.get("cmc")
    rx = cookies.get("rx")
    rxc = cookies.get("rxc")
    logger.info(
        f"[{stage}] aws-waf-token={'PRESENT' if waf else 'MISSING'} | "
        f"cmc={'PRESENT' if cmc else 'MISSING'} | "
        f"rx={'PRESENT' if rx else 'MISSING'} | "
        f"rxc={'PRESENT' if rxc else 'MISSING'}"
    )
    if waf:
        logger.info(f"  aws-waf-token value (first 60): {waf[:60]}")


def _parse_reviews_from_html(html: str) -> list[dict]:
    reviews = []
    # Match each review block by data-hook="review"
    blocks = re.findall(
        r'data-hook="review".*?(?=data-hook="review"|</body>)',
        html,
        re.DOTALL,
    )
    for block in blocks:
        rating_m = re.search(r"i-star-(\d)", block)
        title_m = re.search(
            r'data-hook="review-title"[^>]*>.*?<span[^>]*>([^<]+)', block, re.DOTALL
        )
        author_m = re.search(r'class="a-profile-name"[^>]*>([^<]+)', block)
        body_m = re.search(
            r'data-hook="review-body"[^>]*>.*?<span[^>]*>(.*?)</span>', block, re.DOTALL
        )

        rating = int(rating_m.group(1)) if rating_m else 0
        title = title_m.group(1).strip() if title_m else ""
        author = author_m.group(1).strip() if author_m else ""
        body = re.sub(r"<[^>]+>", "", body_m.group(1)).strip() if body_m else ""

        if title or body:
            reviews.append({"rating": rating, "title": title, "author": author, "body": body[:120]})
    return reviews


def main():
    co = ChromiumOptions()
    random_port = random.randint(10000, 60000)
    co.set_local_port(random_port)

    mac_chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    if os.path.isfile(mac_chrome):
        co.set_browser_path(mac_chrome)

    co.incognito()
    co.set_argument("--disable-gpu")
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-dev-shm-usage")
    co.headless(False)

    ua = AMAZON_UA
    co.set_user_agent(ua)

    page = ChromiumPage(co)
    page.set.load_mode.normal()

    try:
        # ── Stage 1: Navigate to Amazon sign-in ─────────────────────────────
        sign_in_url = (
            "https://www.amazon.com/ap/signin"
            "?openid.pape.max_auth_age=0"
            "&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_signin"
            "&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
            "&openid.assoc_handle=usflex"
            "&openid.mode=checkid_setup"
            "&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
            "&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
        )
        logger.info("Navigating to Amazon sign-in page...")
        page.get(sign_in_url, timeout=60)
        time.sleep(3)

        cookies_after_signin_page = _get_all_cookies_cdp(page)
        _report_waf(cookies_after_signin_page, "AFTER sign-in page load")
        logger.info(f"  All cookie names: {list(cookies_after_signin_page.keys())}")

        # Check if aws-waf-token is in the page HTML source
        html_src = page.html
        if "aws-waf-token" in html_src:
            logger.info("  aws-waf-token FOUND in sign-in page HTML source!")
        else:
            logger.info("  aws-waf-token NOT in sign-in page HTML source.")

        # ── Stage 2: Wait for manual login ──────────────────────────────────
        logger.info("\n>>> Please log in to Amazon in the browser window. <<<")
        logger.info(">>> Waiting up to 120 seconds for login completion...  <<<\n")

        for _ in range(120):
            if page.ele("#nav-item-signout") or page.ele("text:Account & Lists"):
                logger.info("Login detected!")
                break
            time.sleep(1)
        else:
            logger.error("Login timeout — aborting.")
            return

        # Wait for WAF JS challenge after login redirect
        logger.info("Waiting 15s for WAF JS challenge to complete post-login...")
        time.sleep(15)

        cookies_after_login = _get_all_cookies_cdp(page)
        _report_waf(cookies_after_login, "AFTER login + 15s wait")
        logger.info(f"  All cookie names: {list(cookies_after_login.keys())}")

        # Save these cookies + ua to cache for curl_cffi fallback
        _cookie_helper = AmazonCookieHelper(cache_file=COOKIE_CACHE)
        _cookie_helper._save_to_cache(
            {"cookies": cookies_after_login, "user_agent": ua, "is_logged_in": True}
        )
        logger.info(f"Saved {len(cookies_after_login)} cookies to {COOKIE_CACHE}")

        # ── Stage 3: Navigate to product reviews page ────────────────────────
        reviews_url = (
            f"https://www.amazon.com/product-reviews/{ASIN}"
            f"/ref=cm_cr_arp_d_viewopt_sr"
            f"?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
        )
        logger.info(f"\nNavigating to reviews page 1 for ASIN {ASIN}...")
        page.get(reviews_url, timeout=60)
        time.sleep(4)

        cookies_after_reviews = _get_all_cookies_cdp(page)
        _report_waf(cookies_after_reviews, "AFTER reviews page 1 load")

        # Check if we're on reviews page or homepage
        current_url = page.url
        page_html = page.html
        has_reviews = 'data-hook="review"' in page_html
        is_homepage = "nav-logo-sprites" in page_html and not has_reviews
        logger.info(f"  Current URL: {current_url[:80]}")
        logger.info(f"  Has reviews: {has_reviews} | Is homepage (soft-block): {is_homepage}")

        if "aws-waf-token" in page_html:
            logger.info("  aws-waf-token found in reviews page HTML!")

        # ── Stage 3b: Debug pagination area ─────────────────────────────────
        debug_html = page.html
        review_count = debug_html.count('data-hook="review"')
        logger.info(f"  Total data-hook=review occurrences in DOM: {review_count}")

        # Inspect pagination bar
        pag_idx = debug_html.find("cm_cr-pagination_bar")
        if pag_idx >= 0:
            logger.info(f"  Pagination bar HTML snippet:\n{debug_html[pag_idx : pag_idx + 600]}")
        else:
            logger.info("  cm_cr-pagination_bar NOT FOUND in rendered DOM")

        # Check for show-more-button
        smb_idx = debug_html.find("show-more-button")
        if smb_idx >= 0:
            logger.info(f"  show-more-button snippet:\n{debug_html[smb_idx - 100 : smb_idx + 400]}")
        else:
            logger.info("  show-more-button NOT in rendered DOM")

        # Check all a-tags with data-hook
        hooks = re.findall(r'data-hook="([^"]+)"', debug_html)
        from collections import Counter

        hook_counts = Counter(hooks)
        logger.info(f"  All data-hook values (top 20): {hook_counts.most_common(20)}")

        # ── Stage 4: Fetch 10 pages of reviews via "Show 10 more" button ────
        all_reviews = []
        seen_titles: set[str] = set()

        for page_num in range(1, MAX_PAGES + 1):
            current_html = page.html
            current_url_now = page.url

            has_revs = 'data-hook="review"' in current_html
            is_home = "nav-logo-sprites" in current_html and not has_revs

            cookies_now = _get_all_cookies_cdp(page)
            _report_waf(cookies_now, f"Page {page_num}")
            logger.info(f"  URL: {current_url_now[:80]}")
            logger.info(f"  Has reviews: {has_revs} | Soft-blocked: {is_home}")

            # DrissionPage returns HTML entities in attribute values (e.g. &quot; instead of ")
            # Unescape first so we can parse the JSON inside data-reviews-state-param
            decoded_html = html_module.unescape(current_html)
            npt_m = re.search(r'"nextPageToken"\s*:\s*"([^"]+)"', decoded_html)
            npt = npt_m.group(1) if npt_m else None
            logger.info(f"  nextPageToken in HTML: {npt[:60] if npt else 'NOT FOUND'}")

            if not has_revs:
                logger.warning(
                    f"  No reviews found on page {page_num} — may be blocked or last page."
                )
                if "validateCaptcha" in current_html or "captcha" in current_html.lower():
                    logger.warning("  CAPTCHA challenge detected!")
                if is_home:
                    break
                continue

            page_reviews = _parse_reviews_from_html(current_html)
            new_count = 0
            for r in page_reviews:
                if r["title"] not in seen_titles:
                    seen_titles.add(r["title"])
                    all_reviews.append({"page": page_num, **r})
                    new_count += 1

            logger.info(f"  Parsed {len(page_reviews)} reviews, {new_count} new.")

            if page_num >= MAX_PAGES:
                break

            # Click "Show 10 more reviews" button — AJAX appends reviews inline
            # Try multiple DrissionPage selector styles
            show_more = (
                page.ele("@@data-hook=show-more-button", timeout=3)
                or page.ele('xpath://a[@data-hook="show-more-button"]', timeout=3)
                or page.ele(".cm-cr-show-more", timeout=3)
            )
            if not show_more:
                logger.info(f"  No 'Show 10 more' button on page {page_num} — reached last page.")
                break
            logger.info(f"  Clicking 'Show 10 more reviews' for page {page_num + 1}...")
            show_more.click()
            time.sleep(random.uniform(3.5, 5.5))

        # ── Stage 5: Summary ─────────────────────────────────────────────────
        logger.info(f"\n{'=' * 60}")
        logger.info(f"TOTAL UNIQUE REVIEWS FETCHED: {len(all_reviews)}")
        logger.info(f"{'=' * 60}")
        for i, r in enumerate(all_reviews, 1):
            logger.info(f"[{i}] p{r['page']} {r['rating']}★ | {r['author']} | {r['title'][:55]}")

        # Final cookie report
        final_cookies = _get_all_cookies_cdp(page)
        _report_waf(final_cookies, "FINAL")
        waf_token = final_cookies.get("aws-waf-token")
        if waf_token:
            logger.info(f"\naws-waf-token (full): {waf_token[:200]}")

        # Save final cookies (may include aws-waf-token acquired during review page visits)
        _cookie_helper._save_to_cache(
            {"cookies": final_cookies, "user_agent": ua, "is_logged_in": True}
        )
        logger.info(f"Final cookies saved to {COOKIE_CACHE} ({len(final_cookies)} total)")

        input("\n>>> Browser stays open. Press Enter here to close it. <<<\n")

    finally:
        page.quit()


if __name__ == "__main__":
    main()
