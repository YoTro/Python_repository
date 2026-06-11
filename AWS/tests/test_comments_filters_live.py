"""
Live integration test for CommentsExtractor filter params.

Uses DrissionPage (Tier 3) to bootstrap the Akamai session so the test works
even when AJAX / HTML tiers are blocked.  After the browser warms up cookies,
subsequent calls use the faster AJAX tier automatically.

Run:
    PYTHONPATH=. venv311/bin/python tests/test_comments_filters_live.py

The test opens a real Chrome window.  If Amazon prompts for login, sign in
manually — the browser stays open and cookies are reused for every assertion.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.mcp.servers.amazon.extractors.comments import CommentsExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("filter_live_test")

# ── Test config ────────────────────────────────────────────────────────────────
TEST_ASIN = "B0FXFGMD7Z"  # ASIN from the curl sample; has reviews across all star levels

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"


# ── Assertion helpers ──────────────────────────────────────────────────────────


def check(label: str, condition: bool, detail: str = "") -> bool:
    status = PASS if condition else FAIL
    suffix = f"  ({detail})" if detail else ""
    print(f"  {status}  {label}{suffix}")
    return condition


# ── Browser bootstrap ──────────────────────────────────────────────────────────


def _bootstrap_browser(asin: str) -> None:
    """
    Open Chrome and navigate to the Amazon reviews page.
    Blocks until the page loads and AWS WAF sets aws-waf-token (up to 30 s).
    If the user is not logged in they can sign in manually; the window stays open.
    """
    from DrissionPage import ChromiumOptions, ChromiumPage

    print("\n[browser] Opening Chrome — sign in to Amazon if prompted.")
    print("[browser] The window will stay open; close it only after the tests finish.\n")

    co = ChromiumOptions()
    co.set_local_port(19222)
    for candidate in (
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ):
        if sys.platform == "darwin" and os.path.isfile(candidate):
            co.set_browser_path(candidate)
            break

    bp = ChromiumPage(addr_or_opts=co)
    CommentsExtractor._browser_page = bp  # inject into singleton

    url = (
        f"https://www.amazon.com/product-reviews/{asin}"
        f"/ref=cm_cr_arp_d_viewopt_sr?ie=UTF8&reviewerType=all_reviews&pageNumber=1"
    )
    bp.get(url, timeout=60)

    for i in range(30):
        time.sleep(1)
        cookies = {c["name"]: c["value"] for c in bp.cookies()}
        if "aws-waf-token" in cookies:
            print(
                f"[browser] aws-waf-token ready after {i + 1}s. Injecting cookies into extractor."
            )
            break
    else:
        print("[browser] aws-waf-token not seen after 30s — proceeding anyway.")

    print("[browser] Browser bootstrap done.\n")


# ── Individual filter tests ────────────────────────────────────────────────────


async def test_get_negative_reviews(extractor: CommentsExtractor) -> bool:
    print("── get_negative_reviews (filter_by_star=critical, avp_only_reviews, sort_by=recent) ──")
    reviews = await extractor.get_negative_reviews(TEST_ASIN, max_pages=1)
    ok = True
    ok &= check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews:
        bad_stars = [r for r in reviews if r.rating and r.rating > 3]
        ok &= check(
            "all ratings ≤ 3 stars",
            len(bad_stars) == 0,
            f"{len(bad_stars)} reviews with rating > 3" if bad_stars else "",
        )
        unverified = [r for r in reviews if not r.is_verified]
        # avp_only_reviews should yield verified-only; warn rather than fail in case
        # Amazon's badge HTML is missing for edge-case reviews.
        if unverified:
            print(
                f"  {WARN}  {len(unverified)}/{len(reviews)} reviews lack avp-badge "
                "(avp_only_reviews requested — may be badge-parse issue)"
            )
        else:
            print(f"  {PASS}  all {len(reviews)} reviews carry verified-purchase badge")
        print(f"         sample: [{reviews[0].rating}★] {reviews[0].title!r}")
    return ok


async def test_filter_by_star_positive(extractor: CommentsExtractor) -> bool:
    print("── filter_by_star=positive ──")
    reviews = await extractor.get_all_comments(TEST_ASIN, max_pages=1, filter_by_star="positive")
    ok = True
    ok &= check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews:
        bad = [r for r in reviews if r.rating and r.rating < 4]
        ok &= check(
            "all ratings ≥ 4 stars",
            len(bad) == 0,
            f"{len(bad)} reviews with rating < 4" if bad else "",
        )
        print(f"         sample: [{reviews[0].rating}★] {reviews[0].title!r}")
    return ok


async def test_reviewer_type_avp(extractor: CommentsExtractor) -> bool:
    print("── reviewer_type=avp_only_reviews ──")
    reviews = await extractor.get_all_comments(
        TEST_ASIN, max_pages=1, reviewer_type="avp_only_reviews"
    )
    ok = check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews:
        unverified = [r for r in reviews if not r.is_verified]
        if unverified:
            print(
                f"  {WARN}  {len(unverified)}/{len(reviews)} missing badge "
                "(avp_only_reviews requested — badge-parse edge case)"
            )
        else:
            print(f"  {PASS}  all {len(reviews)} reviews verified-purchase")
    return ok


async def test_sort_by_recent(extractor: CommentsExtractor) -> bool:
    print("── sort_by=recent ──")
    reviews = await extractor.get_all_comments(TEST_ASIN, max_pages=1, sort_by="recent")
    ok = check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews:
        print(f"         first review date: {reviews[0].date!r}")
    return ok


async def test_sort_by_helpful(extractor: CommentsExtractor) -> bool:
    print("── sort_by=helpful ──")
    reviews = await extractor.get_all_comments(TEST_ASIN, max_pages=1, sort_by="helpful")
    ok = check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews and len(reviews) >= 2:
        # Top-helpful reviews typically have higher vote counts; just log
        votes = [r.helpful_votes or 0 for r in reviews[:3]]
        print(f"         top-3 helpful_votes: {votes}")
    return ok


async def test_format_type_current(extractor: CommentsExtractor) -> bool:
    print("── format_type=current_format (variant filter) ──")
    reviews = await extractor.get_all_comments(TEST_ASIN, max_pages=1, format_type="current_format")
    # Only verifies the request doesn't error; count may be low for single-variant ASINs
    ok = check("request completed without exception", True, f"got {len(reviews)} reviews")
    return ok


async def test_media_type_media_only(extractor: CommentsExtractor) -> bool:
    print("── media_type=media_reviews_only ──")
    reviews = await extractor.get_all_comments(
        TEST_ASIN, max_pages=1, media_type="media_reviews_only"
    )
    ok = check("request completed without exception", True, f"got {len(reviews)} reviews")
    if reviews:
        with_images = [r for r in reviews if r.image_urls]
        print(f"         {len(with_images)}/{len(reviews)} reviews have parsed image_urls")
    return ok


async def test_filter_by_keyword(extractor: CommentsExtractor) -> bool:
    keyword = "quality"
    print(f"── filter_by_keyword={keyword!r} ──")
    reviews = await extractor.get_all_comments(TEST_ASIN, max_pages=1, filter_by_keyword=keyword)
    ok = check("request completed without exception", True, f"got {len(reviews)} reviews")
    if reviews:
        hits = sum(
            1
            for r in reviews
            if keyword.lower() in (r.content or "").lower()
            or keyword.lower() in (r.title or "").lower()
        )
        print(f"         {hits}/{len(reviews)} reviews mention '{keyword}' in title/content")
    return ok


async def test_combined_filters(extractor: CommentsExtractor) -> bool:
    print(
        "── combined: filter_by_star=critical + reviewer_type=avp_only_reviews + sort_by=recent ──"
    )
    reviews = await extractor.get_all_comments(
        TEST_ASIN,
        max_pages=1,
        filter_by_star="critical",
        reviewer_type="avp_only_reviews",
        sort_by="recent",
    )
    ok = True
    ok &= check("returned at least 1 review", len(reviews) > 0, f"got {len(reviews)}")
    if reviews:
        bad_stars = [r for r in reviews if r.rating and r.rating > 3]
        ok &= check(
            "all ratings ≤ 3",
            len(bad_stars) == 0,
            f"{len(bad_stars)} > 3-star" if bad_stars else "",
        )
        print(
            f"         sample: [{reviews[0].rating}★|verified={reviews[0].is_verified}] {reviews[0].title!r}"
        )
    return ok


# ── Main ───────────────────────────────────────────────────────────────────────


async def run_all() -> None:
    _bootstrap_browser(TEST_ASIN)

    extractor = CommentsExtractor()

    suites = [
        test_get_negative_reviews,
        test_filter_by_star_positive,
        test_reviewer_type_avp,
        test_sort_by_recent,
        test_sort_by_helpful,
        test_format_type_current,
        test_media_type_media_only,
        test_filter_by_keyword,
        test_combined_filters,
    ]

    results: list[tuple[str, bool]] = []
    for suite in suites:
        try:
            passed = await suite(extractor)
        except Exception as exc:
            print(f"  {FAIL}  unhandled exception: {exc}")
            passed = False
        results.append((suite.__name__, passed))
        print()

    print("═" * 60)
    print("SUMMARY")
    print("═" * 60)
    passed_count = sum(1 for _, p in results if p)
    for name, passed in results:
        print(f"  {'✅' if passed else '❌'}  {name}")
    print(f"\n{passed_count}/{len(results)} test suites passed.")


if __name__ == "__main__":
    asyncio.run(run_all())
