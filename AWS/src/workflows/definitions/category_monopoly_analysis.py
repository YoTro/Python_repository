from __future__ import annotations

"""
Category Monopoly Analysis Workflow

Performs a deep-dive analysis of an Amazon category to determine monopoly levels
and competition intensity across 7 dimensions.
"""

import asyncio
import calendar
import csv
import hashlib as _hl
import io
import json
import logging
import math
import os
import re
import statistics
import time
import uuid as _uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser as _dateutil_parser

from src.core.data_cache import data_cache as _data_cache
from src.core.utils.decorators import exponential_backoff
from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
from src.intelligence.processors.sales_estimator import SalesEstimator
from src.intelligence.processors.social_virality import SocialViralityProcessor
from src.intelligence.prompts.manager import prompt_manager
from src.intelligence.router import TaskCategory
from src.mcp.servers.amazon.ads.client import AmazonAdsClient
from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
from src.mcp.servers.amazon.extractors.brand import BrandExtractor
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
from src.mcp.servers.amazon.extractors.profitability_search import ProfitabilitySearchExtractor
from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor
from src.mcp.servers.finance.tools import get_referral_rate
from src.mcp.servers.market.deals.client import DealHistoryClient
from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from src.mcp.servers.social.tiktok.client import TikTokClient
from src.mcp.servers.social.youtube.client import YouTubeClient
from src.workflows.engine import Workflow
from src.workflows.registry import WorkflowRegistry
from src.workflows.steps.base import ComputeTarget
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep

logger = logging.getLogger(__name__)

# ── L2 cache helpers ─────────────────────────────────────────────────────────
_L2_DOMAIN = "cat_monopoly"

_TTL_BSR = 3_600  # 1  h — BSR scrape
_TTL_SALES = 86_400  # 24 h — past-month sales
_TTL_SELLER = 21_600  # 6  h — seller/fulfillment info
_TTL_SIGNALS = 3_600  # 1  h — ABA + SERP + CPC market signals
_TTL_TIMESERIES = 86_400  # 24 h — 12-month historical trends + keyword weekly
_TTL_SS_BSR = 86_400  # 24 h — Sellersprite monthly snapshots
_TTL_KEYWORDS = 21_600  # 6  h — LLM keyword extraction from BSR titles
_TTL_EXTERNAL = 43_200  # 12 h — TikTok/YouTube social PSI
_TTL_DEAL = 21_600  # 6  h — off-site deal promotion intensity (rotates faster than social PSI)
_TTL_TRAFFIC = 21_600  # 6  h — Xiyouzhaoci batch ad-traffic ratios
_TTL_CVR = 7 * 86_400  # 7 d  — Amazon Ads category CVR benchmark

# Structured source-type tokens for category CVR.  Stored in the L2 payload so callers
# can branch on source without parsing the human-readable description string.
_CVR_SRC_ADS = "amazon_ads_validated"  # Ads crossProgramBenchmarks, category matched
_CVR_SRC_SS = "sellersprite_snapshot"  # SellerSprite median conversionRate
_CVR_SRC_POWER_LAW = "power_law"  # price-based model_default_cvr()
_CVR_SRC_DEFAULT = "default_0.10"  # hard-coded fallback (should rarely appear)
_TTL_CRITICAL_REVIEWS = 86_400  # 24 h — recent critical reviews per ASIN

# Synthetic grouping key for Amazon.com direct sales (seller_type "AMZ"). Amazon-as-seller
# listings have no separate "sold by" merchant link, so FulfillmentExtractor never captures
# a scrapable SellerId for them, and the SellerSprite fallback's seller_id field is empty
# too — real per-ASIN seller_id is genuinely unavailable, not just unfetched. Without this,
# every AMZ-sold ASIN has seller_id=None and is silently dropped from seller-level
# concentration (multi-brand-seller detection, Seller CR3/HHI): Amazon selling multiple of
# its own brands (Amazon Basics, Amazon Commercial, Amazon Essentials) in one niche would
# look like N unrelated unidentified sellers instead of one seller spanning N brands.
_AMAZON_DIRECT_SELLER_ID = "AMAZON_DIRECT"

# Stop-word list shared by _ngram_candidates and _filter_category_coherence — keeps
# generic tokens (set, pack, kit, oz…) out of both keyword extraction and cross-title
# similarity, so the two stay aligned by construction rather than by copy-paste.
_STOP_WORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "for",
    "of",
    "in",
    "with",
    "to",
    "from",
    "by",
    "is",
    "are",
    "was",
    "be",
    "as",
    "on",
    "at",
    "up",
    "it",
    "its",
    "this",
    "that",
    "all",
    "new",
    "set",
    "pack",
    "pcs",
    "piece",
    "pieces",
    "count",
    "ct",
    "oz",
    "lb",
    "ft",
    "inch",
}

# Shared semaphores: cap concurrent amazon_scraper calls across all parallel EnrichStep slots.
# EnrichStep runs 5 items concurrently; each fires 2 sub-requests → 10 simultaneous without limits.
# burst:3 in settings.json means anything beyond 3 will timeout. Cap at 3 each.
# Created eagerly at module import (not per-loop) since Python 3.10+ no longer binds
# asyncio.Semaphore to a loop at construction time, only on first await.
_SEM_FULFILLMENT = asyncio.Semaphore(3)
_SEM_REVIEW_COUNT = asyncio.Semaphore(2)
_SEM_COMMENTS = asyncio.Semaphore(3)
# _sem_brand is created inside _run_monopoly_analysis (only used there) to stay loop-safe.


def _parse_int(raw, default: int = 0) -> int:
    """Extract the first integer from a messy string (handles commas, suffixes, parens)."""
    m = re.search(r"\d+", str(raw or "").replace(",", ""))
    if not m:
        return default
    try:
        return int(m.group())
    except ValueError:
        return default


def _new_entrant_cutoff_ms(base_ym: str) -> int:
    """Timestamp (ms) marking the start of the 12-month "new entrant" window.

    Anchored to base_ym (Sellersprite's T-2-month base snapshot) rather than
    wall-clock now, so the window doesn't drift when base_ym trails behind the
    current date. Falls back to a rolling 365-day window when base_ym is absent
    or malformed.
    """
    if base_ym and len(base_ym) == 6:
        by, bm = int(base_ym[:4]), int(base_ym[4:])
        total = by * 12 + (bm - 1) - 12
        cy, cm = total // 12, total % 12 + 1
        return calendar.timegm((cy, cm, 1, 0, 0, 0)) * 1000
    return int((time.time() - 365 * 86400) * 1000)


async def _fill_missing_brands(
    missing_asins, target: dict, sem: asyncio.Semaphore, log_prefix: str
) -> None:
    """Resolve brands for ASINs via BrandExtractor and merge results into target.

    `sem` is passed in (rather than module-level) because the caller's semaphore
    must stay bound to the current event loop — see _sem_brand note above.
    """
    if not missing_asins:
        return
    logger.info(f"[{log_prefix}] brand-fill via BrandExtractor: {len(missing_asins)} ASINs")

    async def _fetch(asin: str) -> tuple[str, str | None]:
        async with sem, BrandExtractor() as extractor:
            try:
                res = await extractor.get_brand(asin)
                return asin, res.get("Brand")
            except Exception as e:
                logger.warning(f"[{log_prefix}] BrandExtractor({asin}): {e}")
                return asin, None

    results = await asyncio.gather(*[_fetch(a) for a in missing_asins])
    resolved = 0
    for asin, brand in results:
        if brand:
            target[asin] = brand
            resolved += 1
    logger.info(f"[{log_prefix}] BrandExtractor resolved {resolved}/{len(missing_asins)} brands")


def _l2_key(ctx, *parts) -> str:
    tid = getattr(ctx, "tenant_id", None) or "default"
    sid = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    return ":".join(str(p) for p in (tid, sid) + parts)


def _l2_get(ctx, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx, value, ttl: int, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value, ttl_seconds=ttl)


# ---------------------------------------------------------------------------
# Extractor Wrappers
# ---------------------------------------------------------------------------


async def _fetch_bsr_list(_items: list[dict], ctx: Any) -> list[dict]:
    """Seed step: fetches up to 100 BSR products (2 pages × 50) and returns them as the new item list.

    Actual count may be less than 100: lazy-loaded items (ranks 31-50 per page)
    are fetched via a separate ACP API and silently skipped on failure.
    Replaces whatever _items were passed in — downstream steps operate on the
    BSR product list, not the workflow seed items.
    """
    url = ctx.config.get("url")
    if not url:
        raise ValueError("No URL provided in workflow config for category_monopoly_analysis.")

    url_hash = _hl.md5(url.encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_BSR, "bsr_list", url_hash)
    if cached is not None:
        logger.info(f"[cat_monopoly] BSR list L2 cache hit for url_hash={url_hash}")
        return cached

    async with BestSellersExtractor() as extractor:
        products = await extractor.get_bestsellers(url, max_pages=2)
    if not products:
        raise ValueError(f"BSR extractor returned no products for URL: {url}")
    _l2_set(ctx, products, _TTL_BSR, "bsr_list", url_hash)
    logger.info(f"[cat_monopoly] Fetched {len(products)} BSR products, cached url_hash={url_hash}")
    return products


async def _enrich_sales(items: list[dict], ctx: Any) -> list[dict]:
    """Fetch past month sales for all items in one batch (20 ASINs per request).
    Cache per-ASIN; only fetches ASINs that are not already in L2.
    """
    all_asins = [(item.get("ASIN") or item.get("asin") or "").strip().upper() for item in items]

    # Resolve from cache where available
    sales_map: dict[str, int] = {}
    missing: list[str] = []
    for asin in all_asins:
        if not asin:
            continue
        hit = _l2_get(ctx, _TTL_SALES, "sales", asin)
        if hit is not None:
            sales_map[asin] = hit
        else:
            missing.append(asin)

    # Batch-fetch only the uncached ASINs
    if missing:
        async with PastMonthSalesExtractor() as extractor:
            fetched = await extractor.get_batch_past_month_sales(missing)
        for asin, val in fetched.items():
            sales_map[asin] = val or 0
            _l2_set(ctx, val or 0, _TTL_SALES, "sales", asin)

    for item, asin in zip(items, all_asins, strict=False):
        item["sales"] = sales_map.get(asin) or 0
    return items


async def _enrich_seller_info(item: dict, ctx: Any) -> dict:
    """Fetch fulfillment, seller feedback, and written-vs-global review counts."""
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {
            "seller_type": "Unknown",
            "seller_id": None,
            "feedback_count": None,
            "global_ratings": None,
            "written_reviews": None,
            "review_ratio": None,
        }

    cached = _l2_get(ctx, _TTL_SELLER, "seller_info", asin)
    if cached is not None:
        return cached

    f_extractor, s_extractor, rc_extractor = (
        FulfillmentExtractor(),
        SellerFeedbackExtractor(),
        ReviewRatioExtractor(),
    )
    try:

        async def _get_fulfillment():
            async with _SEM_FULFILLMENT:
                return await f_extractor.get_fulfillment_info(asin)

        async def _get_review_count():
            async with _SEM_REVIEW_COUNT:
                return await rc_extractor.get_review_count(asin)

        f_res, rc_res = await asyncio.gather(_get_fulfillment(), _get_review_count())

        seller_id = f_res.get("SellerId")
        fulfilled_by = f_res.get("FulfilledBy")

        # SellerSprite fallback when Amazon page scraping fails (FulfilledBy is None)
        if fulfilled_by is None:
            ss_snapshots = ctx.cache.get("sellersprite_snapshots") or {}
            if ss_snapshots:
                _latest = ss_snapshots.get(max(ss_snapshots), [])
                _ss_item = next((p for p in _latest if p.get("asin") == asin), None)
                if _ss_item:
                    _fba = _ss_item.get("fba")
                    _ss_seller_name = _ss_item.get("seller_name") or ""
                    if _fba == "FBA":
                        fulfilled_by = "Amazon"
                    elif _fba is not None:
                        fulfilled_by = _ss_seller_name or "3P"
                    if not seller_id:
                        seller_id = _ss_item.get("seller_id")
                    logger.info(
                        f"[enrich_seller_info] {asin}: SellerSprite fallback → "
                        f"FulfilledBy={fulfilled_by!r}, seller_id={seller_id!r}"
                    )

        feedback_count = None
        if seller_id:
            s_res = await s_extractor.get_seller_feedback_count(seller_id)
            _fc_raw = s_res.get("FeedbackCount")
            # Extractor now returns time-windowed dict; extract lifetime count as the scalar
            # expected by downstream (monopoly analyzer compares it to mega_seller_feedback).
            feedback_count = (
                (_fc_raw.get("lifetime") or {}).get("count")
                if isinstance(_fc_raw, dict)
                else _fc_raw  # None when page unreachable
            )

        # Assign the synthetic Amazon-direct seller ID only after the feedback-count lookup
        # above, so it's never sent to SellerFeedbackExtractor as if it were a real seller ID —
        # it exists purely to group AMZ-sold ASINs together for seller-concentration math.
        if not seller_id and _classify_fulfillment(fulfilled_by) == "AMZ":
            seller_id = _AMAZON_DIRECT_SELLER_ID

        result = {
            "seller_type": fulfilled_by or "Unknown",
            "seller_id": seller_id,
            "feedback_count": feedback_count,
            "global_ratings": rc_res.get("GlobalRatings"),
            "written_reviews": rc_res.get("WrittenReviews"),
            "review_ratio": rc_res.get("Ratio"),
        }
        _l2_set(ctx, result, _TTL_SELLER, "seller_info", asin)
        return result
    finally:
        await asyncio.gather(f_extractor.close(), s_extractor.close(), rc_extractor.close())


def _classify_fulfillment(seller_type: str | None) -> str:
    """
    Bucket the raw scraped `seller_type` string into FBA / FBM / AMZ / Unknown.

    FulfillmentExtractor only reports the "ships from" text, not the separate
    "sold by" seller — so the two Amazon-fulfilled cases collapse to distinct
    literal strings rather than an explicit flag: the ships-from ODF span reads
    exactly "Amazon" when a 3rd-party seller uses Fulfilled-by-Amazon (FBA),
    while Amazon-as-seller-and-shipper reads "Amazon.com" (merchant span, no
    separate ships-from). Anything else is a merchant name (FBM).
    """
    if not seller_type or seller_type in ("Unknown", "3P"):
        return "Unknown"
    normalized = seller_type.strip().lower()
    if normalized == "amazon":
        return "FBA"
    if "amazon.com" in normalized:
        return "AMZ"
    return "FBM"


def _classify_social_momentum(monthly_distribution: list[dict[str, Any]]) -> str:
    """
    Classify video-posting momentum from SocialViralityProcessor's 12-month
    monthly_distribution by comparing the most recent 3 months' video count to
    the prior 3 months. Cheap categorical trend label — avoids passing the full
    12-point array into the prompt just to have the LLM eyeball a trend.
    """
    counts = [m.get("video_count", 0) for m in monthly_distribution]
    if len(counts) < 6:
        return "insufficient_data"
    recent, prior = sum(counts[-3:]), sum(counts[-6:-3])
    if recent == 0 and prior == 0:
        return "no_activity"
    if prior == 0:
        return "accelerating"
    ratio = recent / prior
    if ratio >= 1.3:
        return "accelerating"
    if ratio <= 0.7:
        return "fading"
    return "stable"


def _ngram_candidates(titles: list, min_doc_freq: int = 3, top_n: int = 15) -> list:
    """
    Return unigrams and bigrams ranked by document frequency (# titles containing them).

    Used to anchor LLM keyword extraction to terms actually present in the data,
    preventing hallucination and ensuring stability across runs.
    """
    doc_counts: Counter = Counter()
    for title in titles:
        tokens = [
            t
            for t in re.findall(r"[a-z0-9]+", title.lower())
            if t not in _STOP_WORDS and len(t) > 1
        ]
        seen_in_doc: set = set()
        for n in (1, 2):
            for i in range(len(tokens) - n + 1):
                gram = " ".join(tokens[i : i + n])
                if gram not in seen_in_doc:
                    doc_counts[gram] += 1
                    seen_in_doc.add(gram)
    return [term for term, cnt in doc_counts.most_common(top_n) if cnt >= min_doc_freq]


async def _fetch_core_keywords(items: list[dict], ctx: Any) -> list[dict]:
    """
    Step 1 of 2 for market context.
    Derives the top 3 core search keywords that define the niche.
    Must complete before _fetch_market_signals (ABA, SERP, CPC all key on these).
    Writes: ctx.cache["core_keywords"], ctx.cache["main_keyword"]

    Primary path — ASIN-keyword intersection (Xiyouzhaoci get_asin_keywords):
      1. Call get_asin_keywords concurrently for the top 10 BSR ASINs.
      2. Build a term→{asins} map.  Keep only terms present in ≥ MIN_ASIN_OVERLAP
         ASINs — these are cross-product niche keywords, not sub-niche specific ones.
      3. Sort surviving terms by (overlap_count × median_weekly_volume), take top 10
         as candidate anchors, then ask LLM to pick the best 3.

    Why intersection matters:
      A "killer" BSR top-10 mixes insecticides, Neem Oil, flea/tick, wasp spray, and
      deer repellents.  Terms like "wasp spray" only appear in a subset of ASINs;
      true category-level terms (e.g. "bug killer", "pest control") span all of them.
      Title-level n-grams cannot detect this because sub-niche product names overlap
      by accident; cross-ASIN traffic data reflects actual buyer search behaviour.

    Fallback — n-gram title frequency:
      Used when the Xiyouzhaoci call fails or returns < MIN_CANDIDATES keywords.

    Post-validation: LLM output is checked against the candidate set; fewer than
    2 survivors → use top intersection candidates directly (no LLM).
    """
    _MIN_ASIN_OVERLAP = 3  # keyword must drive traffic to at least this many ASINs
    _MIN_CANDIDATES = 3  # minimum intersection hits before falling back to n-grams

    if not items:
        return []

    top_asins = [
        item.get("ASIN") or item.get("asin")
        for item in items[:10]
        if item.get("ASIN") or item.get("asin")
    ]
    top_titles = [item.get("Title", "") for item in items[:20] if item.get("Title")]
    cache_hash = _hl.md5(("|".join(top_asins) + "|".join(top_titles[:5])).encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_KEYWORDS, "core_keywords", cache_hash)
    if cached is not None:
        ctx.cache["core_keywords"] = cached["core_keywords"]
        ctx.cache["main_keyword"] = cached["main_keyword"]
        logger.info(f"[cat_monopoly] Core keywords L2 cache hit hash={cache_hash}")
        return items

    # ── n-gram fallback (always precomputed) ─────────────────────────────
    freq_candidates = _ngram_candidates(top_titles, min_doc_freq=3)
    stat_keywords = freq_candidates[:3] if freq_candidates else ["unknown niche"]

    # ── Primary: cross-ASIN keyword intersection ──────────────────────────
    intersection_candidates: list = []
    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        api = XiyouZhaociAPI(tenant_id=tenant_id)

        today = datetime.utcnow().date()
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")

        # Fetch keywords for each ASIN concurrently
        async def _kw_for_asin(asin: str) -> tuple[str, list]:
            try:
                res = await asyncio.to_thread(
                    api.get_asin_keywords, country, asin, start_date, end_date, 1, 50
                )
                return asin, res.get("list") or []
            except Exception as e:
                logger.warning(f"[fetch_core_keywords] get_asin_keywords({asin}): {e}")
                return asin, []

        results = await asyncio.gather(*[_kw_for_asin(a) for a in top_asins])

        # Build term → set of ASINs that have this keyword
        term_asins: dict = defaultdict(set)
        term_vol: dict = defaultdict(list)
        for asin, kw_list in results:
            for kw in kw_list:
                term = (kw.get("searchTerm") or "").strip().lower()
                vol = (kw.get("searchTermReport") or {}).get("weeklySearchVolume") or 0
                if term and 1 <= len(term.split()) <= 5:
                    term_asins[term].add(asin)
                    term_vol[term].append(vol)

        # Keep only terms appearing across ≥ MIN_ASIN_OVERLAP distinct ASINs
        qualified = [
            (term, len(asins), statistics.median(term_vol[term]))
            for term, asins in term_asins.items()
            if len(asins) >= _MIN_ASIN_OVERLAP
        ]
        # Sort: more ASINs first, then higher volume as tiebreaker
        qualified.sort(key=lambda x: (x[1], x[2]), reverse=True)
        intersection_candidates = [term for term, _, _ in qualified[:12]]
        logger.info(
            f"[fetch_core_keywords] intersection: {len(qualified)} terms ≥ {_MIN_ASIN_OVERLAP} ASINs; "
            f"top={intersection_candidates[:5]}"
        )
    except Exception as e:
        logger.warning(
            f"[fetch_core_keywords] ASIN intersection failed: {e}; using n-gram fallback"
        )

    # Prefer intersection candidates; fall back to n-grams if too thin
    candidates = (
        intersection_candidates
        if len(intersection_candidates) >= _MIN_CANDIDATES
        else freq_candidates
    )
    if not candidates:
        candidates = stat_keywords

    # ── LLM refinement (pick best 3 from the data-grounded candidates) ───
    _REFUSAL_PREFIXES = (
        "sorry",
        "i ",
        "i'",
        "based on",
        "the keyword",
        "here are",
        "unfortunately",
        "as an",
        "i cannot",
        "i can't",
    )
    core_keywords = candidates[:8]
    try:
        if ctx.router and candidates:
            source_label = (
                "cross-ASIN traffic data" if intersection_candidates else "BSR title frequency"
            )
            candidate_str = ", ".join(candidates[:20])
            prompt = (
                "You are classifying an Amazon BSR niche. "
                f"The following search terms are grounded in {source_label} across the top BSR products: "
                f"[{candidate_str}]. "
                "From these candidates ONLY, pick 5 to 8 that best represent the core buyer "
                "search intent for this niche — terms a buyer would type when category-shopping, "
                "before deciding on a specific type, brand, or variant.\n"
                "A term is SUB-NICHE SPECIFIC (exclude it) if it:\n"
                "  • names only one product subtype or mechanism "
                "(e.g. 'glue board' or 'snap trap' when the category has both)\n"
                "  • targets a specific demographic, species, or location "
                "(e.g. 'for dogs', 'outdoor', 'kitchen')\n"
                "  • is an attribute modifier rather than a product name "
                "(e.g. 'reusable', 'electric', 'organic', 'heavy duty', 'scented')\n"
                "  • is a brand name, proper noun, model number, or quantity descriptor "
                "(e.g. 'Victor', 'v11', '2 pack', '12 count')\n"
                "A term is CATEGORY-LEVEL (prefer it) if a buyer using it would consider "
                "ALL or most of the top BSR products relevant results.\n"
                "Return a comma-separated list of 5 to 8 terms — no explanation, no numbering."
            )
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            raw_text = res.text.strip().replace('"', "").replace("'", "").lower()
            raw_text = re.sub(r"(?m)^\s*\d+[\.\)]\s*", "", raw_text)
            raw_text = re.sub(r"(?m)^\s*[-•*]\s*", "", raw_text)
            raw_text = re.sub(r"\n+", ",", raw_text)
            raw_text = raw_text.replace(";", ",")
            parsed = [k.strip() for k in raw_text.split(",") if k.strip()]
            # Reject model-number / quantity-code patterns: terms starting with
            # 0–3 letters then a digit (v11, xr2, 4pack) or a bare digit (12 count).
            _brand_model_re = re.compile(r"^[a-z]{0,3}\d")
            llm_valid = [
                k
                for k in parsed
                if 1 <= len(k.split()) <= 5
                and not any(k.startswith(p) for p in _REFUSAL_PREFIXES)
                and k in candidates  # must be from the data-grounded set
                and not _brand_model_re.match(k)
            ]
            if len(llm_valid) >= 2:
                core_keywords = llm_valid[:8]
            else:
                logger.warning(
                    f"[fetch_core_keywords] LLM output not in candidates "
                    f"(got {llm_valid!r}); using top candidates {candidates[:8]}"
                )
    except Exception as e:
        logger.warning(f"[fetch_core_keywords] LLM refinement failed: {e}; using top candidates")

    ctx.cache["core_keywords"] = core_keywords
    ctx.cache["main_keyword"] = core_keywords[0]
    _l2_set(
        ctx,
        {"core_keywords": core_keywords, "main_keyword": core_keywords[0]},
        _TTL_KEYWORDS,
        "core_keywords",
        cache_hash,
    )
    logger.info(
        f"[fetch_core_keywords] final={core_keywords} "
        f"(source={'intersection' if intersection_candidates else 'ngram'}, "
        f"candidates={candidates[:5]})"
    )
    return items


async def _filter_category_coherence(items: list[dict], ctx: Any) -> list[dict]:
    """
    Remove off-category products from the BSR list before any metrics are computed.

    Phase 1 — Jaccard + DBSCAN (reference-free, no core_keywords needed):
      Tokenize each title, build a pairwise Jaccard distance matrix, run DBSCAN to
      identify the dominant product cluster.  Items outside that cluster (noise points
      and minority clusters) are removed.

      Escalates to Phase 2 when:
        - No cluster is found (all items are DBSCAN noise)
        - A second cluster reaches ≥ 40% of the dominant cluster's size
          (ambiguous category mix that lexical distance cannot resolve)

    Phase 2 — LLM taxonomy (escalation only):
      Ask the router to identify the dominant product type from the top-30 titles,
      build a refined keyword set from that cluster, then re-score all items.

    Results stored in ctx.cache["contamination_stats"] for report inclusion.
    Must run BEFORE fetch_market_signals.
    """
    _MIN_RETAINED = 10
    _DBSCAN_EPS = 0.75  # Jaccard distance ceiling for two titles to be neighbours
    _DBSCAN_MIN_SAMPLES = 3  # minimum neighbourhood density to form a cluster core
    _AMBIGUOUS_RATIO = 0.40  # second cluster ≥ 40% of dominant → ambiguous, escalate

    if not items:
        return items

    def _stem(t: str) -> str:
        # Naive plural normalisation: "traps"→"trap", "gnats"→"gnat".
        # Only strip trailing-s when len > 4 to avoid mangling short words.
        return t[:-1] if t.endswith("s") and len(t) > 4 else t

    def _tokenize(title: str) -> frozenset:
        return frozenset(
            _stem(t)
            for t in re.findall(r"[a-z0-9]+", title.lower())
            if t not in _STOP_WORDS and len(t) > 1 and not t.isdigit()
        )

    def _jaccard_dist(a: frozenset, b: frozenset) -> float:
        union = len(a | b)
        return 1.0 - len(a & b) / union if union else 0.0

    token_sets = [_tokenize(item.get("Title", "")) for item in items]
    n = len(token_sets)
    _phase2_reason = ""

    # ── Phase 1: Jaccard distance matrix + DBSCAN ────────────────────────────
    try:
        import numpy as np
        from sklearn.cluster import DBSCAN as _DBSCAN

        dist = np.zeros((n, n), dtype=np.float32)
        for i in range(n):
            for j in range(i + 1, n):
                d = _jaccard_dist(token_sets[i], token_sets[j])
                dist[i, j] = dist[j, i] = d

        labels = _DBSCAN(
            eps=_DBSCAN_EPS, min_samples=_DBSCAN_MIN_SAMPLES, metric="precomputed"
        ).fit_predict(dist)

        cluster_sizes = Counter(lbl for lbl in labels if lbl >= 0)
        noise_count = int(np.sum(labels < 0))

        if not cluster_sizes:
            _phase2_reason = "no_clusters (all DBSCAN noise)"
            raise ValueError(_phase2_reason)

        sorted_clusters = cluster_sizes.most_common()
        dominant_label, dominant_size = sorted_clusters[0]
        second_size = sorted_clusters[1][1] if len(sorted_clusters) > 1 else 0

        if second_size >= _AMBIGUOUS_RATIO * dominant_size:
            _phase2_reason = f"ambiguous (dominant={dominant_size}, second={second_size})"
            raise ValueError(_phase2_reason)

        # Partition into dominant-cluster indices vs the rest
        dom_idx = [i for i, lbl in enumerate(labels) if lbl == dominant_label]
        non_idx = [i for i, lbl in enumerate(labels) if lbl != dominant_label]

        # ── Phase 1b: exclusion-token sweep ──────────────────────────────────
        # Tokens that are ≥20% more frequent in the non-dominant set than in
        # the dominant cluster are "off-category signals".  Expel any dominant-
        # cluster item whose token set contains one — this catches products that
        # leaked in through brand-name or generic-word transitive bridges
        # (e.g. "mouse" appears in 0% of gnat traps but 30% of noise items).
        _EXCL_CONTRAST = 0.20  # token must be ≥20% more frequent in non-dominant than dominant
        _EXCL_MIN_COUNT = (
            2  # token must appear in ≥2 non-dominant items (blocks 1-item brand noise)
        )
        # If a token already appears in ≥25% of dominant-cluster items it is a category
        # feature, not an outlier signal.  Without this guard, broad-category words like
        # "band", "battery", "black" get flagged because accessories (watch straps, spare
        # batteries) raise the non-dominant frequency slightly above the dominant rate,
        # then Apple Watch / Samsung items that share the same words get expelled.
        _EXCL_MAX_DOM_FREQ = 0.25
        n_non = len(non_idx)
        if n_non >= 3:
            dom_freq: Counter = Counter(tok for i in dom_idx for tok in token_sets[i])
            non_freq: Counter = Counter(tok for i in non_idx for tok in token_sets[i])
            excl_toks = frozenset(
                tok
                for tok, cnt in non_freq.items()
                if cnt >= _EXCL_MIN_COUNT
                and dom_freq.get(tok, 0) / dominant_size < _EXCL_MAX_DOM_FREQ
                and cnt / n_non - dom_freq.get(tok, 0) / dominant_size >= _EXCL_CONTRAST
            )
            if excl_toks:
                dom_clean = [i for i in dom_idx if not (token_sets[i] & excl_toks)]
                expelled = [i for i in dom_idx if token_sets[i] & excl_toks]
                if len(dom_clean) >= _MIN_RETAINED:
                    logger.info(
                        f"[category_coherence] Phase 1b: expelled {len(expelled)} items "
                        f"via exclusion tokens {sorted(excl_toks)[:6]}"
                    )
                    dom_idx = dom_clean
                    dominant_size = len(dom_idx)
                    non_idx = non_idx + expelled

        # ── Phase 1c: configurable + data-driven off-category token filter ──────
        # Two signal sources are unioned to form `active_contra`:
        #
        #  (A) Domain hints — injectable via ctx.cache["contra_token_hints"].
        #      Defaults to a pest-control set; callers in other verticals can
        #      supply their own (e.g. {"drill", "saw"} for a "screwdriver" BSR).
        #
        #  (B) Data-driven derivation from the non-dominant cluster: tokens
        #      that appear more often in off-category items than in the dominant
        #      cluster and are absent from core_keywords.  Works for any domain
        #      without needing explicit hints.
        #
        # A token is only contra-active when it is NOT present in core_keywords,
        # which prevents misfires (e.g. "ant" stays inactive for "ant trap" BSRs
        # even though it is in the default hint set).
        core_kw_text = " ".join(ctx.cache.get("core_keywords") or []).lower()
        core_kw_toks = frozenset(
            _stem(t)
            for t in re.findall(r"[a-z]+", core_kw_text)
            if len(t) > 2 and t not in _STOP_WORDS
        )
        _DEFAULT_HINTS: frozenset = frozenset(
            {
                "mouse",
                "mice",
                "rat",
                "roach",
                "cockroach",
                "ant",
                "wasp",
                "spider",
                "snake",
                "bedbug",
            }
        )
        _hint_src = ctx.cache.get("contra_token_hints")
        hint_contra = (frozenset(_hint_src) if _hint_src else _DEFAULT_HINTS) - core_kw_toks

        # Data-driven: tokens in non-dom items with ≥20 pp frequency advantage
        # over the dominant cluster and not in core_kw_toks.
        derived_contra: frozenset = frozenset()
        if non_idx and core_kw_toks:
            _n_non = len(non_idx)
            _non_freq: Counter = Counter(tok for i in non_idx for tok in token_sets[i])
            _dom_freq2: Counter = Counter(tok for i in dom_idx for tok in token_sets[i])
            _n_dom = len(dom_idx)
            _c_floor = max(1, _n_non // 3)
            derived_contra = frozenset(
                tok
                for tok, cnt in _non_freq.items()
                if cnt >= _c_floor
                and tok not in core_kw_toks
                and cnt / _n_non - _dom_freq2.get(tok, 0) / max(_n_dom, 1) >= 0.20
            )

        active_contra = hint_contra | derived_contra
        _phase1c_fired = False
        if active_contra and core_kw_toks:
            dom_1c = [i for i in dom_idx if not (token_sets[i] & active_contra)]
            expelled_1c = [i for i in dom_idx if token_sets[i] & active_contra]
            if len(dom_1c) >= max(5, _MIN_RETAINED // 2) and expelled_1c:
                triggered = sorted(
                    active_contra & frozenset().union(*(token_sets[i] for i in expelled_1c))
                )
                logger.info(
                    f"[category_coherence] Phase 1c: expelled {len(expelled_1c)} items "
                    f"via contra-tokens {triggered[:5]}"
                )
                dom_idx = dom_1c
                dominant_size = len(dom_idx)
                non_idx = non_idx + expelled_1c
                _phase1c_fired = True
        # ─────────────────────────────────────────────────────────────────────

        # Clean: DBSCAN found a single cluster with no noise AND neither
        # Phase 1b nor 1c expelled anything — nothing to filter.
        if noise_count == 0 and len(cluster_sizes) == 1 and len(non_idx) == 0:
            ctx.cache["contamination_stats"] = {
                "status": "clean",
                "method": "dbscan_jaccard",
                "n_removed": 0,
                "n_retained": n,
            }
            return items

        kept = [items[i] for i in dom_idx]
        removed = [items[i] for i in non_idx]

        # Phase 1c is semantically grounded (pest type ≠ category keyword), so
        # its results warrant a lower floor than the statistical DBSCAN minimum.
        effective_min = max(5, _MIN_RETAINED // 2) if _phase1c_fired else _MIN_RETAINED
        if len(kept) < effective_min:
            ctx.cache["contamination_stats"] = {
                "status": "warning",
                "method": "dbscan_jaccard",
                "outlier_rate": round(len(removed) / n, 3),
                "n_removed": 0,
                "n_retained": n,
                "note": f"dominant cluster too small ({len(kept)}) — filtering skipped",
                "sample_outliers": [it.get("Title", "")[:60] for it in removed[:5]],
            }
            return items

        sample = [it.get("Title", "")[:60] for it in removed[:5]]
        ctx.cache["contamination_stats"] = {
            "status": "filtered",
            "method": "dbscan_jaccard",
            "outlier_rate": round(len(removed) / n, 3),
            "dominant_cluster_size": dominant_size,
            "noise_count": noise_count,
            "n_removed": len(removed),
            "n_retained": len(kept),
            "sample_removed": sample,
        }
        logger.info(
            f"[category_coherence] DBSCAN: dominant={dominant_size}/{n}, "
            f"removed={len(removed)} ({len(removed) / n:.0%}), noise={noise_count}; "
            f"sample: {sample[:2]}"
        )
        return kept

    except ImportError:
        _phase2_reason = "sklearn/numpy unavailable"
        logger.warning(f"[category_coherence] Phase 1 skipped: {_phase2_reason}")
    except ValueError as exc:
        logger.info(f"[category_coherence] Phase 1 → Phase 2: {exc}")

    # ── Phase 2: LLM taxonomy (escalation only) ───────────────────────────────
    if ctx.router:
        try:
            classify_titles = [
                item.get("Title", f"(no title #{i + 1})") for i, item in enumerate(items[:30])
            ]
            numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(classify_titles))
            prompt = (
                "You are a product taxonomy assistant. The list shows Amazon BSR product "
                "titles from a category page that contains mixed product types.\n\n"
                f"{numbered}\n\n"
                "Identify the single DOMINANT product type (the majority group).\n"
                "Return exactly two lines — no other text:\n"
                "Line 1: dominant product type name (3–7 words, e.g. 'mouse snap trap')\n"
                "Line 2: comma-separated 1-based index numbers of titles in that type"
            )
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            lines = [ln.strip() for ln in res.text.strip().splitlines() if ln.strip()]
            if len(lines) >= 2:
                dominant_type = lines[0].strip("\"'").lower()
                keep_idx = {
                    int(x) - 1
                    for x in re.findall(r"\d+", lines[1])
                    if x.isdigit() and 0 <= int(x) - 1 < len(classify_titles)
                }
                if len(keep_idx) >= _MIN_RETAINED:
                    dom_titles = [items[i].get("Title", "") for i in sorted(keep_idx)]
                    refined_ngrams = set(_ngram_candidates(dom_titles, min_doc_freq=2, top_n=15))
                    refined_tokens = {
                        tok for ng in refined_ngrams for tok in ng.split() if len(tok) > 2
                    }
                    refined_tokens |= {tok for tok in dominant_type.split() if len(tok) > 2}

                    def _refined_score(title: str) -> int:
                        t = title.lower()
                        if dominant_type and dominant_type in t:
                            return 3
                        for ng in refined_ngrams:
                            if len(ng.split()) >= 2 and ng in t:
                                return 2
                        t_toks = set(re.findall(r"[a-z0-9]+", t))
                        overlap = refined_tokens & t_toks
                        return len(overlap) if len(overlap) >= 2 else 0

                    refined_scored = [(it, _refined_score(it.get("Title", ""))) for it in items]
                    retained = [it for it, s in refined_scored if s > 0]
                    if len(retained) < _MIN_RETAINED:
                        retained = items
                    n_removed = len(items) - len(retained)
                    sample = [it.get("Title", "")[:60] for it, s in refined_scored if s == 0][:5]
                    ctx.cache["contamination_stats"] = {
                        "status": "filtered",
                        "method": "llm_taxonomy",
                        "dominant_type": dominant_type,
                        "phase2_reason": _phase2_reason,
                        "outlier_rate": round(n_removed / len(items), 3),
                        "n_removed": n_removed,
                        "n_retained": len(retained),
                        "sample_removed": sample,
                    }
                    logger.info(
                        f"[category_coherence] LLM taxonomy: dominant='{dominant_type}', "
                        f"retained {len(retained)}/{len(items)}, removed {n_removed}"
                    )
                    return retained
        except Exception as e:
            logger.warning(f"[category_coherence] Phase 2 LLM failed: {e}")

    # Fallback: both phases failed or were unsafe → warn only
    sample = [it.get("Title", "")[:60] for it in items[:5]]
    ctx.cache["contamination_stats"] = {
        "status": "warning",
        "n_removed": 0,
        "n_retained": n,
        "sample_outliers": sample,
        "note": f"filtering skipped ({_phase2_reason or 'both phases failed'})",
    }
    logger.warning(
        f"[category_coherence] Filtering skipped ({_phase2_reason or 'both phases failed'})"
    )
    return items


async def _fetch_market_signals(items: list[dict], ctx: Any) -> list[dict]:
    """
    Step 2 of 2 for market context. Depends on ctx.cache["core_keywords"].
    Runs ABA keyword data and CPC bid recommendations concurrently.
    Writes: ctx.cache["keyword_data"], ctx.cache["keyword_data_all"],
            ctx.cache["detailed_bid_analysis"]
    """
    core_keywords = ctx.cache.get("core_keywords", ["unknown niche"])
    kw_hash = _hl.md5(",".join(sorted(core_keywords)).encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_SIGNALS, "market_signals", kw_hash)
    if cached is not None:
        ctx.cache["keyword_data"] = cached.get("keyword_data", {})
        ctx.cache["keyword_data_all"] = cached.get("keyword_data_all", [])
        ctx.cache["detailed_bid_analysis"] = cached.get("detailed_bid_analysis", {})
        logger.info(f"[cat_monopoly] Market signals L2 cache hit kw_hash={kw_hash}")
        return items

    async def _fetch_aba() -> None:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        try:
            aba_res = await asyncio.to_thread(
                XiyouZhaociAPI(tenant_id=tenant_id).get_aba_top_asins, country, core_keywords
            )
            search_terms = aba_res.get("searchTerms") or [] if aba_res else []
            ctx.cache["keyword_data"] = search_terms[0] if search_terms else {}
            ctx.cache["keyword_data_all"] = search_terms
        except Exception as e:
            logger.error(f"[fetch_market_signals] ABA fetch failed: {e}")
            ctx.cache.setdefault("keyword_data", {})
            ctx.cache.setdefault("keyword_data_all", [])

    async def _fetch_cpc_bids() -> None:
        try:
            ads_client = AmazonAdsClient(store_id=ctx.config.get("store_id"))
            kws = [
                {"keyword": kw, "matchType": m} for kw in core_keywords for m in ("EXACT", "PHRASE")
            ]
            # Do NOT pass competitor BSR ASINs — the bid API requires an ASIN owned
            # by the advertiser. The client will auto-discover one via _get_owned_asin_fallback().
            # Two separate calls — the API only accepts one strategy string per request.
            legacy_res, auto_res = await asyncio.gather(
                ads_client.get_keyword_bid_recommendations(
                    keywords=kws, strategy="LEGACY_FOR_SALES"
                ),
                ads_client.get_keyword_bid_recommendations(keywords=kws, strategy="AUTO_FOR_SALES"),
                return_exceptions=True,
            )
            ctx.cache["detailed_bid_analysis"] = {
                "LEGACY_FOR_SALES": (
                    legacy_res.get("bidRecommendations", [])
                    if not isinstance(legacy_res, Exception)
                    else []
                ),
                "AUTO_FOR_SALES": (
                    auto_res.get("bidRecommendations", [])
                    if not isinstance(auto_res, Exception)
                    else []
                ),
            }
            if isinstance(legacy_res, Exception):
                logger.error(f"[fetch_market_signals] LEGACY bid fetch failed: {legacy_res}")
            if isinstance(auto_res, Exception):
                logger.error(f"[fetch_market_signals] AUTO bid fetch failed: {auto_res}")
        except Exception as e:
            logger.error(f"[fetch_market_signals] CPC bid fetch failed: {e}")
            ctx.cache.setdefault("detailed_bid_analysis", {})

    await asyncio.gather(_fetch_aba(), _fetch_cpc_bids())
    _l2_set(
        ctx,
        {
            "keyword_data": ctx.cache.get("keyword_data", {}),
            "keyword_data_all": ctx.cache.get("keyword_data_all", []),
            "detailed_bid_analysis": ctx.cache.get("detailed_bid_analysis", {}),
        },
        _TTL_SIGNALS,
        "market_signals",
        kw_hash,
    )
    return items


_SOCIAL_PLATFORMS = ["tiktok", "youtube"]
_SOCIAL_MAX_RETRIES = 2
# Bump when scraping parameters change (video count, window days, brand caps) so that
# stale cache entries built under old settings are never reused. Also bumped to v4 to
# evict legacy "external_intensity" blobs that carried a category_deal_intensity field
# computed under the pre-fix brand-key bug — deal intensity is no longer part of this
# cache entry at all (see _DEAL_CACHE_V below), but old blobs under kw_hash still had it.
_SOCIAL_CACHE_V = "v4"

# Deal intensity has its own cache, independent of social PSI: different inputs (top-10
# ASIN/brand pairs vs. keyword+brand hashtags), different failure modes, and deal
# promotions rotate faster than social sentiment (see _TTL_DEAL). Bump when deal-lookup
# params change (max_pages, ASIN/brand key resolution) to evict stale entries. Bumped to
# v2: pre-v2 brand resolution read item.get("Brand"), which raw BSR items never carry
# (see _brand_lookup below) — every run with an empty ASIN/brand pair list collided on
# the same deal_hash (md5 of an empty joined string is constant regardless of keyword),
# so a single false "0 deals" result was silently reused across every category analysis.
# v3: cached value's shape changed from a bare int to {deal_intensity, deal_discount_data}
# (discount depth + frequency, not just count) — bump so old bare-int blobs are never
# read back as the new dict shape.
_DEAL_CACHE_V = "v3"

# Must match DealHistoryClient.get_deals_for_asins's own default so a standalone
# call and the workflow's cached fetch_deal_count search the same depth.
_DEAL_MAX_PAGES = 2

# Video count per hashtag per platform fetch.
# TikTok: 50 gives ≥3 months of data for a brand posting 3-4x/week, enough to surface
# KOLs who post infrequently. YouTube: 30 covers a similar depth.
_SOCIAL_VIDEO_COUNT_TIKTOK = 50
_SOCIAL_VIDEO_COUNT_YOUTUBE = 30

# KOL detection looks back 90 days — KOL campaigns span months, not weeks.
# Category keyword PSI uses 30 days (freshness signal); brand PSI uses 90 days.
_SOCIAL_BRAND_WINDOW_DAYS = 90
_SOCIAL_KW_WINDOW_DAYS = 30


async def _enrich_external_intensity(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetches multi-platform social signals and deal promotion intensity for the category.

    Platforms searched: TikTok, YouTube Shorts (concurrently per hashtag).
    Hashtags searched:
      1. Category keyword hashtag  (e.g. #gnattrapsforhouseindoor)
      2. Top-5 brand hashtags by BSR frequency (e.g. #ZEVO, #Catchmaster)
      3. New-entrant brand hashtags (products listed in last 12 months, up to 8)
    Brand data requires fetch_sellersprite_bsr to have run first (previous step).
    """
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword:
        return items

    # ── brand sets from Sellersprite snapshot ────────────────────────────────
    _MAX_TOP_BRANDS = 5
    _MAX_NEW_ENTRANT_BRANDS = 8
    ss_snapshots = ctx.cache.get("sellersprite_snapshots") or {}
    _top_brand_set: set[str] = set()
    _new_entrant_brand_set: set[str] = set()
    # ASIN → brand lookup: raw BSR item dicts never carry a real brand (Amazon's BSR
    # card HTML doesn't expose it — see BestSellersExtractor), so every per-item brand
    # lookup below (weakest-rank-slot brand, deal-intensity ASIN/brand pairs) must
    # resolve through the Sellersprite snapshot instead of item.get("Brand").
    _brand_lookup: dict[str, str] = {}

    if ss_snapshots:
        base_ym = ctx.cache.get("sellersprite_base_ym", "")
        _latest_snap = ss_snapshots.get(max(ss_snapshots), [])
        _brand_lookup = {
            p["asin"].upper(): p["brand"] for p in _latest_snap if p.get("asin") and p.get("brand")
        }

        # Top brands by product count in BSR
        _brand_counts: Counter = Counter(p["brand"] for p in _latest_snap if p.get("brand"))
        _top_brand_set = {b for b, _ in _brand_counts.most_common(_MAX_TOP_BRANDS) if b}

        # New-entrant brands: products listed within the last 12 months
        # Anchor cutoff to base_ym (T-2 months) to match _run_monopoly_analysis logic
        _cutoff_ms = _new_entrant_cutoff_ms(base_ym)

        _new_entrant_products = [
            p
            for p in _latest_snap
            if p.get("available_date_ms")
            and p["available_date_ms"] >= _cutoff_ms
            and p.get("brand")
        ]
        # Preserve rank order (lower rank = higher BSR position = more relevant)
        _seen: set[str] = set()
        for _p in sorted(_new_entrant_products, key=lambda x: x.get("rank") or 999):
            if _p["brand"] not in _seen and len(_seen) < _MAX_NEW_ENTRANT_BRANDS:
                _new_entrant_brand_set.add(_p["brand"])
                _seen.add(_p["brand"])

    # All brands to search: top brands ∪ new entrants, deduped, top brands first
    _all_brand_list = list(
        dict.fromkeys(sorted(_top_brand_set) + sorted(_new_entrant_brand_set - _top_brand_set))
    )

    # Guarantee coverage for the weakest-rank-slot brand (BSR rank 10–50, fewest reviews) —
    # it is the primary displacement target in the report and may fall outside the top-5
    # and new-entrant sets. Must be appended before kw_hash is computed so the cache key
    # reflects the actual fetch scope.
    _wrs_brand: str | None = None
    _wrs_reviews = float("inf")
    for _wrs_item in items:
        _wrs_rank_m = re.search(
            r"\d+", str(_wrs_item.get("Rank") or _wrs_item.get("rank") or "").replace(",", "")
        )
        _wrs_rev_m = re.search(
            r"\d+", str(_wrs_item.get("Reviews") or _wrs_item.get("reviews") or "").replace(",", "")
        )
        if not (_wrs_rank_m and _wrs_rev_m):
            continue
        _wrs_rank_val, _wrs_rev_val = int(_wrs_rank_m.group()), int(_wrs_rev_m.group())
        if 10 <= _wrs_rank_val <= 50 and _wrs_rev_val < _wrs_reviews:
            _wrs_asin = (_wrs_item.get("ASIN") or _wrs_item.get("asin") or "").strip().upper()
            _b = (
                _brand_lookup.get(_wrs_asin)
                or (_wrs_item.get("Brand") or _wrs_item.get("brand") or "").strip()
            )
            if _b:
                _wrs_reviews, _wrs_brand = _wrs_rev_val, _b
    if _wrs_brand and _wrs_brand not in _all_brand_list:
        _all_brand_list.append(_wrs_brand)
        logger.info(
            f"[external_intensity] Appended weakest-rank-slot brand '{_wrs_brand}' "
            f"({int(_wrs_reviews)} reviews, rank 10–50) to social fetch"
        )

    kw_hash = _hl.md5(
        (_SOCIAL_CACHE_V + main_keyword + "|" + ",".join(sorted(_all_brand_list))).encode()
    ).hexdigest()[:12]

    # ── deal intensity (independent cache: own inputs, TTL, and failure mode) ──
    # Computed unconditionally, ahead of the social PSI cache logic below, so a social
    # cache hit/miss can never skip or clobber the deal intensity for this run — the two
    # signals used to share one "external_intensity" cache entry, which meant a social
    # cache hit would return stale (or, under a since-fixed brand-key bug, always-zero)
    # deal intensity without ever recomputing it.
    _deal_asin_brand_pairs = sorted(
        {
            (asin, brand)
            for item in items[:10]
            if (asin := (item.get("ASIN") or item.get("asin") or "").strip().upper())
            and (
                brand := _brand_lookup.get(asin)
                or (item.get("Brand") or item.get("brand") or "").strip()
            )
        }
    )
    if not _deal_asin_brand_pairs:
        # No resolvable ASIN/brand pairs (brand parsing failed for every top-10 item) —
        # the fetch below would run with zero brands and deterministically yield 0, so
        # skip the L2 cache round-trip entirely rather than keying it off a fixed hash
        # (md5 of an empty joined string) that every such category would collide on.
        logger.info("[external_intensity] No resolvable ASIN/brand pairs; deal intensity=0")
        ctx.cache["category_deal_intensity"] = 0
        ctx.cache["category_deal_discount_data"] = {}
    else:
        _deal_hash = _hl.md5(
            (
                _DEAL_CACHE_V
                + "|"
                + str(_DEAL_MAX_PAGES)
                + "|"
                + ",".join(f"{a}:{b}" for a, b in _deal_asin_brand_pairs)
            ).encode()
        ).hexdigest()[:12]

        _cached_deal = _l2_get(ctx, _TTL_DEAL, "deal_intensity", _deal_hash)
        if _cached_deal is not None:
            ctx.cache["category_deal_intensity"] = _cached_deal["deal_intensity"]
            ctx.cache["category_deal_discount_data"] = _cached_deal["deal_discount_data"]
            logger.info(
                f"[external_intensity] Deal intensity L2 cache hit deal_hash={_deal_hash} "
                f"value={_cached_deal['deal_intensity']}"
            )
        else:
            _deal_client = DealHistoryClient()

            # Group by brand: several of the top-10 ASINs often share a brand, and a brand
            # search + candidate resolution pass is identical regardless of which of the
            # brand's ASINs is being confirmed, so search each brand only once.
            _brand_to_asins: dict[str, set[str]] = {}
            for _pair_asin, _pair_brand in _deal_asin_brand_pairs:
                _brand_to_asins.setdefault(_pair_brand, set()).add(_pair_asin)

            async def fetch_brand_deals(brand: str, asins: set[str]) -> list[dict]:
                matched = await _deal_client.get_deals_for_asins(
                    asins=asins, brand=brand, max_pages=_DEAL_MAX_PAGES
                )
                deals = [d for _v in matched.values() for d in _v]
                logger.info(
                    f"[external_intensity] deal_count brand={brand!r} asins={sorted(asins)}: "
                    f"{len(deals)} deals"
                )
                return deals

            try:
                deal_lists = await asyncio.gather(
                    *(fetch_brand_deals(brand, asins) for brand, asins in _brand_to_asins.items())
                )
                all_deals = [d for _lst in deal_lists for d in _lst]
                total_deals = len(all_deals)
                _deal_intensity = (
                    9 if total_deals > 5 else 6 if total_deals > 2 else 3 if total_deals > 0 else 0
                )

                # Discount depth: how deep are these deals, not just how many there are.
                _discounts = [
                    float(d["discount_pct"])
                    for d in all_deals
                    if isinstance(d.get("discount_pct"), int | float) and d["discount_pct"] > 0
                ]
                _avg_discount_pct = round(statistics.mean(_discounts), 1) if _discounts else None
                _discount_buckets: Counter = Counter()
                for _pct in _discounts:
                    _bucket = (
                        "shallow_lt15"
                        if _pct < 15
                        else "moderate_15_30"
                        if _pct < 30
                        else "deep_30plus"
                    )
                    _discount_buckets[_bucket] += 1

                # Frequency: deals per month over the observed date span of the confirmed
                # deals — tells you how often this category goes on sale, not just how many
                # sales happened to land in this one pull.
                _deal_dates: list[datetime] = []
                for d in all_deals:
                    _raw_date = d.get("date")
                    if not _raw_date:
                        continue
                    try:
                        # fuzzy=False: both sites emit a bare date string (ISO timestamp or
                        # "Month D, YYYY"), never prose — fuzzy matching would otherwise
                        # misparse an unrelated numeric string (e.g. a price) as a fake date.
                        _dt = _dateutil_parser.parse(str(_raw_date), fuzzy=False)
                        _deal_dates.append(_dt.replace(tzinfo=None))
                    except (ValueError, OverflowError, TypeError):
                        continue
                _deal_frequency_per_month: float | None = None
                if len(_deal_dates) >= 2:
                    _span_days = (max(_deal_dates) - min(_deal_dates)).days
                    if _span_days > 0:
                        _deal_frequency_per_month = round(total_deals / (_span_days / 30.4), 1)

                _deal_discount_data = {
                    "avg_discount_pct": _avg_discount_pct,
                    "discount_buckets": dict(_discount_buckets),
                    "n_deals_with_discount": len(_discounts),
                    "deal_frequency_per_month": _deal_frequency_per_month,
                    "n_deals_with_date": len(_deal_dates),
                }

                ctx.cache["category_deal_intensity"] = _deal_intensity
                ctx.cache["category_deal_discount_data"] = _deal_discount_data
                _l2_set(
                    ctx,
                    {
                        "deal_intensity": _deal_intensity,
                        "deal_discount_data": _deal_discount_data,
                    },
                    _TTL_DEAL,
                    "deal_intensity",
                    _deal_hash,
                )
                logger.info(
                    f"[external_intensity] deal_count total={total_deals} "
                    f"avg_discount={_avg_discount_pct} freq/mo={_deal_frequency_per_month} "
                    f"across {len(_deal_asin_brand_pairs)} ASINs "
                    f"({len(_brand_to_asins)} brand searches)"
                )
            except Exception as e:
                logger.error(f"[external_intensity] Deal intensity: {e}")
                ctx.cache["category_deal_intensity"] = ctx.cache.get("category_deal_intensity", 0)
                ctx.cache["category_deal_discount_data"] = ctx.cache.get(
                    "category_deal_discount_data", {}
                )

    # ── helpers (defined before cache check so partial-retry can reuse them) ──

    @exponential_backoff(
        max_retries=_SOCIAL_MAX_RETRIES, base_delay=1.0, retry_on_exceptions=(Exception,)
    )
    async def _platform_psi(
        tag: str, platform: str, window_days: int = _SOCIAL_KW_WINDOW_DAYS
    ) -> dict:
        """Fetch PSI for one hashtag on one platform. Non-retryable soft failures
        (no tag, unsupported platform) return a result dict; retryable network/API
        errors are raised so the decorator can back off and retry.

        window_days controls the PSI scoring window and KOL/KOC matrix lookback.
        Use _SOCIAL_KW_WINDOW_DAYS (30d) for category keyword freshness signals;
        use _SOCIAL_BRAND_WINDOW_DAYS (90d) for brand-level KOL campaign detection.
        """
        clean = re.sub(r"[^a-zA-Z0-9]", "", tag)
        result_base = {
            "platform": platform,
            "hashtag": f"#{clean}",
            "psi": 0,
            "video_count": 0,
            "n_kol": 0,
            "n_koc": 0,
            "unique_creators": 0,
            "promo_tag_ratio": 0,
            "native_ad_ratio": 0,
            "engagement_rate": 0,
            "organic_multiplier": 0,
            "hhi_concentration": 0,
            "recent_videos_ratio": 0,
            "avg_views_per_video": 0,
            "amazon_mentions_ratio": 0,
            "trend": "no_data",
        }
        if platform == "tiktok":
            client = TikTokClient()
            tag_info = await asyncio.to_thread(client.get_tag_info, clean)
            if not tag_info.get("id"):
                return {**result_base, "verdict": "No Tag Found"}
            videos = await asyncio.to_thread(
                client.get_hashtag_videos, tag_info["id"], clean, count=_SOCIAL_VIDEO_COUNT_TIKTOK
            )
            # TikTok's challenge page aggregates partial/superset/fuzzy tags server-side,
            # so raw results include videos for #BAIM, #BAIMNOCMSJF, #BGAIMSNOCM when
            # searching #BAIMNOCM.  Keep only videos whose desc contains the exact tag
            # as a standalone token (not preceded or followed by another alphanumeric char).
            _exact = re.compile(
                r"(?<![a-zA-Z0-9])#" + re.escape(clean) + r"(?![a-zA-Z0-9])",
                re.IGNORECASE,
            )
            videos = [v for v in videos if _exact.search(v.get("desc", ""))]
            if not videos:
                return {**result_base, "verdict": "No Exact Tag Match"}
            analysis = SocialViralityProcessor().calculate_promotion_strength(
                videos, tag_metadata=tag_info, platform="tiktok", window_days=window_days
            )
        elif platform == "youtube":
            client = YouTubeClient()
            tag_info = await asyncio.to_thread(client.get_hashtag_info, clean)
            if not tag_info.get("video_count"):
                return {**result_base, "verdict": "No Tag Found"}
            videos = await asyncio.to_thread(
                client.get_hashtag_videos, clean, count=_SOCIAL_VIDEO_COUNT_YOUTUBE
            )
            # get_hashtag_videos returns raw ytInitialData videoRenderer dicts;
            # "youtube_hashtag" is the correct normalizer (not "youtube_shorts" which
            # expects YouTube Data API v3 statistics/snippet fields).
            analysis = SocialViralityProcessor().calculate_promotion_strength(
                videos, tag_metadata=tag_info, platform="youtube_hashtag", window_days=window_days
            )
        else:
            return {**result_base, "verdict": "Unsupported"}
        _kol_koc = analysis.get("kol_koc_matrix", {})
        _n_videos = len(videos) or 1
        return {
            "platform": platform,
            "hashtag": f"#{clean}",
            "psi": analysis.get("strength_score", 0),
            "verdict": analysis.get("verdict", "Unknown"),
            "video_count": len(videos),
            "n_kol": _kol_koc.get("n_kol_unique", 0),
            "n_koc": _kol_koc.get("n_koc_unique", 0),
            "unique_creators": _kol_koc.get("unique_creators_total", 0),
            # Paid-vs-organic split: promo_tag_ratio catches native ad flags + #ad-style
            # hashtags; native_ad_ratio isolates the subset caught by the platform's own
            # ad field (more authoritative, since it doesn't rely on caption text).
            "promo_tag_ratio": analysis.get("promo_tag_ratio", 0),
            "native_ad_ratio": analysis.get("native_ad_ratio", 0),
            # Engagement quality: raw rate + views/follower ratio (virality independent
            # of follower count — a creator can have few followers but go viral).
            "engagement_rate": analysis.get("engagement_rate", 0),
            "organic_multiplier": analysis.get("organic_multiplier", 0),
            # Creator concentration: high HHI means a few creators drive most views —
            # fragile buzz that can vanish if those creators stop posting.
            "hhi_concentration": analysis.get("hhi_concentration", 0),
            # Momentum: recent_videos_ratio is the raw 30-day recency rate (also a PSI
            # input); trend is a cheap categorical label derived from the 12-month
            # monthly_distribution so the LLM doesn't need to reason over a raw array.
            "recent_videos_ratio": analysis.get("recent_videos_ratio", 0),
            "trend": _classify_social_momentum(analysis.get("monthly_distribution", [])),
            "avg_views_per_video": analysis.get("avg_views_per_video", 0),
            # Caption-based buy-intent proxy (no comment API involved — comment_analysis
            # is always "Not Analyzed" in this pipeline since comments are never fetched
            # here, so it's deliberately omitted rather than exposing a dead field).
            "amazon_mentions_ratio": round(analysis.get("amazon_mentions", 0) / _n_videos, 4),
        }

    async def _tag_all_platforms(tag: str, window_days: int = _SOCIAL_KW_WINDOW_DAYS) -> dict:
        """Search one hashtag across all platforms concurrently.
        Each platform call retries internally via @exponential_backoff; only if
        retries are exhausted does gather capture the exception here."""
        clean = re.sub(r"[^a-zA-Z0-9]", "", tag)
        raw = await asyncio.gather(
            *[_platform_psi(tag, p, window_days=window_days) for p in _SOCIAL_PLATFORMS],
            return_exceptions=True,
        )
        platform_results = []
        for p, result in zip(_SOCIAL_PLATFORMS, raw, strict=False):
            if isinstance(result, Exception):
                logger.warning(f"[external_intensity] {p} #{clean} exhausted retries: {result}")
                platform_results.append(
                    {
                        "platform": p,
                        "hashtag": f"#{clean}",
                        "psi": 0,
                        "verdict": "Error",
                        "video_count": 0,
                        "n_kol": 0,
                        "n_koc": 0,
                        "unique_creators": 0,
                        "promo_tag_ratio": 0,
                        "native_ad_ratio": 0,
                        "engagement_rate": 0,
                        "organic_multiplier": 0,
                        "hhi_concentration": 0,
                        "recent_videos_ratio": 0,
                        "avg_views_per_video": 0,
                        "amazon_mentions_ratio": 0,
                        "trend": "no_data",
                    }
                )
            else:
                platform_results.append(result)
        best = max(platform_results, key=lambda x: x["psi"])
        return {
            "psi": best["psi"],
            "verdict": best["verdict"],
            "video_count": sum(r["video_count"] for r in platform_results),
            "n_kol": sum(r.get("n_kol", 0) for r in platform_results),
            "n_koc": sum(r.get("n_koc", 0) for r in platform_results),
            "unique_creators": sum(r.get("unique_creators", 0) for r in platform_results),
            "platforms": platform_results,
        }

    async def _try_kw_candidates(
        candidates: list[str], window_days: int = _SOCIAL_KW_WINDOW_DAYS
    ) -> tuple[str, dict]:
        """Try keyword hashtag candidates in order; return (winning_tag, result).

        Stops at the first candidate that any platform actually finds (verdict is
        not 'No Tag Found').  This lets "#fruitflytrap" (real 3-word tag) resolve
        correctly while still falling back to "#fruitfly" or "#fruit" when the
        full concatenation doesn't exist.
        """
        last_tag, last_result = candidates[-1], {}
        for candidate in candidates:
            result = await _tag_all_platforms(candidate, window_days=window_days)
            last_tag, last_result = candidate, result
            if any(
                p.get("verdict") not in ("No Tag Found", "No Exact Tag Match", "Error")
                for p in result.get("platforms", [])
            ):
                break
        return last_tag, last_result

    def _collect_failed(results: list[dict]) -> list[tuple]:
        """Return (result_idx, platform_idx) pairs where verdict == 'Error'."""
        return [
            (ri, pi)
            for ri, r in enumerate(results)
            for pi, p in enumerate(r.get("platforms", []))
            if p.get("verdict") == "Error"
        ]

    def _recompute_aggregate(result: dict) -> None:
        """Recompute top-level aggregates from platforms list in-place."""
        platforms = result["platforms"]
        best = max(platforms, key=lambda x: x["psi"])
        result.update(
            {
                "psi": best["psi"],
                "verdict": best["verdict"],
                "video_count": sum(p["video_count"] for p in platforms),
                "n_kol": sum(p.get("n_kol", 0) for p in platforms),
                "n_koc": sum(p.get("n_koc", 0) for p in platforms),
                "unique_creators": sum(p.get("unique_creators", 0) for p in platforms),
            }
        )

    # ── L2 cache check ────────────────────────────────────────────────────────
    cached = _l2_get(ctx, _TTL_EXTERNAL, "external_intensity", kw_hash)
    if cached is not None:
        # Reconstruct tag_results shape from cache to reuse retry logic uniformly
        _c_kw = {
            "psi": cached.get("category_social_psi", 0),
            "verdict": cached.get("category_social_verdict", "Unknown"),
            "video_count": sum(
                p.get("video_count", 0) for p in cached.get("category_social_platforms", [])
            ),
            "platforms": [dict(p) for p in cached.get("category_social_platforms", [])],
        }
        _c_brands_raw = cached.get("brand_social_data", [])
        _c_brand_results = [
            {
                "psi": e["psi"],
                "verdict": e["verdict"],
                "video_count": e["video_count"],
                "platforms": [dict(p) for p in e.get("platforms", [])],
            }
            for e in _c_brands_raw
        ]
        _c_all = [_c_kw] + _c_brand_results
        _c_tags = [cached.get("category_kw_tag", "".join(main_keyword.split()))] + [
            e["brand"] for e in _c_brands_raw
        ]

        if not _collect_failed(_c_all):
            ctx.cache.update(cached)
            logger.info(f"[cat_monopoly] External intensity L2 cache hit kw_hash={kw_hash}")
            return items

        _failed = _collect_failed(_c_all)
        logger.info(
            f"[cat_monopoly] Cache hit but {len(_failed)} platform error(s) found, "
            f"re-fetching kw_hash={kw_hash}"
        )
        # Call _platform_psi directly — @exponential_backoff handles retries per call.
        retry_raw = await asyncio.gather(
            *[
                _platform_psi(_c_tags[ri], _c_all[ri]["platforms"][pi]["platform"])
                for ri, pi in _failed
            ],
            return_exceptions=True,
        )
        for (ri, pi), result in zip(_failed, retry_raw, strict=False):
            if isinstance(result, Exception):
                logger.warning(
                    f"[external_intensity] {_c_all[ri]['platforms'][pi]['platform']} "
                    f"#{_c_tags[ri]} still failing after retries: {result}"
                )
            else:
                _c_all[ri]["platforms"][pi] = result
            _recompute_aggregate(_c_all[ri])

        _upd_brand_data = [
            {
                **{
                    k: v
                    for k, v in e.items()
                    if k not in ("psi", "verdict", "video_count", "platforms")
                },
                **tr,
            }
            for e, tr in zip(_c_brands_raw, _c_all[1:], strict=False)
        ]
        _upd_brand_data.sort(key=lambda x: x["psi"], reverse=True)
        _rt_kw_psi = _c_all[0]["psi"]
        _rt_kw_verdict = _c_all[0]["verdict"]
        _rt_top_brand_entries = sorted(
            [
                (r["psi"], r["brand"], r["verdict"])
                for r in _upd_brand_data
                if r.get("is_top_brand") and r["psi"] > 0
            ],
            reverse=True,
        )
        _rt_best_bp = _rt_top_brand_entries[0][0] if _rt_top_brand_entries else 0.0
        if _rt_best_bp > _rt_kw_psi:
            _rt_eff_psi = _rt_best_bp
            _rt_eff_verdict = (
                f"{_rt_top_brand_entries[0][2]} (via #{_rt_top_brand_entries[0][1]} brand tag)"
            )
        else:
            _rt_eff_psi = _rt_kw_psi
            _rt_eff_verdict = _rt_kw_verdict
        _upd_cached = {
            **cached,
            "category_social_psi": _rt_eff_psi,
            "category_social_verdict": _rt_eff_verdict,
            "category_kw_psi": _rt_kw_psi,
            "category_kw_verdict": _rt_kw_verdict,
            "category_social_platforms": _c_all[0]["platforms"],
            "brand_social_data": _upd_brand_data,
        }
        ctx.cache.update(_upd_cached)
        _still_errors = len(_collect_failed(_c_all))
        if _still_errors == 0:
            _l2_set(ctx, _upd_cached, _TTL_EXTERNAL, "external_intensity", kw_hash)
            logger.info(
                f"[external_intensity] Partial re-fetch succeeded, cache refreshed kw_hash={kw_hash}"
            )
        else:
            logger.warning(
                f"[external_intensity] {_still_errors} platform(s) still failing after retries, "
                f"skipping cache write kw_hash={kw_hash}"
            )
        return items

    # ── fresh fetch ───────────────────────────────────────────────────────────
    # Build ordered keyword tag candidates: full concat first so "#fruitflytrap"
    # resolves when it exists; shorter forms are tried only if the full tag has no
    # presence on any platform.  Without this, long Amazon search phrases like
    # "gnat killer indoor" would create "#gnatkillernindoor" — a non-existent tag.
    _kw_words = main_keyword.split()
    _kw_candidates = list(
        dict.fromkeys(
            [
                "".join(_kw_words),  # full:   "fruitflytrap"
                "".join(_kw_words[:2]),  # 2-word: "fruitfly"
                _kw_words[0],  # 1-word: "fruit"
            ]
        )
    )

    async def _kw_coro() -> tuple[str, dict]:
        return await _try_kw_candidates(_kw_candidates, window_days=_SOCIAL_KW_WINDOW_DAYS)

    _all_coros = [_kw_coro()] + [
        _tag_all_platforms(b, window_days=_SOCIAL_BRAND_WINDOW_DAYS) for b in _all_brand_list
    ]
    _all_gathered = await asyncio.gather(*_all_coros)
    kw_tag, kw_result = _all_gathered[0]
    _brand_tag_results = list(_all_gathered[1:])
    # _try_kw_candidates / _tag_all_platforms exhausted per-platform retries via
    # @exponential_backoff; any remaining "Error" entries are unrecoverable.

    brand_results = [
        {
            "brand": brand,
            "is_top_brand": brand in _top_brand_set,
            "is_new_entrant": brand in _new_entrant_brand_set,
            **res,
        }
        for brand, res in zip(_all_brand_list, _brand_tag_results, strict=False)
    ]
    brand_results.sort(key=lambda x: x["psi"], reverse=True)

    # Effective category PSI: max across keyword hashtag and top-brand hashtags.
    # Amazon multi-word search terms rarely exist as TikTok/YouTube tags, so the
    # keyword PSI is often 0 even when the category has strong social activity.
    # Brand hashtags are a more reliable proxy for the creator ecosystem around a niche.
    _kw_psi = kw_result["psi"]
    _kw_verdict = kw_result["verdict"]
    _top_brand_psi_entries = sorted(
        [
            (r["psi"], r["brand"], r["verdict"])
            for r in brand_results
            if r.get("is_top_brand") and r["psi"] > 0
        ],
        reverse=True,
    )
    _best_brand_psi = _top_brand_psi_entries[0][0] if _top_brand_psi_entries else 0.0
    if _best_brand_psi > _kw_psi:
        _eff_psi = _best_brand_psi
        _eff_verdict = (
            f"{_top_brand_psi_entries[0][2]} (via #{_top_brand_psi_entries[0][1]} brand tag)"
        )
    else:
        _eff_psi = _kw_psi
        _eff_verdict = _kw_verdict
    ctx.cache.update(
        {
            "category_social_psi": _eff_psi,
            "category_social_verdict": _eff_verdict,
            "category_kw_psi": _kw_psi,
            "category_kw_verdict": _kw_verdict,
            "category_social_platforms": kw_result["platforms"],
            "brand_social_data": brand_results,
        }
    )
    _platform_summary = ", ".join(
        r["platform"] + "=" + str(r["psi"]) for r in kw_result["platforms"]
    )
    _brand_summary = " | ".join(
        "#"
        + re.sub(r"[^a-zA-Z0-9]", "", r["brand"])
        + "("
        + ("top" if r["is_top_brand"] else "")
        + ("/" if r["is_top_brand"] and r["is_new_entrant"] else "")
        + ("new" if r["is_new_entrant"] else "")
        + ")"
        + " PSI="
        + str(r["psi"])
        for r in brand_results
    )
    logger.info(
        f"[external_intensity] #{kw_tag} PSI={kw_result['psi']} ({_platform_summary})"
        + (f" | {_brand_summary}" if _brand_summary else "")
    )

    # Deal intensity was already fetched/cached independently above; not part of this
    # (social-only) cache entry — see the "deal intensity" block before the L2 cache check.
    _ext = {
        "category_social_psi": ctx.cache.get("category_social_psi", 0),
        "category_social_verdict": ctx.cache.get("category_social_verdict", "Unknown"),
        "category_kw_psi": ctx.cache.get("category_kw_psi", 0),
        "category_kw_verdict": ctx.cache.get("category_kw_verdict", "Unknown"),
        "category_social_platforms": ctx.cache.get("category_social_platforms", []),
        "brand_social_data": ctx.cache.get("brand_social_data", []),
        "category_kw_tag": kw_tag,
    }
    _remaining_errors = len(_collect_failed([kw_result] + _brand_tag_results))
    if _remaining_errors == 0:
        _l2_set(ctx, _ext, _TTL_EXTERNAL, "external_intensity", kw_hash)
    else:
        logger.warning(
            f"[external_intensity] {_remaining_errors} platform(s) still failing after retries, "
            f"skipping cache write kw_hash={kw_hash}"
        )
    logger.info(
        f"External intensity: Social PSI={_ext['category_social_psi']}, "
        f"Deal Intensity={ctx.cache.get('category_deal_intensity', 0)}, "
        f"Brands searched: {len(_ext['brand_social_data'])}"
    )
    return items


async def _fetch_historical_trends(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 12-month daily BSR, rating, and price time-series for Top 20 ASINs.

    Stored fields per day: date, bsr, stars, ratings, price.
    Consumers:
      - CategoryMonopolyAnalyzer._analyze_market_churn()  (bsr)
      - CategoryMonopolyAnalyzer._analyze_seasonality()   (bsr)
      - _run_monopoly_analysis price-trend block           (price: first-30d vs last-30d median)
    Runs concurrently; failures are soft-skipped so the workflow is never blocked.
    """
    top_asins = [
        (item.get("ASIN") or item.get("asin"))
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    ]
    if not top_asins:
        return items

    _tz = ZoneInfo(
        ctx.config.get("timezone", "America/Los_Angeles")
        if hasattr(ctx, "config")
        else "America/Los_Angeles"
    )
    _now = datetime.now(tz=_tz)
    end_date = (_now - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (_now - timedelta(days=365)).strftime("%Y-%m-%d")
    country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
    api = XiyouZhaociAPI(tenant_id=tenant_id)
    historical_data: dict[str, list[dict[str, Any]]] = {}

    def _parse_daily_records(res: dict, asin: str) -> list:
        """
        Extract daily records from the Xiyouzhaoci get_asin_daily_trends response.

        Actual API shape (confirmed via test_xiyou_date_limits.py):
          res["entities"][i]["trends"][j]["localDate" | "date", "bsr_rank", "rating",
                                          "review_count", "price"]

        Legacy / alternative shapes also handled:
          res["data"]["entities"][i]["dailyData"]
          res["data"][asin]["dailyData"]
          res["data"]   (flat list)

        Returns a normalised list of {"date", "bsr", "stars", "ratings", "price"}.
        """

        def _normalise(daily_list: list) -> list:
            out = []
            for d in daily_list:
                # Date field: actual API uses "localDate"; legacy used "date"
                raw_date = d.get("localDate") or d.get("date") or ""
                date_str = str(raw_date)[:10]
                if not date_str or date_str == "N":
                    continue
                out.append(
                    {
                        "date": date_str,
                        # Actual API: "bsr_rank"; legacy: "bsr" / "bestSellerRank"
                        "bsr": d.get("bsr_rank") or d.get("bsr") or d.get("bestSellerRank"),
                        # Actual API: "rating"; legacy: "stars" / "avgStarRating"
                        "stars": d.get("rating") or d.get("stars") or d.get("avgStarRating"),
                        # Actual API: "review_count"; legacy: "ratings" / "reviewCount"
                        "ratings": d.get("review_count")
                        or d.get("ratings")
                        or d.get("reviewCount"),
                        "price": d.get("price"),
                    }
                )
            return out

        def _entity_trends(entity: dict) -> list:
            # Actual API uses "trends"; legacy used "dailyData"
            return entity.get("trends") or entity.get("dailyData") or []

        # ── Actual shape: {"entities": [...]} at top level (no "data" wrapper) ──
        top_entities = res.get("entities")
        if isinstance(top_entities, list):
            for entity in top_entities:
                if entity.get("asin") == asin:
                    return _normalise(_entity_trends(entity))

        data = res.get("data") or {}

        # ── Legacy shape A: {"data": {"entities": [...]}} ──
        if isinstance(data, dict) and "entities" in data:
            for entity in data["entities"] or []:
                if entity.get("asin") == asin:
                    return _normalise(_entity_trends(entity))

        # ── Legacy shape B: {"data": {"B0xxx": {"dailyData": [...]}}} ──
        if isinstance(data, dict) and asin in data:
            asin_data = data[asin]
            daily = (
                _entity_trends(asin_data)
                if isinstance(asin_data, dict)
                else (asin_data if isinstance(asin_data, list) else [])
            )
            return _normalise(daily)

        # ── Legacy shape C: {"data": [...]} flat list ──
        if isinstance(data, list):
            return _normalise(data)

        logger.debug(
            f"[historical_trends] Unrecognised response shape for {asin}: "
            f"top-level keys={list(res.keys())}"
        )
        return []

    async def _fetch_one(asin: str) -> None:
        try:
            res = await asyncio.to_thread(
                api.get_asin_daily_trends, country, asin, start_date, end_date
            )
            records = _parse_daily_records(res, asin)
            if records:
                historical_data[asin] = records
            else:
                logger.debug(
                    f"[historical_trends] No records parsed for {asin}; top-level keys: {list(res.keys())}"
                )
        except Exception as e:
            logger.warning(f"Historical trend fetch skipped for {asin}: {e}")

    await asyncio.gather(*[_fetch_one(asin) for asin in top_asins])
    ctx.cache["historical_data"] = historical_data
    logger.info(
        f"Historical trends fetched: {len(historical_data)}/{len(top_asins)} ASINs ({start_date} → {end_date})"
    )
    return items


async def _enrich_batch_traffic_scores(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetches batch traffic scores for Top 20 ASINs and derives three ad-dependency signals:
      - actual_bsr_ad_ratio: mean ad_traffic_ratio across the set (steady-state ad burden input)
      - ad_ratio_stdev: population stdev — high spread means the category is split between
        ad-reliant and organically-driven sellers rather than uniformly ad-dependent
      - ad_ratio_rank_correlation: Pearson r between BSR rank and ad_traffic_ratio. Positive
        (better rank ↔ lower ad reliance) signals an organic moat at the top; near-zero/negative
        signals rank is bought, i.e. the category is winnable by outspending on ads.
    Also caches ad_ratio_by_asin so the dominant-brand-portfolio step downstream can check
    whether ad dependency is consistent across one brand's own ASINs (brand-moat signal).
    """
    if not items or not ctx.mcp:
        return items

    _top_items = [item for item in items[:20] if (item.get("ASIN") or item.get("asin"))]
    top_asins = sorted(
        {(item.get("ASIN") or item.get("asin")).strip().upper() for item in _top_items}
    )
    if not top_asins:
        return items

    # BSR rank per ASIN, from the same Top-20 slice used to build top_asins. Falls back to
    # ordinal position when the Rank field is missing/unparseable.
    _rank_by_asin: dict[str, int] = {}
    for _pos, item in enumerate(_top_items, start=1):
        _asin = (item.get("ASIN") or item.get("asin")).strip().upper()
        _rank_m = re.search(
            r"\d+", str(item.get("Rank") or item.get("rank") or "").replace(",", "")
        )
        _rank_by_asin.setdefault(_asin, int(_rank_m.group()) if _rank_m else _pos)

    asins_hash = _hl.md5(",".join(top_asins).encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_TRAFFIC, "traffic_scores", asins_hash)
    if cached is not None:
        ctx.cache["actual_bsr_ad_ratio"] = cached["actual_bsr_ad_ratio"]
        ctx.cache["ad_ratio_stdev"] = cached.get("ad_ratio_stdev")
        ctx.cache["ad_ratio_rank_correlation"] = cached.get("ad_ratio_rank_correlation")
        ctx.cache["ad_ratio_by_asin"] = cached.get("ad_ratio_by_asin") or {}
        logger.info(f"[cat_monopoly] Traffic scores L2 cache hit asins_hash={asins_hash}")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        # call_tool_json() already unwraps and JSON-decodes the tool's response —
        # it returns the parsed payload directly (a dict here), not a list of
        # raw content items requiring a second .get("text") + json.loads().
        resp = await ctx.mcp.call_tool_json(
            "xiyou_get_traffic_scores", {"asins": top_asins, "country": country}
        )
        entities = resp.get("entities") or [] if isinstance(resp, dict) else []

        ratio_by_asin: dict[str, float] = {}
        for d in entities:
            _asin = (d.get("asin") or "").strip().upper()
            v = d.get("advertisingTrafficScoreRatio")
            if not _asin or v is None:
                continue
            try:
                ratio_by_asin[_asin] = float(v)
            except (TypeError, ValueError):
                continue

        if ratio_by_asin:
            ratios = list(ratio_by_asin.values())
            avg_ratio = statistics.mean(ratios)
            stdev_ratio = statistics.pstdev(ratios) if len(ratios) > 1 else 0.0

            # Need a handful of points for the correlation to mean anything — below that,
            # noise dominates and we'd rather report "insufficient data" than a spurious ±1.
            rank_corr: float | None = None
            _paired = [
                (_rank_by_asin[a], r) for a, r in ratio_by_asin.items() if a in _rank_by_asin
            ]
            if len(_paired) >= 5:
                _ranks, _rs = zip(*_paired, strict=False)
                try:
                    rank_corr = statistics.correlation(_ranks, _rs)
                except statistics.StatisticsError:
                    rank_corr = None

            ctx.cache["actual_bsr_ad_ratio"] = avg_ratio
            ctx.cache["ad_ratio_stdev"] = stdev_ratio
            ctx.cache["ad_ratio_rank_correlation"] = rank_corr
            ctx.cache["ad_ratio_by_asin"] = ratio_by_asin
            _l2_set(
                ctx,
                {
                    "actual_bsr_ad_ratio": avg_ratio,
                    "ad_ratio_stdev": stdev_ratio,
                    "ad_ratio_rank_correlation": rank_corr,
                    "ad_ratio_by_asin": ratio_by_asin,
                },
                _TTL_TRAFFIC,
                "traffic_scores",
                asins_hash,
            )
            logger.info(
                f"[cat_monopoly] ad_traffic_ratio mean={avg_ratio:.2%} stdev={stdev_ratio:.2%} "
                f"rank_corr={rank_corr if rank_corr is not None else 'N/A (n<5)'} (n={len(ratios)})"
            )
    except Exception as e:
        logger.error(f"Failed to fetch batch traffic scores: {e}")
    return items


async def _fetch_keyword_weekly_trends(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 3-year weekly search-volume trends for the primary keyword via
    XiyouZhaociAPI.get_search_term_trends().

    Stored in ctx.cache["keyword_weekly_trends"] for the monopoly analyzer.
    When present, the analyzer uses keyword-based seasonality (more direct
    demand signal) instead of BSR-proxy seasonality.

    Soft-fails: login errors or missing auth do not block the workflow.
    """
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword:
        logger.warning("[keyword_weekly_trends] No main_keyword in cache; skipping")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        api = XiyouZhaociAPI(tenant_id=tenant_id)
        res = await asyncio.to_thread(
            api.get_search_term_trends,
            country=country,
            search_term=main_keyword,
        )
        terms = (res or {}).get("searchTerms") or []
        if terms and terms[0].get("trends", {}).get("weekSearch"):
            ctx.cache["keyword_weekly_trends"] = res
            week_count = len(terms[0]["trends"]["weekSearch"])
            logger.info(
                f"[keyword_weekly_trends] Fetched {week_count} weekly data points "
                f"for '{main_keyword}'"
            )
        else:
            logger.warning(f"[keyword_weekly_trends] Empty trends data for '{main_keyword}'")
    except Exception as e:
        logger.warning(f"[keyword_weekly_trends] Failed, seasonality will use BSR proxy: {e}")

    return items


async def _fetch_time_series_data(items: list[dict], ctx: Any) -> list[dict]:
    """
    Parallel wrapper: runs _fetch_historical_trends and _fetch_keyword_weekly_trends
    concurrently. Both are independent XiyouZhaoci reads with no mutual dependency.
    Replaces the two sequential steps to save ~5-10s of serial wait time.
    """
    top_asins = sorted(
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    )
    main_keyword = ctx.cache.get("main_keyword", "")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    ts_hash = _hl.md5(
        (",".join(top_asins) + main_keyword + start_date + end_date).encode()
    ).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_TIMESERIES, "time_series", ts_hash)
    if cached is not None:
        ctx.cache["historical_data"] = cached.get("historical_data", {})
        ctx.cache["keyword_weekly_trends"] = cached.get("keyword_weekly_trends")
        logger.info(f"[cat_monopoly] Time series L2 cache hit ts_hash={ts_hash}")
        return items

    await asyncio.gather(
        _fetch_historical_trends(items, ctx),
        _fetch_keyword_weekly_trends(items, ctx),
    )
    _l2_set(
        ctx,
        {
            "historical_data": ctx.cache.get("historical_data", {}),
            "keyword_weekly_trends": ctx.cache.get("keyword_weekly_trends"),
        },
        _TTL_TIMESERIES,
        "time_series",
        ts_hash,
    )
    return items


async def _fetch_sellersprite_bsr(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 4 monthly BSR snapshots from Sellersprite to calculate
    (true list churn rate): T, T-3, T-6, T-12 months.

    Each snapshot stores only the core fields needed for set comparison:
        {"asin", "rank", "brand"}
    Stored in ctx.cache["sellersprite_snapshots"] as Dict[YYYYMM, List[dict]].

    Churn rate = fraction of ASINs in T that were NOT present N months ago.
    Soft-fails: missing auth or API errors do not block the workflow.
    """
    url = ctx.config.get("url", "")
    m = re.search(r"/(?:gp/bestsellers|zgbs)/[^/]+/(\d+)", url)
    if not m:
        logger.warning("[sellersprite_bsr] Could not extract node ID from URL; skipping")
        return items

    node_id = m.group(1)
    store_id = ctx.config.get("store_id", "US")
    market_id = {"US": 1, "DE": 6, "JP": 8, "UK": 3, "FR": 4, "IT": 5, "ES": 7, "CA": 2}.get(
        store_id, 1
    )

    def month_offset(base_ym: str, delta: int) -> str:
        """Return YYYYMM string `delta` months before base_ym."""
        y, mo = int(base_ym[:4]), int(base_ym[4:])
        total = y * 12 + (mo - 1) - delta
        return f"{total // 12:04d}{total % 12 + 1:02d}"

    # Base = 2 months prior (ensures Sellersprite data is fully published)
    now = datetime.now()
    base_y = now.year if now.month > 2 else now.year - 1
    base_mo = now.month - 2 if now.month > 2 else now.month + 10
    base_ym = f"{base_y:04d}{base_mo:02d}"

    # 4 snapshot keys: T, T-3, T-6, T-12
    snapshot_yms = [
        base_ym,
        month_offset(base_ym, 3),
        month_offset(base_ym, 6),
        month_offset(base_ym, 12),
    ]

    try:
        ss_cache_key = f"{node_id}:{base_ym}"
        cached = _l2_get(ctx, _TTL_SS_BSR, "ss_bsr", ss_cache_key)
        if cached is not None:
            ctx.cache["sellersprite_snapshots"] = cached.get("snapshots", {})
            ctx.cache["sellersprite_base_ym"] = cached.get("base_ym", base_ym)
            if cached.get("ss_median_cvr"):
                ctx.cache["ss_median_cvr"] = cached["ss_median_cvr"]
            if cached.get("ss_node_id_path"):
                ctx.cache["ss_node_id_path"] = cached["ss_node_id_path"]
                ctx.cache["ss_market_id"] = cached.get("ss_market_id", market_id)
            if cached.get("ss_return_rate_pct") is not None:
                ctx.cache["ss_return_rate_pct"] = cached["ss_return_rate_pct"]
            logger.info(
                f"[cat_monopoly] Sellersprite BSR L2 cache hit node={node_id} base_ym={base_ym}"
            )
            return items

        tenant_id = ctx.config.get("tenant_id", "default")
        api = SellerspriteAPI(tenant_id=tenant_id)
        if not api.auth_token:
            logger.warning("[sellersprite_bsr] No auth token; skipping")
            return items

        # Resolve the full nodeIdPath by searching each snapshot table.
        # Primary: bare Amazon numeric node_id (works when it appears in SellerSprite's
        #   nodeIdPath, i.e. the SellerSprite internal ID matches Amazon's).
        # Fallback: label-based search derived from the URL slug, tried from shortest
        #   leaf label to progressively wider prefixes.  Needed when Amazon's short
        #   node ID (e.g. 3741941 for Air Fryers) differs from SellerSprite's internal
        #   IDs (e.g. 17659096011), so the numeric query returns no match.
        def _label_candidates_from_url(bsr_url: str) -> list[str]:
            m2 = re.search(r"/Best-Sellers[-_](.+?)/zgbs/", bsr_url, re.IGNORECASE)
            if not m2:
                return []
            words = m2.group(1).replace("-", " ").replace("_", " ").split()
            return [" ".join(words[-n:]) for n in range(2, min(5, len(words) + 1))]

        node_id_path = None
        _resolve_queries = [node_id] + _label_candidates_from_url(url)
        for query in _resolve_queries:
            for ym in snapshot_yms:
                table = f"bsr_sales_monthly_{ym}"
                nodes = await asyncio.to_thread(
                    api.resolve_node_path, market_id=market_id, table=table, query=query
                )
                if nodes:
                    node_id_path = nodes[0].get("id")
                if node_id_path:
                    logger.info(
                        f"[sellersprite_bsr] Resolved query={query!r} → {node_id_path} (table={table})"
                    )
                    break
            if node_id_path:
                break

        if not node_id_path:
            logger.warning(
                f"[sellersprite_bsr] Could not resolve nodeIdPath for node_id={node_id} "
                f"or URL labels in any snapshot"
            )
            return items

        # Persist so _fetch_category_cvr can use SellerSprite market research as CVR fallback.
        ctx.cache["ss_node_id_path"] = node_id_path
        ctx.cache["ss_market_id"] = market_id

        async def fetch_snapshot(ym: str) -> tuple:
            table = f"bsr_sales_monthly_{ym}"
            try:
                result = await asyncio.to_thread(
                    api.get_competing_lookup,
                    market=store_id,
                    month_name=table,
                    node_id_paths=[node_id_path],
                    size=100,
                )
                raw_items = result.get("items") or []
                # Log actual field names on the first item so mismatches surface immediately.
                if raw_items and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"[fetch_snapshot] item keys (ym={ym}): {sorted(raw_items[0].keys())}"
                    )
                slim = [
                    {
                        "asin": p.get("asin") or p.get("parent") or "",
                        # API field is "bsrRank"; "rank"/"rankingPosition" are legacy fallbacks
                        "rank": p.get("bsrRank")
                        or p.get("rank")
                        or p.get("rankingPosition")
                        or (i + 1),
                        # "brand0" is the brand name at snapshot time; "brand" is the current value
                        "brand": (p.get("brand") or p.get("brand0") or p.get("brandName") or ""),
                        # availableDate is ms-since-epoch; kept as-is for downstream math
                        "available_date_ms": p.get("availableDate"),
                        # per-product click-to-purchase rate; used as category CVR proxy
                        "conversion_rate": p.get("conversionRate"),
                        # seller fields — used as FulfillmentExtractor fallback
                        "fba": p.get("sellerType"),
                        "seller_id": p.get("sellerId"),
                        "seller_name": p.get("sellerName"),
                        # snapshot-time price — used for cross-snapshot distribution shift
                        "price": p.get("price"),
                    }
                    for i, p in enumerate(raw_items)
                    if p.get("asin") or p.get("parent")
                ]
                missing_brand = sum(1 for p in slim if not p["brand"])
                if missing_brand:
                    logger.warning(
                        f"[fetch_snapshot] ym={ym}: {missing_brand}/{len(slim)} items have no brand"
                    )
                return ym, slim
            except Exception as e:
                logger.warning(f"[sellersprite_bsr] Snapshot {ym} failed: {e}")
                return ym, []

        results = await asyncio.gather(*[fetch_snapshot(ym) for ym in snapshot_yms])
        snapshots = {ym: products for ym, products in results if products}

        # Supplemental brand-fill: any Amazon live-BSR ASIN that is absent from all
        # snapshot brand lookups gets a targeted Sellersprite query in the T-snapshot.
        # This covers ASINs ranked outside Sellersprite's sorted top-100 cutoff but
        # still present in their database (e.g. Amazon rank 4 but Sellersprite
        # estimated rank > 100 due to sales-model differences).
        _brand_covered = {
            p["asin"].upper() for ym_data in snapshots.values() for p in ym_data if p.get("brand")
        }
        _bsr_asins = [
            (itm.get("ASIN") or itm.get("asin") or "").upper()
            for itm in items
            if itm.get("ASIN") or itm.get("asin")
        ]
        _missing_brand = [a for a in _bsr_asins if a and a not in _brand_covered]
        if _missing_brand:
            logger.info(
                f"[sellersprite_bsr] brand-fill: {len(_missing_brand)} BSR ASINs missing "
                f"brand — targeted lookup in {base_ym}"
            )
            try:
                _fill_result = await asyncio.to_thread(
                    api.get_competing_lookup,
                    market=store_id,
                    month_name=f"bsr_sales_monthly_{base_ym}",
                    node_id_paths=[node_id_path],
                    size=len(_missing_brand),
                    asins=_missing_brand,
                )
                _fill_slim = []
                for _fp in _fill_result.get("items") or []:
                    _fa = (_fp.get("asin") or _fp.get("parent") or "").upper()
                    _fb = _fp.get("brand") or _fp.get("brand0") or _fp.get("brandName") or ""
                    if _fa and _fb:
                        _fill_slim.append(
                            {
                                "asin": _fa,
                                "rank": _fp.get("bsrRank") or 999,
                                "brand": _fb,
                                "available_date_ms": _fp.get("availableDate"),
                                "conversion_rate": _fp.get("conversionRate"),
                                "fba": _fp.get("fba"),
                                "seller_id": _fp.get("sellerId"),
                                "seller_name": _fp.get("sellerName"),
                            }
                        )
                if _fill_slim:
                    snapshots.setdefault(base_ym, []).extend(_fill_slim)
                    logger.info(f"[sellersprite_bsr] brand-fill: resolved {len(_fill_slim)} brands")
            except Exception as _fe:
                logger.warning(f"[sellersprite_bsr] brand-fill failed: {_fe}")

        ctx.cache["sellersprite_snapshots"] = snapshots
        ctx.cache["sellersprite_base_ym"] = base_ym

        # Compute category-level CVR from the latest snapshot so _fetch_category_cvr
        # can use it directly without an extra get_market_research API call.
        _latest_for_cvr = snapshots.get(max(snapshots)) if snapshots else []
        _cvr_vals = [
            float(p["conversion_rate"])
            for p in _latest_for_cvr
            if p.get("conversion_rate") is not None and float(p["conversion_rate"]) > 0
        ]
        if _cvr_vals:
            _raw_median = statistics.median(_cvr_vals)
            # SellerSprite conversionRate is in per-mille (‰), e.g. 20.4 → 2.04%.
            # Divide by 1000 to get a decimal fraction for _fetch_category_cvr's 0 < v < 1 guard.
            ctx.cache["ss_median_cvr"] = _raw_median / 1000 if _raw_median > 1 else _raw_median
            logger.info(
                f"[sellersprite_bsr] ss_median_cvr={ctx.cache['ss_median_cvr']:.2%} "
                f"(n={len(_cvr_vals)})"
            )

        # Fetch category return rate via SellerSprite market research.
        # Source: Amazon Marketplace Product Guidance (surfaced by SellerSprite).
        #
        # Field definitions (per Amazon):
        #   return_rate_pct     — sub-category returns ÷ total purchases over the
        #                         past 3 months.
        #   avg_return_rate_pct — average return rate across same-type sub-categories
        #                         over the past 3 months.  e.g. under Shoes, 18
        #                         sub-categories (Women's Road Running, Men's Road
        #                         Running, …) are averaged to produce one figure.
        #
        # We take the median of avg_return_rate_pct across all returned items to
        # represent the category-level return rate.  Soft-fails — a missing value
        # does not block the workflow.
        try:
            _mr_res = await asyncio.to_thread(
                api.get_market_research,
                market_id=market_id,
                node_id_path=node_id_path,
                size=100,
            )
            _mr_rates = [
                it["avg_return_rate_pct"]
                for it in (_mr_res.get("items") or [])
                if it.get("avg_return_rate_pct") is not None
            ]
            if _mr_rates:
                ctx.cache["ss_return_rate_pct"] = statistics.median(_mr_rates)
                logger.info(
                    f"[sellersprite_bsr] category return rate: "
                    f"{ctx.cache['ss_return_rate_pct']:.2f}% (n={len(_mr_rates)} sub-cats)"
                )
        except Exception as _mr_e:
            logger.warning(f"[sellersprite_bsr] market-research return rate failed: {_mr_e}")

        _l2_set(
            ctx,
            {
                "snapshots": snapshots,
                "base_ym": base_ym,
                "ss_node_id_path": node_id_path,
                "ss_market_id": market_id,
                "ss_median_cvr": ctx.cache.get("ss_median_cvr"),
                "ss_return_rate_pct": ctx.cache.get("ss_return_rate_pct"),
            },
            _TTL_SS_BSR,
            "ss_bsr",
            ss_cache_key,
        )
        logger.info(
            "[sellersprite_bsr] Snapshots: "
            + ", ".join(f"{ym}({len(p)})" for ym, p in sorted(snapshots.items()))
        )
    except Exception as e:
        logger.warning(f"[sellersprite_bsr] Failed: {e}")

    return items


# ---------------------------------------------------------------------------
# Compliance Risk Detection
# ---------------------------------------------------------------------------

# Each entry covers one regulatory domain.
# "triggers" are lowercase substrings matched against titles + keywords.
# Ordered from most to least severe so the first match sets the floor level.
_COMPLIANCE_DB: list = [
    {
        "id": "pesticide_fifra",
        "label": "EPA-Registered Pesticide (FIFRA)",
        "risk_level": "CRITICAL",
        "triggers": [
            "insecticide",
            "pesticide",
            "pest control",
            "bug killer",
            "insect killer",
            "mosquito killer",
            "roach killer",
            "ant killer",
            "flea killer",
            "pyrethrin",
            "pyrethroid",
            "permethrin",
            "bifenthrin",
            "cypermethrin",
            "imidacloprid",
            "spinosad",
            "fipronil",
            "deltamethrin",
            "malathion",
            "glyphosate",
            "weed killer",
            "herbicide",
            "fungicide",
            "rodenticide",
            "rat killer",
            "mouse killer",
            "rat poison",
            "kills insects",
            "kills bugs",
            "kills mosquito",
        ],
        "regulations": [
            "FIFRA (Federal Insecticide, Fungicide, and Rodenticide Act) — EPA registration required",
            "Amazon Pesticide Policy — EPA Reg. No. must appear in listing; unregistered claims blocked",
            "State Registration — CA, NY, and others maintain separate approved-product registries",
            "Prop 65 (CA) — specific active ingredients trigger cancer/reproductive-harm disclosure",
        ],
        "fba_note": (
            "Concentrated pesticides and aerosol formulations are typically Amazon Hazmat/Dangerous Goods. "
            "Requires Safety Data Sheet (SDS), UN number, and FBA hazmat approval. "
            "Some products restricted to seller-fulfilled only."
        ),
        "new_entrant_burden": (
            "Obtain EPA registration (18–36 months, $50k–$500k+) OR use an already-registered "
            "formulation under a supplemental label. Budget for state-by-state registration fees. "
            "Mandatory: hire a regulatory consultant before listing."
        ),
    },
    {
        "id": "flea_tick_pet",
        "label": "Flea / Tick / Pet Parasite Control",
        "risk_level": "HIGH",
        "triggers": [
            "flea",
            "tick",
            "flea and tick",
            "tick repellent",
            "flea repellent",
            "flea collar",
            "tick collar",
            "heartworm",
            "parasite control",
            "flea treatment",
            "flea spray for pets",
        ],
        "regulations": [
            "EPA registration required when chemical active ingredient is used (FIFRA §3)",
            "FDA oversight if product makes drug claims (e.g. 'treats' infestation)",
            "Amazon Pet Insecticide Policy — requires EPA Reg. No. in back-end keywords",
            "Topical spot-on treatments: additional FIFRA data requirements (pet safety studies)",
        ],
        "fba_note": (
            "Aerosol flea sprays and concentrated dips may be classified as hazmat. "
            "Flea collars with DDVP or tetrachlorvinphos face additional state bans (CA, NY)."
        ),
        "new_entrant_burden": (
            "EPA registration or tolerance exemption required. "
            "If selling under 'natural/essential oil' claims, verify each active is on EPA's 25(b) exempt list. "
            "Dual EPA + FDA exposure if any drug claims are made."
        ),
    },
    {
        "id": "repellent_deet",
        "label": "Human / Animal Repellent",
        "risk_level": "HIGH",
        "triggers": [
            "mosquito repellent",
            "insect repellent",
            "deet",
            "picaridin",
            "ir3535",
            "repel mosquito",
            "bug repellent",
            "bug spray",
            "deer repellent",
            "rabbit repellent",
            "animal repellent",
            "rodent repellent",
            "snake repellent",
        ],
        "regulations": [
            "DEET / Picaridin / IR3535 skin-applied repellents: EPA registration (FIFRA §3)",
            "Amazon DEET Policy — concentration limits enforced at listing level",
            "Deer/animal repellents with chemical actives: EPA registration required; "
            "'natural' repellents may qualify for FIFRA §25(b) exemption if active ingredients are listed",
        ],
        "fba_note": (
            "Aerosol repellents and DEET concentrations >40% are common FBA hazmat triggers. "
            "Verify UN number and FBA category approval before shipping to fulfilment centres."
        ),
        "new_entrant_burden": (
            "DEET-based products require EPA registration. "
            "§25(b) (natural/exempt) path requires all active ingredients to appear on the EPA exempt list — "
            "do NOT assume 'natural' or 'essential oil' means unregulated."
        ),
    },
    {
        "id": "aerosol_flammable",
        "label": "Aerosol / Flammable / Pressurised Container",
        "risk_level": "MEDIUM",
        "triggers": [
            "aerosol",
            "spray can",
            "pressurized",
            "pressurised",
            "fogger",
            "total release fogger",
            "fumigator",
            "propellant",
            "flammable",
            "combustible",
        ],
        "regulations": [
            "DOT Hazardous Materials Regulations (49 CFR) — aerosols classified as UN 1950",
            "CPSC 16 CFR §1500 — flammable aerosols consumer product safety standard",
            "IATA DGR — air-freight restrictions; affects Amazon's inbound freight lanes",
        ],
        "fba_note": (
            "Aerosols and flammable liquids require FBA Dangerous Goods approval. "
            "Must submit SDS and pass Amazon's hazmat review (typical 1–4 weeks). "
            "Storage quantity limits apply per fulfilment centre."
        ),
        "new_entrant_burden": (
            "Prepare SDS per GHS/OSHA HazCom 2012. "
            "Submit for Amazon Dangerous Goods review before first inbound shipment. "
            "Plan for longer lead time and possible repackaging to meet DOT specification packaging."
        ),
    },
    {
        "id": "disinfectant_sanitizer",
        "label": "Disinfectant / Sanitizer / Antimicrobial",
        "risk_level": "HIGH",
        "triggers": [
            "disinfectant",
            "sanitizer",
            "sanitiser",
            "antimicrobial",
            "kills 99",
            "kills bacteria",
            "kills virus",
            "kills germs",
            "hospital grade",
            "kills covid",
            "virucidal",
            "bactericidal",
            "bleach",
            "hydrogen peroxide",
            "quaternary ammonium",
        ],
        "regulations": [
            "EPA Pesticide Registration (FIFRA) — all antimicrobial claims require Reg. No.",
            "FDA OTC Drug Monograph — hand sanitisers regulated as drugs (21 CFR 333)",
            "Amazon Disinfectant/Sanitizer Policy — EPA Reg. No. mandatory in listing",
            "FTC Green Guides — 'kills 99.9%' efficacy claims require substantiated test data",
        ],
        "fba_note": (
            "Bleach-based and alcohol-based disinfectants (>70% alcohol) are FBA hazmat. "
            "Corrosive products require UN-specification packaging."
        ),
        "new_entrant_burden": (
            "EPA registration for each claimed pathogen/surface combination. "
            "FDA NDA/ANDA if drug claims. "
            "Third-party efficacy testing (AOAC or EN standards) required for '99.9% kill' claims."
        ),
    },
    {
        "id": "pool_spa_chemical",
        "label": "Pool / Spa / Water Treatment Chemical",
        "risk_level": "MEDIUM",
        "triggers": [
            "pool chemical",
            "pool shock",
            "pool algaecide",
            "algaecide",
            "pool chlorine",
            "chlorine tablet",
            "spa chemical",
            "hot tub chemical",
            "water treatment",
            "pond treatment",
            "clarifier",
            "water clarifier",
        ],
        "regulations": [
            "EPA registration (FIFRA) for algaecides and biocidal water treatments",
            "DOT UN1791 (sodium hypochlorite), UN2468 (trichloroisocyanuric acid) — hazmat",
            "CPSC — oxidising pool chemicals have consumer safety labelling requirements",
        ],
        "fba_note": (
            "Oxidising chemicals (chlorine, peroxide-based) are Amazon Hazmat. "
            "Incompatible with many other FBA product types; separate storage required."
        ),
        "new_entrant_burden": (
            "EPA registration for any algaecide/biocide claim. "
            "DOT hazmat packaging and labelling. "
            "Amazon DG approval required before first shipment."
        ),
    },
]

# Risk level ordering for aggregation
_RISK_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0}


def _detect_compliance_risks(scan_texts: list) -> dict:
    """
    Scan product titles and keywords for regulated-substance signals.

    Returns a dict with:
      detected   — list of matched compliance DB entries (id, label, risk_level, ...)
      overall_risk — highest risk_level across all matches ("NONE" if no hits)
      fba_hazmat  — True when any HIGH+ match is found
      triggered_by — sample of trigger terms actually found in the text
    """
    combined = " ".join(t.lower() for t in scan_texts if t)
    detected = []
    all_triggers: list = []

    def _trigger_hit(trigger: str) -> bool:
        # Use word-boundary regex so "tick" does not match inside "nonstick",
        # "ant" does not match "antifog", etc.
        return bool(re.search(r"\b" + re.escape(trigger) + r"\b", combined))

    for entry in _COMPLIANCE_DB:
        hit_triggers = [t for t in entry["triggers"] if _trigger_hit(t)]
        if not hit_triggers:
            continue
        detected.append(
            {
                "id": entry["id"],
                "label": entry["label"],
                "risk_level": entry["risk_level"],
                "regulations": entry["regulations"],
                "fba_note": entry["fba_note"],
                "burden": entry["new_entrant_burden"],
                "triggered_by": hit_triggers[:5],
            }
        )
        all_triggers.extend(hit_triggers[:3])

    if not detected:
        return {"detected": [], "overall_risk": "NONE", "fba_hazmat": False, "triggered_by": []}

    best = max(detected, key=lambda d: _RISK_ORDER.get(d["risk_level"], 0))
    fba_hazmat = any(_RISK_ORDER.get(d["risk_level"], 0) >= _RISK_ORDER["HIGH"] for d in detected)
    return {
        "detected": detected,
        "overall_risk": best["risk_level"],
        "fba_hazmat": fba_hazmat,
        "triggered_by": list(dict.fromkeys(all_triggers))[:10],  # dedup, preserve order
    }


# Amazon top-level department vocabularies.  Each frozenset holds the distinctive
# tokens of one department; a label maps to the first department whose vocabulary it
# touches.  Tokens are chosen to be non-overlapping across departments so first-match
# ordering is unambiguous.  Used both to validate the Ads benchmark's browse_category
# against the analyzed list's authoritative primary category and (as a weaker signal)
# to guard against cross-domain keyword matches.
_AMAZON_DEPARTMENTS: list[frozenset[str]] = [
    frozenset(
        {
            "electronics",
            "computers",
            "camera",
            "phone",
            "tv",
            "audio",
            "video",
            "software",
            "headphone",
            "speaker",
            "bluetooth",
            "wireless",
        }
    ),
    frozenset({"book", "books", "kindle", "magazine", "music", "movie", "dvd"}),
    frozenset(
        {
            "clothing",
            "shoes",
            "jewelry",
            "apparel",
            "fashion",
            "watch",
            "watches",
            "handbag",
            "luggage",
        }
    ),
    frozenset(
        {
            "grocery",
            "food",
            "gourmet",
            "beverage",
            "coffee",
            "tea",
            "snack",
            "supplement",
            "vitamin",
        }
    ),
    frozenset({"automotive", "car", "truck", "motorcycle", "vehicle", "tire"}),
    frozenset({"industrial", "scientific", "laboratory", "commercial"}),
    frozenset({"baby", "infant", "toddler", "diaper", "nursery"}),
    frozenset({"beauty", "skincare", "makeup", "cosmetic", "hair", "shampoo", "lotion"}),
    # Home & Kitchen — distinct from Patio, Lawn & Garden below.
    frozenset(
        {
            "kitchen",
            "dining",
            "home",
            "furniture",
            "decor",
            "bedding",
            "bath",
            "cookware",
            "appliances",
        }
    ),
    # Patio, Lawn & Garden — outdoor/gardening, NOT the same department as kitchen.
    frozenset({"patio", "lawn", "garden", "gardening"}),
    frozenset({"pet", "pets", "dog", "cat", "aquarium"}),
    frozenset({"toys", "toy", "games", "puzzle"}),
    frozenset({"health", "household", "wellness"}),
    frozenset({"sports", "outdoors", "fitness", "exercise", "cycling"}),
    frozenset({"tools", "improvement", "hardware", "hvac"}),
]

_DEPT_STOP = {"the", "a", "an", "for", "in", "of", "and", "&", "or", "to", "with", "by", "us"}


def _amazon_department(label: str | None) -> int | None:
    """Map a category label to its Amazon top-level department index, or None if ambiguous.

    Returns the index of the first department in ``_AMAZON_DEPARTMENTS`` whose
    vocabulary overlaps the label's tokens; None when the label is empty or matches
    no known department (treated as ambiguous by callers).
    """
    if not label:
        return None
    tokens = {t for t in re.split(r"[\s,&/>]+", label.lower()) if t and t not in _DEPT_STOP}
    for i, dept in enumerate(_AMAZON_DEPARTMENTS):
        if tokens & dept:
            return i
    return None


def _ads_category_matches_niche(
    browse_category: str | None,
    core_keywords: list[str],
    primary_category: str | None = None,
) -> bool:
    """
    Return True when the Amazon Ads benchmark category is relevant to the analyzed niche.

    Strong check (authoritative): when the analyzed list's primary Amazon category
    (``salesRankContextName``) is known, the decision is department equality — the
    primary category comes from the actual products being analyzed, so if it and the
    benchmark's browse_category resolve to distinct, known Amazon departments the
    benchmark is for an unrelated category and MUST be rejected (e.g. primary
    "Kitchen & Dining" vs. benchmark "Patio, Lawn & Garden").  When both departments
    are known this check is decisive; keyword overlap cannot override it.

    Weak fallback (primary category unavailable/ambiguous): the older keyword heuristic:
    - Pass 1 (token overlap): if any non-stop token is shared, accept immediately.
    - Pass 2 (domain exclusion): accept unless *both* sides map to distinct known
      departments.  When either side is ambiguous the benchmark is kept.
    """
    if not browse_category:
        return False

    cat_dept = _amazon_department(browse_category)

    # ── Strong check: authoritative primary category decides when both are known ──
    if primary_category:
        primary_dept = _amazon_department(primary_category)
        if cat_dept is not None and primary_dept is not None:
            return cat_dept == primary_dept

    # ── Weak fallback: keyword heuristic (primary category unavailable/ambiguous) ──
    if not core_keywords:
        return False

    _STOP = {"the", "a", "an", "for", "in", "of", "and", "&", "or", "to", "with", "by"}
    cat_tokens = {t for t in re.split(r"[\s,&/]+", browse_category.lower()) if t and t not in _STOP}
    kw_tokens = {t for kw in core_keywords for t in kw.lower().split() if t not in _STOP}

    # Pass 1: fast accept on direct token overlap
    if cat_tokens & kw_tokens:
        return True

    # Pass 2: reject only when both sides map to distinct known departments.
    kw_dept = _amazon_department(" ".join(core_keywords))

    if cat_dept is not None and kw_dept is not None and cat_dept != kw_dept:
        return False

    return True


def _model_default_cvr(median_price: float, amazon_category: str | None = None) -> float:
    """
    Power-law price × category CVR estimate used when live benchmark data is unavailable.

    Baseline:  CVR = 0.45 × price^(−0.477)
    Calibrated to observed Amazon SP median ranges:
      $10 → ~15%,  $25 → ~10%,  $50 → ~7%,  $100 → ~5%,  $200 → ~3.5%

    When `amazon_category` (the flat primary-category label from
    ProfitabilitySearchExtractor.salesRankContextName, e.g. "Electronics", "Pet Supplies")
    is available it is used to scale the baseline via a category multiplier.
    Without it the baseline is returned as-is — precision is not required here since
    this is a last-resort fallback when no real benchmark CVR can be matched.

    Result is clamped to [0.02, 0.25].
    """
    # Tokens drawn from Amazon's actual primary-category label vocabulary so they
    # match salesRankContextName values such as "Grocery & Gourmet Food" or "Baby".
    # Ordered highest → lowest CVR multiplier; first match wins.
    _CATEGORY_SIGNALS: list[tuple[frozenset, float]] = [
        (frozenset({"grocery", "gourmet", "food"}), 1.40),  # Grocery & Gourmet Food
        (frozenset({"beauty", "personal", "care"}), 1.30),  # Beauty & Personal Care
        (frozenset({"baby"}), 1.25),  # Baby Products
        (frozenset({"kitchen", "dining"}), 1.20),  # Home & Kitchen / Kitchen & Dining
        (frozenset({"home", "garden"}), 1.35),  # Home & Garden
        (frozenset({"toys", "games"}), 1.10),  # Toys & Games
        (frozenset({"pet"}), 1.05),  # Pet Supplies
        (frozenset({"health", "household"}), 1.70),  # Health & Household
        (frozenset({"sports", "outdoors"}), 1.00),  # Sports & Outdoors
        (frozenset({"clothing", "shoes", "jewelry", "apparel"}), 0.90),  # Clothing, Shoes & Jewelry
        (
            frozenset({"tools", "automotive", "industrial", "improvement"}),
            0.85,
        ),  # Tools & Home Improvement / Automotive
        (
            frozenset({"electronics", "computers", "office"}),
            0.80,
        ),  # Electronics / Computers / Office
    ]

    # salesRankContextName is a flat primary-category label ("Electronics", "Pet Supplies").
    cat_tokens = frozenset((amazon_category or "").lower().split())
    best_mult = 1.0
    for signal_toks, mult in _CATEGORY_SIGNALS:
        if cat_tokens & signal_toks:
            best_mult = mult
            break

    # Power-law decay: α=0.45, β=0.477
    price = max(median_price, 1.0)
    base_cvr = 0.45 * (price**-0.477)
    return max(0.02, min(0.25, base_cvr * best_mult))


async def _fetch_category_cvr(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch category-median click-to-purchase CVR for steady-state ACOS estimation.

    Primary:   Amazon Ads crossProgramBenchmarks → newToBrandPurchaseRateP50,
               accepted only when browse_category resolves to the same Amazon
               department as the analyzed list's authoritative primary category
               (salesRankContextName); guards against the store's ad account being
               registered under an unrelated category.
    Fallback:  SellerSprite get_market_research → search_to_buy_ratio_pm / 1000
               (requires fetch_sellersprite_bsr to have run first so ss_node_id_path
               is already in ctx.cache).
    Default:   0.10  (conservative Amazon SP category average)

    Stores:
      ctx.cache["category_cvr"]             float
      ctx.cache["category_cvr_source"]      str   (human-readable)
      ctx.cache["category_cvr_source_type"] str   (_CVR_SRC_* constant)
      ctx.cache["category_cpc_p50"]         float | None  (Ads benchmark CPC only;
                                            None when Ads was not used — callers then
                                            derive CPC from detailed_bid_analysis)

    L2 cache key: md5(ss_node_id_path) — category identity, not store identity.
    Skipped when ss_node_id_path is empty (unsafe: unrelated categories could share
    the same blank key).  bid-rec CPC is never persisted here because it is
    keyword-specific and would become stale on re-runs with different keywords.
    """
    store_id = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    ss_node = ctx.cache.get("ss_node_id_path") or ""
    core_keywords: list[str] = ctx.cache.get("core_keywords") or []
    keywords_hash = _hl.md5(":".join(sorted(core_keywords)).encode()).hexdigest()[:8]

    # Cache key is category-scoped (ss_node_id_path only; store_id omitted because
    # category consistency — not store identity — is what validates the entry).
    # Skip L2 entirely when ss_node is empty to avoid cross-category key collisions.
    cvr_hash: str | None = _hl.md5(ss_node.encode()).hexdigest()[:12] if ss_node else None

    if cvr_hash is not None:
        cached = _l2_get(ctx, _TTL_CVR, "category_cvr", cvr_hash)
        if cached is not None:
            ctx.cache["category_cvr"] = cached["cvr"]
            ctx.cache["category_cvr_source_type"] = cached["source_type"]
            ctx.cache["category_cvr_source"] = cached["source"]
            ctx.cache["category_cpc_p50"] = cached.get("ads_cpc_p50")  # None if Ads wasn't used
            logger.info(
                f"[cat_monopoly] Category CVR L2 hit node={ss_node[:20]} "
                f"source_type={cached['source_type']} "
                f"cached_kw_hash={cached.get('keywords_hash', '?')} current={keywords_hash}"
            )
            return items

    cvr: float | None = None
    ads_cpc_p50: float | None = None  # only set when source is Amazon Ads
    source_type = _CVR_SRC_DEFAULT
    source = "default_0.10"
    browse_category: str | None = None

    # Resolve the authoritative primary Amazon category (salesRankContextName) from a
    # representative ASIN once, up front.  This is the ground truth for validating the
    # Amazon Ads benchmark (its ad account may be registered under a different
    # category) and is reused by the power-law fallback below.  One call suffices —
    # all BSR items share the same primary category.
    _amazon_category: str | None = None
    _rep_asin = next(
        (
            (item.get("ASIN") or item.get("asin") or "").strip().upper()
            for item in items
            if item.get("ASIN") or item.get("asin")
        ),
        None,
    )
    if _rep_asin:
        try:
            async with ProfitabilitySearchExtractor() as _ps_extractor:
                _ps_products = await _ps_extractor.search_products(_rep_asin)
            _ps_match = next(
                (p for p in _ps_products if (p.get("asin") or "").upper() == _rep_asin),
                _ps_products[0] if _ps_products else None,
            )
            if _ps_match:
                _amazon_category = _ps_match.get("salesRankContextName") or None
        except Exception as _e:
            logger.warning(f"[fetch_category_cvr] ProfitabilitySearch for {_rep_asin}: {_e}")

    # ── Primary: Amazon Ads benchmark ────────────────────────────────────────
    try:
        ads_result = await AmazonAdsClient(store_id=store_id).get_category_cvr_benchmark(
            days=30, time_unit="MONTHLY"
        )
        browse_category = ads_result.get("browse_category")
        median_cvr = ads_result.get("category_median_cvr")
        raw_cpc_p50 = ads_result.get("cpc_p50")

        if median_cvr and 0 < median_cvr < 1:
            if _ads_category_matches_niche(browse_category, core_keywords, _amazon_category):
                cvr = median_cvr
                ads_cpc_p50 = raw_cpc_p50  # only trust CPC when category matched
                source_type = _CVR_SRC_ADS
                source = (
                    f"amazon_ads_benchmark newToBrandPurchaseRateP50"
                    f" (category={browse_category!r}"
                    f", peers={ads_result.get('peer_set_size', 'N/A')})"
                )
                logger.info(
                    f"[fetch_category_cvr] Amazon Ads CVR={cvr:.2%}, "
                    f"cpcP50={'$' + f'{ads_cpc_p50:.2f}' if ads_cpc_p50 else 'N/A'} — {source}"
                )
            else:
                logger.warning(
                    f"[fetch_category_cvr] Amazon Ads browse_category={browse_category!r} "
                    f"does not match primary category={_amazon_category!r} "
                    f"(niche keywords {core_keywords}); "
                    f"discarding benchmark CVR and CPC (raw_cpc=${raw_cpc_p50})"
                )
    except Exception as e:
        logger.warning(f"[fetch_category_cvr] Amazon Ads benchmark failed: {e}")

    # ── Fallback: SellerSprite median conversionRate from BSR snapshot ───────
    # conversionRate is already fetched per-product during _fetch_sellersprite_bsr
    # (stored as ss_median_cvr); no extra API call needed.
    if cvr is None:
        ss_cvr = ctx.cache.get("ss_median_cvr")
        if ss_cvr and 0 < ss_cvr < 1:
            cvr = ss_cvr
            source_type = _CVR_SRC_SS
            # SellerSprite's conversionRate here surfaces Amazon Marketplace Product
            # Guidance's 30-day search-to-purchase ratio (number of times the ASIN was
            # purchased ÷ total searches on it), taken as the median across the BSR
            # snapshot. Describe it by that true provenance, not the SellerSprite field.
            source = (
                f"Amazon Marketplace Product Guidance 30-day search-to-purchase ratio "
                f"(times these ASINs were purchased ÷ total searches on them), "
                # Displayed in per-mille (‰) — the native unit for this ratio.
                f"median across BSR ASINs via SellerSprite ({cvr * 1000:.1f}‰)"
            )
            logger.info(f"[fetch_category_cvr] SellerSprite CVR={cvr:.2%} — {source}")

    # ── Last resort: price-based power-law model ─────────────────────────────
    if cvr is None:
        _raw_prices = [
            float(m.group())
            for item in items
            if item.get("Price")
            for m in [re.search(r"\d+(?:\.\d+)?", str(item["Price"]).replace(",", ""))]
            if m
        ]
        _median_price = statistics.median(_raw_prices) if _raw_prices else 25.0

        # Reuse the authoritative primary Amazon category (salesRankContextName)
        # resolved up front so _model_default_cvr can scale by category multiplier
        # (e.g. "Pet Supplies > Dogs") instead of relying solely on keyword matching.
        cvr = _model_default_cvr(_median_price, _amazon_category)
        source_type = _CVR_SRC_POWER_LAW
        source = (
            f"model_derived_power_law (price=${_median_price:.0f}→{cvr:.2%}"
            + (f", category='{_amazon_category}'" if _amazon_category else "")
            + "; Amazon Ads mismatch, SellerSprite unavailable)"
        )
        ctx.cache["profitability_rank_category"] = _amazon_category

    # CPC P50: only the Ads-benchmark value is category-level and cacheable.
    # Bid-recommendation CPC is keyword-specific and must NOT be stored in the
    # category CVR cache — it is derived fresh each run from detailed_bid_analysis
    # (already in ctx.cache) so callers that need it see the current keyword set.
    ctx.cache["category_cvr"] = cvr
    ctx.cache["category_cvr_source_type"] = source_type
    ctx.cache["category_cvr_source"] = source
    ctx.cache["category_cpc_p50"] = ads_cpc_p50  # None when Ads was not used

    # Persist only when we have a safe category-scoped key and a validated source.
    # power_law and default_0.10 are not persisted so real data can replace them.
    if cvr_hash is not None and source_type in (_CVR_SRC_ADS, _CVR_SRC_SS):
        _l2_set(
            ctx,
            {
                "cvr": cvr,
                "source_type": source_type,
                "source": source,
                "ads_cpc_p50": ads_cpc_p50,  # category-level; None for non-Ads sources
                "browse_category": browse_category,
                "keywords_hash": keywords_hash,  # audit: which keywords validated the match
            },
            _TTL_CVR,
            "category_cvr",
            cvr_hash,
        )
    return items


# ---------------------------------------------------------------------------
# Non-fixable review pre-filter
# ---------------------------------------------------------------------------

# Class A — Logistics / fulfillment: shipping damage, wrong item, carrier delays.
# Entirely outside the manufacturer's control; give no actionable entry signal.
_NON_FIXABLE_LOGISTICS_RE = re.compile(
    r"\b("
    r"shipping|delivery|delivered|in[- ]transit|arrived damaged"
    r"|package[d]?|box was (crushed|damaged|dented)|packaging (damaged|broken)"
    r"|carrier|postal service|fedex|u\.?p\.?s\.?|usps"
    r"|never arrived|lost in (mail|transit)"
    r"|wrong item|wrong product|not what i ordered|sent the wrong|received (wrong|incorrect)"
    r"|return (policy|process)|refund (request|issue)"
    r"|amazon (fulfillment|warehouse)|prime delivery"
    r")\b",
    re.IGNORECASE,
)

# Class B — "Works as designed": reviewer dislikes the product category, not the
# execution. A new entrant cannot fix an inherent category trade-off.
_NON_FIXABLE_WAD_RE = re.compile(
    r"("
    r"works (exactly )?as (described|expected|advertised|intended|designed)"
    r"|does (exactly )?what it('s| is) (supposed|meant) to"
    r"|product (itself )?is (fine|good|great|okay|ok)\b"
    r"|no (product |design )?(defect|flaw|issue|problem)"
    r")",
    re.IGNORECASE,
)

# Override: clear product-defect signal that overrides a single incidental logistics mention
# (e.g. "fast shipping but the product broke" is a product complaint, not a logistics one).
_PRODUCT_DEFECT_SIGNAL_RE = re.compile(
    r"\b("
    r"broke|broken|cracked|snapped|shattered|defective|defect|malfunction"
    r"|stopped (working|after)|doesn'?t work|does not work|dead on arrival"
    r"|poor (quality|build|material|construction)|cheap (plastic|material|quality|build)"
    r"|flimsy|fell apart|peeled|rusted|leaked|leaking|disintegrat"
    r"|missing (part|piece|component|accessory|screw|hardware)"
    r"|wrong size|incorrect size|doesn'?t fit|too small|too large|too short|too long"
    r")\b",
    re.IGNORECASE,
)


def _is_fixable_review(title: str | None, content: str | None) -> bool:
    """
    Return True when the review describes a fixable product deficiency.

    Rejection rules (applied in order):
    1. "Works as designed" signal — unconditional reject.
    2. 2+ logistics hits — logistics-dominant, reject.
    3. 1 logistics hit in a short review (< 40 words) WITHOUT a product-defect
       signal — logistics-only short rant, reject.
       Exception: if a product-defect signal is also present (e.g. "fast
       shipping but the product broke"), the logistics mention is incidental
       and the review is kept.
    """
    text = f"{title or ''} {content or ''}"
    if _NON_FIXABLE_WAD_RE.search(text):
        return False
    logistics_hits = _NON_FIXABLE_LOGISTICS_RE.findall(text)
    if len(logistics_hits) >= 2:
        return False
    if logistics_hits and len(text.split()) < 40:
        if not _PRODUCT_DEFECT_SIGNAL_RE.search(text):
            return False
    return True


async def _fetch_reviews_for_entries(
    entries: list[dict], recency_days: int = 90, max_pages: int = 2
) -> dict:
    """
    Fetch, recency-filter (<= recency_days), and fixability-filter recent
    critical (1-3 star) reviews for a list of {"asin", "brand", "rank",
    "stratum"} entries.

    Shared by the stratified top-brands sampler
    (_fetch_critical_reviews_top_brands) and the dominant-brand-portfolio
    follow-up fetch (_fetch_critical_reviews_dominant_portfolio) so both
    apply identical recency/fixability filtering and result shaping.
    """
    # Parse "Reviewed in the United States on March 15, 2025" or bare "March 15, 2025"
    _DATE_RE = re.compile(r"(\w+ \d+, \d{4})")
    _cutoff = datetime.now() - timedelta(days=recency_days)

    def _is_recent(date_str: str | None) -> bool:
        if not date_str:
            return False
        m = _DATE_RE.search(date_str)
        if not m:
            return False
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y") >= _cutoff
        except ValueError:
            return False

    async def _fetch_one(entry: dict) -> tuple[str, list]:
        asin = entry["asin"]
        async with _SEM_COMMENTS:
            try:
                async with CommentsExtractor() as extractor:
                    reviews = await extractor.get_negative_reviews(asin, max_pages=max_pages)
                recent = [r for r in reviews if _is_recent(r.date)]
                fixable = [r for r in recent if _is_fixable_review(r.title, r.content)]
                n_dropped = len(recent) - len(fixable)
                logger.info(
                    f"[critical_reviews] [{entry['stratum']}] ASIN={asin} "
                    f"brand={entry['brand']}: {len(fixable)} fixable "
                    f"(dropped {n_dropped} non-fixable, {len(reviews) - len(recent)} stale) "
                    f"of {len(reviews)} total"
                )
                return asin, fixable
            except Exception as e:
                logger.warning(f"[critical_reviews] Failed for ASIN={asin}: {e}")
                return asin, []

    results = await asyncio.gather(*[_fetch_one(e) for e in entries])

    critical_data: dict = {}
    for entry, (asin, reviews) in zip(entries, results, strict=False):
        critical_data[asin] = {
            "brand": entry["brand"],
            "rank": entry["rank"],
            "stratum": entry["stratum"],
            "recent_critical_reviews": [
                {
                    "rating": r.rating,
                    "title": r.title,
                    "content": (r.content or "")[:500],
                    "date": r.date,
                    "is_verified": r.is_verified,
                    "helpful_votes": r.helpful_votes,
                }
                for r in reviews
            ],
        }
    return critical_data


async def _fetch_critical_reviews_top_brands(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch recent critical (1–3 star) reviews using stratified sampling across
    three market segments so no single segment dominates the signal:

      Stratum A — Leaders     (BSR rank  1–10):  up to 2 brands
      Stratum B — Mid-tier    (BSR rank 11–30):  up to 2 brands  (excl. A)
      Stratum C — New entrants (listed ≤12 mo):  up to 1 brand   (excl. A+B)

    Cross-stratum complaint themes are the strongest category-wide entry
    signals; stratum-specific complaints surface segment weaknesses only.
    Reviews older than 90 days are discarded — incumbents may have already
    patched those issues.

    Stores: ctx.cache["critical_reviews_data"]
      {asin: {"brand": str, "rank": int, "stratum": str,
              "recent_critical_reviews": [...]}}

    Note: the dominant brand's own portfolio (dominant_brand_portfolio) isn't
    known until _run_monopoly_analysis runs later in the pipeline, so its
    ASINs are covered separately by _fetch_critical_reviews_dominant_portfolio
    after that step.
    """
    _RECENCY_DAYS = 90
    _MAX_PAGES = 2
    _STRATA_ALLOC = {"leaders": 2, "mid_tier": 2, "new_entrant": 1}

    # Sellersprite snapshot → authoritative brand byline + listing date.
    # Iterate oldest→newest so the latest snapshot wins on any conflict.
    # Sellersprite's competing-lookup sorts by their estimated sales rank, which
    # can differ from Amazon's live BSR rank. An ASIN with Amazon BSR rank 4 may
    # still be outside Sellersprite's top-100 cutoff in recent months if their
    # model underestimates it — but the T-12 snapshot often still captures it.
    ss_snapshots = ctx.cache.get("sellersprite_snapshots", {})
    _brand_lookup: dict = {}
    _asin_available_ms: dict = {}
    for _ym in sorted(ss_snapshots):
        for p in ss_snapshots[_ym]:
            if p.get("asin"):
                _key = p["asin"].upper()
                if p.get("brand"):
                    _brand_lookup[_key] = p["brand"]
                if p.get("available_date_ms"):
                    _asin_available_ms[_key] = p["available_date_ms"]

    # "New entrant" cutoff anchored to Sellersprite base snapshot (same as elsewhere)
    base_ym = ctx.cache.get("sellersprite_base_ym", "")
    new_entrant_cutoff_ms = _new_entrant_cutoff_ms(base_ym)

    # Build candidate list with rank, brand, and new-entrant flag
    candidates: list[dict] = []
    for item in items:
        asin = (item.get("ASIN") or item.get("asin") or "").strip().upper()
        if not asin:
            continue
        brand = _brand_lookup.get(asin) or item.get("Brand") or item.get("brand") or "Unknown"
        if brand == "Unknown":
            continue
        rank = _parse_int(item.get("Rank"), default=9999)
        avail_ms = _asin_available_ms.get(asin)
        is_new = avail_ms is not None and avail_ms >= new_entrant_cutoff_ms
        candidates.append({"asin": asin, "brand": brand, "rank": rank, "is_new": is_new})

    def _pick_stratum(pool: list[dict], n: int, exclude_brands: set) -> list[dict]:
        """Return up to n entries — one per brand — sorted by rank, excluding known brands."""
        brand_best: dict = {}
        for e in pool:
            b = e["brand"]
            if b in exclude_brands:
                continue
            if b not in brand_best or e["rank"] < brand_best[b]["rank"]:
                brand_best[b] = e
        return sorted(brand_best.values(), key=lambda x: x["rank"])[:n]

    leaders_pool = [e for e in candidates if e["rank"] <= 10]
    mid_tier_pool = [e for e in candidates if 11 <= e["rank"] <= 30]
    new_entrant_pool = [e for e in candidates if e["is_new"]]

    leaders = _pick_stratum(leaders_pool, _STRATA_ALLOC["leaders"], set())
    used = {e["brand"] for e in leaders}
    mid_tier = _pick_stratum(mid_tier_pool, _STRATA_ALLOC["mid_tier"], used)
    used |= {e["brand"] for e in mid_tier}
    new_entrants = _pick_stratum(new_entrant_pool, _STRATA_ALLOC["new_entrant"], used)

    for e in leaders:
        e["stratum"] = "leaders"
    for e in mid_tier:
        e["stratum"] = "mid_tier"
    for e in new_entrants:
        e["stratum"] = "new_entrant"

    top_entries = leaders + mid_tier + new_entrants
    if not top_entries:
        logger.warning("[critical_reviews] No stratified candidates found; skipping")
        return items

    logger.info(
        f"[critical_reviews] Stratified selection: "
        f"leaders={len(leaders)}, mid_tier={len(mid_tier)}, new_entrant={len(new_entrants)}"
    )

    asin_key = ",".join(sorted(e["asin"] for e in top_entries))
    reviews_hash = _hl.md5(asin_key.encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_CRITICAL_REVIEWS, "critical_reviews", reviews_hash)
    if cached is not None:
        ctx.cache["critical_reviews_data"] = cached
        logger.info(f"[cat_monopoly] Critical reviews L2 cache hit hash={reviews_hash}")
        return items

    critical_data = await _fetch_reviews_for_entries(
        top_entries, recency_days=_RECENCY_DAYS, max_pages=_MAX_PAGES
    )

    ctx.cache["critical_reviews_data"] = critical_data
    _l2_set(ctx, critical_data, _TTL_CRITICAL_REVIEWS, "critical_reviews", reviews_hash)
    total = sum(len(v["recent_critical_reviews"]) for v in critical_data.values())
    logger.info(
        f"[critical_reviews] {total} recent critical reviews collected "
        f"({_RECENCY_DAYS}d filter): "
        f"leaders={len(leaders)}, mid_tier={len(mid_tier)}, new_entrant={len(new_entrants)}"
    )
    return items


async def _fetch_critical_reviews_dominant_portfolio(items: list[dict], ctx: Any) -> list[dict]:
    """
    Extend critical-reviews coverage to the dominant brand's own portfolio.

    _fetch_critical_reviews_top_brands samples by BSR stratum (leaders /
    mid-tier / new-entrant) and only keeps the single best-ranked ASIN per
    brand, so a dominant brand's other ASINs in dominant_brand_portfolio are
    never selected — yet the report's Top Complaint column
    (monopoly_report.yaml) needs critical_reviews_data for every ASIN listed
    there. dominant_brand_portfolio doesn't exist until _run_monopoly_analysis
    runs, so this step runs right after it (calculate_monopoly_score) and
    only fetches ASINs not already covered by the stratified sample.

    Reads: items[0]["dominant_brand_portfolio"] / ["dominant_brand_name"]
           (the single-dict output of _run_monopoly_analysis)
    Merges into: ctx.cache["critical_reviews_data"], and refreshes
           items[0]["critical_reviews_data"] so deliver_report sees the
           merged data.
    """
    if not items:
        return items
    result = items[0]

    try:
        portfolio = json.loads(result.get("dominant_brand_portfolio") or "[]")
    except (TypeError, ValueError):
        portfolio = []
    if not portfolio:
        return items

    dom_brand = result.get("dominant_brand_name") or "Unknown"
    existing = ctx.cache.get("critical_reviews_data") or {}

    missing: list[dict] = []
    seen: set = set()
    for p in portfolio:
        asin = (p.get("asin") or "").strip().upper()
        if not asin or asin == "N/A" or asin in seen or asin in existing:
            continue
        seen.add(asin)
        missing.append(
            {"asin": asin, "brand": dom_brand, "rank": None, "stratum": "dominant_portfolio"}
        )

    if not missing:
        return items

    asin_key = ",".join(sorted(e["asin"] for e in missing))
    reviews_hash = _hl.md5(asin_key.encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_CRITICAL_REVIEWS, "critical_reviews_dom", reviews_hash)
    if cached is not None:
        new_data = cached
        logger.info(
            f"[cat_monopoly] Dominant-portfolio critical reviews L2 cache hit hash={reviews_hash}"
        )
    else:
        new_data = await _fetch_reviews_for_entries(missing)
        _l2_set(ctx, new_data, _TTL_CRITICAL_REVIEWS, "critical_reviews_dom", reviews_hash)

    merged = {**existing, **new_data}
    ctx.cache["critical_reviews_data"] = merged
    result["critical_reviews_data"] = json.dumps(merged, ensure_ascii=False)

    total_new = sum(len(v["recent_critical_reviews"]) for v in new_data.values())
    logger.info(
        f"[critical_reviews] dominant_portfolio brand={dom_brand!r}: "
        f"{len(missing)} new ASIN(s) fetched, {total_new} recent critical reviews collected"
    )
    return items


def _calc_new_entrant_ratio(
    snapshots: dict[str, list[dict]],
    base_ym: str,
    dominant_brands: set[str] | None = None,
) -> tuple[float | None, str]:
    """
    Fraction of current Top-100 ASINs listed within the past 12 months.

    When dominant_brands is provided, returns the *competitive* ratio — computed
    only over non-dominant-brand ASINs — so that incumbent annual model refreshes
    (e.g. Apple releasing a 2026 MacBook Air) do not inflate the new-entrant signal.
    The raw ratio is preserved in the display string for transparency.

    Returns (ratio_float | None, display_str).  ratio_float is None when there is
    no T-snapshot (bsr_metabolism treats None as neutral 50.0 rather than zero).

    Anchors the 12-month cutoff to base_ym (not wall-clock now) so the window does
    not drift when base_ym is T-2 months behind the current date.
    """
    t_snap = snapshots.get(base_ym) or (snapshots.get(max(snapshots)) if snapshots else [])
    if not t_snap:
        return None, "unknown (no T-snapshot)"
    cutoff_ms = _new_entrant_cutoff_ms(base_ym)
    dated = [p for p in t_snap if p.get("available_date_ms")]
    new_ents_all = [p for p in dated if p["available_date_ms"] >= cutoff_ms]
    raw_ratio = len(new_ents_all) / len(t_snap)

    if dominant_brands:
        challenger_dated = [p for p in dated if (p.get("brand") or "") not in dominant_brands]
        if not challenger_dated:
            # Every dated ASIN belongs to a dominant brand — fall back to raw ratio.
            display = (
                f"{raw_ratio:.0%} raw ({len(new_ents_all)}/{len(t_snap)} ASINs new; "
                f"all dated ASINs are dominant-brand — competitive filter not applied)"
            )
            return raw_ratio or None, display
        new_ents_challenger = [p for p in challenger_dated if p["available_date_ms"] >= cutoff_ms]
        competitive_ratio = len(new_ents_challenger) / len(challenger_dated)
        excl_count = len(dated) - len(challenger_dated)
        dom_list = ", ".join(sorted(dominant_brands))
        display = (
            f"{competitive_ratio:.0%} competitive / {raw_ratio:.0%} raw — "
            f"{len(new_ents_challenger)}/{len(challenger_dated)} challenger ASINs "
            f"listed in last 12 months"
            f" ({excl_count} dominant-brand ASINs excluded; "
            f"dominant brand = individual sales share ≥ 20%: {dom_list})"
        )
        return competitive_ratio, display

    ratio = len(new_ents_all) / len(t_snap)
    display = f"{ratio:.0%} ({len(new_ents_all)}/{len(t_snap)} ASINs listed in last 12 months)"
    return ratio, display


def _rebase_rating_series(
    pts: list[tuple[str, Any, int | None]], confirm_days: int
) -> list[tuple[str, Any, int]]:
    """
    Re-base a per-ASIN (date, stars, ratings) series against bad ratings reads.

    `ratings` is a cumulative count that should never decrease under normal
    conditions. A missing reading (None) or a lower reading that doesn't
    persist is a bad data point — a transient scrape failure or a
    variation-merge reshuffle — and is dropped, frozen at the last confirmed
    count, so it can't fabricate a huge fake RSR spike (e.g. treating a bad 0
    as real gives (15529-0)/4000 = 388%). But a lower reading seen on
    `confirm_days` consecutive days is trusted as a real re-base — Amazon
    purging fake reviews or a seller voluntarily splitting variations both
    legitimately lower the count — so it replaces the stale high-water mark
    instead of freezing it out for the rest of the ASIN's history.
    """
    rating_pts: list[tuple[str, Any, int]] = []
    last_valid: int | None = None
    pending: list[tuple[str, Any, int]] = []

    def _discard_pending(pending: list[tuple[str, Any, int]], last_valid: int | None) -> None:
        for p_date, p_stars, _p_ratings in pending:
            if last_valid is not None:
                rating_pts.append((p_date, p_stars, last_valid))
        pending.clear()

    for date, stars, ratings in pts:
        if ratings is None:
            _discard_pending(pending, last_valid)
            if last_valid is not None:
                rating_pts.append((date, stars, last_valid))
            continue

        if last_valid is None or ratings >= last_valid:
            _discard_pending(pending, last_valid)
            last_valid = ratings
            rating_pts.append((date, stars, ratings))
            continue

        # ratings < last_valid: candidate real decline vs. noise — buffer it
        # until it either persists long enough to confirm or gets discarded
        # above by a rebound/missing reading.
        pending.append((date, stars, ratings))
        if len(pending) >= confirm_days:
            rating_pts.extend(pending)
            last_valid = pending[-1][2]
            pending.clear()

    _discard_pending(pending, last_valid)  # trailing streak that never got confirmed
    return rating_pts


async def _run_monopoly_analysis(items: list[dict], ctx: Any) -> list[dict]:
    """Calculates scores and generates flattened niche benchmarks."""
    # Local semaphore: limits concurrent BrandExtractor calls within this invocation.
    # Must be local (not module-level) to stay bound to the current event loop.
    _sem_brand = asyncio.Semaphore(3)

    def _parse_float(raw, default: float = 0.0) -> float:
        """Extract the first decimal number from a US-locale price/rating string.

        Handles: currency symbols ($), thousand-separator commas, ranges ($9–$15),
        suffixes/spaces.  Commas are always thousand separators in this context
        (US locale), so they are stripped before matching — avoids misreading
        "$1,299.99" as 1.299 when comma is naively replaced by a dot.
        """
        s = str(raw or "").replace(",", "")  # strip thousand separators
        m = re.search(r"\d+(?:\.\d+)?", s)
        if not m:
            return default
        try:
            return float(m.group())
        except ValueError:
            return default

    def _fc_lifetime(raw) -> int | None:
        """Normalize feedback_count: extract lifetime count from time-windowed dict or pass through."""
        if isinstance(raw, dict):
            return (raw.get("lifetime") or {}).get("count")
        return raw

    analyzer = CategoryMonopolyAnalyzer()
    external_data = {
        "social_psi": ctx.cache.get("category_social_psi"),
        "deal_intensity": ctx.cache.get("category_deal_intensity"),
    }

    contamination_stats = ctx.cache.get("contamination_stats", {})
    _cs_status = contamination_stats.get("status", "not_run")
    _cs_n_removed = contamination_stats.get("n_removed", 0)

    # Build ASIN→brand lookup across all 4 Sellersprite snapshots (T, T-3, T-6, T-12).
    # Oldest-first so the latest snapshot overwrites on conflict — most-current brand name wins.
    # Sellersprite's estimated rank can differ from Amazon's BSR rank; an ASIN visible on
    # Amazon's live BSR may be outside Sellersprite's top-100 in recent snapshots but still
    # captured in T-12, which serves as a backstop for brand resolution.
    ss_snapshots = ctx.cache.get("sellersprite_snapshots", {})
    _brand_lookup: dict = {}
    for _ym in sorted(ss_snapshots):
        for _p in ss_snapshots[_ym]:
            if _p.get("asin") and _p.get("brand"):
                _brand_lookup[_p["asin"].upper()] = _p["brand"]

    # For ASINs that have no brand from snapshots or item fields, fetch via BrandExtractor.
    _brand_missing = [
        (item.get("ASIN") or item.get("asin") or "").upper()
        for item in items
        if (item.get("ASIN") or item.get("asin"))
        and not _brand_lookup.get((item.get("ASIN") or item.get("asin") or "").upper())
        and not item.get("Brand")
        and not item.get("brand")
    ]
    await _fill_missing_brands(_brand_missing, _brand_lookup, _sem_brand, "run_monopoly_analysis")

    analysis_input = [
        {
            "asin": (item.get("ASIN") or item.get("asin") or "").strip().upper() or None,
            "rank": _parse_int(item.get("Rank"), default=999),
            "price": _parse_float(item.get("Price")),
            "sales": item.get("sales") or 0,
            "brand": _brand_lookup.get((item.get("ASIN") or item.get("asin") or "").upper())
            or item.get("Brand")
            or item.get("brand")
            or None,
            "seller_type": item.get("seller_type", "Unknown"),
            "seller_id": item.get("seller_id"),
            "feedback_count": _fc_lifetime(item.get("feedback_count")),
            "review_count": _parse_int(item.get("Reviews")),
            "rating": _parse_float(item.get("Stars")),
            # Written reviews vs global ratings (from ReviewCountExtractor)
            "global_ratings": item.get("global_ratings"),
            "written_reviews": item.get("written_reviews"),
            "review_ratio": item.get("review_ratio"),
        }
        for item in items
    ]

    # Combined Ad Data with Multi-Keyword CPC
    detailed_bids = ctx.cache.get("detailed_bid_analysis", {})
    ad_data = {
        "actual_bsr_ad_ratio": ctx.cache.get("actual_bsr_ad_ratio"),
        "detailed_bids": detailed_bids,
    }

    # Pre-compute acos_data for the monopoly scorer using the same deterministic
    # fallback paths as the downstream report section. This keeps the ad_burden
    # dimension score consistent with the final report's ad burden verdict.
    # Remaining gap vs. downstream: (a) live FBA API call is async and runs after
    # analyze() — JSON fee schedule used here instead; (b) AGL inbound (~1-3% of
    # price) excluded to avoid duplicating the range-matching helper.
    _pre_prices = [
        _parse_float(i.get("price")) for i in analysis_input if _parse_float(i.get("price"))
    ]
    _pre_median_price = statistics.median(_pre_prices) if _pre_prices else 0.0
    _pre_cvr = ctx.cache.get("category_cvr") or 0.10

    # CPC: bid-rec median from detailed_bids (already cached, no keyword-trust filtering
    # needed for scoring). Falls back to category_cpc_p50 benchmark when unavailable.
    _pre_cpc_vals: list[float] = []
    for _s_recs in (detailed_bids or {}).values():
        if isinstance(_s_recs, list):
            for _rec in _s_recs:
                for _expr in _rec.get("bidRecommendationsForTargetingExpressions", []):
                    _bs = [
                        float(b["suggestedBid"])
                        for b in _expr.get("bidValues", [])
                        if b.get("suggestedBid")
                    ]
                    if _bs:
                        _pre_cpc_vals.append(statistics.median(_bs))
    _pre_cpc = (
        statistics.median(_pre_cpc_vals) if _pre_cpc_vals else ctx.cache.get("category_cpc_p50")
    )

    # Nominal ACOS adjusted for category return rate. ss_return_rate_pct is cached
    # by _fetch_sellersprite_market_data, which runs before this point.
    _pre_nominal_acos: float | None = (
        _pre_cpc / (_pre_median_price * _pre_cvr)
        if (_pre_cpc and _pre_median_price > 0 and _pre_cvr > 0)
        else None
    )
    _pre_rr = ctx.cache.get("ss_return_rate_pct")
    _pre_return_rate: float | None = (
        _pre_rr / 100 if _pre_rr is not None and 0 < _pre_rr < 50 else None
    )
    _pre_est_acos: float | None = (
        _pre_nominal_acos / (1 - _pre_return_rate)
        if (_pre_nominal_acos is not None and _pre_return_rate)
        else _pre_nominal_acos
    )

    # Breakeven ACOS: FBA fee from local 2026 fee schedule (live API updates downstream).
    _pre_fba_pct = 0.18  # default: Large Standard 12-16 oz at ~$25
    try:
        _pre_fba_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../mcp/servers/finance/fba_fee.json",
        )
        with open(_pre_fba_path) as _pff:
            _pre_fba_data = json.load(_pff)
        _pre_fuel = 1 + _pre_fba_data["meta"]["fuel_surcharge"]["rate_pct"] / 100
        _pre_comp = [
            p for p in _pre_prices if _pre_median_price * 0.65 <= p <= _pre_median_price * 1.35
        ]
        _pre_ref = statistics.median(_pre_comp) if _pre_comp else _pre_median_price
        _PRICE_TO_TIER_PRE: list[tuple[float, str, str]] = [
            (10, "Small Standard", "8+ to 10"),
            (18, "Small Standard", "14+ to 16"),
            (30, "Large Standard", "8+ to 12"),
            (50, "Large Standard", "12+ to 16"),
            (75, "Large Standard", "1+ to 1.25 lb"),
            (float("inf"), "Large Standard", "1.75+ to 2 lb"),
        ]
        for _pmax, _psize, _pwt in _PRICE_TO_TIER_PRE:
            if _pre_ref < _pmax:
                _ptier = next(
                    (
                        t
                        for t in _pre_fba_data["fba_fulfillment_fees"]["standard_non_apparel"][
                            "tiers"
                        ]
                        if t.get("size_tier") == _psize
                        and _pwt in (t.get("weight_range") or "")
                        and isinstance(t.get("price_brackets", {}).get("10_to_50"), (int, float))
                    ),
                    None,
                )
                if _ptier and _pre_median_price > 0:
                    _pbkt = (
                        "under_10"
                        if _pre_median_price < 10
                        else ("over_50" if _pre_median_price > 50 else "10_to_50")
                    )
                    _pre_fba_pct = (
                        float(_ptier["price_brackets"][_pbkt]) * _pre_fuel / _pre_median_price
                    )
                break
    except Exception:
        pass  # keep default 0.18

    acos_data = {
        "estimated_acos": _pre_est_acos,
        "breakeven_acos": max(0.0, 1.0 - 0.30 - 0.15 - _pre_fba_pct),
    }

    # new_entrant_ratio: computed once; float drives the bsr_metabolism score,
    # display string goes into the report prompt. snapshots/base_ym are also
    # referenced by the BSR composition shift section below.
    snapshots = ctx.cache.get("sellersprite_snapshots") or {}
    base_ym = ctx.cache.get("sellersprite_base_ym", "")
    # Dominant brands: exclude from new-entrant count so that annual model refreshes
    # by incumbents (e.g. Apple releasing a 2026 MacBook) don't inflate the ratio.
    _brand_sales_pre: Counter = Counter()
    for _p in analysis_input:
        _brand_sales_pre[_p.get("brand") or "Unknown"] += _p.get("sales") or 0
    _total_sales_pre = sum(_brand_sales_pre.values())
    _dominant_brands_pre: set[str] = (
        {_br for _br, _sl in _brand_sales_pre.items() if _sl / _total_sales_pre >= 0.20}
        if _total_sales_pre > 0
        else set()
    )
    new_entrant_ratio_val, new_entrant_str = _calc_new_entrant_ratio(
        snapshots, base_ym, dominant_brands=_dominant_brands_pre
    )

    # brand_search_data: pass None so the analyzer records a neutral 50.0 placeholder.
    # After the full brand_search_monopoly (trusted-keyword filter + BrandExtractor fill)
    # is computed below, the result is retroactively corrected so score and report agree.
    result = analyzer.analyze(
        analysis_input,
        keyword_data=ctx.cache.get("keyword_data"),
        keyword_data_all=ctx.cache.get("keyword_data_all"),
        ad_data=ad_data,
        external_data=external_data,
        historical_data=ctx.cache.get("historical_data"),
        bsr_snapshots=ctx.cache.get("sellersprite_snapshots"),
        keyword_weekly_trends=ctx.cache.get("keyword_weekly_trends"),
        acos_data=acos_data,
        new_entrant_ratio=new_entrant_ratio_val,
        brand_search_data=None,
    )

    # ── Keyword click concentration: per-keyword ABA click share + CPC gap ───
    # Surfaces which brands own click share on each core keyword and whether
    # PHRASE match offers a cheaper entry angle than EXACT (EXACT/PHRASE gap).
    _kw_all = ctx.cache.get("keyword_data_all") or []
    _asin_brand = {p["asin"].upper(): (p["brand"] or "") for p in analysis_input if p.get("asin")}

    def _parse_share(raw: Any) -> float:
        if raw is None:
            return 0.0
        try:
            value = float(str(raw).strip().rstrip("%"))
        except (TypeError, ValueError):
            return 0.0
        if value > 1:
            value /= 100
        return max(0.0, min(value, 1.0))

    def _parse_bid(raw: Any) -> float | None:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return None
        return value if value > 0 else None

    def _aba_top_asins(term: dict[str, Any]) -> list[dict[str, Any]]:
        top_asins = term.get("topAsins") or term.get("top_asins") or []
        if isinstance(top_asins, dict):
            top_asins = top_asins.get("list") or []
        return top_asins if isinstance(top_asins, list) else []

    # Single-word keywords are noisy by default (often naive title n-gram fallback
    # tokens like "spray"/"trap" with no real buyer-search backing) but ABA can
    # legitimately return real single-word terms too (brand names, one-word
    # product categories — "ipad", "toothpaste"). Rather than excluding single-word
    # terms outright, trust them only when ABA itself shows meaningful click-share
    # data for that exact term. Multi-word terms are never subject to this check —
    # only single-word terms are gated. Fully-generic single-word fallback (no ABA
    # signal required) is reserved for the Social PSI hashtag lookup, which already
    # handles it separately via main_keyword.split() in _fetch_external_intensity.
    _single_word_has_signal: dict[str, bool] = {}
    for _term in _kw_all:
        _kwt = (_term.get("searchTerm") or "").strip().lower()
        if _kwt and len(_kwt.split()) == 1:
            _tas = _aba_top_asins(_term)
            _single_word_has_signal[_kwt] = bool(_tas) and any(
                _parse_share(t.get("clickShare")) > 0 for t in _tas[:3]
            )

    def _kw_is_trusted(kw_text: str) -> bool:
        words = (kw_text or "").strip().lower().split()
        if len(words) != 1:
            return True
        return _single_word_has_signal.get(words[0], False)

    # Pass raw bid recommendations directly — no pre-formatting beyond dropping
    # untrusted single-word entries (see _kw_is_trusted above).
    # The LLM receives the full API payload for both strategies and renders it.
    def _filter_bid_strategy(strategy_recs: list[dict]) -> list[dict]:
        filtered = []
        for rec in strategy_recs:
            exprs = [
                expr
                for expr in rec.get("bidRecommendationsForTargetingExpressions", [])
                if _kw_is_trusted((expr.get("targetingExpression") or {}).get("value"))
            ]
            if exprs:
                filtered.append({**rec, "bidRecommendationsForTargetingExpressions": exprs})
        return filtered

    bid_raw = {
        "LEGACY_FOR_SALES": _filter_bid_strategy(detailed_bids.get("LEGACY_FOR_SALES", [])),
        "AUTO_FOR_SALES": _filter_bid_strategy(detailed_bids.get("AUTO_FOR_SALES", [])),
    }
    # Count total keyword-level entries for data-quality tracking
    bid_entry_count = sum(
        len(rec.get("bidRecommendationsForTargetingExpressions", []))
        for strategy_recs in bid_raw.values()
        for rec in strategy_recs
    )

    # Per-keyword LEGACY CPC by match type: {keyword_lower: {PHRASE: float, EXACT: float}}
    _cpc_by_kw: dict[str, dict[str, float]] = {}
    for _rec in detailed_bids.get("LEGACY_FOR_SALES", []):
        for _expr in _rec.get("bidRecommendationsForTargetingExpressions", []):
            _te = _expr.get("targetingExpression") or {}
            _kw = (_te.get("value") or "").strip().lower()
            _raw_mt = (_te.get("type") or "").strip().upper()
            # API echoes long-form types (KEYWORD_PHRASE_MATCH, KEYWORD_EXACT_MATCH) — normalize
            _mt = "PHRASE" if "PHRASE" in _raw_mt else "EXACT" if "EXACT" in _raw_mt else _raw_mt
            _bv = [
                bid
                for b in _expr.get("bidValues", [])
                if (bid := _parse_bid(b.get("suggestedBid"))) is not None
            ]
            if _kw and _bv and _mt in ("PHRASE", "EXACT"):
                _cpc_by_kw.setdefault(_kw, {})[_mt] = statistics.median(_bv)

    # Brand-fill for ABA top-click-share ASINs: these can reference products
    # outside the BSR `items` list (e.g. ranked below the snapshot window), so
    # the `items`-scoped BrandExtractor fill above never resolved them — without
    # this, every keyword whose #1-click ASIN falls outside `items` reports
    # "unknown" regardless of how well-resolved the BSR brands are.
    _kw_top1_missing = {
        _a
        for _term in _kw_all
        if _kw_is_trusted((_term.get("searchTerm") or "").strip().lower())
        and (_tas := _aba_top_asins(_term))
        and (_a := (_tas[0].get("asin") or "").upper())
        and _a not in _asin_brand
    }
    await _fill_missing_brands(
        _kw_top1_missing, _asin_brand, _sem_brand, "keyword_click_concentration"
    )

    keyword_click_concentration = []
    for _term in _kw_all:
        _kw_text = (_term.get("searchTerm") or "").strip().lower()
        if not _kw_is_trusted(_kw_text):
            continue
        _top_asins = _aba_top_asins(_term)
        _top3_share = sum(_parse_share(t.get("clickShare")) for t in _top_asins[:3])
        _top1 = _top_asins[0] if _top_asins else {}
        _top1_asin = (_top1.get("asin") or "").upper()
        _top1_share = _parse_share(_top1.get("clickShare"))
        _top1_brand = _asin_brand.get(_top1_asin, "") or "unknown"
        _concentration = (
            "concentrated"
            if _top3_share > 0.50
            else "moderate"
            if _top3_share > 0.30
            else "fragmented"
        )
        _cpcs = _cpc_by_kw.get(_kw_text, {})
        _phrase_cpc = _cpcs.get("PHRASE")
        _exact_cpc = _cpcs.get("EXACT")
        _gap_pct = (_exact_cpc - _phrase_cpc) / _phrase_cpc if _phrase_cpc and _exact_cpc else None
        keyword_click_concentration.append(
            {
                "keyword": _kw_text,
                "top3_click_share": f"{_top3_share:.0%}",
                "concentration": _concentration,
                "top_asin_brand": _top1_brand,
                "top_asin_click_share": f"{_top1_share:.0%}",
                "phrase_cpc": f"${_phrase_cpc:.2f}" if _phrase_cpc else "N/A",
                "exact_cpc": f"${_exact_cpc:.2f}" if _exact_cpc else "N/A",
                "exact_phrase_gap": f"{_gap_pct:+.0%}" if _gap_pct is not None else "N/A",
            }
        )

    # ── Cross-keyword brand search monopoly ───────────────────────────────────
    # Count how many keywords each brand leads the #1 click-share ASIN.
    # A brand leading ≥50% of core keywords controls search-traffic distribution
    # across the niche — a PPC-layer moat independent of BSR position share.
    _brand_kw_lead: Counter = Counter()
    for _entry in keyword_click_concentration:
        _b = _entry.get("top_asin_brand") or ""
        if _b and _b != "unknown":
            _brand_kw_lead[_b] += 1
    _n_kw_total = len(keyword_click_concentration)
    brand_search_monopoly: list[dict] = []
    for _b, _led in _brand_kw_lead.most_common(3):
        _share = _led / _n_kw_total if _n_kw_total else 0
        brand_search_monopoly.append(
            {
                "brand": _b,
                "keywords_led": _led,
                "n_keywords": _n_kw_total,
                "keyword_lead_share": round(_share, 3),
                "verdict": (
                    "monopoly" if _share >= 0.50 else "dominant" if _share >= 0.30 else "notable"
                ),
            }
        )
    logger.info(
        f"[brand_search_monopoly] n_kw={_n_kw_total}, "
        f"top={brand_search_monopoly[0] if brand_search_monopoly else None}"
    )

    # Retroactively correct the brand_search_monopoly score now that the full computation
    # (trusted-keyword filter + BrandExtractor fill) is available. The analyzer received
    # brand_search_data=None (neutral 50.0 placeholder). Only correct when keyword_data_all
    # was actually fetched: pass the real list (possibly empty → score 0.0) so the score
    # reflects the genuine data. Leave the neutral 50.0 in place when keyword data was not
    # fetched, because _kw_all coerces None→[] and can't distinguish the two cases.
    if ctx.cache.get("keyword_data_all") is not None:
        _bsm_full_score = analyzer._analyze_brand_search_monopoly(brand_search_monopoly)
        _bsm_weight = analyzer.weights.get("brand_search_monopoly", 0.0)
        _bsm_dim = result["dimension_details"].setdefault("brand_search_monopoly", {})
        _bsm_delta = round(_bsm_full_score * _bsm_weight, 2) - _bsm_dim.get(
            "weighted_contribution", 0.0
        )
        _bsm_dim["raw_score"] = round(_bsm_full_score, 2)
        _bsm_dim["weighted_contribution"] = round(_bsm_full_score * _bsm_weight, 2)
        result["overall_score"] = round(result["overall_score"] + _bsm_delta, 2)
        result["status"] = analyzer._interpret_score(
            result["overall_score"],
            result.get("market_churn"),
            result.get("seasonality"),
            result.get("bsr_churn"),
        )

    prices = [p["price"] for p in analysis_input if p["price"] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    total_monthly_units = result.get("niche_benchmarks", {}).get("total_estimated_monthly_units", 0)
    niche_monthly_gmv = int(total_monthly_units * median_price)

    # ── Steady-state ad burden ────────────────────────────────────────────────
    # Estimated ACOS = median_cpc / (median_price × category_cvr)
    # Ad profit drag = actual_bsr_ad_ratio × estimated_acos
    # Both inputs (CPC and CVR) come from the Amazon Ads ecosystem so the
    # estimate is internally consistent regardless of category.
    _category_cvr = ctx.cache.get("category_cvr") or _model_default_cvr(
        median_price, ctx.cache.get("profitability_rank_category")
    )

    _cpc_values: list[float] = []
    for _strategy_recs in bid_raw.values():
        for _rec in _strategy_recs:
            for _expr in _rec.get("bidRecommendationsForTargetingExpressions", []):
                _bids = [
                    float(b["suggestedBid"])
                    for b in _expr.get("bidValues", [])
                    if b.get("suggestedBid")
                ]
                if _bids:
                    _cpc_values.append(statistics.median(_bids))

    # Fall back to category benchmark cpcP50 when bid-recommendations API is unavailable
    # (e.g. no owned listing in this category — the bid API requires an advertiser ASIN).
    _median_cpc: float | None = (
        statistics.median(_cpc_values) if _cpc_values else ctx.cache.get("category_cpc_p50")
    )
    _actual_ad_ratio: float | None = ctx.cache.get("actual_bsr_ad_ratio")

    # Breakeven ACOS: 1 - COGS% - referral% - FBA%.
    # Primary: call Amazon's public fee calculator API for the BSR product whose
    # price is closest to the category median — this gives the real 2026 fee for
    # the actual product weight/size class in this category.
    # Fallback: select the appropriate weight tier from the local 2026 fee schedule
    # using a price→weight heuristic on comparable-price BSR products.
    _COGS_PCT = 0.30
    _fba_fee_pct = 0.18  # fallback: Large Standard 12-16 oz at $25 ≈ $4.76/25
    _fba_fee_source = "2026 fee schedule estimate, Large Standard 12-16 oz"
    _REFERRAL_FEE_PCT = 0.15  # fallback: "Everything Else" catch-all rate
    _referral_fee_source = "2026 schedule, 'Everything Else' catch-all (category unresolved)"

    # Pick the BSR product with price closest to median_price as the representative ASIN.
    _rep_item = (
        min(
            (item for item in analysis_input if item.get("price", 0) > 0),
            key=lambda x: abs(x["price"] - median_price),
        )
        if any(item.get("price", 0) > 0 for item in analysis_input)
        else None
    )
    _rep_asin: str | None = (
        ((_rep_item.get("asin") or _rep_item.get("ASIN") or "").strip().upper() or None)
        if _rep_item
        else None
    )

    # --- Primary: live API (Amazon Revenue Calculator getfees) ---
    # Real response shape: {"programFeeResultMap": {"Core#0": {"otherFeeInfoMap": {
    #   "FulfillmentFee": {"total": {"amount": N}, ...}, "ReferralFee": {"total": {"amount": N}, ...}, ...
    # }}}}. One call yields both the FBA fulfillment fee AND the referral fee for
    # this specific ASIN/category/price — more accurate than either static schedule
    # below, since referral rates vary 5%-45% by category (referral_fee_rates.json).
    _api_fba_resolved = False
    _api_referral_resolved = False
    if _rep_asin and median_price > 0:
        try:
            async with ProfitabilitySearchExtractor() as _fee_extractor:
                _fee_data = await _fee_extractor.get_fees(_rep_asin, median_price)
            _fee_map = ((_fee_data.get("programFeeResultMap") or {}).get("Core#0") or {}).get(
                "otherFeeInfoMap"
            ) or {}

            def _fee_amount(fee_name: str) -> float | None:
                entry = _fee_map.get(fee_name) or {}
                amt = (entry.get("total") or {}).get("amount")
                if amt is None:
                    amt = (entry.get("feeAmount") or {}).get("amount")
                return float(amt) if amt is not None else None

            _fulfillment_amt = _fee_amount("FulfillmentFee")
            if _fulfillment_amt and _fulfillment_amt > 0:
                _fba_fee_pct = _fulfillment_amt / median_price
                _api_fba_resolved = True
                _fba_fee_source = (
                    f"Amazon Revenue Calculator (live API, ASIN {_rep_asin} @ ${median_price:.2f})"
                )
                logger.info(
                    f"[monopoly] FBA fee from API: ${_fulfillment_amt:.2f} ({_fba_fee_pct:.1%}) "
                    f"for ASIN {_rep_asin} @ ${median_price:.2f}"
                )

            _referral_amt = _fee_amount("ReferralFee")
            if _referral_amt and _referral_amt > 0:
                _REFERRAL_FEE_PCT = _referral_amt / median_price
                _api_referral_resolved = True
                _referral_fee_source = (
                    f"Amazon Revenue Calculator (live API, ASIN {_rep_asin} @ ${median_price:.2f})"
                )
                logger.info(
                    f"[monopoly] Referral fee from API: ${_referral_amt:.2f} "
                    f"({_REFERRAL_FEE_PCT:.1%}) for ASIN {_rep_asin} @ ${median_price:.2f}"
                )
        except Exception as _e:
            logger.warning(f"[monopoly] Live fee API failed for {_rep_asin}: {_e}")

    # --- Fallback: category-specific rate from the 2026 referral fee schedule ---
    if not _api_referral_resolved:
        _rep_category_label: str | None = ctx.cache.get("profitability_rank_category")
        if not _rep_category_label and _rep_asin:
            try:
                async with ProfitabilitySearchExtractor() as _ps_extractor:
                    _ps_products = await _ps_extractor.search_products(_rep_asin)
                _ps_match = next(
                    (p for p in _ps_products if (p.get("asin") or "").upper() == _rep_asin),
                    _ps_products[0] if _ps_products else None,
                )
                if _ps_match:
                    _rep_category_label = _ps_match.get("salesRankContextName") or None
                    if _rep_category_label:
                        ctx.cache["profitability_rank_category"] = _rep_category_label
            except Exception as _e:
                logger.warning(f"[monopoly] Category resolution for referral fee failed: {_e}")
        if _rep_category_label:
            try:
                _REFERRAL_FEE_PCT = get_referral_rate(_rep_category_label, median_price)
                _referral_fee_source = f"2026 category schedule, '{_rep_category_label}'"
            except Exception as _e:
                logger.warning(f"[monopoly] Referral rate lookup failed: {_e}")

    # --- Fallback: local 2026 FBA fee schedule with price→weight heuristic ---
    if not _api_fba_resolved:
        try:
            _fba_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../../mcp/servers/finance/fba_fee.json",
            )
            with open(_fba_path) as _fba_f:
                _fba_data = json.load(_fba_f)
            _fuel_mult = 1 + _fba_data["meta"]["fuel_surcharge"]["rate_pct"] / 100
            # Narrow to products priced within ±35% of the category median to avoid
            # outliers skewing the representative weight tier.
            _comp_prices = [p for p in prices if median_price * 0.65 <= p <= median_price * 1.35]
            _ref_price = statistics.median(_comp_prices) if _comp_prices else median_price
            # Price → typical weight tier for standard US consumer goods (2026 schedule).
            # Lighter/cheaper items map to Small Standard; heavier/pricier to Large Standard.
            _PRICE_TO_TIER: list[tuple[float, str, str]] = [
                (10, "Small Standard", "8+ to 10"),
                (18, "Small Standard", "14+ to 16"),
                (30, "Large Standard", "8+ to 12"),
                (50, "Large Standard", "12+ to 16"),
                (75, "Large Standard", "1+ to 1.25 lb"),
                (float("inf"), "Large Standard", "1.75+ to 2 lb"),
            ]
            _rep_tier = None
            for _max_p, _size, _wt_fragment in _PRICE_TO_TIER:
                if _ref_price < _max_p:
                    _rep_tier = next(
                        (
                            t
                            for t in _fba_data["fba_fulfillment_fees"]["standard_non_apparel"][
                                "tiers"
                            ]
                            if t.get("size_tier") == _size
                            and _wt_fragment in (t.get("weight_range") or "")
                            and isinstance(
                                t.get("price_brackets", {}).get("10_to_50"), (int, float)
                            )
                        ),
                        None,
                    )
                    break
            if _rep_tier and median_price > 0:
                _bracket = (
                    "under_10"
                    if median_price < 10
                    else ("over_50" if median_price > 50 else "10_to_50")
                )
                _fba_fee_usd = float(_rep_tier["price_brackets"][_bracket]) * _fuel_mult
                _fba_fee_pct = _fba_fee_usd / median_price
                _fba_fee_source = f"2026 fee schedule, {_rep_tier.get('size_tier', 'Standard')} {_rep_tier.get('weight_range', '')}".strip()
        except Exception:
            pass  # use fallback 0.18

    # ── FBA Inbound Shipping (AGL Standard Ocean LCL, 5-FC split) ────────────
    # Amazon splits inbound across 5 FCs: 3 East (60%), 1 Midwest (20%), 1 West (20%).
    # Default: Shenzhen → US FC, Standard Ocean SMP, cheapest bracket per region.
    # Product dimensions are estimated from median price when not directly available.
    _inbound_fee_pct = 0.0
    _inbound_fee_usd = 0.0
    _inbound_freight_detail = "N/A"
    try:
        _agl_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../../mcp/servers/finance/agl_rates.json",
        )
        with open(_agl_path) as _agl_f:
            _agl_data = json.load(_agl_f)
        _agl_lane = next(
            (
                ln
                for ln in _agl_data.get("lanes", [])
                if ln.get("lane_id") == "CN-SZX_US-FC_LCL" and ln.get("status") == "active"
            ),
            None,
        )
        if _agl_lane and median_price > 0:
            # Price → estimated per-unit dimensions (CBM, KG) for standard consumer goods
            _PRICE_TO_DIM: list[tuple[float, float, float]] = [
                (15.0, 0.002, 0.20),
                (30.0, 0.004, 0.40),
                (55.0, 0.008, 0.60),
                (90.0, 0.014, 1.00),
                (float("inf"), 0.020, 1.50),
            ]
            _cbm_per_unit, _kg_per_unit = 0.008, 0.60
            for _max_p, _cbm_u, _kg_u in _PRICE_TO_DIM:
                if median_price < _max_p:
                    _cbm_per_unit, _kg_per_unit = _cbm_u, _kg_u
                    break

            _agl_ship_units = 500  # representative batch size for bracket matching
            _total_cbm_ship = _cbm_per_unit * _agl_ship_units

            def _cbm_in_range(cbm: float, cbm_range: str) -> bool:
                m = re.match(
                    r"([\[\(])([\d.]+|\+inf|-inf),([\d.]+|\+inf|-inf)([\]\)])",
                    cbm_range.replace(" ", ""),
                )
                if not m:
                    return False
                lo_b, lo_s, hi_s, hi_b = m.groups()
                lo = float("-inf") if "inf" in lo_s else float(lo_s)
                hi = float("inf") if "inf" in hi_s else float(hi_s)
                lo_ok = cbm > lo if lo_b == "(" else cbm >= lo
                hi_ok = cbm < hi if hi_b == ")" else cbm <= hi
                return lo_ok and hi_ok

            _agl_brackets = _agl_lane.get("volume_brackets", [])
            _agl_bracket = next(
                (b for b in _agl_brackets if _cbm_in_range(_total_cbm_ship, b["cbm_range"])),
                _agl_brackets[-1] if _agl_brackets else None,
            )
            if _agl_bracket:
                _std_smp = [
                    q
                    for q in _agl_bracket.get("quotes", [])
                    if q.get("shipping_type") == "Standard Ocean"
                    and q.get("container_type") == "SMP"
                ]
                # 3 East FCs → North East, 1 Midwest FC → South Central, 1 West FC → West
                _FC_REGION_WEIGHTS = [
                    ("North East, US", 0.60),
                    ("South Central, US", 0.20),
                    ("West, US", 0.20),
                ]
                _w_cost = 0.0
                _sel_quotes: list = []
                for _fc_region, _fc_w in _FC_REGION_WEIGHTS:
                    _rq = [q for q in _std_smp if q.get("destination_region") == _fc_region]
                    _cq = (
                        min(_rq, key=lambda q: q.get("price_per_cbm_usd", float("inf")))
                        if _rq
                        else (
                            min(
                                _std_smp,
                                key=lambda q: q.get("price_per_cbm_usd", float("inf")),
                            )
                            if _std_smp
                            else None
                        )
                    )
                    if _cq:
                        _sel_quotes.append(_cq)
                        _w_cost += _fc_w * max(
                            _cq["price_per_cbm_usd"] * _cbm_per_unit,
                            _cq["price_per_kg_usd"] * _kg_per_unit,
                        )
                if _w_cost > 0 and _sel_quotes:
                    _inbound_fee_usd = round(_w_cost, 4)
                    _inbound_fee_pct = _inbound_fee_usd / median_price
                    _iq_mode = _agl_lane.get("mode", "LCL")
                    _iq_term = _agl_lane.get("trade_term", "DDP")
                    _iq_shipping = _sel_quotes[0].get("shipping_type", "Standard Ocean")
                    _iq_container = _sel_quotes[0].get("container_type", "SMP")
                    _td_mins = [
                        q["transit_days"]["min"]
                        for q in _sel_quotes
                        if isinstance(q.get("transit_days"), dict)
                    ]
                    _td_maxs = [
                        q["transit_days"]["max"]
                        for q in _sel_quotes
                        if isinstance(q.get("transit_days"), dict)
                    ]
                    _iq_transit = (
                        f"{min(_td_mins)}–{max(_td_maxs)} days"
                        if _td_mins and _td_maxs
                        else "transit N/A"
                    )
                    _inbound_freight_detail = (
                        f"{_iq_mode} ({_iq_container}), {_iq_shipping}, {_iq_transit}, {_iq_term}"
                    )
    except Exception:
        pass  # use default 0.0

    _breakeven_acos = max(
        0.0, 1.0 - _COGS_PCT - _REFERRAL_FEE_PCT - _fba_fee_pct - _inbound_fee_pct
    )

    # Category return rate — used to adjust both ACOS and inventory capital below.
    # Derived early so the ACOS block and the capital block share one value.
    _rr_raw = ctx.cache.get("ss_return_rate_pct")
    _return_rate = _rr_raw / 100 if _rr_raw is not None and 0 < _rr_raw < 50 else None

    if _median_cpc and median_price > 0 and _category_cvr > 0:
        # Nominal ACOS = CPC / (price × CVR).
        # Effective ACOS adjusts for returns: each returned unit reverses the sale
        # revenue but the ad spend is already sunk, so effective revenue per click
        # is price × CVR × (1 − return_rate).
        _nominal_acos = _median_cpc / (median_price * _category_cvr)
        _estimated_acos = _nominal_acos / (1 - _return_rate) if _return_rate else _nominal_acos
        _ad_profit_drag = (
            _actual_ad_ratio if _actual_ad_ratio is not None else 0.5
        ) * _estimated_acos
        _ad_burden_verdict = (
            "Critical"
            if _estimated_acos >= _breakeven_acos * 1.5
            else "High"
            if _estimated_acos >= _breakeven_acos
            else "Moderate"
            if _estimated_acos >= _breakeven_acos * 0.7
            else "Low"
        )
    else:
        _estimated_acos = _ad_profit_drag = None
        _ad_burden_verdict = "unknown"

    # ── Ad-dependency dispersion & rank correlation (from Top-20 traffic scores) ──
    # stdev: is the category uniformly ad-reliant, or split between ad-driven and
    # organically-driven sellers? rank_correlation: does earning a better BSR rank
    # track with LOWER ad reliance (organic moat) or is rank simply bought/defended
    # with ad spend (winnable by outspending, or leaders defend rank with ads)?
    _ad_ratio_stdev: float | None = ctx.cache.get("ad_ratio_stdev")
    _ad_ratio_rank_corr: float | None = ctx.cache.get("ad_ratio_rank_correlation")

    _ad_dispersion_verdict = (
        "Bifurcated — organic and ad-reliant sellers coexist"
        if _ad_ratio_stdev is not None and _ad_ratio_stdev >= 0.20
        else "Uniform ad reliance across sellers"
        if _ad_ratio_stdev is not None
        else "unknown"
    )
    _ad_rank_corr_verdict = (
        "Organic moat — better rank tracks with lower ad reliance"
        if _ad_ratio_rank_corr is not None and _ad_ratio_rank_corr >= 0.25
        else "Ad-defended — better rank tracks with higher ad reliance"
        if _ad_ratio_rank_corr is not None and _ad_ratio_rank_corr <= -0.25
        else "Mixed — no clear rank/ad-reliance pattern"
        if _ad_ratio_rank_corr is not None
        else "unknown"
    )

    estimator = SalesEstimator()
    node_id = ctx.config.get("category_node_id")
    baseline = estimator.category_params.get(str(node_id), {}).get("market_logic", {})

    # ── review_disparity: top-10 avg / bottom-tail avg ────────────────────
    # Require ≥5 products in both buckets; otherwise the ratio is meaningless
    # (tail empty → tail_avg collapses to 1 → disparity = raw top-10 average, not a ratio).
    _MIN_BUCKET = 5
    top10_reviews = [p["review_count"] for p in analysis_input[:10]]
    tail_reviews = [p["review_count"] for p in analysis_input[50:]]
    if len(top10_reviews) >= _MIN_BUCKET and len(tail_reviews) >= _MIN_BUCKET:
        top10_avg = statistics.mean(top10_reviews)
        tail_avg = statistics.mean(tail_reviews)
        review_disparity_val = round(top10_avg / max(tail_avg, 1), 1)
    else:
        review_disparity_val = None

    # Total scraped BSR products. Use 1 as the denominator fallback for coverage
    # percentages, matching the existing data-quality reporting semantics.
    n_total = len(analysis_input) or 1

    # ── price distribution ────────────────────────────────────────────────────
    valid_prices = sorted(p["price"] for p in analysis_input if p["price"] > 0)
    _price_sales_pairs = [
        (p["price"], p.get("sales") or 0) for p in analysis_input if p["price"] > 0
    ]
    _total_bucket_sales = sum(s for _, s in _price_sales_pairs)
    if valid_prices:

        def _pct_val(lst, pct):
            k = (len(lst) - 1) * pct / 100
            lo, hi = int(k), min(int(k) + 1, len(lst) - 1)
            return lst[lo] + (lst[hi] - lst[lo]) * (k - lo)

        def _tier_stats(prices: list) -> dict:
            if not prices:
                return {}
            return {
                "n": len(prices),
                "pct": f"{len(prices) / len(valid_prices):.0%}",
                "min": f"${prices[0]:.2f}",
                "median": f"${statistics.median(prices):.2f}",
                "max": f"${prices[-1]:.2f}",
            }

        price_p10 = _pct_val(valid_prices, 10)
        price_p25 = _pct_val(valid_prices, 25)
        price_p75 = _pct_val(valid_prices, 75)
        price_p90 = _pct_val(valid_prices, 90)
        price_mean = statistics.mean(valid_prices)
        price_min = valid_prices[0]
        price_max = valid_prices[-1]

        # Dynamic buckets: ~5-7 equal-width bands spanning min→max, rounded to $5
        _step = max(5, round((price_max - price_min) / 5 / 5) * 5) or 5
        _lo = int(price_min // _step) * _step
        buckets: list[dict] = []
        b = _lo
        while b < price_max + _step:
            lo_b, hi_b = b, b + _step
            cnt = sum(1 for p in valid_prices if lo_b <= p < hi_b)
            pct = cnt / len(valid_prices) * 100
            b_sales = sum(s for pr, s in _price_sales_pairs if lo_b <= pr < hi_b)
            sales_pct = f"{b_sales / _total_bucket_sales:.0%}" if _total_bucket_sales else "N/A"
            buckets.append(
                {
                    "range": f"${lo_b:.0f}–${hi_b:.0f}",
                    "lo": lo_b,
                    "hi": hi_b,
                    "count": cnt,
                    "pct": f"{pct:.0f}%",
                    "sales_pct": sales_pct,
                }
            )
            b += _step

        # ── Bimodal / tier detection ──────────────────────────────────────────
        # A "valley" bucket separates two populated clusters when:
        #   - its count ≤ 5% of total AND both neighbouring regions have ≥ 10% each.
        # Scan interior buckets only (skip first and last).
        _VALLEY_THRESH = 0.05  # bucket share ≤ 5% = sparse
        _CLUSTER_THRESH = 0.10  # region share ≥ 10% = populated
        n_total_prices = len(valid_prices)

        tiers: list[dict] = []
        is_bimodal = False
        valley_range: str = ""

        for vi in range(1, len(buckets) - 1):
            vb = buckets[vi]
            if vb["count"] / n_total_prices > _VALLEY_THRESH:
                continue
            left_cnt = sum(bk["count"] for bk in buckets[:vi])
            right_cnt = sum(bk["count"] for bk in buckets[vi + 1 :])
            if (
                left_cnt / n_total_prices >= _CLUSTER_THRESH
                and right_cnt / n_total_prices >= _CLUSTER_THRESH
            ):
                # Found a valley — split here
                is_bimodal = True
                valley_range = vb["range"]
                left_prices = [p for p in valid_prices if p < vb["lo"]]
                right_prices = [p for p in valid_prices if p >= vb["hi"]]
                tiers = [
                    {
                        "label": "Budget tier",
                        **_tier_stats(left_prices),
                        "range": f"${left_prices[0]:.0f}–${left_prices[-1]:.0f}",
                    },
                    {
                        "label": "Premium tier",
                        **_tier_stats(right_prices),
                        "range": f"${right_prices[0]:.0f}–${right_prices[-1]:.0f}",
                    },
                ]
                break  # first valley is sufficient; deeper splits are edge cases

        price_dist = {
            "n": n_total_prices,
            "total_bsr": n_total,
            "min": f"${price_min:.2f}",
            "p10": f"${price_p10:.2f}",
            "p25": f"${price_p25:.2f}",
            "median": f"${median_price:.2f}",
            "mean": f"${price_mean:.2f}",
            "p75": f"${price_p75:.2f}",
            "p90": f"${price_p90:.2f}",
            "max": f"${price_max:.2f}",
            "buckets": [{k: v for k, v in bk.items() if k not in ("lo", "hi")} for bk in buckets],
            "is_bimodal": is_bimodal,
            "valley_range": valley_range,
            "tiers": tiers,
        }
    else:
        price_dist = {"n": 0, "total_bsr": n_total, "buckets": [], "is_bimodal": False, "tiers": []}

    # ── price trend: compare first-30d median vs last-30d median per ASIN ─
    historical_data = ctx.cache.get("historical_data") or {}
    price_deltas = []
    for _asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r["price"]) for r in records if r.get("price") and r["price"] > 0],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        early = statistics.median([p for _, p in pts[:30]])
        late = statistics.median([p for _, p in pts[-30:]])
        if early > 0:
            price_deltas.append((late - early) / early)
    if price_deltas:
        avg_price_change = statistics.mean(price_deltas)
        price_trend_direction = (
            "deflating"
            if avg_price_change < -0.05
            else "inflating"
            if avg_price_change > 0.05
            else "stable"
        )
        price_trend_str = f"{price_trend_direction} ({avg_price_change:+.1%} avg over 12 months, n={len(price_deltas)})"
    else:
        price_trend_str = "unknown (insufficient price history)"

    # ── BSR composition shift: cross-snapshot price distribution trend ────────
    # snapshots/base_ym were set before analyzer.analyze(); new_entrant_ratio_val
    # and new_entrant_str were produced by _calc_new_entrant_ratio() at that point.
    # Compares snapshot-level price medians across T-12 → T to detect whether
    # lower-priced products are gaining BSR rank slots — a signal the per-ASIN
    # price_trend cannot capture because it only sees repricing of currently-tracked
    # ASINs, not the entrant-vs-dropout dynamics that drive composition change.
    _snap_medians: list[tuple[str, float]] = []
    for _ym in sorted(snapshots):
        _snap_prices: list[float] = []
        for _p in snapshots[_ym]:
            try:
                _v = float(_p["price"]) if _p.get("price") is not None else 0.0
            except (TypeError, ValueError):
                _v = 0.0
            if _v > 0:
                _snap_prices.append(_v)
        if len(_snap_prices) >= 5:
            _snap_medians.append((_ym, statistics.median(_snap_prices)))

    if len(_snap_medians) >= 2:
        _oldest_ym, _oldest_med = _snap_medians[0]
        _newest_ym, _newest_med = _snap_medians[-1]
        _comp_pct = (_newest_med - _oldest_med) / _oldest_med
        _comp_label = (
            "lower-price products gaining BSR share"
            if _comp_pct < -0.05
            else "higher-price products gaining BSR share"
            if _comp_pct > 0.05
            else "distribution stable across snapshots"
        )
        price_trend_str += (
            f" | BSR composition: snapshot median"
            f" ${_oldest_med:.2f} ({_oldest_ym}) → ${_newest_med:.2f} ({_newest_ym})"
            f" ({_comp_pct:+.1%}, n={len(_snap_medians)} snapshots) — {_comp_label}"
        )

    # ── Integrity Alert: category-level review manipulation risk ─────────
    # Signal 1 — RSR (Review-to-Sales Ratio): adapted from ReviewSummarizer.
    #   Monthly new reviews / monthly sales. Natural rate 1–3%; >10% suspicious.
    # Signal 2 — Rating jump: stars increase > 0.3 within any 30-day window
    #   (positive review bombing indicator).
    # Signal 3 — Written/Global ratio: written_reviews / global_ratings.
    #   Natural ≈ 0.10 (1:10). Paid reviewers always leave text → ratio spikes.
    #   e.g. 298 written / 470 global = 0.63 → highly suspicious.
    # Seasonal categories raise threshold to 0.40 to suppress false positives.
    sales_map = {
        str(item.get("ASIN") or item.get("asin") or "").upper(): item.get("sales", 0)
        for item in items
    }
    # ── Named thresholds (documented here for auditability) ─────────────────
    # Signal 1 — Review-to-Sales Ratio (RSR): monthly review growth / monthly sales.
    #   Natural ≈ 1–5%; >10% suggests coordinated review injection.
    _RSR_THRESHOLD = 0.10  # monthly-review-growth / monthly-sales; >10% = suspicious
    #   Ratings counts are cumulative and shouldn't decrease. A single lower
    #   reading is treated as a bad scrape and dropped; one that persists this
    #   many consecutive days is trusted as a real re-base (fake-review purge,
    #   voluntary variation split) instead of frozen out indefinitely.
    _RSR_CONFIRM_DAYS = 3
    # Signal 2 — Rating jump: sustained +0.3★ rise in 30 days is implausible organically.
    _JUMP_STARS = 0.3  # minimum stars rise over a 30-day window
    _JUMP_WINDOW = 30  # days
    # Signal 3 — Written/global ratio: natural ≈ 0.10; paid reviewers always leave text.
    _RATIO_THRESHOLD = 0.50  # written_reviews / global_ratings; >50% = suspicious
    # Combined trigger thresholds (fraction of eligible ASINs flagged):
    _THRESH_HIGH_BASE = 0.30  # >30% flagged → HIGH (non-seasonal)
    _THRESH_HIGH_SEAS = 0.40  # >40% flagged → HIGH (seasonal — fewer natural spikes expected)
    _THRESH_MEDIUM = 0.15  # >15% flagged → MEDIUM

    flagged_rsr = 0
    flagged_jump = 0
    integrity_total = 0
    total_bsr = len(items)
    # Review-to-sales rate: same Signal 1 RSR value (monthly new reviews / monthly
    # sales), kept per-ASIN so the category can report a typical level (median)
    # rather than only a flagged/not-flagged count. Natural rate is ~1-5%.
    rsr_values: list[float] = []
    # Review-to-sales growth rate: RSR recomputed separately on the first half vs.
    # second half of each ASIN's history window (monthly_sales held constant, since
    # only a single current estimate is available) — a rising rate signals
    # accelerating review injection rather than a steady natural pace.
    rsr_growth_values: list[float] = []
    # Per-ASIN peak-window RSR: a category-level median (above) hides an ASIN that
    # is quiet most of the time but spiked for a few months (the manipulation
    # pattern). Sliding a 30-day window across each ASIN's own history and keeping
    # the max — instead of one whole-period average — surfaces that spike directly;
    # consumed by top_asin_table (per-ASIN evidence), not the Data Breakdown table.
    _RSR_WINDOW_DAYS = 30
    asin_review_spike: dict[str, dict[str, Any]] = {}

    for asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r.get("stars"), r.get("ratings")) for r in records if r.get("date")],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        integrity_total += 1

        # Signal 1: RSR — data source: Xiyouzhaoci daily_trends ratings field
        # + SalesEstimator monthly sales estimate
        monthly_sales = sales_map.get(asin.upper(), 0)
        if monthly_sales > 0:
            rating_pts = _rebase_rating_series(pts, _RSR_CONFIRM_DAYS)

            if len(rating_pts) >= 2:
                review_delta = rating_pts[-1][2] - rating_pts[0][2]
                months_spanned = max(len(rating_pts) / 30, 1)
                rsr = (review_delta / months_spanned) / monthly_sales
                rsr_values.append(rsr)
                if rsr > _RSR_THRESHOLD:
                    flagged_rsr += 1

                mid = len(rating_pts) // 2
                first_half, second_half = rating_pts[: mid + 1], rating_pts[mid:]
                if len(first_half) >= 2 and len(second_half) >= 2:
                    first_months = max(len(first_half) / 30, 1)
                    second_months = max(len(second_half) / 30, 1)
                    first_rsr = (
                        (first_half[-1][2] - first_half[0][2]) / first_months
                    ) / monthly_sales
                    second_rsr = (
                        (second_half[-1][2] - second_half[0][2]) / second_months
                    ) / monthly_sales
                    if first_rsr > 0:
                        rsr_growth_values.append((second_rsr - first_rsr) / first_rsr)

                _latest_day = rating_pts[-1][0][:10]  # latest scraped day may still be partial
                window_rsrs = [
                    (
                        (rating_pts[i][2] - rating_pts[i - _RSR_WINDOW_DAYS][2]) / monthly_sales,
                        rating_pts[i][0],
                        rating_pts[i - _RSR_WINDOW_DAYS][0],
                    )
                    for i in range(_RSR_WINDOW_DAYS, len(rating_pts))
                    if rating_pts[i][0][:10]
                    < _latest_day  # exclude only windows ending on the partial latest day
                ]
            else:
                window_rsrs = []
            if window_rsrs:
                peak_rsr, peak_end_date, peak_start_date = max(window_rsrs, key=lambda w: w[0])
                baseline_rsr = statistics.median(w[0] for w in window_rsrs)
                asin_review_spike[asin.upper()] = {
                    "peak_rsr": peak_rsr,
                    "peak_date_range": f"{peak_start_date[:10]} → {peak_end_date[:10]}",
                    "baseline_rsr": baseline_rsr,
                    "spike_multiple": (peak_rsr / baseline_rsr) if baseline_rsr > 0 else None,
                }

        # Signal 2: rating jump — data source: Xiyouzhaoci daily_trends stars field
        stars_series = [s for _, s, _ in pts if s]
        for i in range(_JUMP_WINDOW, len(stars_series)):
            if stars_series[i] - stars_series[i - _JUMP_WINDOW] > _JUMP_STARS:
                flagged_jump += 1
                break

    # Review-to-sales rate/growth: median across eligible ASINs, category-level
    # complements to the per-ASIN Signal 1 flag above.
    if rsr_values:
        review_to_sales_rate_str = (
            f"{statistics.median(rsr_values):.1%} (median, {len(rsr_values)}/{total_bsr} ASINs; "
            f"natural range ~1-5%)"
        )
    else:
        review_to_sales_rate_str = "N/A (no ASINs with both ≥60-day history and a sales estimate)"

    if rsr_growth_values:
        _growth_median = statistics.median(rsr_growth_values)
        _growth_label = (
            "accelerating"
            if _growth_median > 0.20
            else "decelerating"
            if _growth_median < -0.20
            else "stable"
        )
        review_to_sales_growth_str = (
            f"{_growth_median:+.0%} ({_growth_label}, {len(rsr_growth_values)}/{total_bsr} ASINs; "
            f"second-half vs. first-half of history window)"
        )
    else:
        review_to_sales_growth_str = "N/A (insufficient history to split into two windows)"

    # Signal 3: written/global ratio — data source: ReviewCountExtractor (BSR page scrape)
    ratio_eligible = [p for p in analysis_input if p.get("review_ratio") is not None]
    flagged_ratio = [p for p in ratio_eligible if p["review_ratio"] > _RATIO_THRESHOLD]
    flagged_ratio_count = len(flagged_ratio)
    ratio_flagged_pct = len(flagged_ratio) / len(ratio_eligible) if ratio_eligible else 0.0

    seasonality_pattern_for_threshold = result.get("seasonality", {}).get("pattern", "")
    is_seasonal = "seasonal" in seasonality_pattern_for_threshold
    integrity_threshold = _THRESH_HIGH_SEAS if is_seasonal else _THRESH_HIGH_BASE

    if integrity_total > 0 or ratio_eligible:
        ts_ratio = max(flagged_rsr, flagged_jump) / integrity_total if integrity_total > 0 else 0.0
        # Either time-series signals OR ratio signal can elevate risk independently
        combined_ratio = max(ts_ratio, ratio_flagged_pct)
        integrity_risk = (
            "HIGH"
            if combined_ratio >= integrity_threshold
            else "MEDIUM"
            if combined_ratio >= _THRESH_MEDIUM
            else "LOW"
        )
        seasonal_note = (
            f" [seasonal category — HIGH threshold raised to {_THRESH_HIGH_SEAS:.0%} to suppress false positives]"
            if is_seasonal
            else ""
        )
        integrity_str = (
            f"{integrity_risk}{seasonal_note} — "
            f"Signal 1 RSR (Xiyouzhaoci trends + sales estimate, threshold >{_RSR_THRESHOLD:.0%}): "
            f"{flagged_rsr}/{integrity_total} ASINs flagged; "
            f"Signal 2 rating-jump (Xiyouzhaoci trends, >{_JUMP_STARS}★ in {_JUMP_WINDOW}d): "
            f"{flagged_jump}/{integrity_total} ASINs flagged; "
            f"time-series coverage: {integrity_total}/{total_bsr} BSR products had ≥60 days history; "
            f"Signal 3 written/global ratio (BSR page scrape, threshold >{_RATIO_THRESHOLD:.0%}): "
            f"{flagged_ratio_count}/{len(ratio_eligible)} products flagged "
            f"({ratio_flagged_pct:.0%} of {len(ratio_eligible)} with ratio data); "
            f"combined trigger threshold: >{integrity_threshold:.0%}"
        )
    else:
        integrity_risk = "unknown"
        integrity_str = (
            "unknown — no ASINs had ≥60 days of Xiyouzhaoci price/rating history "
            "and ReviewCountExtractor returned no written/global ratio data; "
            "integrity signals cannot be computed"
        )

    # ── Compliance risk detection ────────────────────────────────────────────
    # Scan BSR titles + core keywords for regulated-substance signals.
    # Pure heuristic — no LLM, no extra API call.
    _scan_texts = (
        [ctx.cache.get("main_keyword", "")]
        + list(ctx.cache.get("core_keywords", []))
        + [item.get("Title", "") for item in items[:20]]
    )
    compliance_risks = _detect_compliance_risks(_scan_texts)

    # ── Opportunity signals ──────────────────────────────────────────────────
    # Concrete, number-backed anchors for the LLM's opportunity section.
    # All derived from already-computed data — no extra API calls.
    # _asin_sub_niche is populated inside the sub-niche clustering block below
    # and consumed later by dominant_brand_portfolio.
    _asin_sub_niche: dict[str, str] = {}
    opportunity_signals: dict = {}
    try:
        # 1. Review floor to crack BSR top-20
        top20_reviews = [
            p["review_count"] for p in analysis_input[:20] if (p.get("review_count") or 0) > 0
        ]
        if top20_reviews:
            opportunity_signals["review_floor_top20"] = int(min(top20_reviews))
            opportunity_signals["review_median_top20"] = int(statistics.median(top20_reviews))

        # 2. Largest price gap: find the [$step]-wide band with fewest products
        if valid_prices and len(valid_prices) >= 5:
            _step = max(5, round((valid_prices[-1] - valid_prices[0]) / 5 / 5) * 5) or 5
            _lo = int(valid_prices[0] // _step) * _step
            _gap_band, _gap_cnt = None, len(valid_prices)
            b = _lo
            while b < valid_prices[-1]:
                cnt = sum(1 for p in valid_prices if b <= p < b + _step)
                if cnt < _gap_cnt:
                    _gap_cnt = cnt
                    _gap_band = f"${b:.0f}–${b + _step:.0f}"
                b += _step
            if _gap_band and _gap_cnt <= max(2, len(valid_prices) // 10):
                opportunity_signals["price_gap_band"] = _gap_band
                opportunity_signals["price_gap_count"] = _gap_cnt

        # 3. Sub-niche fragmentation: cluster top-50 BSR titles via LLM
        if ctx.router and items[:50]:
            try:
                _sub_titles = [
                    (raw_item.get("Title") or f"(no title #{i + 1})")[:80]
                    for i, raw_item in enumerate(items[:50])
                ]
                _numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(_sub_titles))
                _sub_prompt = (
                    f"You are a product analyst for the '{ctx.cache.get('main_keyword', 'unknown')}' "
                    f"Amazon category. Group the following {len(_sub_titles)} BSR product titles "
                    "into 3–7 sub-niches based on distinct product type or use case.\n\n"
                    f"{_numbered}\n\n"
                    "Return ONLY a JSON object: keys are short snake_case sub-niche names "
                    "(e.g. 'snap_trap', 'glue_board'), values are arrays of 1-based title indices. "
                    "Every title must appear in exactly one sub-niche. No explanation, no markdown."
                )
                _sub_res = await ctx.router.route_and_execute(
                    _sub_prompt, category=TaskCategory.SIMPLE_CLEANING
                )
                _raw = _sub_res.text.strip()
                _m = re.search(r"\{[\s\S]*\}", _raw)
                if _m:
                    _sub_map: dict = json.loads(_m.group())
                    sub_counts = {
                        k: len(v) for k, v in _sub_map.items() if isinstance(v, list) and v
                    }
                    if sub_counts:
                        opportunity_signals["sub_niche_counts"] = sub_counts
                        smallest = min(sub_counts, key=sub_counts.get)
                        if sub_counts[smallest] >= 2:
                            opportunity_signals["least_crowded_sub_niche"] = {
                                "name": smallest.replace("_", " "),
                                "count": sub_counts[smallest],
                            }
                        # Build ASIN → sub_niche reverse index for portfolio tagging
                        for _sn_name, _sn_indices in _sub_map.items():
                            if not isinstance(_sn_indices, list):
                                continue
                            _human_name = _sn_name.replace("_", " ")
                            for _sn_idx in _sn_indices:
                                try:
                                    _sn_item_idx = int(_sn_idx) - 1  # 1-based → 0-based
                                except (TypeError, ValueError):
                                    continue
                                if 0 <= _sn_item_idx < len(items):
                                    _sn_asin = (
                                        items[_sn_item_idx].get("ASIN")
                                        or items[_sn_item_idx].get("asin")
                                        or ""
                                    ).upper()
                                    if _sn_asin:
                                        _asin_sub_niche[_sn_asin] = _human_name
            except Exception as _sub_err:
                logger.warning(f"[sub_niche_fragmentation] LLM clustering failed: {_sub_err}")

        # 4. Rank positions with the lowest review counts (easiest to displace)
        ranked_by_reviews = sorted(
            [
                {"rank": p["rank"], "reviews": p["review_count"]}
                for p in analysis_input
                if p.get("rank") and 10 <= p["rank"] <= 50 and (p.get("review_count") or 0) > 0
            ],
            key=lambda x: x["reviews"],
        )

        # Category-level rating benchmarks — computed once, used by weakest_rank_slot
        # to assess rating integrity relative to this BSR's own distribution rather
        # than against hard-coded absolute thresholds.
        _top10_ratings = [p["rating"] for p in analysis_input[:10] if (p.get("rating") or 0) > 0]
        _top10_avg_rating = round(statistics.mean(_top10_ratings), 2) if _top10_ratings else None

        if ranked_by_reviews:
            weakest = ranked_by_reviews[0]
            _wrs_idx = next(
                (i for i, p in enumerate(analysis_input) if p.get("rank") == weakest["rank"]),
                None,
            )
            _wrs_full = analysis_input[_wrs_idx] if _wrs_idx is not None else None
            _wrs_brand_name = _wrs_full.get("brand") if _wrs_full else None
            _wrs_price = _wrs_full.get("price") if _wrs_full else None
            _wrs_rating = _wrs_full.get("rating") if _wrs_full else None
            _wrs_review_ratio = _wrs_full.get("review_ratio") if _wrs_full else None
            _wrs_title = (items[_wrs_idx].get("Title") or "")[:70] if _wrs_idx is not None else None
            # Delta vs top-10 average: positive = outperforms incumbents on rating
            # despite far fewer reviews, which is the manipulation signal — not the
            # absolute value, since category baselines vary widely.
            _wrs_rating_delta = (
                round(_wrs_rating - _top10_avg_rating, 2)
                if _wrs_rating and _top10_avg_rating
                else None
            )
            # Per-ASIN estimated ACOS: use target price, not niche median, since ad
            # economics differ at a lower/higher price point than the category average.
            _wrs_est_acos: str | None = None
            if _wrs_price and _median_cpc and _category_cvr:
                _wrs_nom = _median_cpc / (_wrs_price * _category_cvr)
                _wrs_raw = _wrs_nom / (1 - _return_rate) if _return_rate else _wrs_nom
                _wrs_est_acos = f"{_wrs_raw:.0%}"
            opportunity_signals["weakest_rank_slot"] = {
                "rank": weakest["rank"],
                "reviews": weakest["reviews"],
                "brand": _wrs_brand_name,
                "title": _wrs_title,
                "price": round(_wrs_price, 2) if _wrs_price else None,
                "rating": round(_wrs_rating, 1) if _wrs_rating else None,
                "top10_avg_rating": _top10_avg_rating,
                "rating_delta_vs_top10": _wrs_rating_delta,
                "review_ratio": (
                    round(_wrs_review_ratio, 4) if _wrs_review_ratio is not None else None
                ),
                "estimated_acos": _wrs_est_acos or "N/A",
                "implication": (
                    f"BSR #{weakest['rank']} held with only {weakest['reviews']:,} reviews — "
                    f"lowest in the rank-10–50 band, suggesting a beachhead entry point"
                ),
            }

        # 5. Compliance-accessible angle (when CRITICAL regulatory risk detected,
        #    flag the §25(b) natural/exempt path as the lower-barrier entry route)
        if compliance_risks.get("overall_risk") == "CRITICAL":
            natural_ids = {"neem_natural", "repellent"}
            detected_ids = {d["id"] for d in compliance_risks.get("detected", [])}
            if not natural_ids & detected_ids:
                opportunity_signals["compliance_accessible_angle"] = (
                    "FIFRA §25(b) exempt path: products using only EPA-listed minimum-risk "
                    "active ingredients (e.g. citric acid, peppermint oil, garlic) require NO "
                    "EPA registration, dramatically reducing time-to-market and regulatory cost. "
                    "The natural/organic sub-segment is underrepresented in this BSR top-50."
                )

        # 6. New entrant proof points from Sellersprite snapshot
        ss_new_asins: list[str] = []
        _t_snap = snapshots.get(base_ym) or (snapshots.get(max(snapshots)) if snapshots else [])
        if _t_snap:
            _cutoff_n = _new_entrant_cutoff_ms(base_ym)
            ss_new_asins = [
                p["asin"]
                for p in _t_snap
                if p.get("available_date_ms") and p["available_date_ms"] >= _cutoff_n
            ]
        if ss_new_asins:
            # Cross-reference with raw BSR items to get titles
            asin_to_title = {
                (it.get("ASIN") or it.get("asin")): it.get("Title", "") for it in items
            }
            examples = [asin_to_title[a][:60] for a in ss_new_asins[:3] if asin_to_title.get(a)]
            if examples:
                opportunity_signals["recent_breakthrough_examples"] = examples

    except Exception as _opp_err:
        logger.warning(f"[opportunity_signals] computation failed: {_opp_err}")

    # Extract churn / seasonality / BSR churn signals for prompt template
    churn = result.get("market_churn", {})
    seasonality = result.get("seasonality", {})
    bsr_churn = result.get("bsr_churn", {})
    bsr_lifecycle = result.get("bsr_lifecycle", {})
    _bsr_lc_categories = bsr_lifecycle.get("categories", {})
    _bsr_lc_departed = bsr_lifecycle.get("departed", {})

    def _bsr_lc_cell(cat: dict | None) -> str:
        if not cat or cat.get("pct") is None:
            return "N/A"
        return f"{cat['count']} ({cat['pct']:.0%})"

    peak_months_str = ", ".join(str(m) for m in seasonality.get("peak_months", [])) or "N/A"
    platform_warning = (
        " ⚠️ Peak overlaps platform events (Prime Day/Black Friday)"
        if seasonality.get("platform_event_in_peak")
        else ""
    )

    # ── Data quality coverage (passed to LLM so it can caveat low-coverage claims) ──
    dq_sales = sum(1 for p in analysis_input if (p.get("sales") or 0) > 0)
    dq_seller = sum(1 for p in analysis_input if p.get("seller_type") not in (None, "", "Unknown"))
    dq_rating = sum(1 for p in analysis_input if (p.get("rating") or 0) > 0)
    dq_reviews = sum(1 for p in analysis_input if (p.get("review_count") or 0) > 0)
    dq_brand = sum(1 for p in analysis_input if p.get("brand"))
    dq_hist = integrity_total  # ASINs with ≥60-day Xiyouzhaoci history
    dq_ratio = len(ratio_eligible)  # ASINs with written/global ratio from scrape
    dq_snapshots = len(bsr_churn.get("snapshots_available", []))
    dq_cpc = bid_entry_count  # number of keyword CPC entries available

    def _pct(n: int) -> str:
        return f"{n}/{n_total} ({n / n_total:.0%})"

    data_quality_str = (
        f"BSR products scraped: {n_total} | "
        f"sales estimate coverage: {_pct(dq_sales)} | "
        f"seller-type coverage: {_pct(dq_seller)} | "
        f"star-rating coverage: {_pct(dq_rating)} | "
        f"review-count coverage: {_pct(dq_reviews)} | "
        f"brand coverage: {_pct(dq_brand)} | "
        f"price coverage: {_pct(len(valid_prices))} | "
        f"Xiyouzhaoci ≥60-day history: {dq_hist}/{n_total} ({dq_hist / n_total:.0%}) | "
        f"written/global ratio data: {dq_ratio}/{n_total} ({dq_ratio / n_total:.0%}) | "
        f"Sellersprite BSR snapshots available: {dq_snapshots} months | "
        f"CPC keyword entries: {dq_cpc} | "
        f"contamination check: {_cs_n_removed} products removed ({_cs_status})"
    )

    # ── Contamination warning (full human-readable string for the report) ────
    _cs = contamination_stats
    if _cs_status == "clean":
        contamination_warning = "none — all BSR products matched core category keywords"
    elif _cs_status == "filtered":
        _cs_dom = _cs.get("dominant_type", "")
        _cs_dom_str = f"; dominant type: '{_cs_dom}'" if _cs_dom else ""
        _cs_sample = _cs.get("sample_removed", [])
        _cs_sample_str = f"; examples removed: {_cs_sample[:2]}" if _cs_sample else ""
        contamination_warning = (
            f"{_cs_n_removed} off-category products removed "
            f"({_cs.get('outlier_rate', 0):.0%}) via {_cs.get('method', 'unknown')}"
            f"{_cs_dom_str}; "
            f"{_cs.get('n_retained', n_total)} products retained for analysis"
            f"{_cs_sample_str}"
        )
    elif _cs_status == "warning":
        contamination_warning = (
            f"⚠️ HIGH CONTAMINATION ({_cs.get('outlier_rate', 0):.0%}) — "
            f"filtering skipped; metrics may be skewed across mixed product types. "
            f"Sample outliers: {_cs.get('sample_outliers', [])[:2]}"
        )
    elif _cs_status == "skipped":
        contamination_warning = f"check skipped ({_cs.get('reason', 'unknown')})"
    else:
        contamination_warning = "not run"

    # ── Startup capital breakdown ─────────────────────────────────────────────
    # Conservative rule-of-thumb for a new FBA product launch.
    # All constants are auditable here; change them to tune the recommendation.
    #
    # Basis: ONE first batch of _CAP_UNITS units sold at median price.
    # Inventory, PPC, and fees all use the same _CAP_UNITS denominator so the
    # totals are directly comparable.  _CAP_SELL_MONTHS is a transparency label
    # only (how long it realistically takes to sell one batch); it must NOT be
    # used as a multiplier on PPC or fees — that would imply selling 3× the
    # batch, making the three line items inconsistent.
    _CAP_UNITS = 1000  # first-batch order quantity (units)
    _CAP_COGS = 0.30  # COGS as fraction of retail price (China-manufactured)
    _CAP_ACOS = 0.30  # target ACOS during the ranking phase
    _CAP_FEES = (
        _REFERRAL_FEE_PCT + _fba_fee_pct
    )  # category-specific referral + FBA from 2026 fee schedule
    _CAP_SELL_MONTHS = 3  # expected months to sell through the first batch (display only)
    _CAP_OVERHEAD = 2000  # fixed launch costs: photography, A+, listing ($)
    _CAP_BUFFER = 0.20  # working capital buffer on subtotal

    # Category return rate from Amazon Marketplace Product Guidance (via SellerSprite).
    # avg_return_rate_pct = average return rate of same-type sub-categories over the
    # past 3 months.  Already read into _rr_raw above; aliased here for the output dict.
    _ss_return_rate_pct: float | None = _rr_raw

    # If bimodal, the overall median falls in the empty valley between the two
    # price clusters and matches no real product position. Size the headline
    # launch capital to the HIGHER (premium) tier instead: it is the larger
    # capital commitment, so budgeting to it avoids under-funding a premium-tier
    # entry, and it carries more margin headroom to absorb PPC during ranking.
    # The per-tier breakdown below still lets the operator pick the budget tier.
    _cap_price = median_price  # overall median — used only when NOT bimodal
    _cap_basis_tier = ""  # non-empty label only when bimodal (names the chosen tier)
    if price_dist.get("is_bimodal") and price_dist.get("tiers"):
        _prem_tier, _prem_med = None, -1.0
        for _t in price_dist["tiers"]:
            try:
                _tm = float(str(_t.get("median", "")).lstrip("$"))
            except ValueError:
                continue
            if _tm > _prem_med:
                _prem_med, _prem_tier = _tm, _t
        if _prem_tier is not None:
            _cap_price = _prem_med
            _cap_basis_tier = _prem_tier.get("label", "Premium tier")
    # Gross units to ORDER: net target / (1 − return_rate).  Returns consume inventory
    # that was bought at full COGS but cannot be resold, so the sourcing cost must
    # scale with gross units.  PPC and referral fees are computed on net units sold
    # (referral fee is refunded on returns; PPC spend is sunk regardless).
    _cap_gross_units = round(_CAP_UNITS / (1 - _return_rate)) if _return_rate else _CAP_UNITS
    _cap_inv = int(_cap_gross_units * _cap_price * _CAP_COGS)
    _cap_ppc = int(_CAP_UNITS * _cap_price * _CAP_ACOS)
    _cap_fees = int(_CAP_UNITS * _cap_price * _CAP_FEES)
    _cap_inbound = int(_CAP_UNITS * _inbound_fee_usd)
    _cap_sub = _cap_inv + _cap_ppc + _cap_fees + _cap_inbound + _CAP_OVERHEAD
    _cap_total = int(_cap_sub * (1 + _CAP_BUFFER))

    # Per-tier capital estimates when bimodal
    _tier_capitals: list[dict] = []
    if price_dist.get("is_bimodal"):
        for tier in price_dist.get("tiers", []):
            t_median_str = tier.get("median", "")
            try:
                t_med = float(t_median_str.lstrip("$"))
            except ValueError:
                continue
            t_inv = int(_cap_gross_units * t_med * _CAP_COGS)  # gross-up for returns, as headline
            t_ppc = int(_CAP_UNITS * t_med * _CAP_ACOS)
            t_fees = int(_CAP_UNITS * t_med * _CAP_FEES)
            t_sub = t_inv + t_ppc + t_fees + _cap_inbound + _CAP_OVERHEAD
            _tier_capitals.append(
                {
                    "tier": tier["label"],
                    "median": t_median_str,
                    "inventory": f"${t_inv:,}",
                    "ppc": f"${t_ppc:,}",
                    "fees": f"${t_fees:,}",
                    "inbound": f"${_cap_inbound:,}",
                    "overhead": f"${_CAP_OVERHEAD:,}",
                    "total": f"${int(t_sub * (1 + _CAP_BUFFER)):,}",
                }
            )

    # ── Brand & Seller concentration ──────────────────────────────────────────
    _brand_pos: Counter = Counter()
    _brand_sales_cnt: Counter = Counter()
    _seller_pos: Counter = Counter()
    _seller_sales_cnt: Counter = Counter()
    _seller_brands: dict = {}

    for _ai in analysis_input:
        _b = _ai.get("brand") or "Unknown"
        _s = _ai.get("sales") or 0
        _sid = _ai.get("seller_id")
        _brand_pos[_b] += 1
        _brand_sales_cnt[_b] += _s
        if _sid:
            _seller_pos[_sid] += 1
            _seller_sales_cnt[_sid] += _s
            _seller_brands.setdefault(_sid, set()).add(_b)

    _N_items = len(analysis_input)
    _total_brand_sales = sum(_brand_sales_cnt.values())

    def _conc_cr(counter: Counter, top_n: int) -> float:
        _tot = sum(counter.values()) or 1
        return round(sum(v for _, v in counter.most_common(top_n)) / _tot, 4)

    def _conc_hhi(counter: Counter) -> float:
        _tot = sum(counter.values()) or 1
        return round(sum((v / _tot) ** 2 for v in counter.values()), 4)

    _conc_brand = {
        "cr3_position": _conc_cr(_brand_pos, 3),
        "cr5_position": _conc_cr(_brand_pos, 5),
        "hhi_position": _conc_hhi(_brand_pos),
        "cr3_sales": _conc_cr(_brand_sales_cnt, 3) if _total_brand_sales else None,
        "cr5_sales": _conc_cr(_brand_sales_cnt, 5) if _total_brand_sales else None,
        "hhi_sales": _conc_hhi(_brand_sales_cnt) if _total_brand_sales else None,
        "top_brands_by_position": [
            {"brand": _b, "positions": _c, "position_share": round(_c / _N_items, 4)}
            for _b, _c in _brand_pos.most_common(10)
        ],
        "top_brands_by_sales": [
            {
                "brand": _b,
                "monthly_units": _c,
                "sales_share": round(_c / max(_total_brand_sales, 1), 4),
            }
            for _b, _c in _brand_sales_cnt.most_common(10)
            if _c > 0
        ],
    }
    _multi_brand_sellers = sorted(
        [
            {
                "seller_id": _sid,
                "brands": sorted(_brands),
                "positions": _seller_pos[_sid],
                "monthly_units": _seller_sales_cnt[_sid],
                "position_share": round(_seller_pos[_sid] / _N_items, 4),
                "sales_share": round(_seller_sales_cnt[_sid] / max(_total_brand_sales, 1), 4),
            }
            for _sid, _brands in _seller_brands.items()
            if len(_brands) > 1
        ],
        key=lambda x: x["positions"],
        reverse=True,
    )
    _n_asins_with_seller_id = sum(_seller_pos.values())
    _conc_seller = {
        "n_sellers_identified": len(_seller_pos),
        "n_asins_with_seller_id": _n_asins_with_seller_id,
        "seller_id_coverage_pct": round(_n_asins_with_seller_id / _N_items, 4) if _N_items else 0,
        "cr3_position": _conc_cr(_seller_pos, 3) if _seller_pos else None,
        "hhi_position": _conc_hhi(_seller_pos) if _seller_pos else None,
        "n_multi_brand_sellers": len(_multi_brand_sellers),
        "multi_brand_sellers": _multi_brand_sellers[:5],
    }
    concentration_data = {
        "n_products": _N_items,
        "n_brands": len(_brand_pos),
        "brand": _conc_brand,
        "seller": _conc_seller,
    }

    # ── Fulfillment distribution (FBA / FBM / AMZ) ─────────────────────────────
    _fulfillment_counts: Counter = Counter(
        _classify_fulfillment(_ai.get("seller_type")) for _ai in analysis_input
    )
    _n_classified = _N_items - _fulfillment_counts.get("Unknown", 0)
    fulfillment_distribution = {
        "n_products": _N_items,
        "n_classified": _n_classified,
        "coverage_pct": round(_n_classified / _N_items, 4) if _N_items else 0,
        "breakdown": [
            {
                "type": _t,
                "count": _c,
                "pct": round(_c / _N_items, 4) if _N_items else 0,
            }
            for _t, _c in sorted(_fulfillment_counts.items(), key=lambda kv: kv[1], reverse=True)
        ],
    }

    # ── Dominant brand portfolio ──────────────────────────────────────────────
    # Triggered when the #1 brand holds ≥15% sales-share OR ≥20% BSR position-share
    # and has ≥2 distinct ASINs visible in the BSR sample.
    dominant_brand_portfolio: list[dict] = []
    _dom_brand: str | None = None
    if _total_brand_sales > 0 and _brand_sales_cnt:
        _top_sales_brand, _top_sales_cnt = _brand_sales_cnt.most_common(1)[0]
        if _top_sales_cnt / _total_brand_sales >= 0.15:
            _dom_brand = _top_sales_brand
    if _dom_brand is None and _brand_pos:
        _top_pos_brand, _top_pos_cnt = _brand_pos.most_common(1)[0]
        if _top_pos_cnt / _N_items >= 0.20:
            _dom_brand = _top_pos_brand
    _ad_ratio_by_asin: dict[str, float] = ctx.cache.get("ad_ratio_by_asin") or {}
    _dom_brand_ad_consistency = "insufficient data"
    if _dom_brand:
        _portfolio_items = sorted(
            [p for p in analysis_input if (p.get("brand") or "Unknown") == _dom_brand],
            key=lambda x: x.get("sales") or 0,
            reverse=True,
        )
        if len(_portfolio_items) >= 2:
            dominant_brand_portfolio = [
                {
                    "asin": p["asin"] or "N/A",
                    "price": f"${p['price']:.2f}" if p.get("price") else "N/A",
                    "reviews": p.get("review_count"),
                    "rating": p.get("rating"),
                    "monthly_units": p.get("sales") or 0,
                    "sub_niche_tag": _asin_sub_niche.get(p["asin"] or ""),
                    "ad_traffic_ratio": (
                        f"{_ad_ratio_by_asin[p['asin']]:.0%}"
                        if p.get("asin") in _ad_ratio_by_asin
                        else "N/A"
                    ),
                }
                for p in _portfolio_items
            ]
            # Brand-moat signal: does this brand hold its portfolio's ASINs at a consistently
            # low (or consistently high) ad reliance, or does it vary per-ASIN? Low variance
            # across the brand's own ASINs (regardless of level) means the brand's traffic mix
            # is a deliberate, repeatable playbook rather than one lucky/unlucky listing.
            _dom_brand_ad_ratios = [
                _ad_ratio_by_asin[p["asin"]]
                for p in _portfolio_items
                if p.get("asin") in _ad_ratio_by_asin
            ]
            if len(_dom_brand_ad_ratios) >= 2:
                _dom_brand_ad_consistency = (
                    "Consistent (repeatable traffic playbook across the brand's ASINs)"
                    if statistics.pstdev(_dom_brand_ad_ratios) < 0.10
                    else "Inconsistent (ad reliance varies per ASIN — no clear brand playbook)"
                )
    logger.info(
        f"[dominant_brand_portfolio] brand={_dom_brand!r}, "
        f"portfolio_size={len(dominant_brand_portfolio)}, "
        f"ad_consistency={_dom_brand_ad_consistency!r}"
    )

    # ── Top ASIN evidence table (top 10 by BSR rank) ──────────────────────────
    # analysis_input[i] corresponds to items[i] (same index).
    # Sort by rank using the original index so both arrays stay aligned.
    _TOP_N = 10
    sorted_indices = sorted(
        range(len(items)),
        key=lambda i: _parse_int(items[i].get("Rank"), default=9999),
    )
    top_asin_rows = []
    for i in sorted_indices[:_TOP_N]:
        raw = items[i]
        enriched = analysis_input[i]
        title_raw = raw.get("Title") or ""
        title = title_raw[:40].rstrip() + ("…" if len(title_raw) > 40 else "")
        _spike = asin_review_spike.get((raw.get("ASIN") or raw.get("asin") or "").upper())
        top_asin_rows.append(
            {
                "rank": enriched["rank"],
                "asin": (raw.get("ASIN") or raw.get("asin") or "N/A"),
                "brand": (enriched.get("brand") or "Unknown")[:20],
                "title": title,
                "price": f"${enriched['price']:.2f}" if enriched["price"] else "N/A",
                "rating": f"{enriched['rating']:.1f}★" if enriched["rating"] else "N/A",
                "reviews": f"{enriched['review_count']:,}" if enriched["review_count"] else "N/A",
                "feedback_count": enriched["feedback_count"]
                if enriched.get("feedback_count") is not None
                else "N/A",
                "review_ratio": f"{enriched['review_ratio']:.2%}"
                if enriched.get("review_ratio") is not None
                else "N/A",
                "review_to_sales_rate": (
                    f"{_spike['peak_rsr']:.1%} peak ({_spike['peak_date_range']})"
                    if _spike
                    else "N/A"
                ),
                "review_to_sales_growth": (
                    f"{_spike['spike_multiple']:.1f}x baseline"
                    if _spike and _spike.get("spike_multiple") is not None
                    else "N/A"
                ),
                "units_mo": f"{enriched['sales']:,}" if enriched["sales"] else "N/A",
                # Pre-classified (FBA/FBM/AMZ/Unknown), not the raw scraped seller_type
                # string — "Amazon" (FBA: 3rd-party seller, Amazon-fulfilled) and
                # "Amazon.com" (AMZ: Amazon itself is the seller) are easy to conflate
                # by surface text alone, so classification happens here rather than
                # leaving the LLM to infer FBA vs. AMZ from the raw label.
                "fulfillment": _classify_fulfillment(enriched.get("seller_type")),
            }
        )

    # ── Full BSR dataset export ───────────────────────────────────────────────
    # Upload all scraped + enriched rows as a CSV so users can access the raw data.
    bsr_dataset_url: str | None = None
    try:
        _store_id = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        _amz_domain = {
            "US": "amazon.com",
            "UK": "amazon.co.uk",
            "DE": "amazon.de",
            "JP": "amazon.co.jp",
            "FR": "amazon.fr",
            "IT": "amazon.it",
            "ES": "amazon.es",
            "CA": "amazon.ca",
        }.get(_store_id, "amazon.com")

        all_bsr_rows = []
        for _raw, _enriched in zip(items, analysis_input, strict=False):
            _asin = (_raw.get("ASIN") or _raw.get("asin") or "").strip().upper()
            _rr = _enriched.get("review_ratio")
            all_bsr_rows.append(
                {
                    "rank": _enriched["rank"],
                    "asin": _asin,
                    "title": _raw.get("Title") or "",
                    "brand": _enriched.get("brand") or _raw.get("Brand") or "",
                    "price_usd": _enriched["price"] if _enriched["price"] else "",
                    "rating": _enriched["rating"] if _enriched["rating"] else "",
                    "review_count": _enriched["review_count"] if _enriched["review_count"] else "",
                    "global_ratings": _enriched.get("global_ratings") or "",
                    "written_reviews": _enriched.get("written_reviews") or "",
                    "review_ratio_pct": f"{_rr:.2%}" if _rr is not None else "",
                    "monthly_sales_est": _enriched["sales"] if _enriched["sales"] else "",
                    "seller_type": _enriched.get("seller_type") or "",
                    "seller_id": _enriched.get("seller_id") or "",
                    "feedback_count": (
                        _enriched["feedback_count"]
                        if _enriched.get("feedback_count") is not None
                        else ""
                    ),
                    "image_url": _raw.get("Image") or "",
                    "product_url": f"https://www.{_amz_domain}/dp/{_asin}" if _asin else "",
                }
            )

        if all_bsr_rows:
            _buf = io.StringIO()
            _writer = csv.DictWriter(_buf, fieldnames=list(all_bsr_rows[0].keys()))
            _writer.writeheader()
            _writer.writerows(all_bsr_rows)
            _csv_bytes = _buf.getvalue().encode("utf-8-sig")

            _kw_slug = re.sub(r"[^\w]", "_", ctx.cache.get("main_keyword", "bsr"), flags=re.ASCII)[
                :30
            ].strip("_")
            _tz_exp = ZoneInfo(
                ctx.config.get("timezone", "America/Los_Angeles")
                if hasattr(ctx, "config")
                else "America/Los_Angeles"
            )
            _ts = datetime.now(tz=_tz_exp).strftime("%Y%m%d_%H%M")
            _filename = f"bsr_{_kw_slug}_{_ts}.csv"

            try:
                from src.core.storage import get_storage_backend

                _storage = get_storage_backend()
                _key = f"exports/{_uuid.uuid4().hex[:8]}_{_filename}"
                bsr_dataset_url = _storage.upload(_key, _csv_bytes, "text/csv; charset=utf-8-sig")
                logger.info(f"[bsr_export] Uploaded {len(all_bsr_rows)} rows → {bsr_dataset_url}")
            except (ValueError, KeyError):
                _report_dir = os.path.abspath("data/reports")
                os.makedirs(_report_dir, exist_ok=True)
                _local_path = os.path.join(_report_dir, _filename)
                with open(_local_path, "wb") as _f:
                    _f.write(_csv_bytes)
                bsr_dataset_url = _local_path
                logger.info(f"[bsr_export] Storage not configured; saved locally: {_local_path}")
    except Exception as _exp_err:
        logger.warning(f"[bsr_export] Failed to export full BSR dataset: {_exp_err}")

    # Deal discount depth: avg discount % and cadence, not just how many deals were found.
    _deal_discount_data = ctx.cache.get("category_deal_discount_data") or {}
    _deal_avg_discount_pct = _deal_discount_data.get("avg_discount_pct")
    _deal_frequency_per_month = _deal_discount_data.get("deal_frequency_per_month")
    _deal_buckets = _deal_discount_data.get("discount_buckets") or {}
    _deal_discount_distribution = (
        ", ".join(
            f"{_label}: {_deal_buckets.get(_key, 0)}"
            for _key, _label in (
                ("shallow_lt15", "Shallow (<15%)"),
                ("moderate_15_30", "Moderate (15-30%)"),
                ("deep_30plus", "Deep (30%+)"),
            )
            if _deal_buckets.get(_key, 0) > 0
        )
        or "N/A"
    )

    # ── Entry verdict (deterministic — pre-computed for the Seller Decision Snapshot) ──
    # Decision DIRECTION only: ENTER / ENTER WITH CONDITIONS / AVOID. Neutral labels are
    # deliberate — P0/P1/P2 are reserved EXCLUSIVELY for Actionable Prioritization priority
    # tiers downstream and must never be re-used here as direction labels. Considers the
    # monopoly score, margin feasibility (ACOS vs breakeven), review integrity, compliance,
    # demand trend, and rating-collapse risk.
    _ev_monopoly = float(result.get("overall_score", 0) or 0)
    _ev_integrity = str(integrity_risk or "unknown").upper()
    _ev_collapse = float(churn.get("collapse_rate", 0) or 0)
    _ev_compliance = str((compliance_risks or {}).get("overall_risk", "NONE")).upper()
    _ev_demand = str(seasonality.get("demand_trend", "unknown")).lower()
    _ev_acos = _estimated_acos
    _ev_be = _breakeven_acos

    _ev_reasons: list[str] = []
    if _ev_monopoly >= 65:
        _ev_reasons.append(f"monopoly {_ev_monopoly:.0f}/100")
    if _ev_acos is not None and _ev_be > 0 and _ev_acos > 1.5 * _ev_be:
        _ev_reasons.append(f"ACOS {_ev_acos:.0%} > 1.5× breakeven {_ev_be:.0%}")
    if _ev_integrity == "HIGH" and _ev_collapse > 0.40:
        _ev_reasons.append("integrity HIGH + rating collapse > 40%")
    if _ev_compliance == "CRITICAL":
        _ev_reasons.append("critical compliance risk")
    if _ev_demand == "collapsing":
        _ev_reasons.append("collapsing demand")

    if _ev_reasons:
        entry_verdict = "AVOID — " + "; ".join(_ev_reasons[:3])
    else:
        _ev_enter = (
            _ev_monopoly < 40
            and (_ev_acos is not None and _ev_be > 0 and _ev_acos <= 0.9 * _ev_be)
            and _ev_integrity != "HIGH"
            and _ev_compliance not in ("HIGH", "CRITICAL")
            and _ev_demand not in ("declining", "collapsing")
        )
        if _ev_enter:
            entry_verdict = "ENTER"
        else:
            _ev_cond: list[str] = []
            if _ev_monopoly >= 40:
                _ev_cond.append(f"monopoly {_ev_monopoly:.0f}/100")
            if _ev_acos is not None and _ev_be > 0 and _ev_acos > _ev_be:
                _ev_cond.append(f"ACOS {_ev_acos:.0%} vs breakeven {_ev_be:.0%}")
            if _ev_integrity == "HIGH":
                _ev_cond.append("integrity HIGH")
            if _ev_compliance in ("HIGH", "MEDIUM"):
                _ev_cond.append(f"{_ev_compliance.lower()} compliance risk")
            if _ev_demand in ("declining", "collapsing"):
                _ev_cond.append(f"{_ev_demand} demand")
            if _ev_collapse > 0.25:
                _ev_cond.append(f"rating collapse {_ev_collapse:.0%}")
            entry_verdict = "ENTER WITH CONDITIONS"
            if _ev_cond:
                entry_verdict += " — " + "; ".join(_ev_cond[:3])

    # FBA share of fulfillment (for the snapshot Logistics Risk row) — derived from the
    # same fulfillment_distribution breakdown the diagnostic table renders.
    _fba_entry = next(
        (
            b
            for b in (fulfillment_distribution or {}).get("breakdown", [])
            if b.get("type") == "FBA"
        ),
        None,
    )
    _fba_pct_str = (
        f"{_fba_entry['pct']:.0%}" if _fba_entry and _fba_entry.get("pct") is not None else "N/A"
    )

    return [
        {
            "analysis_result": json.dumps(result, ensure_ascii=False),
            "monopoly_score": round(result.get("overall_score", 0), 2),
            "monopoly_status": result.get("status", "N/A"),
            # Deterministic snapshot verdict + supporting snapshot inputs (promoted from
            # nested data so the template's {entry_verdict} / {review_floor_top20} /
            # {fulfillment_fba_pct} placeholders resolve at render time).
            "entry_verdict": entry_verdict,
            "review_floor_top20": (
                str(opportunity_signals.get("review_floor_top20"))
                if opportunity_signals.get("review_floor_top20") is not None
                else "N/A"
            ),
            "fulfillment_fba_pct": _fba_pct_str,
            "main_keyword": ctx.cache.get("main_keyword"),
            "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
            "niche_median_price": f"${median_price:.2f}",
            "niche_monthly_units": f"{total_monthly_units:,} units",
            "niche_monthly_gmv": f"${niche_monthly_gmv:,}",
            "bid_insight": json.dumps(bid_raw, ensure_ascii=False)
            if bid_entry_count
            else "N/A (no CPC data fetched)",
            "keyword_click_concentration": json.dumps(
                keyword_click_concentration, ensure_ascii=False
            ),
            "brand_search_monopoly": json.dumps(brand_search_monopoly, ensure_ascii=False),
            "data_quality": data_quality_str,
            "review_disparity": (
                f"{review_disparity_val}x"
                if review_disparity_val is not None
                else "N/A (fewer than 51 BSR products — top/tail buckets too small)"
            ),
            "price_trend": price_trend_str,
            "new_entrant_ratio": new_entrant_str,
            "integrity_alert": integrity_str,
            "integrity_hist_cov": (
                f"{integrity_total}/{n_total} ({integrity_total / n_total:.0%})"
            ),
            "integrity_ratio_cov": (
                f"{len(ratio_eligible)}/{n_total} ({len(ratio_eligible) / n_total:.0%})"
            ),
            "integrity_ratio_threshold": f"{_RATIO_THRESHOLD:.0%}",
            "review_to_sales_rate": review_to_sales_rate_str,
            "review_to_sales_growth_rate": review_to_sales_growth_str,
            "recommended_capital": f"${_cap_total:,}",
            "capital_basis_price": f"${_cap_price:.2f}",
            "capital_basis_tier": _cap_basis_tier,
            "capital_inventory": f"${_cap_inv:,}",
            "capital_ppc": f"${_cap_ppc:,}",
            "capital_fees": f"${_cap_fees:,}",
            "capital_inbound": f"${_cap_inbound:,}",
            "capital_overhead": f"${_CAP_OVERHEAD:,}",
            "capital_units": str(_CAP_UNITS),
            "capital_gross_units": str(_cap_gross_units),
            "capital_cogs_pct": f"{_CAP_COGS:.0%}",
            "capital_acos_pct": f"{_CAP_ACOS:.0%}",
            "capital_fees_pct": f"{_CAP_FEES:.0%}",
            "capital_sell_months": str(_CAP_SELL_MONTHS),
            "return_rate_pct": (
                f"{_ss_return_rate_pct:.1f}%" if _ss_return_rate_pct is not None else "N/A"
            ),
            "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%",
            "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get(
                "r_squared", 0.95
            ),
            "social_psi": ctx.cache.get("category_social_psi", "N/A"),
            "social_verdict": ctx.cache.get("category_social_verdict", "N/A"),
            "deal_intensity": ctx.cache.get("category_deal_intensity", "N/A"),
            "deal_avg_discount_pct": (
                f"{_deal_avg_discount_pct:.1f}%" if _deal_avg_discount_pct is not None else "N/A"
            ),
            "deal_discount_distribution": _deal_discount_distribution,
            "deal_frequency_per_month": (
                f"{_deal_frequency_per_month:.1f}/mo"
                if _deal_frequency_per_month is not None
                else "N/A"
            ),
            "brand_social_data": json.dumps(
                ctx.cache.get("brand_social_data", []), ensure_ascii=False
            ),
            # Rating-based churn (xiyouzhaoci daily trends)
            "churn_pattern": churn.get("pattern", "unknown"),
            "churn_score": churn.get("churn_score", "N/A"),
            "new_product_ratio": f"{churn.get('new_product_ratio', 0):.0%}",
            "collapse_rate": f"{churn.get('collapse_rate', 0):.0%}",
            # BSR listing metabolism (set-comparison across monthly snapshots)
            "bsr_churn_label": bsr_churn.get("label", "unknown"),
            "bsr_churn_3m": f"{bsr_churn['churn_3m']:.0%}"
            if bsr_churn.get("churn_3m") is not None
            else "N/A",
            "bsr_churn_6m": f"{bsr_churn['churn_6m']:.0%}"
            if bsr_churn.get("churn_6m") is not None
            else "N/A",
            "bsr_churn_12m": f"{bsr_churn['churn_12m']:.0%}"
            if bsr_churn.get("churn_12m") is not None
            else "N/A",
            "bsr_snapshots": ", ".join(bsr_churn.get("snapshots_available", [])) or "N/A",
            # BSR composition lifecycle (T vs. T-6 vs. T-12 presence, current Top-100)
            "bsr_lifecycle_incumbents": _bsr_lc_cell(
                _bsr_lc_categories.get("long_term_incumbents")
            ),
            "bsr_lifecycle_returners": _bsr_lc_cell(_bsr_lc_categories.get("returners")),
            "bsr_lifecycle_entrants_survived": _bsr_lc_cell(
                _bsr_lc_categories.get("entrants_survived")
            ),
            "bsr_lifecycle_recent_entrants": _bsr_lc_cell(
                _bsr_lc_categories.get("recent_entrants")
            ),
            "bsr_lifecycle_departed": _bsr_lc_cell(_bsr_lc_departed),
            # Seasonality
            "seasonality_pattern": seasonality.get("pattern", "unknown"),
            "seasonality_score": seasonality.get("seasonality_score", "N/A"),
            # seasonality_score is a 0-100 rescaling of monthly_amplitude (log-space peak/trough
            # swing: score = min(100, amplitude / 1.386 * 100), so a score of 100 means amplitude
            # ≥ 1.386, i.e. a ≥4x swing — see _analyze_seasonality in monopoly_analyzer.py).
            # Surfaced as a fold-change so the LLM cites the real-world swing, not just the score.
            "seasonality_amplitude_fold": (
                f"{math.exp(seasonality['monthly_amplitude']):.1f}x"
                if seasonality.get("monthly_amplitude") is not None
                else "N/A"
            ),
            "seasonality_source": seasonality.get("source", "bsr_daily_trends"),
            "seasonality_n_points": seasonality.get("n_data_points", 0),
            "peak_months": peak_months_str + platform_warning,
            # YOY demand trend (keyword_weekly_trends source only)
            "demand_trend": seasonality.get("demand_trend", "unknown"),
            "yoy_1y_pct": (
                f"{seasonality['yoy_1y_pct']:+.1f}%"
                if seasonality.get("yoy_1y_pct") is not None
                else "N/A"
            ),
            "yoy_2y_pct": (
                f"{seasonality['yoy_2y_pct']:+.1f}%"
                if seasonality.get("yoy_2y_pct") is not None
                else "N/A"
            ),
            "yoy_peak_1y_pct": (
                f"{seasonality['yoy_peak_1y_pct']:+.1f}%"
                if seasonality.get("yoy_peak_1y_pct") is not None
                else "N/A"
            ),
            # Top ASIN evidence table (JSON → rendered as markdown table by LLM)
            "top_asin_table": json.dumps(top_asin_rows, ensure_ascii=False),
            # Full BSR dataset download link (all enriched rows as CSV)
            "bsr_dataset_url": bsr_dataset_url or "N/A",
            # Price distribution (JSON → rendered as table by LLM)
            # n and total_bsr are also promoted to top-level vars so the template
            # coverage footnote rule can reference them via {n}/{total_bsr} directly.
            "price_distribution": json.dumps(price_dist, ensure_ascii=False),
            "n": str(price_dist.get("n", 0)),
            "total_bsr": str(price_dist.get("total_bsr", 0)),
            # Per-tier capital when bimodal (empty list if unimodal)
            "tier_capitals": json.dumps(_tier_capitals, ensure_ascii=False),
            # Compliance / regulatory / hazmat risk (heuristic scan of titles + keywords)
            "compliance_risks": json.dumps(compliance_risks, ensure_ascii=False),
            # Concrete opportunity anchors for the actionable section
            "opportunity_signals": json.dumps(opportunity_signals, ensure_ascii=False),
            # Category contamination: off-category products detected and removed
            "contamination_warning": contamination_warning,
            "contamination_stats": json.dumps(contamination_stats, ensure_ascii=False),
            # Steady-state ad burden (no owned product required — derived from market data)
            # Marketplace Product Guidance search-to-purchase ratio is conventionally
            # shown in per-mille (‰); keep the % in parens for the ACOS formula readers.
            "category_cvr": (
                f"{_category_cvr * 1000:.1f}‰ ({_category_cvr:.2%})"
                if ctx.cache.get("category_cvr_source_type") == _CVR_SRC_SS
                else f"{_category_cvr:.1%}"
            ),
            "category_cvr_source": ctx.cache.get("category_cvr_source", "N/A"),
            "median_cpc": f"${_median_cpc:.2f}" if _median_cpc else "N/A",
            "estimated_steady_state_acos": f"{_estimated_acos:.0%}" if _estimated_acos else "N/A",
            "ad_traffic_ratio": (
                f"{_actual_ad_ratio:.0%}"
                if _actual_ad_ratio is not None
                else "50% (default — traffic-score data unavailable)"
            ),
            # Dispersion of ad reliance across the Top-20 (stdev) and whether a better
            # BSR rank tracks with lower ad reliance (Pearson r, rank vs ad_traffic_ratio)
            "ad_traffic_ratio_stdev": (
                f"{_ad_ratio_stdev:.0%}" if _ad_ratio_stdev is not None else "N/A"
            ),
            "ad_traffic_dispersion_verdict": _ad_dispersion_verdict,
            "ad_traffic_rank_correlation": (
                f"{_ad_ratio_rank_corr:+.2f}" if _ad_ratio_rank_corr is not None else "N/A"
            ),
            "ad_traffic_rank_verdict": _ad_rank_corr_verdict,
            "ad_profit_drag": f"{_ad_profit_drag:.0%}" if _ad_profit_drag else "N/A",
            "referral_fee_pct": f"{_REFERRAL_FEE_PCT:.0%}",
            "referral_fee_source": _referral_fee_source,
            "fba_fee_pct": f"{_fba_fee_pct:.1%}",
            "fba_fee_source": _fba_fee_source,
            "inbound_fee_pct": f"{_inbound_fee_pct:.1%}",
            "inbound_freight_detail": _inbound_freight_detail,
            "breakeven_acos": f"{_breakeven_acos:.0%}",
            "ad_burden_verdict": _ad_burden_verdict,
            # Brand & seller concentration (position share, sales share, CR3/CR5, HHI)
            "concentration_data": json.dumps(concentration_data, ensure_ascii=False),
            # Fulfillment mix across the BSR sample (FBA / FBM / AMZ / Unknown)
            "fulfillment_distribution": json.dumps(fulfillment_distribution, ensure_ascii=False),
            # Dominant brand portfolio (≥2 ASINs, ≥15% sales-share or ≥20% position-share).
            # Each item carries ad_traffic_ratio; dominant_brand_ad_consistency flags whether
            # the brand holds that ratio steady across its own ASINs (repeatable playbook, a
            # brand-moat signal) or not.
            "dominant_brand_portfolio": json.dumps(dominant_brand_portfolio, ensure_ascii=False),
            # Not a prompt var (absent from monopoly_report.yaml's required/optional
            # vars) — carried only so _fetch_critical_reviews_dominant_portfolio can
            # label its entries without recomputing brand-dominance from scratch.
            "dominant_brand_name": _dom_brand or "",
            "dominant_brand_ad_consistency": _dom_brand_ad_consistency,
            # Recent critical reviews for top brands (≤90 days — fixable deficiency signals)
            "critical_reviews_data": json.dumps(
                ctx.cache.get("critical_reviews_data", {}), ensure_ascii=False
            ),
        }
    ]


def _trim_repetition(text: str, min_run: int = 4) -> str:
    """
    Remove trailing repetitive content caused by LLM degeneration near max_output_tokens.

    When a model hits its output limit inside a markdown table it starts repeating
    the last line (e.g. the separator row "| :--- | :--- |") dozens of times.
    This function detects any run of ≥ min_run consecutive identical non-empty lines
    and trims everything from the start of that run onward, keeping exactly one copy.

    Returns the original text unchanged if no repetition is detected.
    """
    lines = text.splitlines()
    n = len(lines)
    if n < min_run * 2:
        return text

    i = n - 1
    while i >= min_run - 1:
        line = lines[i].strip()
        if not line:
            i -= 1
            continue
        # Walk backward to find the start of a run of this exact stripped line
        run_start = i
        while run_start > 0 and lines[run_start - 1].strip() == line:
            run_start -= 1
        run_len = i - run_start + 1
        if run_len >= min_run:
            trimmed = "\n".join(lines[: run_start + 1])
            logger.warning(
                f"[prepare_report_artifact] Trimmed {run_len - 1} repeated lines "
                f"(pattern: {line[:60]!r})"
            )
            return trimmed.rstrip() + "\n\n*（报告在此截断：模型输出已达上限，重复内容已删除）*\n"
        # No long run ending at i — move up past this line
        i = run_start - 1

    return text


async def _prepare_report_artifact(items: list[dict], ctx: Any) -> list[dict]:
    """Saves the report to a local Markdown file, stripping trailing repetition."""
    if not items or "deliver_report" not in items[0]:
        return items
    report_data = items[0]["deliver_report"]
    report_text = (
        report_data.text
        if hasattr(report_data, "text")
        else report_data.get("text")
        if isinstance(report_data, dict)
        else str(report_data)
    )
    if not report_text or report_text == "None":
        return items

    # Strip LLM degeneration artifacts before persisting
    report_text = _trim_repetition(report_text)

    raw_kw = str(ctx.cache.get("main_keyword", "niche"))
    keyword = re.sub(r"[^\w]", "_", raw_kw, flags=re.ASCII)[:40].strip("_") or "niche"
    _tz = ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now(tz=_tz).strftime('%Y%m%d_%H%M')}.md"
    report_dir = os.path.abspath("data/reports")
    os.makedirs(report_dir, exist_ok=True)
    file_path = os.path.join(report_dir, filename)
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        items[0]["report_file_path"] = file_path
        logger.info(f"Artifact prepared at: {file_path} ({len(report_text)} chars)")
    except Exception as e:
        logger.error(f"Failed to write report file: {e}")
    return items


@WorkflowRegistry.register("category_monopoly_analysis")
def build_category_monopoly_analysis(config: dict) -> Workflow:
    monopoly_spec = prompt_manager.get_spec("monopoly_report")
    ctx_vars = {
        name: f"{{{name}}}" for name in (monopoly_spec.required_vars if monopoly_spec else [])
    }

    return Workflow(
        name="category_monopoly_analysis",
        steps=[
            ProcessStep(name="fetch_bsr_top_100", fn=_fetch_bsr_list),
            ProcessStep(name="fetch_sellersprite_bsr", fn=_fetch_sellersprite_bsr),
            ProcessStep(name="fetch_core_keywords", fn=_fetch_core_keywords),
            ProcessStep(name="filter_category_coherence", fn=_filter_category_coherence),
            ProcessStep(name="enrich_sales_data", fn=_enrich_sales),
            EnrichStep(
                name="enrich_seller_background",
                extractor_fn=_enrich_seller_info,
                parallel=True,
                concurrency=5,
            ),
            ProcessStep(name="fetch_market_signals", fn=_fetch_market_signals),
            ProcessStep(name="fetch_category_cvr", fn=_fetch_category_cvr),
            ProcessStep(name="enrich_external_intensity", fn=_enrich_external_intensity),
            ProcessStep(name="enrich_batch_traffic_scores", fn=_enrich_batch_traffic_scores),
            ProcessStep(name="fetch_time_series_data", fn=_fetch_time_series_data),
            ProcessStep(
                name="fetch_critical_reviews_top_brands",
                fn=_fetch_critical_reviews_top_brands,
            ),
            ProcessStep(name="calculate_monopoly_score", fn=_run_monopoly_analysis),
            ProcessStep(
                name="fetch_critical_reviews_dominant_portfolio",
                fn=_fetch_critical_reviews_dominant_portfolio,
            ),
            ProcessStep(
                name="deliver_report",
                # batch_threshold=1,
                prompt_template=prompt_manager.render_spec("monopoly_report", ctx_vars),
                compute_target=ComputeTarget.CLOUD_LLM,
            ),
            ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact),
        ],
    )
