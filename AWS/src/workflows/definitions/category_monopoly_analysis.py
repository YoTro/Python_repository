from __future__ import annotations

"""
Category Monopoly Analysis Workflow

Performs a deep-dive analysis of an Amazon category to determine monopoly levels
and competition intensity across 7 dimensions.
"""

import asyncio
import hashlib as _hl
import logging
from typing import Any

from src.core.data_cache import data_cache as _data_cache
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
_TTL_EXTERNAL = 43_200  # 12 h — TikTok PSI + deal intensity
_TTL_TRAFFIC = 21_600  # 6  h — Xiyouzhaoci batch ad-traffic ratios


def _l2_key(ctx, *parts) -> str:
    tid = getattr(ctx, "tenant_id", None) or "default"
    sid = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    return ":".join(str(p) for p in (tid, sid) + parts)


def _l2_get(ctx, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)


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

    from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor

    extractor = BestSellersExtractor()
    products = await extractor.get_bestsellers(url, max_pages=2)
    if not products:
        raise ValueError(f"BSR extractor returned no products for URL: {url}")
    _l2_set(ctx, products, "bsr_list", url_hash)
    logger.info(f"[cat_monopoly] Fetched {len(products)} BSR products, cached url_hash={url_hash}")
    return products


async def _enrich_sales(items: list[dict], ctx: Any) -> list[dict]:
    """Fetch past month sales for all items in one batch (20 ASINs per request).
    Cache per-ASIN; only fetches ASINs that are not already in L2.
    """
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor

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
        extractor = PastMonthSalesExtractor()
        fetched = await extractor.get_batch_past_month_sales(missing)
        for asin, val in fetched.items():
            sales_map[asin] = val or 0
            _l2_set(ctx, val or 0, "sales", asin)

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
            "feedback_count": 0,
            "global_ratings": None,
            "written_reviews": None,
            "review_ratio": None,
        }

    cached = _l2_get(ctx, _TTL_SELLER, "seller_info", asin)
    if cached is not None:
        return cached

    from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor

    f_extractor, s_extractor, rc_extractor = (
        FulfillmentExtractor(),
        SellerFeedbackExtractor(),
        ReviewRatioExtractor(),
    )
    f_res, rc_res = await asyncio.gather(
        f_extractor.get_fulfillment_info(asin),
        rc_extractor.get_review_count(asin),
    )

    seller_id = f_res.get("SellerId")
    feedback_count = 0
    if seller_id:
        s_res = await s_extractor.get_seller_feedback_count(seller_id)
        feedback_count = s_res.get("FeedbackCount", 0)

    result = {
        "seller_type": f_res.get("FulfilledBy", "Unknown"),
        "seller_id": seller_id,
        "feedback_count": feedback_count,
        "global_ratings": rc_res.get("GlobalRatings"),
        "written_reviews": rc_res.get("WrittenReviews"),
        "review_ratio": rc_res.get("Ratio"),
    }
    _l2_set(ctx, result, "seller_info", asin)
    return result


def _ngram_candidates(titles: list, min_doc_freq: int = 3, top_n: int = 15) -> list:
    """
    Return unigrams and bigrams ranked by document frequency (# titles containing them).

    Used to anchor LLM keyword extraction to terms actually present in the data,
    preventing hallucination and ensuring stability across runs.
    """
    import re as _re_ng
    from collections import Counter

    _STOP = {
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
    doc_counts: Counter = Counter()
    for title in titles:
        tokens = [
            t for t in _re_ng.findall(r"[a-z0-9]+", title.lower()) if t not in _STOP and len(t) > 1
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
        from datetime import datetime, timedelta

        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

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
        from collections import defaultdict

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
        import statistics as _stats

        qualified = [
            (term, len(asins), _stats.median(term_vol[term]))
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
    core_keywords = candidates[:3]
    try:
        from src.intelligence.router import TaskCategory

        if ctx.router and candidates:
            source_label = (
                "cross-ASIN traffic data" if intersection_candidates else "BSR title frequency"
            )
            candidate_str = ", ".join(candidates[:12])
            prompt = (
                "You are classifying an Amazon BSR niche. "
                f"The following search terms are grounded in {source_label} across the top BSR products: "
                f"[{candidate_str}]. "
                "From these candidates ONLY, pick the TOP 3 that best represent the core buyer "
                "search intent for this niche — favour terms a buyer would type to find ANY of the "
                "top products, not sub-niche specific ones. "
                "Return a comma-separated list of exactly 3 terms — no explanation, no numbering."
            )
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            import re as _re_kw

            raw_text = res.text.strip().replace('"', "").replace("'", "").lower()
            raw_text = _re_kw.sub(r"(?m)^\s*\d+[\.\)]\s*", "", raw_text)
            raw_text = _re_kw.sub(r"(?m)^\s*[-•*]\s*", "", raw_text)
            raw_text = _re_kw.sub(r"\n+", ",", raw_text)
            raw_text = raw_text.replace(";", ",")
            parsed = [k.strip() for k in raw_text.split(",") if k.strip()]
            llm_valid = [
                k
                for k in parsed
                if 1 <= len(k.split()) <= 5
                and not any(k.startswith(p) for p in _REFUSAL_PREFIXES)
                and k in candidates  # must be from the data-grounded set
            ]
            if len(llm_valid) >= 2:
                core_keywords = llm_valid[:3]
            else:
                logger.warning(
                    f"[fetch_core_keywords] LLM output not in candidates "
                    f"(got {llm_valid!r}); using top candidates {candidates[:3]}"
                )
    except Exception as e:
        logger.warning(f"[fetch_core_keywords] LLM refinement failed: {e}; using top candidates")

    ctx.cache["core_keywords"] = core_keywords
    ctx.cache["main_keyword"] = core_keywords[0]
    _l2_set(
        ctx,
        {"core_keywords": core_keywords, "main_keyword": core_keywords[0]},
        "core_keywords",
        cache_hash,
    )
    logger.info(
        f"[fetch_core_keywords] final={core_keywords} "
        f"(source={'intersection' if intersection_candidates else 'ngram'}, "
        f"candidates={candidates[:5]})"
    )
    return items


async def _fetch_market_signals(items: list[dict], ctx: Any) -> list[dict]:
    """
    Step 2 of 2 for market context. Depends on ctx.cache["core_keywords"].
    Runs ABA keyword data, SERP ad ratio, and CPC bid recommendations concurrently.
    Writes: ctx.cache["keyword_data"], ctx.cache["ad_ratio"],
            ctx.cache["detailed_bid_analysis"]
    """
    core_keywords = ctx.cache.get("core_keywords", ["unknown niche"])
    main_keyword = core_keywords[0]
    kw_hash = _hl.md5(",".join(sorted(core_keywords)).encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_SIGNALS, "market_signals", kw_hash)
    if cached is not None:
        ctx.cache["keyword_data"] = cached.get("keyword_data", {})
        ctx.cache["ad_ratio"] = cached.get("ad_ratio", 0.3)
        ctx.cache["detailed_bid_analysis"] = cached.get("detailed_bid_analysis", {})
        logger.info(f"[cat_monopoly] Market signals L2 cache hit kw_hash={kw_hash}")
        return items

    async def _fetch_aba() -> None:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        try:
            aba_res = await asyncio.to_thread(
                XiyouZhaociAPI(tenant_id=tenant_id).get_aba_top_asins, country, [main_keyword]
            )
            ctx.cache["keyword_data"] = (
                aba_res["searchTerms"][0] if aba_res and aba_res.get("searchTerms") else {}
            )
        except Exception as e:
            logger.error(f"[fetch_market_signals] ABA fetch failed: {e}")
            ctx.cache.setdefault("keyword_data", {})

    async def _fetch_ad_ratio() -> None:
        from src.mcp.servers.amazon.extractors.search import SearchExtractor

        try:
            search_results = await SearchExtractor().search(main_keyword, page=1)
            sponsored = sum(1 for r in search_results if getattr(r, "is_sponsored", False))
            ctx.cache["ad_ratio"] = sponsored / (len(search_results) or 1)
        except Exception as e:
            logger.error(f"[fetch_market_signals] SERP ad ratio fetch failed: {e}")
            ctx.cache.setdefault("ad_ratio", 0.3)

    async def _fetch_cpc_bids() -> None:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient

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

    await asyncio.gather(_fetch_aba(), _fetch_ad_ratio(), _fetch_cpc_bids())
    _l2_set(
        ctx,
        {
            "keyword_data": ctx.cache.get("keyword_data", {}),
            "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
            "detailed_bid_analysis": ctx.cache.get("detailed_bid_analysis", {}),
        },
        "market_signals",
        kw_hash,
    )
    return items


async def _enrich_external_intensity(items: list[dict], ctx: Any) -> list[dict]:
    """Fetches Social (TikTok) and Deal promotion intensity for the category."""
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword:
        return items

    kw_hash = _hl.md5(main_keyword.encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_EXTERNAL, "external_intensity", kw_hash)
    if cached is not None:
        ctx.cache.update(cached)
        logger.info(f"[cat_monopoly] External intensity L2 cache hit kw_hash={kw_hash}")
        return items

    from src.intelligence.processors.social_virality import SocialViralityProcessor
    from src.mcp.servers.social.tiktok.client import TikTokClient

    try:
        tag_info = await asyncio.to_thread(
            TikTokClient().get_tag_info, main_keyword.replace(" ", "")
        )
        if tag_info.get("id"):
            videos = await asyncio.to_thread(
                TikTokClient().get_hashtag_videos,
                tag_info["id"],
                main_keyword.replace(" ", ""),
                count=20,
            )
            social_analysis = SocialViralityProcessor().calculate_promotion_strength(
                videos, tag_metadata=tag_info
            )
            ctx.cache.update(
                {
                    "category_social_psi": social_analysis.get("strength_score", 0),
                    "category_social_verdict": social_analysis.get("verdict", "Unknown"),
                }
            )
        else:
            ctx.cache.update({"category_social_psi": 0, "category_social_verdict": "No Tag Found"})
    except Exception as e:
        logger.error(f"Error during social intensity analysis: {e}")
        ctx.cache.update({"category_social_psi": 0, "category_social_verdict": "Analysis Failed"})

    from src.mcp.servers.market.deals.client import DealHistoryClient

    async def fetch_deal_count(item):
        return len(
            await DealHistoryClient().get_deal_history(
                asin=item.get("ASIN", ""), keyword=item.get("Title", ""), max_pages=1
            )
        )

    try:
        results = await asyncio.gather(*(fetch_deal_count(item) for item in items[:10]))
        total_deals_found = sum(results)
        deal_intensity_score = (
            9
            if total_deals_found > 5
            else 6
            if total_deals_found > 2
            else 3
            if total_deals_found > 0
            else 0
        )
        ctx.cache["category_deal_intensity"] = deal_intensity_score
    except Exception as e:
        logger.error(f"Error during deal intensity analysis: {e}")

    _ext = {
        "category_social_psi": ctx.cache.get("category_social_psi", 0),
        "category_social_verdict": ctx.cache.get("category_social_verdict", "Unknown"),
        "category_deal_intensity": ctx.cache.get("category_deal_intensity", 0),
    }
    _l2_set(ctx, _ext, "external_intensity", kw_hash)
    logger.info(
        f"External intensity: Social PSI={_ext['category_social_psi']}, Deal Intensity={_ext['category_deal_intensity']}"
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
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

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
        Attempt to extract daily records from multiple known response shapes:
          A. res["data"]["entities"][i]["dailyData"]   (list of entities)
          B. res["data"][asin]["dailyData"]            (ASIN-keyed dict)
          C. res["data"]                               (flat list of day dicts)
        Returns a normalised list of {"date", "bsr", "stars", "ratings", "price"}.
        price is retained for the price-trend calculation in _run_monopoly_analysis
        (first-30d vs last-30d median per ASIN → avg_price_change / price_trend_direction).
        """

        def _normalise(daily_list: list) -> list:
            return [
                {
                    "date": str(d.get("date", ""))[:10],
                    "bsr": d.get("bsr") or d.get("bestSellerRank"),
                    "stars": d.get("stars") or d.get("avgStarRating"),
                    "ratings": d.get("ratings") or d.get("reviewCount"),
                    "price": d.get("price"),
                }
                for d in daily_list
                if d.get("date")
            ]

        data = res.get("data") or {}

        # Shape A: {"data": {"entities": [{"asin": ..., "dailyData": [...]}]}}
        if isinstance(data, dict) and "entities" in data:
            for entity in data["entities"] or []:
                if entity.get("asin") == asin:
                    return _normalise(entity.get("dailyData") or [])

        # Shape B: {"data": {"B0xxx": {"dailyData": [...]}}}
        if isinstance(data, dict) and asin in data:
            asin_data = data[asin]
            daily = asin_data.get("dailyData") or (asin_data if isinstance(asin_data, list) else [])
            return _normalise(daily)

        # Shape C: {"data": [{"date": ..., "bsr": ...}]}
        if isinstance(data, list):
            return _normalise(data)

        logger.debug(
            f"[historical_trends] Unrecognised response shape for {asin}: keys={list(data.keys()) if isinstance(data, dict) else type(data)}"
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
    """Fetches batch traffic scores for Top 20 ASINs to calculate average ad dependency."""
    if not items or not ctx.mcp:
        return items

    top_asins = sorted(
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    )
    if not top_asins:
        return items

    asins_hash = _hl.md5(",".join(top_asins).encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_TRAFFIC, "traffic_scores", asins_hash)
    if cached is not None:
        ctx.cache["actual_bsr_ad_ratio"] = cached["actual_bsr_ad_ratio"]
        logger.info(f"[cat_monopoly] Traffic scores L2 cache hit asins_hash={asins_hash}")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        resp = await ctx.mcp.call_tool_json(
            "xiyou_get_traffic_scores", {"asins": top_asins, "country": country}
        )
        if isinstance(resp, list) and len(resp) > 0:
            import json

            data = json.loads(resp[0].get("text", "{}"))
            if data.get("success") and data.get("data"):
                ratios = [d.get("advertisingTrafficScoreRatio", 0.0) for d in data["data"]]
                if ratios:
                    import statistics

                    avg_ratio = statistics.mean(ratios)
                    ctx.cache["actual_bsr_ad_ratio"] = avg_ratio
                    _l2_set(ctx, {"actual_bsr_ad_ratio": avg_ratio}, "traffic_scores", asins_hash)
                    logger.info(f"Calculated average BSR ad dependency: {avg_ratio:.2%}")
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
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

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
    from datetime import datetime, timedelta

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
    import re
    from datetime import datetime

    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI

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
            logger.info(
                f"[cat_monopoly] Sellersprite BSR L2 cache hit node={node_id} base_ym={base_ym}"
            )
            return items

        tenant_id = ctx.config.get("tenant_id", "default")
        api = SellerspriteAPI(tenant_id=tenant_id)
        if not api.auth_token:
            logger.warning("[sellersprite_bsr] No auth token; skipping")
            return items

        # Resolve the full nodeIdPath by searching each snapshot table until a
        # match is found. get_category_nodes with the bare node_id returns the
        # node entry whose ``id`` field is the colon-joined full path we need.
        # node_id is a numeric ID from the URL → exact match, take items[0]["id"]
        node_id_path = None
        for ym in snapshot_yms:
            table = f"bsr_sales_monthly_{ym}"
            nodes = await asyncio.to_thread(
                api.resolve_node_path, market_id=market_id, table=table, query=node_id
            )
            if nodes:
                node_id_path = nodes[0].get("id")
            if node_id_path:
                logger.info(
                    f"[sellersprite_bsr] Resolved node_id={node_id} → {node_id_path} (table={table})"
                )
                break
            logger.debug(
                f"[sellersprite_bsr] node_id={node_id} not found in table={table}, trying next"
            )

        if not node_id_path:
            logger.warning(
                f"[sellersprite_bsr] Could not resolve nodeIdPath for node_id={node_id} in any snapshot"
            )
            return items

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
                slim = [
                    {
                        "asin": p.get("asin") or p.get("parentAsin") or "",
                        "rank": p.get("rank") or p.get("rankingPosition") or (i + 1),
                        "brand": p.get("brand") or p.get("brandName") or "",
                        # availableDate is ms-since-epoch; kept as-is for downstream math
                        "available_date_ms": p.get("availableDate"),
                    }
                    for i, p in enumerate(result.get("items") or [])
                    if p.get("asin") or p.get("parentAsin")
                ]
                return ym, slim
            except Exception as e:
                logger.warning(f"[sellersprite_bsr] Snapshot {ym} failed: {e}")
                return ym, []

        results = await asyncio.gather(*[fetch_snapshot(ym) for ym in snapshot_yms])
        snapshots = {ym: products for ym, products in results if products}
        ctx.cache["sellersprite_snapshots"] = snapshots
        ctx.cache["sellersprite_base_ym"] = base_ym
        _l2_set(ctx, {"snapshots": snapshots, "base_ym": base_ym}, "ss_bsr", ss_cache_key)
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

    for entry in _COMPLIANCE_DB:
        hit_triggers = [t for t in entry["triggers"] if t in combined]
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


async def _run_monopoly_analysis(items: list[dict], ctx: Any) -> list[dict]:
    """Calculates scores and generates flattened niche benchmarks."""
    import json
    import re as _re
    import statistics

    from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
    from src.intelligence.processors.sales_estimator import SalesEstimator

    def _parse_float(raw, default: float = 0.0) -> float:
        """Extract the first decimal number from a US-locale price/rating string.

        Handles: currency symbols ($), thousand-separator commas, ranges ($9–$15),
        suffixes/spaces.  Commas are always thousand separators in this context
        (US locale), so they are stripped before matching — avoids misreading
        "$1,299.99" as 1.299 when comma is naively replaced by a dot.
        """
        s = str(raw or "").replace(",", "")  # strip thousand separators
        m = _re.search(r"\d+(?:\.\d+)?", s)
        if not m:
            return default
        try:
            return float(m.group())
        except ValueError:
            return default

    def _parse_int(raw, default: int = 0) -> int:
        """Extract the first integer from a messy string (handles commas, suffixes, parens)."""
        m = _re.search(r"\d+", str(raw or "").replace(",", ""))
        if not m:
            return default
        try:
            return int(m.group())
        except ValueError:
            return default

    analyzer = CategoryMonopolyAnalyzer()
    external_data = {
        "social_psi": ctx.cache.get("category_social_psi"),
        "deal_intensity": ctx.cache.get("category_deal_intensity"),
    }

    # Build ASIN→brand lookup from the most recent Sellersprite snapshot.
    # Amazon's BSR card HTML carries no brand byline, so this is the authoritative source.
    ss_snapshots = ctx.cache.get("sellersprite_snapshots", {})
    _brand_lookup: dict = {}
    if ss_snapshots:
        _latest = ss_snapshots.get(max(ss_snapshots), [])
        _brand_lookup = {
            p["asin"]: p.get("brand") for p in _latest if p.get("asin") and p.get("brand")
        }

    analysis_input = [
        {
            "rank": _parse_int(item.get("Rank"), default=999),
            "price": _parse_float(item.get("Price")),
            "sales": item.get("sales", 0),
            "brand": _brand_lookup.get(item.get("ASIN") or item.get("asin"))
            or item.get("Brand")
            or item.get("brand")
            or None,
            "seller_type": item.get("seller_type", "Unknown"),
            "feedback_count": item.get("feedback_count", 0),
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
        "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
        "actual_bsr_ad_ratio": ctx.cache.get("actual_bsr_ad_ratio"),
        "detailed_bids": detailed_bids,
    }

    result = analyzer.analyze(
        analysis_input,
        keyword_data=ctx.cache.get("keyword_data"),
        ad_data=ad_data,
        external_data=external_data,
        historical_data=ctx.cache.get("historical_data"),
        bsr_snapshots=ctx.cache.get("sellersprite_snapshots"),
        keyword_weekly_trends=ctx.cache.get("keyword_weekly_trends"),
    )

    # Pass raw bid recommendations directly — no pre-formatting.
    # The LLM receives the full API payload for both strategies and renders it.
    bid_raw = {
        "LEGACY_FOR_SALES": detailed_bids.get("LEGACY_FOR_SALES", []),
        "AUTO_FOR_SALES": detailed_bids.get("AUTO_FOR_SALES", []),
    }
    # Count total keyword-level entries for data-quality tracking
    bid_entry_count = sum(
        len(rec.get("bidRecommendationsForTargetingExpressions", []))
        for strategy_recs in bid_raw.values()
        for rec in strategy_recs
    )

    prices = [p["price"] for p in analysis_input if p["price"] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    total_monthly_units = result.get("niche_benchmarks", {}).get("total_estimated_monthly_units", 0)
    niche_monthly_gmv = int(total_monthly_units * median_price)
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
            buckets.append(
                {
                    "range": f"${lo_b:.0f}–${hi_b:.0f}",
                    "lo": lo_b,
                    "hi": hi_b,
                    "count": cnt,
                    "pct": f"{pct:.0f}%",
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

    # ── New entrant ratio: % of T-snapshot ASINs listed within last 12 months ─
    # availableDate from Sellersprite is ms-since-epoch (e.g. 1686873600000).
    # "new" = product went live within 12 months before the snapshot month (base_ym).
    import time as _time

    base_ym = ctx.cache.get("sellersprite_base_ym", "")
    snapshots = ctx.cache.get("sellersprite_snapshots") or {}
    t_snapshot = snapshots.get(base_ym) or (snapshots.get(max(snapshots)) if snapshots else [])
    if t_snapshot:
        # Cutoff = first day of (base_ym − 12 months), converted to ms-since-epoch.
        # Must anchor to base_ym, NOT to now — base_ym is T-2 months, so using now
        # would shift the window 2 months forward and under-count new entrants.
        if base_ym and len(base_ym) == 6:
            _by, _bm = int(base_ym[:4]), int(base_ym[4:])
            _total = _by * 12 + (_bm - 1) - 12
            _cy, _cm = _total // 12, _total % 12 + 1
            import calendar as _cal

            cutoff_ms = _cal.timegm((_cy, _cm, 1, 0, 0, 0)) * 1000
        else:
            cutoff_ms = (_time.time() - 365 * 86400) * 1000  # fallback when base_ym missing
        dated = [p for p in t_snapshot if p.get("available_date_ms")]
        new_entrants = [p for p in dated if p["available_date_ms"] >= cutoff_ms]
        new_entrant_ratio_val = len(new_entrants) / len(t_snapshot) if t_snapshot else 0.0
        new_entrant_str = f"{new_entrant_ratio_val:.0%} ({len(new_entrants)}/{len(t_snapshot)} ASINs listed in last 12 months)"
    else:
        new_entrant_ratio_val = 0.0
        new_entrant_str = "unknown (no T-snapshot)"

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

    for asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r.get("stars"), r.get("ratings") or 0) for r in records if r.get("date")],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        integrity_total += 1

        # Signal 1: RSR — data source: Xiyouzhaoci daily_trends ratings field
        # + SalesEstimator monthly sales estimate
        monthly_sales = sales_map.get(asin.upper(), 0)
        if monthly_sales > 0:
            review_delta = pts[-1][2] - pts[0][2]
            months_spanned = max(len(pts) / 30, 1)
            rsr = (review_delta / months_spanned) / monthly_sales
            if rsr > _RSR_THRESHOLD:
                flagged_rsr += 1

        # Signal 2: rating jump — data source: Xiyouzhaoci daily_trends stars field
        stars_series = [s for _, s, _ in pts if s]
        for i in range(_JUMP_WINDOW, len(stars_series)):
            if stars_series[i] - stars_series[i - _JUMP_WINDOW] > _JUMP_STARS:
                flagged_jump += 1
                break

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

        # 3. Sub-niche fragmentation: classify each top-50 BSR title into a sub-niche
        _SUB_NICHES = {
            "insect_spray": [
                "insect killer",
                "bug killer",
                "bug spray",
                "mosquito killer",
                "mosquito spray",
                "ant killer",
                "roach killer",
                "spider killer",
            ],
            "flea_tick": ["flea", "tick"],
            "weed_herbicide": ["weed killer", "herbicide", "weed control"],
            "neem_natural": ["neem", "natural pest", "organic pest", "essential oil spray"],
            "wasp_hornet": ["wasp", "hornet", "yellow jacket"],
            "rodent": ["rat killer", "mouse killer", "rodent", "mice killer", "rat poison"],
            "repellent": ["repellent", "repel", "deter", "deer repellent", "rabbit repellent"],
            "aerosol_spray": ["spray concentrate", "spray barrier", "spray treatment"],
        }
        sub_counts: dict = dict.fromkeys(_SUB_NICHES, 0)
        for raw_item in items[:50]:
            title = (raw_item.get("Title") or "").lower()
            for sub, triggers in _SUB_NICHES.items():
                if any(t in title for t in triggers):
                    sub_counts[sub] += 1
                    break  # assign first match only
        # Keep non-zero sub-niches; flag the smallest with ≥2 products
        sub_counts_nz = {k: v for k, v in sub_counts.items() if v > 0}
        if sub_counts_nz:
            opportunity_signals["sub_niche_counts"] = sub_counts_nz
            smallest = min(sub_counts_nz, key=sub_counts_nz.get)
            if sub_counts_nz[smallest] >= 2:
                opportunity_signals["least_crowded_sub_niche"] = {
                    "name": smallest.replace("_", " "),
                    "count": sub_counts_nz[smallest],
                }

        # 4. Rank positions with the lowest review counts (easiest to displace)
        ranked_by_reviews = sorted(
            [
                {"rank": p["rank"], "reviews": p["review_count"]}
                for p in analysis_input
                if p.get("rank") and 10 <= p["rank"] <= 50 and (p.get("review_count") or 0) > 0
            ],
            key=lambda x: x["reviews"],
        )
        if ranked_by_reviews:
            weakest = ranked_by_reviews[0]
            opportunity_signals["weakest_rank_slot"] = {
                "rank": weakest["rank"],
                "reviews": weakest["reviews"],
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
        ss_new_asins = []
        if t_snapshot and "cutoff_ms" in dir():
            ss_new_asins = [
                p["asin"]
                for p in t_snapshot
                if p.get("available_date_ms") and p["available_date_ms"] >= cutoff_ms  # type: ignore[name-defined]
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
        f"CPC keyword entries: {dq_cpc}"
    )

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
    _CAP_FEES = 0.25  # Amazon platform fees (referral ~15% + FBA ~10%)
    _CAP_SELL_MONTHS = 3  # expected months to sell through the first batch (display only)
    _CAP_OVERHEAD = 2000  # fixed launch costs: photography, A+, listing, freight ($)
    _CAP_BUFFER = 0.20  # working capital buffer on subtotal

    # If bimodal, compute capital against each tier's median separately
    # so the operator can see how the budget changes by tier choice.
    _cap_price = median_price  # overall median — may be in the gap if bimodal
    _cap_inv = int(_CAP_UNITS * _cap_price * _CAP_COGS)
    _cap_ppc = int(_CAP_UNITS * _cap_price * _CAP_ACOS)
    _cap_fees = int(_CAP_UNITS * _cap_price * _CAP_FEES)
    _cap_sub = _cap_inv + _cap_ppc + _cap_fees + _CAP_OVERHEAD
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
            t_inv = int(_CAP_UNITS * t_med * _CAP_COGS)
            t_ppc = int(_CAP_UNITS * t_med * _CAP_ACOS)
            t_fees = int(_CAP_UNITS * t_med * _CAP_FEES)
            t_sub = t_inv + t_ppc + t_fees + _CAP_OVERHEAD
            _tier_capitals.append(
                {
                    "tier": tier["label"],
                    "median": t_median_str,
                    "inventory": f"${t_inv:,}",
                    "ppc": f"${t_ppc:,}",
                    "fees": f"${t_fees:,}",
                    "overhead": f"${_CAP_OVERHEAD:,}",
                    "total": f"${int(t_sub * (1 + _CAP_BUFFER)):,}",
                }
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
        top_asin_rows.append(
            {
                "rank": enriched["rank"],
                "asin": (raw.get("ASIN") or raw.get("asin") or "N/A"),
                "brand": (enriched.get("brand") or "Unknown")[:20],
                "title": title,
                "price": f"${enriched['price']:.2f}" if enriched["price"] else "N/A",
                "rating": f"{enriched['rating']:.1f}★" if enriched["rating"] else "N/A",
                "reviews": f"{enriched['review_count']:,}" if enriched["review_count"] else "N/A",
                "units_mo": f"{enriched['sales']:,}" if enriched["sales"] else "N/A",
                "seller_type": enriched.get("seller_type") or "Unknown",
            }
        )

    return [
        {
            "analysis_result": json.dumps(result, ensure_ascii=False),
            "main_keyword": ctx.cache.get("main_keyword"),
            "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
            "niche_median_price": f"${median_price:.2f}",
            "niche_monthly_units": f"{total_monthly_units:,} units",
            "niche_monthly_gmv": f"${niche_monthly_gmv:,}",
            "bid_insight": json.dumps(bid_raw, ensure_ascii=False)
            if bid_entry_count
            else "N/A (no CPC data fetched)",
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
            "recommended_capital": f"${_cap_total:,}",
            "capital_inventory": f"${_cap_inv:,}",
            "capital_ppc": f"${_cap_ppc:,}",
            "capital_fees": f"${_cap_fees:,}",
            "capital_overhead": f"${_CAP_OVERHEAD:,}",
            "capital_units": str(_CAP_UNITS),
            "capital_cogs_pct": f"{_CAP_COGS:.0%}",
            "capital_acos_pct": f"{_CAP_ACOS:.0%}",
            "capital_fees_pct": f"{_CAP_FEES:.0%}",
            "capital_sell_months": str(_CAP_SELL_MONTHS),
            "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%",
            "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get(
                "r_squared", 0.95
            ),
            "social_psi": ctx.cache.get("category_social_psi", "N/A"),
            "social_verdict": ctx.cache.get("category_social_verdict", "N/A"),
            "deal_intensity": ctx.cache.get("category_deal_intensity", "N/A"),
            # Rating-based churn (xiyouzhaoci daily trends)
            "churn_pattern": churn.get("pattern", "unknown"),
            "churn_score": churn.get("churn_score", "N/A"),
            "new_product_ratio": f"{churn.get('new_product_ratio', 0):.0%}",
            "collapse_rate": f"{churn.get('collapse_rate', 0):.0%}",
            # BSR listing metabolism (set-comparison across monthly snapshots)
            "bsr_churn_label": bsr_churn.get("label", "unknown"),
            "bsr_churn_3m": f"{bsr_churn.get('churn_3m') or 0:.0%}",
            "bsr_churn_6m": f"{bsr_churn.get('churn_6m') or 0:.0%}",
            "bsr_churn_12m": f"{bsr_churn.get('churn_12m') or 0:.0%}",
            "bsr_snapshots": ", ".join(bsr_churn.get("snapshots_available", [])) or "N/A",
            # Seasonality
            "seasonality_pattern": seasonality.get("pattern", "unknown"),
            "seasonality_score": seasonality.get("seasonality_score", "N/A"),
            "seasonality_source": seasonality.get("source", "bsr_daily_trends"),
            "seasonality_n_points": seasonality.get("n_data_points", 0),
            "peak_months": peak_months_str + platform_warning,
            # Top ASIN evidence table (JSON → rendered as markdown table by LLM)
            "top_asin_table": json.dumps(top_asin_rows, ensure_ascii=False),
            # Price distribution (JSON → rendered as table by LLM)
            "price_distribution": json.dumps(price_dist, ensure_ascii=False),
            # Per-tier capital when bimodal (empty list if unimodal)
            "tier_capitals": json.dumps(_tier_capitals, ensure_ascii=False),
            # Compliance / regulatory / hazmat risk (heuristic scan of titles + keywords)
            "compliance_risks": json.dumps(compliance_risks, ensure_ascii=False),
            # Concrete opportunity anchors for the actionable section
            "opportunity_signals": json.dumps(opportunity_signals, ensure_ascii=False),
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

    import os
    import re as _re
    from datetime import datetime
    from zoneinfo import ZoneInfo

    raw_kw = str(ctx.cache.get("main_keyword", "niche"))
    keyword = _re.sub(r"[^\w]", "_", raw_kw, flags=_re.ASCII)[:40].strip("_") or "niche"
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
    from src.intelligence.prompts.manager import prompt_manager

    monopoly_spec = prompt_manager.get_spec("monopoly_report")
    ctx_vars = {
        name: f"{{{name}}}" for name in (monopoly_spec.required_vars if monopoly_spec else [])
    }

    return Workflow(
        name="category_monopoly_analysis",
        steps=[
            ProcessStep(name="fetch_bsr_top_100", fn=_fetch_bsr_list),
            ProcessStep(name="enrich_sales_data", fn=_enrich_sales),
            EnrichStep(
                name="enrich_seller_background",
                extractor_fn=_enrich_seller_info,
                parallel=True,
                concurrency=5,
            ),
            ProcessStep(name="fetch_core_keywords", fn=_fetch_core_keywords),
            ProcessStep(name="fetch_market_signals", fn=_fetch_market_signals),
            ProcessStep(name="enrich_external_intensity", fn=_enrich_external_intensity),
            ProcessStep(name="enrich_batch_traffic_scores", fn=_enrich_batch_traffic_scores),
            ProcessStep(name="fetch_time_series_data", fn=_fetch_time_series_data),
            ProcessStep(name="fetch_sellersprite_bsr", fn=_fetch_sellersprite_bsr),
            ProcessStep(name="calculate_monopoly_score", fn=_run_monopoly_analysis),
            ProcessStep(
                name="deliver_report",
                prompt_template=prompt_manager.render_spec("monopoly_report", ctx_vars),
                compute_target=ComputeTarget.CLOUD_LLM,
            ),
            ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact),
        ],
    )
