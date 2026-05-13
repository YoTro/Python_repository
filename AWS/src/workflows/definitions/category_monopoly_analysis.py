from __future__ import annotations
"""
Category Monopoly Analysis Workflow

Performs a deep-dive analysis of an Amazon category to determine monopoly levels
and competition intensity across 7 dimensions.
"""

import hashlib as _hl
import logging
import asyncio
from typing import List, Dict, Any
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import ComputeTarget
from src.core.data_cache import data_cache as _data_cache

logger = logging.getLogger(__name__)

# ── L2 cache helpers ─────────────────────────────────────────────────────────
_L2_DOMAIN = "cat_monopoly"

_TTL_BSR        = 3_600    # 1  h — BSR scrape
_TTL_SALES      = 86_400   # 24 h — past-month sales
_TTL_SELLER     = 21_600   # 6  h — seller/fulfillment info
_TTL_SIGNALS    = 3_600    # 1  h — ABA + SERP + CPC market signals
_TTL_TIMESERIES = 86_400   # 24 h — 12-month historical trends + keyword weekly
_TTL_SS_BSR     = 86_400   # 24 h — Sellersprite monthly snapshots
_TTL_KEYWORDS   = 21_600   # 6  h — LLM keyword extraction from BSR titles
_TTL_EXTERNAL   = 43_200   # 12 h — TikTok PSI + deal intensity
_TTL_TRAFFIC    = 21_600   # 6  h — Xiyouzhaoci batch ad-traffic ratios


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

async def _fetch_bsr_list(items: List[dict], ctx: Any) -> List[dict]:
    """Fetches the Top 100 BSR products from a category URL."""
    url = ctx.config.get("url")
    if not url:
        logger.error("No URL provided in workflow config for category_monopoly_analysis.")
        return []

    url_hash = _hl.md5(url.encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_BSR, "bsr_list", url_hash)
    if cached is not None:
        logger.info(f"[cat_monopoly] BSR list L2 cache hit for url_hash={url_hash}")
        return cached

    from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
    extractor = BestSellersExtractor()
    products = await extractor.get_bestsellers(url, max_pages=2)
    _l2_set(ctx, products, "bsr_list", url_hash)
    logger.info(f"[cat_monopoly] Fetched {len(products)} BSR products, cached url_hash={url_hash}")
    return products

async def _enrich_sales(items: List[dict], ctx: Any) -> List[dict]:
    """Fetch past month sales for all items in one batch (20 ASINs per request).
    Cache per-ASIN; only fetches ASINs that are not already in L2.
    """
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor

    all_asins = [
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items
    ]

    # Resolve from cache where available
    sales_map: Dict[str, int] = {}
    missing: List[str] = []
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

    for item, asin in zip(items, all_asins):
        item["sales"] = sales_map.get(asin) or 0
    return items

async def _enrich_seller_info(item: dict, ctx: Any) -> dict:
    """Fetch fulfillment, seller feedback, and written-vs-global review counts."""
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {"seller_type": "Unknown", "seller_id": None, "feedback_count": 0,
                "global_ratings": None, "written_reviews": None, "review_ratio": None}

    cached = _l2_get(ctx, _TTL_SELLER, "seller_info", asin)
    if cached is not None:
        return cached

    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
    from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor

    f_extractor, s_extractor, rc_extractor = (
        FulfillmentExtractor(), SellerFeedbackExtractor(), ReviewRatioExtractor()
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
        "seller_type":    f_res.get("FulfilledBy", "Unknown"),
        "seller_id":      seller_id,
        "feedback_count": feedback_count,
        "global_ratings": rc_res.get("GlobalRatings"),
        "written_reviews": rc_res.get("WrittenReviews"),
        "review_ratio":   rc_res.get("Ratio"),
    }
    _l2_set(ctx, result, "seller_info", asin)
    return result

async def _fetch_core_keywords(items: List[dict], ctx: Any) -> List[dict]:
    """
    Step 1 of 2 for market context.
    Uses LLM to extract the top 3 core search terms from BSR product titles.
    Must complete before _fetch_market_signals since all downstream signals
    (ABA, SERP, CPC) are keyed to these keywords.
    Writes: ctx.cache["core_keywords"], ctx.cache["main_keyword"]
    """
    if not items:
        return []

    top_titles = [item.get("Title", "") for item in items[:20] if item.get("Title")]
    titles_hash = _hl.md5("|".join(top_titles).encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_KEYWORDS, "core_keywords", titles_hash)
    if cached is not None:
        ctx.cache["core_keywords"] = cached["core_keywords"]
        ctx.cache["main_keyword"]  = cached["main_keyword"]
        logger.info(f"[cat_monopoly] Core keywords L2 cache hit titles_hash={titles_hash}")
        return items

    prompt = (
        "Analyze these 20 Amazon Best Seller product titles and identify the TOP 3 most accurate CORE search terms (keywords). "
        "Return them as a comma-separated list, most important first. "
        "Ignore brands and attributes. Titles: "
        f"{top_titles}"
    )

    # Prefixes that indicate an LLM refusal / error — must not become keywords
    _REFUSAL_PREFIXES = ("sorry", "i ", "i'", "based on", "the keyword", "here are",
                         "unfortunately", "as an", "i cannot", "i can't")

    core_keywords = ["unknown niche"]
    try:
        from src.intelligence.router import TaskCategory
        if ctx.router:
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            import re as _re_kw
            raw_text = res.text.strip().replace('"', '').replace("'", "").lower()
            # Strip numbered/bullet prefixes first, while line boundaries still exist
            raw_text = _re_kw.sub(r"(?m)^\s*\d+[\.\)]\s*", "", raw_text)  # "1. " / "1) "
            raw_text = _re_kw.sub(r"(?m)^\s*[-•*]\s*", "", raw_text)      # "- " / "• "
            # Then normalise remaining separators to commas
            raw_text = _re_kw.sub(r"\n+", ",", raw_text)           # newlines → comma
            raw_text = raw_text.replace(";", ",")                   # semicolons → comma
            candidates = [k.strip() for k in raw_text.split(",") if k.strip()]
            # Keep only short phrases that look like actual search keywords
            valid = [
                k for k in candidates
                if 1 <= len(k.split()) <= 6
                and not any(k.startswith(p) for p in _REFUSAL_PREFIXES)
            ]
            if valid:
                core_keywords = valid[:3]
            else:
                logger.warning(
                    f"[fetch_core_keywords] LLM returned no valid keywords "
                    f"(raw: {raw_text[:120]!r}); keeping default"
                )
    except Exception as e:
        logger.warning(f"[fetch_core_keywords] Keyword extraction failed: {e}")

    ctx.cache["core_keywords"] = core_keywords
    ctx.cache["main_keyword"]  = core_keywords[0]
    _l2_set(ctx, {"core_keywords": core_keywords, "main_keyword": core_keywords[0]},
            "core_keywords", titles_hash)
    logger.info(f"[fetch_core_keywords] Extracted: {core_keywords}")
    return items


async def _fetch_market_signals(items: List[dict], ctx: Any) -> List[dict]:
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
        ctx.cache["keyword_data"]         = cached.get("keyword_data", {})
        ctx.cache["ad_ratio"]             = cached.get("ad_ratio", 0.3)
        ctx.cache["detailed_bid_analysis"] = cached.get("detailed_bid_analysis", {})
        logger.info(f"[cat_monopoly] Market signals L2 cache hit kw_hash={kw_hash}")
        return items

    async def _fetch_aba() -> None:
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
        country   = ctx.config.get("store_id",  "US")      if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        try:
            aba_res = await asyncio.to_thread(
                XiyouZhaociAPI(tenant_id=tenant_id).get_aba_top_asins, country, [main_keyword]
            )
            ctx.cache["keyword_data"] = (
                aba_res["searchTerms"][0]
                if aba_res and aba_res.get("searchTerms")
                else {}
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
            kws = [{"keyword": kw, "matchType": m}
                   for kw in core_keywords for m in ("EXACT", "PHRASE")]
            # Do NOT pass competitor BSR ASINs — the bid API requires an ASIN owned
            # by the advertiser. The client will auto-discover one via _get_owned_asin_fallback().
            # Two separate calls — the API only accepts one strategy string per request.
            legacy_res, auto_res = await asyncio.gather(
                ads_client.get_keyword_bid_recommendations(
                    keywords=kws, strategy="LEGACY_FOR_SALES"),
                ads_client.get_keyword_bid_recommendations(
                    keywords=kws, strategy="AUTO_FOR_SALES"),
                return_exceptions=True,
            )
            ctx.cache["detailed_bid_analysis"] = {
                "LEGACY_FOR_SALES": (legacy_res.get("bidRecommendations", [])
                                     if not isinstance(legacy_res, Exception) else []),
                "AUTO_FOR_SALES":   (auto_res.get("bidRecommendations", [])
                                     if not isinstance(auto_res, Exception) else []),
            }
            if isinstance(legacy_res, Exception):
                logger.error(f"[fetch_market_signals] LEGACY bid fetch failed: {legacy_res}")
            if isinstance(auto_res, Exception):
                logger.error(f"[fetch_market_signals] AUTO bid fetch failed: {auto_res}")
        except Exception as e:
            logger.error(f"[fetch_market_signals] CPC bid fetch failed: {e}")
            ctx.cache.setdefault("detailed_bid_analysis", {})

    await asyncio.gather(_fetch_aba(), _fetch_ad_ratio(), _fetch_cpc_bids())
    _l2_set(ctx, {
        "keyword_data":          ctx.cache.get("keyword_data", {}),
        "ad_ratio":              ctx.cache.get("ad_ratio", 0.3),
        "detailed_bid_analysis": ctx.cache.get("detailed_bid_analysis", {}),
    }, "market_signals", kw_hash)
    return items

async def _enrich_external_intensity(items: List[dict], ctx: Any) -> List[dict]:
    """Fetches Social (TikTok) and Deal promotion intensity for the category."""
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword: return items

    kw_hash = _hl.md5(main_keyword.encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_EXTERNAL, "external_intensity", kw_hash)
    if cached is not None:
        ctx.cache.update(cached)
        logger.info(f"[cat_monopoly] External intensity L2 cache hit kw_hash={kw_hash}")
        return items

    from src.mcp.servers.social.tiktok.client import TikTokClient
    from src.intelligence.processors.social_virality import SocialViralityProcessor
    try:
        tag_info = await asyncio.to_thread(TikTokClient().get_tag_info, main_keyword.replace(" ", ""))
        if tag_info.get("id"):
            videos = await asyncio.to_thread(TikTokClient().get_hashtag_videos, tag_info["id"], main_keyword.replace(" ", ""), count=20)
            social_analysis = SocialViralityProcessor().calculate_promotion_strength(videos, tag_metadata=tag_info)
            ctx.cache.update({"category_social_psi": social_analysis.get("strength_score", 0), "category_social_verdict": social_analysis.get("verdict", "Unknown")})
        else: ctx.cache.update({"category_social_psi": 0, "category_social_verdict": "No Tag Found"})
    except Exception as e:
        logger.error(f"Error during social intensity analysis: {e}")
        ctx.cache.update({"category_social_psi": 0, "category_social_verdict": "Analysis Failed"})
        
    from src.mcp.servers.market.deals.client import DealHistoryClient
    async def fetch_deal_count(item):
        return len(await DealHistoryClient().get_deal_history(asin=item.get("ASIN", ""), keyword=item.get("Title", ""), max_pages=1))
    try:
        results = await asyncio.gather(*(fetch_deal_count(item) for item in items[:10]))
        total_deals_found = sum(results)
        deal_intensity_score = 9 if total_deals_found > 5 else 6 if total_deals_found > 2 else 3 if total_deals_found > 0 else 0
        ctx.cache["category_deal_intensity"] = deal_intensity_score
    except Exception as e: logger.error(f"Error during deal intensity analysis: {e}")

    _ext = {
        "category_social_psi":     ctx.cache.get("category_social_psi", 0),
        "category_social_verdict": ctx.cache.get("category_social_verdict", "Unknown"),
        "category_deal_intensity": ctx.cache.get("category_deal_intensity", 0),
    }
    _l2_set(ctx, _ext, "external_intensity", kw_hash)
    logger.info(f"External intensity: Social PSI={_ext['category_social_psi']}, Deal Intensity={_ext['category_deal_intensity']}")
    return items

async def _fetch_historical_trends(items: List[dict], ctx: Any) -> List[dict]:
    """
    Fetch 12-month daily BSR/rating trends for Top 20 ASINs.
    Feeds CategoryMonopolyAnalyzer._analyze_market_churn() and ._analyze_seasonality().
    Runs concurrently; failures are soft-skipped so the workflow is never blocked.
    """
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    from datetime import datetime, timedelta

    top_asins = [
        (item.get("ASIN") or item.get("asin"))
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    ]
    if not top_asins:
        return items

    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    country   = ctx.config.get("store_id",  "US")      if hasattr(ctx, "config") else "US"
    tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
    api = XiyouZhaociAPI(tenant_id=tenant_id)
    historical_data: Dict[str, List[Dict[str, Any]]] = {}

    def _parse_daily_records(res: dict, asin: str) -> list:
        """
        Attempt to extract daily records from multiple known response shapes:
          A. res["data"]["entities"][i]["dailyData"]   (list of entities)
          B. res["data"][asin]["dailyData"]            (ASIN-keyed dict)
          C. res["data"]                               (flat list of day dicts)
        Returns a normalised list of {"date", "bsr", "stars", "ratings", "price"}.
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
            for entity in (data["entities"] or []):
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

        logger.debug(f"[historical_trends] Unrecognised response shape for {asin}: keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
        return []

    async def _fetch_one(asin: str) -> None:
        try:
            res = await asyncio.to_thread(api.get_asin_daily_trends, country, asin, start_date, end_date)
            records = _parse_daily_records(res, asin)
            if records:
                historical_data[asin] = records
            else:
                logger.debug(f"[historical_trends] No records parsed for {asin}; top-level keys: {list(res.keys())}")
        except Exception as e:
            logger.warning(f"Historical trend fetch skipped for {asin}: {e}")

    await asyncio.gather(*[_fetch_one(asin) for asin in top_asins])
    ctx.cache["historical_data"] = historical_data
    logger.info(f"Historical trends fetched: {len(historical_data)}/{len(top_asins)} ASINs ({start_date} → {end_date})")
    return items


async def _enrich_batch_traffic_scores(items: List[dict], ctx: Any) -> List[dict]:
    """Fetches batch traffic scores for Top 20 ASINs to calculate average ad dependency."""
    if not items or not ctx.mcp: return items

    top_asins = sorted(
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items[:20] if (item.get("ASIN") or item.get("asin"))
    )
    if not top_asins: return items

    asins_hash = _hl.md5(",".join(top_asins).encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_TRAFFIC, "traffic_scores", asins_hash)
    if cached is not None:
        ctx.cache["actual_bsr_ad_ratio"] = cached["actual_bsr_ad_ratio"]
        logger.info(f"[cat_monopoly] Traffic scores L2 cache hit asins_hash={asins_hash}")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        resp = await ctx.mcp.call_tool_json("xiyou_get_traffic_scores", {"asins": top_asins, "country": country})
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

async def _fetch_keyword_weekly_trends(items: List[dict], ctx: Any) -> List[dict]:
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
        country   = ctx.config.get("store_id",  "US")      if hasattr(ctx, "config") else "US"
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
            logger.warning(
                f"[keyword_weekly_trends] Empty trends data for '{main_keyword}'"
            )
    except Exception as e:
        logger.warning(f"[keyword_weekly_trends] Failed, seasonality will use BSR proxy: {e}")

    return items


async def _fetch_time_series_data(items: List[dict], ctx: Any) -> List[dict]:
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
    end_date   = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    ts_hash = _hl.md5(
        (",".join(top_asins) + main_keyword + start_date).encode()
    ).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_TIMESERIES, "time_series", ts_hash)
    if cached is not None:
        ctx.cache["historical_data"]       = cached.get("historical_data", {})
        ctx.cache["keyword_weekly_trends"]  = cached.get("keyword_weekly_trends")
        logger.info(f"[cat_monopoly] Time series L2 cache hit ts_hash={ts_hash}")
        return items

    await asyncio.gather(
        _fetch_historical_trends(items, ctx),
        _fetch_keyword_weekly_trends(items, ctx),
    )
    _l2_set(ctx, {
        "historical_data":      ctx.cache.get("historical_data", {}),
        "keyword_weekly_trends": ctx.cache.get("keyword_weekly_trends"),
    }, "time_series", ts_hash)
    return items


async def _fetch_sellersprite_bsr(items: List[dict], ctx: Any) -> List[dict]:
    """
    Fetch 4 monthly BSR snapshots from Sellersprite to calculate
    (true list churn rate): T, T-3, T-6, T-12 months.

    Each snapshot stores only the core fields needed for set comparison:
        {"asin", "rank", "brand"}
    Stored in ctx.cache["sellersprite_snapshots"] as Dict[YYYYMM, List[dict]].

    Churn rate = fraction of ASINs in T that were NOT present N months ago.
    Soft-fails: missing auth or API errors do not block the workflow.
    """
    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
    from datetime import datetime
    import re

    url = ctx.config.get("url", "")
    m = re.search(r"/(?:gp/bestsellers|zgbs)/[^/]+/(\d+)", url)
    if not m:
        logger.warning("[sellersprite_bsr] Could not extract node ID from URL; skipping")
        return items

    node_id = m.group(1)
    store_id = ctx.config.get("store_id", "US")
    market_id = {"US": 1, "DE": 6, "JP": 8, "UK": 3, "FR": 4, "IT": 5, "ES": 7, "CA": 2}.get(store_id, 1)

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
            ctx.cache["sellersprite_base_ym"]   = cached.get("base_ym", base_ym)
            logger.info(f"[cat_monopoly] Sellersprite BSR L2 cache hit node={node_id} base_ym={base_ym}")
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
                logger.info(f"[sellersprite_bsr] Resolved node_id={node_id} → {node_id_path} (table={table})")
                break
            logger.debug(f"[sellersprite_bsr] node_id={node_id} not found in table={table}, trying next")

        if not node_id_path:
            logger.warning(f"[sellersprite_bsr] Could not resolve nodeIdPath for node_id={node_id} in any snapshot")
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


async def _run_monopoly_analysis(items: List[dict], ctx: Any) -> List[dict]:
    """Calculates scores and generates flattened niche benchmarks."""
    from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
    from src.intelligence.processors.sales_estimator import SalesEstimator
    import statistics, json
    
    import re as _re

    def _parse_float(raw, default: float = 0.0) -> float:
        """Extract the first decimal number from a messy string (handles ranges, symbols, suffixes)."""
        m = _re.search(r"\d+(?:[.,]\d+)?", str(raw or "").replace(",", "."))
        if not m:
            return default
        try:
            return float(m.group().replace(",", "."))
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
    external_data = {"social_psi": ctx.cache.get("category_social_psi"), "deal_intensity": ctx.cache.get("category_deal_intensity")}
    analysis_input = [
        {
            "rank":           _parse_int(item.get("Rank"), default=999),
            "price":          _parse_float(item.get("Price")),
            "sales":          item.get("sales", 0),
            "brand":          item.get("brand", "Unknown"),
            "seller_type":    item.get("seller_type", "Unknown"),
            "feedback_count": item.get("feedback_count", 0),
            "review_count":   _parse_int(item.get("Reviews")),
            "rating":         _parse_float(item.get("Stars")),
            # Written reviews vs global ratings (from ReviewCountExtractor)
            "global_ratings":  item.get("global_ratings"),
            "written_reviews": item.get("written_reviews"),
            "review_ratio":    item.get("review_ratio"),
        }
        for item in items
    ]
    
    # Combined Ad Data with Multi-Keyword CPC
    detailed_bids = ctx.cache.get("detailed_bid_analysis", {})
    ad_data = {
        "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
        "actual_bsr_ad_ratio": ctx.cache.get("actual_bsr_ad_ratio"),
        "detailed_bids": detailed_bids
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
        "AUTO_FOR_SALES":   detailed_bids.get("AUTO_FOR_SALES", []),
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
    tail_reviews  = [p["review_count"] for p in analysis_input[50:]]
    if len(top10_reviews) >= _MIN_BUCKET and len(tail_reviews) >= _MIN_BUCKET:
        top10_avg = statistics.mean(top10_reviews)
        tail_avg  = statistics.mean(tail_reviews)
        review_disparity_val = round(top10_avg / max(tail_avg, 1), 1)
    else:
        review_disparity_val = None

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
                "n":      len(prices),
                "pct":    f"{len(prices)/len(valid_prices):.0%}",
                "min":    f"${prices[0]:.2f}",
                "median": f"${statistics.median(prices):.2f}",
                "max":    f"${prices[-1]:.2f}",
            }

        price_p10  = _pct_val(valid_prices, 10)
        price_p25  = _pct_val(valid_prices, 25)
        price_p75  = _pct_val(valid_prices, 75)
        price_p90  = _pct_val(valid_prices, 90)
        price_mean = statistics.mean(valid_prices)
        price_min  = valid_prices[0]
        price_max  = valid_prices[-1]

        # Dynamic buckets: ~5-7 equal-width bands spanning min→max, rounded to $5
        _step = max(5, round((price_max - price_min) / 5 / 5) * 5) or 5
        _lo   = int(price_min // _step) * _step
        buckets: list[dict] = []
        b = _lo
        while b < price_max + _step:
            lo_b, hi_b = b, b + _step
            cnt = sum(1 for p in valid_prices if lo_b <= p < hi_b)
            pct = cnt / len(valid_prices) * 100
            buckets.append({"range": f"${lo_b:.0f}–${hi_b:.0f}", "lo": lo_b, "hi": hi_b,
                             "count": cnt, "pct": f"{pct:.0f}%"})
            b += _step

        # ── Bimodal / tier detection ──────────────────────────────────────────
        # A "valley" bucket separates two populated clusters when:
        #   - its count ≤ 5% of total AND both neighbouring regions have ≥ 10% each.
        # Scan interior buckets only (skip first and last).
        _VALLEY_THRESH  = 0.05   # bucket share ≤ 5% = sparse
        _CLUSTER_THRESH = 0.10   # region share ≥ 10% = populated
        n_total_prices  = len(valid_prices)

        tiers: list[dict] = []
        is_bimodal = False
        valley_range: str = ""

        for vi in range(1, len(buckets) - 1):
            vb = buckets[vi]
            if vb["count"] / n_total_prices > _VALLEY_THRESH:
                continue
            left_cnt  = sum(bk["count"] for bk in buckets[:vi])
            right_cnt = sum(bk["count"] for bk in buckets[vi + 1:])
            if (left_cnt / n_total_prices >= _CLUSTER_THRESH and
                    right_cnt / n_total_prices >= _CLUSTER_THRESH):
                # Found a valley — split here
                is_bimodal = True
                valley_range = vb["range"]
                left_prices  = [p for p in valid_prices if p < vb["lo"]]
                right_prices = [p for p in valid_prices if p >= vb["hi"]]
                tiers = [
                    {"label": "Budget tier",  **_tier_stats(left_prices),
                     "range": f"${left_prices[0]:.0f}–${left_prices[-1]:.0f}"},
                    {"label": "Premium tier", **_tier_stats(right_prices),
                     "range": f"${right_prices[0]:.0f}–${right_prices[-1]:.0f}"},
                ]
                break   # first valley is sufficient; deeper splits are edge cases

        price_dist = {
            "n":          n_total_prices,
            "min":        f"${price_min:.2f}",
            "p10":        f"${price_p10:.2f}",
            "p25":        f"${price_p25:.2f}",
            "median":     f"${median_price:.2f}",
            "mean":       f"${price_mean:.2f}",
            "p75":        f"${price_p75:.2f}",
            "p90":        f"${price_p90:.2f}",
            "max":        f"${price_max:.2f}",
            "buckets":    [{k: v for k, v in bk.items() if k not in ("lo", "hi")}
                           for bk in buckets],
            "is_bimodal": is_bimodal,
            "valley_range": valley_range,
            "tiers":      tiers,
        }
    else:
        price_dist = {"n": 0, "buckets": [], "is_bimodal": False, "tiers": []}

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
        late  = statistics.median([p for _, p in pts[-30:]])
        if early > 0:
            price_deltas.append((late - early) / early)
    if price_deltas:
        avg_price_change = statistics.mean(price_deltas)
        price_trend_direction = (
            "deflating" if avg_price_change < -0.05 else
            "inflating" if avg_price_change > 0.05  else
            "stable"
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
    t_snapshot = snapshots.get(base_ym) or (
        snapshots.get(max(snapshots)) if snapshots else []
    )
    if t_snapshot:
        # Cutoff = 12 months before now (conservative: base_ym is already T-2 months)
        cutoff_ms = (_time.time() - 365 * 86400) * 1000
        dated = [p for p in t_snapshot if p.get("available_date_ms")]
        new_entrants = [p for p in dated if p["available_date_ms"] >= cutoff_ms]
        new_entrant_ratio_val = len(new_entrants) / len(t_snapshot) if t_snapshot else 0.0
        new_entrant_str = (
            f"{new_entrant_ratio_val:.0%} ({len(new_entrants)}/{len(t_snapshot)} ASINs listed in last 12 months)"
        )
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
    _RSR_THRESHOLD     = 0.10   # monthly-review-growth / monthly-sales; >10% = suspicious
    # Signal 2 — Rating jump: sustained +0.3★ rise in 30 days is implausible organically.
    _JUMP_STARS        = 0.3    # minimum stars rise over a 30-day window
    _JUMP_WINDOW       = 30     # days
    # Signal 3 — Written/global ratio: natural ≈ 0.10; paid reviewers always leave text.
    _RATIO_THRESHOLD   = 0.50   # written_reviews / global_ratings; >50% = suspicious
    # Combined trigger thresholds (fraction of eligible ASINs flagged):
    _THRESH_HIGH_BASE  = 0.30   # >30% flagged → HIGH (non-seasonal)
    _THRESH_HIGH_SEAS  = 0.40   # >40% flagged → HIGH (seasonal — fewer natural spikes expected)
    _THRESH_MEDIUM     = 0.15   # >15% flagged → MEDIUM

    flagged_rsr = 0
    flagged_jump = 0
    integrity_total = 0
    total_bsr = len(items)

    for asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r.get("stars"), r.get("ratings") or 0)
             for r in records if r.get("date")],
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
    ratio_eligible      = [p for p in analysis_input if p.get("review_ratio") is not None]
    flagged_ratio       = [p for p in ratio_eligible  if p["review_ratio"] > _RATIO_THRESHOLD]
    flagged_ratio_count = len(flagged_ratio)
    ratio_flagged_pct   = len(flagged_ratio) / len(ratio_eligible) if ratio_eligible else 0.0

    seasonality_pattern_for_threshold = result.get("seasonality", {}).get("pattern", "")
    is_seasonal       = "seasonal" in seasonality_pattern_for_threshold
    integrity_threshold = _THRESH_HIGH_SEAS if is_seasonal else _THRESH_HIGH_BASE

    if integrity_total > 0 or ratio_eligible:
        ts_ratio = max(flagged_rsr, flagged_jump) / integrity_total if integrity_total > 0 else 0.0
        # Either time-series signals OR ratio signal can elevate risk independently
        combined_ratio = max(ts_ratio, ratio_flagged_pct)
        integrity_risk = (
            "HIGH"   if combined_ratio >= integrity_threshold else
            "MEDIUM" if combined_ratio >= _THRESH_MEDIUM      else
            "LOW"
        )
        seasonal_note = (
            f" [seasonal category — HIGH threshold raised to {_THRESH_HIGH_SEAS:.0%} to suppress false positives]"
            if is_seasonal else ""
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

    # Extract churn / seasonality / BSR churn signals for prompt template
    churn = result.get("market_churn", {})
    seasonality = result.get("seasonality", {})
    bsr_churn = result.get("bsr_churn", {})
    peak_months_str = (
        ", ".join(str(m) for m in seasonality.get("peak_months", []))
        or "N/A"
    )
    platform_warning = (
        " ⚠️ Peak overlaps platform events (Prime Day/Black Friday)"
        if seasonality.get("platform_event_in_peak")
        else ""
    )

    # ── Data quality coverage (passed to LLM so it can caveat low-coverage claims) ──
    n_total  = len(analysis_input) or 1
    dq_sales    = sum(1 for p in analysis_input if (p.get("sales") or 0) > 0)
    dq_seller   = sum(1 for p in analysis_input if p.get("seller_type") not in (None, "", "Unknown"))
    dq_rating   = sum(1 for p in analysis_input if (p.get("rating") or 0) > 0)
    dq_reviews  = sum(1 for p in analysis_input if (p.get("review_count") or 0) > 0)
    dq_hist     = integrity_total          # ASINs with ≥60-day Xiyouzhaoci history
    dq_ratio    = len(ratio_eligible)      # ASINs with written/global ratio from scrape
    dq_snapshots = len(bsr_churn.get("snapshots_available", []))
    dq_cpc      = bid_entry_count          # number of keyword CPC entries available

    def _pct(n: int) -> str:
        return f"{n}/{n_total} ({n / n_total:.0%})"

    data_quality_str = (
        f"BSR products scraped: {n_total} | "
        f"sales estimate coverage: {_pct(dq_sales)} | "
        f"seller-type coverage: {_pct(dq_seller)} | "
        f"star-rating coverage: {_pct(dq_rating)} | "
        f"review-count coverage: {_pct(dq_reviews)} | "
        f"Xiyouzhaoci ≥60-day history: {dq_hist}/{n_total} ({dq_hist / n_total:.0%}) | "
        f"written/global ratio data: {dq_ratio}/{n_total} ({dq_ratio / n_total:.0%}) | "
        f"Sellersprite BSR snapshots available: {dq_snapshots} months | "
        f"CPC keyword entries: {dq_cpc}"
    )

    # ── Startup capital breakdown ─────────────────────────────────────────────
    # Conservative rule-of-thumb for a new FBA product launch.
    # All constants are auditable here; change them to tune the recommendation.
    _CAP_UNITS     = 1000   # first-batch order quantity (units)
    _CAP_COGS      = 0.30   # COGS as fraction of retail price (China-manufactured)
    _CAP_ACOS      = 0.30   # target ACOS during the ranking phase
    _CAP_FEES      = 0.25   # Amazon platform fees (referral ~15% + FBA ~10%)
    _CAP_PPC_MO    = 3      # months of PPC seed budget
    _CAP_OVERHEAD  = 2000   # fixed launch costs: photography, A+, listing, freight ($)
    _CAP_BUFFER    = 0.20   # working capital buffer on subtotal

    # If bimodal, compute capital against each tier's median separately
    # so the operator can see how the budget changes by tier choice.
    _cap_price = median_price   # overall median — may be in the gap if bimodal
    _cap_inv   = int(_CAP_UNITS * _cap_price * _CAP_COGS)
    _cap_ppc   = int(_CAP_UNITS * _cap_price * _CAP_ACOS * _CAP_PPC_MO)
    _cap_fees  = int(_CAP_UNITS * _cap_price * _CAP_FEES * _CAP_PPC_MO)
    _cap_sub   = _cap_inv + _cap_ppc + _cap_fees + _CAP_OVERHEAD
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
            t_inv  = int(_CAP_UNITS * t_med * _CAP_COGS)
            t_ppc  = int(_CAP_UNITS * t_med * _CAP_ACOS * _CAP_PPC_MO)
            t_fees = int(_CAP_UNITS * t_med * _CAP_FEES * _CAP_PPC_MO)
            t_sub  = t_inv + t_ppc + t_fees + _CAP_OVERHEAD
            _tier_capitals.append({
                "tier":      tier["label"],
                "median":    t_median_str,
                "inventory": f"${t_inv:,}",
                "ppc":       f"${t_ppc:,}",
                "fees":      f"${t_fees:,}",
                "overhead":  f"${_CAP_OVERHEAD:,}",
                "total":     f"${int(t_sub*(1+_CAP_BUFFER)):,}",
            })

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
        raw      = items[i]
        enriched = analysis_input[i]
        title_raw = raw.get("Title") or ""
        title = title_raw[:40].rstrip() + ("…" if len(title_raw) > 40 else "")
        top_asin_rows.append({
            "rank":        enriched["rank"],
            "asin":        (raw.get("ASIN") or raw.get("asin") or "N/A"),
            "brand":       (enriched.get("brand") or "Unknown")[:20],
            "title":       title,
            "price":       f"${enriched['price']:.2f}" if enriched["price"] else "N/A",
            "rating":      f"{enriched['rating']:.1f}★" if enriched["rating"] else "N/A",
            "reviews":     f"{enriched['review_count']:,}" if enriched["review_count"] else "N/A",
            "units_mo":    f"{enriched['sales']:,}" if enriched["sales"] else "N/A",
            "seller_type": enriched.get("seller_type") or "Unknown",
        })

    return [{
        "analysis_result": json.dumps(result, ensure_ascii=False),
        "main_keyword": ctx.cache.get("main_keyword"),
        "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
        "niche_median_price": f"${median_price:.2f}",
        "niche_monthly_units": f"{total_monthly_units:,} units",
        "niche_monthly_gmv": f"${niche_monthly_gmv:,}",
        "bid_insight": json.dumps(bid_raw, ensure_ascii=False) if bid_entry_count else "N/A (no CPC data fetched)",
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
        "recommended_capital":  f"${_cap_total:,}",
        "capital_inventory":    f"${_cap_inv:,}",
        "capital_ppc":          f"${_cap_ppc:,}",
        "capital_fees":         f"${_cap_fees:,}",
        "capital_overhead":     f"${_CAP_OVERHEAD:,}",
        "capital_units":        str(_CAP_UNITS),
        "capital_cogs_pct":     f"{_CAP_COGS:.0%}",
        "capital_acos_pct":     f"{_CAP_ACOS:.0%}",
        "capital_fees_pct":     f"{_CAP_FEES:.0%}",
        "capital_ppc_months":   str(_CAP_PPC_MO),
        "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%",
        "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get("r_squared", 0.95),
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
    }]

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
            trimmed = "\n".join(lines[:run_start + 1])
            logger.warning(
                f"[prepare_report_artifact] Trimmed {run_len - 1} repeated lines "
                f"(pattern: {line[:60]!r})"
            )
            return trimmed.rstrip() + "\n\n*（报告在此截断：模型输出已达上限，重复内容已删除）*\n"
        # No long run ending at i — move up past this line
        i = run_start - 1

    return text


async def _prepare_report_artifact(items: List[dict], ctx: Any) -> List[dict]:
    """Saves the report to a local Markdown file, stripping trailing repetition."""
    if not items or "deliver_report" not in items[0]:
        return items
    report_data = items[0]["deliver_report"]
    report_text = (
        report_data.text if hasattr(report_data, "text")
        else report_data.get("text") if isinstance(report_data, dict)
        else str(report_data)
    )
    if not report_text or report_text == "None":
        return items

    # Strip LLM degeneration artifacts before persisting
    report_text = _trim_repetition(report_text)

    import os
    from datetime import datetime
    import re as _re
    raw_kw = str(ctx.cache.get("main_keyword", "niche"))
    keyword = _re.sub(r"[^\w]", "_", raw_kw, flags=_re.ASCII)[:40].strip("_") or "niche"
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
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
    
    # Dynamically assemble the SSOT instructions
    base_instructions = prompt_manager.assemble_report_instructions(
        role_id="senior_strategist",
        framework_ids=["psi_benchmarking", "strategic_analysis"]
    )

    return Workflow(name="category_monopoly_analysis", steps=[
        ProcessStep(name="fetch_bsr_top_100", fn=_fetch_bsr_list),
        ProcessStep(name="enrich_sales_data", fn=_enrich_sales),
        EnrichStep(name="enrich_seller_background", extractor_fn=_enrich_seller_info, parallel=True, concurrency=5),
        ProcessStep(name="fetch_core_keywords", fn=_fetch_core_keywords),
        ProcessStep(name="fetch_market_signals", fn=_fetch_market_signals),
        ProcessStep(name="enrich_external_intensity", fn=_enrich_external_intensity),
        ProcessStep(name="enrich_batch_traffic_scores", fn=_enrich_batch_traffic_scores),
        ProcessStep(name="fetch_time_series_data", fn=_fetch_time_series_data),
        ProcessStep(name="fetch_sellersprite_bsr", fn=_fetch_sellersprite_bsr),
        ProcessStep(name="calculate_monopoly_score", fn=_run_monopoly_analysis),
        ProcessStep(
            name="deliver_report",
            prompt_template=(
                f"{base_instructions}\n\n"
                "### DATA QUALITY\n"
                "{data_quality}\n"
                "MANDATORY CAVEAT RULE: for any metric whose underlying data source has "
                "coverage < 50% of BSR products, you MUST prefix the claim with "
                "'(limited data — N/M ASINs)' and treat it as directional only, not a "
                "precise benchmark. Specifically:\n"
                "- If Xiyouzhaoci ≥60-day history < 50%: prefix price_trend, integrity signals 1 & 2, "
                "and churn_pattern with '(limited history — N/M ASINs)'. "
                "EXCEPTION: if seasonality_source == 'keyword_weekly_trends', do NOT apply this caveat "
                "to seasonality — keyword ABA data is independent of Xiyouzhaoci BSR history.\n"
                "- If sales estimate coverage < 50%: prefix recommended_capital and any "
                "sales-derived figure with '(estimated from thin data)'\n"
                "- If CPC keyword entries = 0: write 'CPC data unavailable — bid barrier "
                "analysis skipped' instead of citing bid_insight\n"
                "- If Sellersprite BSR snapshots < 2: treat bsr_churn signals as "
                "insufficient and write 'BSR metabolism data unavailable'\n"
                "NEVER present a default value (e.g. 0, Unknown, N/A) as a confirmed fact.\n\n"
                "### TASK-SPECIFIC CONTEXT\n"
                "Advising on a **{recommended_capital}** minimum launch budget "
                "({capital_units} units × {niche_median_price} median price).\n"
                "CAPITAL TABLE RULE: In the Data Breakdown table, Investment Capital MUST appear as "
                "FIVE consecutive rows — never collapsed into one:\n"
                "  Row 1: Metric='Investment Capital (Total)' | Value='{recommended_capital}' | "
                "Definition='Subtotal × 1.20 working-capital buffer'\n"
                "  Row 2: Metric='  └ Inventory ({capital_units} units, COGS {capital_cogs_pct})' | "
                "Value='{capital_inventory}' | Definition='{capital_units} units × {niche_median_price} × {capital_cogs_pct} COGS'\n"
                "  Row 3: Metric='  └ PPC Seed ({capital_ppc_months}mo @ {capital_acos_pct} ACOS)' | "
                "Value='{capital_ppc}' | Definition='Ranking-phase ad spend estimate'\n"
                "  Row 4: Metric='  └ Platform Fees ({capital_ppc_months}mo @ {capital_fees_pct})' | "
                "Value='{capital_fees}' | Definition='Amazon referral ~15% + FBA ~10%'\n"
                "  Row 5: Metric='  └ Fixed Overhead' | Value='{capital_overhead}' | "
                "Definition='Photography, A+ content, listing, inbound freight'\n"
                "NEVER show a single Investment Capital row without these four sub-rows.\n"
                "BIMODAL CAPITAL RULE: tier_capitals={tier_capitals}. "
                "If tier_capitals is non-empty (bimodal market detected), append a second "
                "capital table immediately after the breakdown rows under the sub-heading "
                "'### Capital by Price Tier' with columns: "
                "Tier | Tier Median | Inventory | PPC | Fees | Overhead | Total. "
                "Warn the operator: 'The base Investment Capital above uses the overall median "
                "price which falls in the gap between tiers — choose the tier you plan to enter "
                "and use the corresponding Total as your actual budget.'\n"
                "Primary Niche: **{main_keyword}** | Related Terms: {core_keywords}\n"
                "Data Confidence (R²): **{data_confidence_r2}**\n\n"
                "### DYNAMIC BENCHMARKS\n"
                "- Median Price: {niche_median_price}\n"
                "- Est. Niche Monthly Unit Sales: {niche_monthly_units} "
                "(scraped from Amazon 'X+ bought in past month' badge — unit count, NOT revenue)\n"
                "- Est. Niche Monthly GMV: {niche_monthly_gmv} "
                "(= unit_sales × median_price; use as market-size proxy)\n"
                "UNIT/REVENUE RULE: '{niche_monthly_units}' is a unit COUNT — NEVER prefix it with $. "
                "'{niche_monthly_gmv}' is revenue — always use the $ prefix. "
                "In the Data Breakdown table, report BOTH as separate rows: "
                "'Total Est. Monthly Unit Sales' (integer, e.g. 40,694 units) "
                "and 'Est. Niche Monthly GMV' (dollar figure, e.g. $702,400). "
                "NEVER merge them into a single 'Total Est. Monthly Sales $...' row.\n"
                "- Keyword Bid Recommendations (raw Amazon Ads API response): {bid_insight}\n"
                "  Structure: {{LEGACY_FOR_SALES: [...], AUTO_FOR_SALES: [...]}}. "
                "Each entry contains targetingExpression (keyword + matchType) and suggestedBid (amount, "
                "and possibly rangeStart/rangeEnd). Render this verbatim as a markdown table in the report "
                "with columns: Keyword | Match Type | Manual Bid (LEGACY) | Auto Bid (AUTO). "
                "LEGACY_FOR_SALES = flat manual bid cost per click. "
                "AUTO_FOR_SALES = Amazon's dynamic strategy estimate (usually higher — reflects "
                "platform-assessed competition). A large LEGACY→AUTO gap means the platform thinks "
                "this keyword is highly contested and automated bidding will aggressively overpay.\n"
                "- Review Disparity: {review_disparity}\n"
                "- Typical Industry CR3: {industry_typical_cr3}\n"
                "- Social PSI: {social_psi} ({social_verdict})\n"
                "- Deal Intensity: {deal_intensity}/10\n\n"
                "### MARKET HEALTH SIGNALS\n"
                "- Price Trend: {price_trend}\n"
                "- Rating Churn Pattern: {churn_pattern} (score: {churn_score})\n"
                "- New Product Flood Ratio: {new_product_ratio} | Rating Collapse Rate: {collapse_rate}\n"
                "- BSR Listing Metabolism: **{bsr_churn_label}** | Snapshots: {bsr_snapshots}\n"
                "  - 3-Month Churn: {bsr_churn_3m} | 6-Month Churn: {bsr_churn_6m} | 12-Month Churn: {bsr_churn_12m}\n"
                "- New Entrant Ratio (Sellersprite T-snapshot): {new_entrant_ratio}\n"
                "- Seasonality: {seasonality_pattern} (score: {seasonality_score}, "
                "source: {seasonality_source}, n={seasonality_n_points} data points) | Peak Months: {peak_months}\n"
                "  Source legend: 'keyword_weekly_trends' = ABA weekly search-volume (direct consumer intent, "
                "independent of BSR history); 'bsr_daily_trends' = Xiyouzhaoci BSR proxy (indirect — "
                "reflects competitive rank, not raw demand). Prefer keyword source when available.\n"
                "- Integrity Alert: {integrity_alert}\n"
                "  Coverage — Signal 1+2 (rating history): {integrity_hist_cov}\n"
                "  Coverage — Signal 3 (written/global ratio): {integrity_ratio_cov}\n"
                "INTEGRITY TABLE RULE: In the Data Breakdown table, the Integrity Alert block MUST "
                "occupy THREE consecutive rows (never collapse into one):\n"
                "  Row 1: Metric='Integrity Alert' | Value=LOW/MEDIUM/HIGH/unknown | "
                "Definition=trigger ratio vs threshold (e.g. 'RSR 0/8, ratio 0/15, threshold >30%')\n"
                "  Row 2: Metric='Integrity: Rating History Coverage' | Value='{integrity_hist_cov}' | "
                "Definition=ASINs with ≥60-day Xiyouzhaoci history; basis for Signal 1 RSR + Signal 2 rating-jump\n"
                "  Row 3: Metric='Integrity: Review Ratio Coverage' | Value='{integrity_ratio_cov}' | "
                "Definition=ASINs with written/global ratio data; basis for Signal 3\n"
                "If either coverage is below 20%, append '(low coverage — treat as directional only)' "
                "to the Integrity Alert value cell.\n\n"
                "### TOP ASIN EVIDENCE TABLE\n"
                "{top_asin_table}\n"
                "Render the above JSON as a markdown table with these exact columns "
                "(in this order): Rank | ASIN | Brand | Title | Price | Rating | Reviews | "
                "Units/Mo | Seller Type. "
                "Title: truncate at 40 chars with '…'. "
                "Place this table immediately after the Data Breakdown section under the heading "
                "'## Top 10 BSR Products (Evidence)'. "
                "Below the table add one line: "
                "'*Source: Amazon BSR page scrape + PastMonthSales badge. "
                "Units/Mo = Amazon-displayed past-month purchase count.*'\n\n"
                "### PRICE DISTRIBUTION\n"
                "{price_distribution}\n"
                "Render this JSON under the heading '## Price Distribution' immediately after "
                "the Top 10 BSR Products section. Always include:\n"
                "  1. A one-row percentile summary table: "
                "Min | P10 | P25 | Median | Mean | P75 | P90 | Max\n"
                "  2. A bucket frequency table: Price Range | # Products | Share\n"
                "BIMODAL RULE: if 'is_bimodal' is true, the overall median is NOT "
                "representative — it falls in the gap between two clusters. You MUST:\n"
                "  a) Add a '⚠️ Bimodal distribution detected' alert above the tables, "
                "citing the valley range from 'valley_range'.\n"
                "  b) Render the 'tiers' array as a third table: "
                "Tier | Price Range | # Products | Share | Tier Median\n"
                "  c) State which tier the recommended_capital targets (it is always based on "
                "the overall median, which may be misleading — flag this explicitly).\n"
                "  d) In the Strategic Insights section, analyse each tier separately: "
                "barrier, competitive intensity, and positioning advice differ by tier.\n"
                "If 'is_bimodal' is false, add one sentence on whether the distribution is "
                "skewed low, skewed high, or balanced, and the pricing implication.\n\n"
                "### DATA: {analysis_result}\n\n"
                "### ADDITIONAL TACTICAL RULES\n"
                "- 400-550 words. No filler.\n"
                "- ANALYZE BID BARRIERS: Compare the suggested CPC to the median price. If CPC > 10% of median price, highlight extreme capital risk.\n"
                "- Identify which specific keywords are 'High Barrier' and if PHRASE/EXACT gaps offer opportunities.\n"
                "- CHURN ALERT: If churn_pattern is 'predatory_competition' or 'lemon_market', dedicate a paragraph to survival risk and whether the category is worth entering at all.\n"
                "- BSR METABOLISM ALERT: If bsr_churn_label is 'fomo_spike_die', warn that this is a trend-driven category where products spike and die — advise on quick-in/quick-out strategy or avoidance. If 'blue_ocean', highlight that new entrants CAN sustain rankings and explain why. If 'mature_stable', note the high survivor_sales_share and advise on differentiation required to displace incumbents.\n"
                "- SEASONALITY ALERT: If seasonality_pattern is 'strong_seasonal' or 'multi_peak_seasonal', advise on launch timing relative to peak_months and warn about off-season inventory risk.\n"
                "- PRICE TREND ALERT: If price_trend is 'deflating', warn that this category is in a price war — margin compression is accelerating and late entrants face structural disadvantage. If 'inflating', note that premium positioning may be viable.\n"
                "- NEW ENTRANT ALERT: If new_entrant_ratio > 40%, the category has low moat — many new products are breaking into the top-100, which means low ranking stickiness but also means a new entrant has a realistic shot. If < 15%, incumbents dominate and new entries rarely survive in the ranking.\n"
                "- INTEGRITY ALERT: If integrity_alert is 'HIGH', explicitly warn that this category has a severely compromised compliance environment — widespread fake review injection or coordinated negative review attacks are likely. New entrants should budget for additional compliance overhead, and the true review barrier is likely higher than the raw data suggests. Combine with review_disparity to assess whether high-review incumbents are defensible or artificially inflated.\n"
                "- PORTER'S FIVE FORCES — DIRECTION RULE (CRITICAL): "
                "In Porter's Five Forces, the 'Threat' rating refers to how easy it is for a new player "
                "or substitute to enter and succeed — NOT how hard the barriers are. "
                "The direction is always the INVERSE of barrier height. "
                "MANDATORY mappings from this report's data:\n"
                "  • Threat of New Entrants: LOW when new_entrant_ratio < 15% OR avg_reviews_top10 > 5,000 "
                "OR review_disparity > 5x — high barriers mean low threat. "
                "HIGH only when new_entrant_ratio > 40% AND review barrier is low.\n"
                "  • Bargaining Power of Buyers: HIGH when deal_intensity > 7 OR price compression is severe "
                "(many sellers forced to discount). LOW when buyers have few alternatives.\n"
                "  • Threat of Substitutes: HIGH when related keywords dominate search volume; "
                "LOW when the product category is specific and has no close alternatives.\n"
                "  • Rivalry: HIGH when CR3 < 30% (fragmented competition, many rivals) OR churn is high. "
                "LOW when a few brands dominate and competition is stable.\n"
                "  • Supplier Power: LOW for commodity inputs (most physical goods); "
                "HIGH only when key raw materials are scarce or proprietary.\n"
                "FORBIDDEN: do NOT write 'Threat of New Entrants: High' and then explain high review barriers "
                "or low new_entrant_ratio — those data points map to LOW threat. "
                "If you catch yourself writing this, flip the label to match the evidence."
            ),
            compute_target=ComputeTarget.CLOUD_LLM
        ),
        ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact)
    ])
