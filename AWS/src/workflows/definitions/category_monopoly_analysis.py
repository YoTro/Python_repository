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

_TTL_BSR       = 3_600    # 1  h — BSR scrape
_TTL_SALES     = 86_400   # 24 h — past-month sales
_TTL_SELLER    = 21_600   # 6  h — seller/fulfillment info
_TTL_SIGNALS   = 3_600    # 1  h — ABA + SERP + CPC market signals
_TTL_TIMESERIES = 86_400  # 24 h — 12-month historical trends + keyword weekly
_TTL_SS_BSR    = 86_400   # 24 h — Sellersprite monthly snapshots


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
            raw_text = res.text.strip().replace('"', '').replace("'", "").lower()
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
    ctx.cache["main_keyword"] = core_keywords[0]
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
        try:
            aba_res = await asyncio.to_thread(
                XiyouZhaociAPI().get_aba_top_asins, "US", [main_keyword]
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
            match_types = ["EXACT", "PHRASE"]
            strategies = ["AUTO_FOR_SALES", "LEGACY_FOR_SALES"]
            bid_res = await ads_client.get_keyword_bid_recommendations(
                keywords=[{"keyword": kw, "matchType": m} for kw in core_keywords for m in match_types],
                asins=[(item.get("ASIN") or item.get("asin")) for item in items[:5]
                       if (item.get("ASIN") or item.get("asin"))],
                strategy=strategies,
            )
            ctx.cache["detailed_bid_analysis"] = {
                s: bid_res.get(s, {}).get("bidRecommendations", [])
                for s in strategies
            }
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
    logger.info(f"External intensity: Social PSI={ctx.cache.get('category_social_psi', 'N/A')}, Deal Intensity={ctx.cache.get('category_deal_intensity', 'N/A')}")
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
    api = XiyouZhaociAPI()
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
            daily = asin_data.get("dailyData") or asin_data if isinstance(asin_data, list) else []
            return _normalise(daily)

        # Shape C: {"data": [{"date": ..., "bsr": ...}]}
        if isinstance(data, list):
            return _normalise(data)

        logger.debug(f"[historical_trends] Unrecognised response shape for {asin}: keys={list(data.keys()) if isinstance(data, dict) else type(data)}")
        return []

    async def _fetch_one(asin: str) -> None:
        try:
            res = await asyncio.to_thread(api.get_asin_daily_trends, "US", asin, start_date, end_date)
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
    
    top_asins = [(item.get("ASIN") or item.get("asin")) for item in items[:20] if (item.get("ASIN") or item.get("asin"))]
    if not top_asins: return items
    
    try:
        resp = await ctx.mcp.call_tool_json("xiyou_get_traffic_scores", {"asins": top_asins, "country": "US"})
        if isinstance(resp, list) and len(resp) > 0:
            import json
            data = json.loads(resp[0].get("text", "{}"))
            if data.get("success") and data.get("data"):
                ratios = [d.get("advertisingTrafficScoreRatio", 0.0) for d in data["data"]]
                if ratios:
                    import statistics
                    avg_ratio = statistics.mean(ratios)
                    ctx.cache["actual_bsr_ad_ratio"] = avg_ratio
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
        api = XiyouZhaociAPI()
        res = await asyncio.to_thread(
            api.get_search_term_trends,
            country="US",
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
    
    analyzer = CategoryMonopolyAnalyzer()
    external_data = {"social_psi": ctx.cache.get("category_social_psi"), "deal_intensity": ctx.cache.get("category_deal_intensity")}
    analysis_input = [
        {
            "rank":           item.get("Rank", 999),
            "price":          float(str(item.get("Price") or "0").replace("$", "").replace(",", "")),
            "sales":          item.get("sales", 0),
            "brand":          item.get("brand", "Unknown"),
            "seller_type":    item.get("seller_type", "Unknown"),
            "feedback_count": item.get("feedback_count", 0),
            "review_count":   int(str(item.get("Reviews") or "0").replace(",", "")),
            "rating":         float(str(item.get("Rating") or "0").split(" ")[0]),
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

    # Format Bid Insight for LLM
    bid_insight = []
    legacy_recs = detailed_bids.get("LEGACY_FOR_SALES", [])
    for rec in legacy_recs:
        for expr in rec.get("bidRecommendationsForTargetingExpressions", []):
            kw = expr.get("targetingExpression", {}).get("value")
            m_type = expr.get("targetingExpression", {}).get("type")
            bid = expr.get("suggestedBid", {}).get("amount", 0)
            if bid > 0:
                bid_insight.append(f"{kw}({m_type}): ${bid:.2f}")

    prices = [p["price"] for p in analysis_input if p["price"] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    estimator = SalesEstimator()
    node_id = ctx.config.get("category_node_id")
    baseline = estimator.category_params.get(str(node_id), {}).get("market_logic", {})

    # ── review_disparity: top-10 avg / bottom-tail avg ────────────────────
    # Guard: analysis_input[50:] is empty when len == 50, making max(1, mean([]))
    # return 1 and the ratio collapses to top-10 avg — meaningless.
    top10_reviews = [p["review_count"] for p in analysis_input[:10]]
    tail_reviews  = [p["review_count"] for p in analysis_input[50:]]
    top10_avg  = statistics.mean(top10_reviews) if top10_reviews else 0
    tail_avg   = statistics.mean(tail_reviews)  if tail_reviews  else 1
    review_disparity_val = round(top10_avg / max(tail_avg, 1), 1)

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
    flagged_rsr = 0
    flagged_jump = 0
    integrity_total = 0

    for asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r.get("stars"), r.get("ratings") or 0)
             for r in records if r.get("date")],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        integrity_total += 1

        # Signal 1: RSR
        monthly_sales = sales_map.get(asin.upper(), 0)
        if monthly_sales > 0:
            review_delta = pts[-1][2] - pts[0][2]
            months_spanned = max(len(pts) / 30, 1)
            rsr = (review_delta / months_spanned) / monthly_sales
            if rsr > 0.10:
                flagged_rsr += 1

        # Signal 2: rating jump
        stars_series = [s for _, s, _ in pts if s]
        for i in range(30, len(stars_series)):
            if stars_series[i] - stars_series[i - 30] > 0.3:
                flagged_jump += 1
                break

    # Signal 3: written/global ratio (per-product, from ReviewCountExtractor)
    RATIO_THRESHOLD = 0.50
    ratio_eligible  = [p for p in analysis_input if p.get("review_ratio") is not None]
    flagged_ratio   = [p for p in ratio_eligible  if p["review_ratio"] > RATIO_THRESHOLD]
    flagged_ratio_count = len(flagged_ratio)
    ratio_flagged_pct   = len(flagged_ratio) / len(ratio_eligible) if ratio_eligible else 0.0

    seasonality_pattern_for_threshold = result.get("seasonality", {}).get("pattern", "")
    integrity_threshold = 0.40 if "seasonal" in seasonality_pattern_for_threshold else 0.30

    if integrity_total > 0 or ratio_eligible:
        ts_ratio = max(flagged_rsr, flagged_jump) / integrity_total if integrity_total > 0 else 0.0
        # Either time-series signals OR ratio signal can elevate risk independently
        combined_ratio = max(ts_ratio, ratio_flagged_pct)
        integrity_risk = (
            "HIGH"   if combined_ratio >= integrity_threshold else
            "MEDIUM" if combined_ratio >= 0.15               else
            "LOW"
        )
        integrity_str = (
            f"{integrity_risk} — "
            f"time-series: {ts_ratio:.0%} flagged (RSR={flagged_rsr}, jump={flagged_jump}, n={integrity_total}); "
            f"written/global ratio > {RATIO_THRESHOLD:.0%}: {ratio_flagged_pct:.0%} of products "
            f"({flagged_ratio_count}/{len(ratio_eligible)} with data, threshold={integrity_threshold:.0%})"
        )
    else:
        integrity_risk = "unknown"
        integrity_str = "unknown (insufficient data)"

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

    return [{
        "analysis_result": json.dumps(result, ensure_ascii=False),
        "main_keyword": ctx.cache.get("main_keyword"),
        "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
        "niche_median_price": f"${median_price:.2f}",
        "bid_insight": " | ".join(bid_insight[:10]),
        "review_disparity": f"{review_disparity_val}x",
        "price_trend": price_trend_str,
        "new_entrant_ratio": new_entrant_str,
        "integrity_alert": integrity_str,
        "recommended_capital": f"${int(median_price * 2500):,}",
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
        "peak_months": peak_months_str + platform_warning,
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

    import os, tempfile
    from datetime import datetime
    import re as _re
    raw_kw = str(ctx.cache.get("main_keyword", "niche"))
    keyword = _re.sub(r"[^\w]", "_", raw_kw, flags=_re.ASCII)[:40].strip("_") or "niche"
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
    file_path = os.path.normpath(os.path.join(tempfile.gettempdir(), filename))
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
                "### TASK-SPECIFIC CONTEXT\n"
                "Advising on a **{recommended_capital}** investment.\n"
                "Primary Niche: **{main_keyword}** | Related Terms: {core_keywords}\n"
                "Data Confidence (R²): **{data_confidence_r2}**\n\n"
                "### DYNAMIC BENCHMARKS\n"
                "- Median Price: {niche_median_price}\n"
                "- Detailed CPC Insight: {bid_insight}\n"
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
                "- Seasonality: {seasonality_pattern} (score: {seasonality_score}, source: {seasonality_source}) | Peak Months: {peak_months}\n"
                "- Integrity Alert: {integrity_alert}\n\n"
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
                "- INTEGRITY ALERT: If integrity_alert is 'HIGH', explicitly warn that this category has a severely compromised compliance environment — widespread fake review injection or coordinated negative review attacks are likely. New entrants should budget for additional compliance overhead, and the true review barrier is likely higher than the raw data suggests. Combine with review_disparity to assess whether high-review incumbents are defensible or artificially inflated."
            ),
            compute_target=ComputeTarget.CLOUD_LLM
        ),
        ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact)
    ])
