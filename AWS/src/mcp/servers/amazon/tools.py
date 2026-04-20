from __future__ import annotations
import os
import json
import logging
import asyncio
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
from src.mcp.servers.amazon.extractors.product_details import ProductDetailsExtractor
from src.mcp.servers.amazon.extractors.search import SearchExtractor
from src.mcp.servers.amazon.extractors.ranks import RanksExtractor
from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor
from src.mcp.servers.amazon.extractors.cart_stock import CartStockExtractor
from src.mcp.servers.amazon.extractors.keywords_rank import KeywordsRankExtractor
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
from src.mcp.servers.amazon.extractors.dimensions import DimensionsExtractor
from src.mcp.servers.amazon.extractors.images import ImageExtractor
from src.mcp.servers.amazon.extractors.videos import VideoExtractor
from src.mcp.servers.amazon.extractors.products_num import ProductsNumExtractor
from src.mcp.servers.amazon.extractors.search_result_asins import AsinExtractor
from src.mcp.servers.amazon.extractors.profitability_search import ProfitabilitySearchExtractor
from src.mcp.servers.amazon.ads.client import AmazonAdsClient
from src.mcp.servers.amazon.sp_api.client import SPAPIClient
from src.core.utils.cookie_helper import AmazonCookieHelper
from src.mcp.servers.amazon.extractors.bsr_category_extractor import BSRCategoryExtractor
from src.core.data_cache import data_cache
from src.intelligence.processors import ReviewSummarizer, SalesEstimator
from src.intelligence.providers.factory import ProviderFactory
from src.core.models.review import Review
from src.core.models.product import Product

logger = logging.getLogger("mcp-amazon")

BSR_URL_FILE = os.path.join(os.path.dirname(__file__), "amazon_bsr_url.json")


def _to_serializable(data):
    """Convert Pydantic models, dataclasses, or plain dicts to JSON-safe structures."""
    if hasattr(data, "model_dump"):
        return data.model_dump()
    if hasattr(data, "__dataclass_fields__"):
        from dataclasses import asdict
        return asdict(data)
    if isinstance(data, list):
        return [_to_serializable(item) for item in data]
    return data


def _json_response(data) -> list[TextContent]:
    """Serialize data to JSON TextContent."""
    return [TextContent(type="text", text=json.dumps(_to_serializable(data), indent=2, ensure_ascii=False, default=str))]


async def handle_amazon_tool(name: str, arguments: dict) -> list[TextContent]:
    """Dispatcher for atomic Amazon tools."""

    # ── Session ──────────────────────────────────────────────────────────
    if name == "refresh_amazon_cookies":
        headless = arguments.get("headless", False)
        wait_for_login = arguments.get("wait_for_login", True)
        helper = AmazonCookieHelper(headless=headless)
        cookies = await asyncio.to_thread(helper.fetch_fresh_cookies, wait_for_manual=wait_for_login)
        if cookies and "session-id" in cookies:
            return [TextContent(type="text", text="Successfully refreshed Amazon cookies. Session captured.")]
        return [TextContent(type="text", text="Failed to capture Amazon cookies. Please ensure you logged in if requested.")]

    # ── Tier 1: Core screening ───────────────────────────────────────────
    if name == "get_amazon_bestsellers":
        url = arguments.get("url")
        max_pages = arguments.get("max_pages", 2)
        extractor = BestSellersExtractor()
        products = await extractor.get_bestsellers(url, max_pages=max_pages)
        for p in products:
            if isinstance(p, dict) and "asin" in p:
                data_cache.set("amazon", p["asin"], p)
        return _json_response(products)

    if name == "get_product_details":
        extractor = ProductDetailsExtractor()
        product = await extractor.get_product_details(arguments["asin"])
        if product and product.get("asin"):
            data_cache.set("amazon", product["asin"].upper(), product)
        return _json_response(product)

    if name == "search_products":
        extractor = SearchExtractor()
        products = await extractor.search(
            arguments["keyword"],
            page=arguments.get("page", 1),
        )
        if isinstance(products, list):
            for p in products:
                if isinstance(p, dict) and p.get("asin"):
                    data_cache.set("amazon", p["asin"].upper(), p)
        return _json_response(products)

    if name == "search_profitability_products":
        extractor = ProfitabilitySearchExtractor()
        results = await extractor.search_products(
            arguments["keyword"],
            page_offset=arguments.get("page_offset", 1),
        )
        if isinstance(results, list):
            for p in results:
                if isinstance(p, dict) and p.get("asin"):
                    data_cache.set("amazon", p["asin"].upper(), p)
        return _json_response(results)

    if name == "get_bsr_rank":
        extractor = RanksExtractor()
        asin = arguments["asin"].upper()
        result = await extractor.get_product_ranks(
            asin,
            host=arguments.get("host", "https://www.amazon.com"),
        )
        if result and result.get("PrimaryRank"):
            cached = data_cache.get("amazon", asin) or {}
            cached.update({
                "bsr":              result["PrimaryRank"],
                "category":         result.get("Category"),
                "top_level_node_id": result.get("TopLevelNodeId"),
                "leaf_node_id":     result.get("LeafNodeId"),
            })
            data_cache.set("amazon", asin, cached)
        return _json_response(result)

    if name == "get_batch_past_month_sales":
        extractor = PastMonthSalesExtractor()
        asins = arguments["asins"]
        if isinstance(asins, str):
            asins = [asins]
        result = await extractor.get_batch_past_month_sales(asins)
        if isinstance(result, dict):
            for asin_key, sales_val in result.items():
                if sales_val is not None:
                    cached = data_cache.get("amazon", asin_key.upper()) or {}
                    cached["past_month_sales"] = sales_val
                    data_cache.set("amazon", asin_key.upper(), cached)
        return _json_response(result)

    # ── Tier 2: Competitive analysis ─────────────────────────────────────
    if name == "get_review_count":
        extractor = ReviewRatioExtractor()
        asin = arguments["asin"].upper()
        result = await extractor.get_review_count(
            asin,
            host=arguments.get("host", "https://www.amazon.com"),
        )
        if isinstance(result, dict) and result.get("GlobalRatings") is not None:
            cached = data_cache.get("amazon", asin) or {}
            cached.update({
                "global_ratings":  result.get("GlobalRatings"),
                "written_reviews": result.get("WrittenReviews"),
                "review_ratio":    result.get("Ratio"),
            })
            data_cache.set("amazon", asin, cached)
        return _json_response(result)

    if name == "get_stock_estimate":
        extractor = CartStockExtractor()
        asin = arguments["asin"].upper()
        result = await extractor.get_stock(
            asin,
            host=arguments.get("host", "https://www.amazon.com"),
        )
        if isinstance(result, dict) and result.get("stock") is not None:
            cached = data_cache.get("amazon", asin) or {}
            cached["stock_estimate"] = result["stock"]
            data_cache.set("amazon", asin, cached)
        return _json_response(result)

    if name == "get_keyword_rank":
        extractor = KeywordsRankExtractor()
        result = await extractor.get_asin_ranks_for_keyword(
            arguments["keyword"],
            target_asins=arguments["target_asins"],
            max_pages=arguments.get("max_pages", 3),
        )
        return _json_response(result)

    if name == "search_return_asins":
        extractor = AsinExtractor()
        result = await extractor.get_asins(
            arguments["keyword"],
            page=arguments.get("page", 1),
        )
        return _json_response(result)

    # ── Tier 3: Detail enrichment ────────────────────────────────────────
    if name == "get_reviews":
        asin = arguments["asin"]
        extractor = CommentsExtractor()
        reviews = await extractor.get_all_comments(
            asin,
            max_pages=arguments.get("max_pages", 2),
        )
        if reviews:
            data_cache.set("amazon", f"reviews:{asin}", reviews)
        return _json_response(reviews)

    if name == "analyze_reviews":
        asin = arguments["asin"]
        reviews = data_cache.get_model("amazon", f"reviews:{asin}", Review, ttl_seconds=86400)
        if not reviews:
            extractor = CommentsExtractor()
            raw = await extractor.get_all_comments(
                asin,
                max_pages=arguments.get("max_pages", 3),
            )
            if raw:
                data_cache.set("amazon", f"reviews:{asin}", raw)
            reviews = raw or []
        if not reviews:
            return [TextContent(type="text", text="No reviews found for this ASIN.")]
        try:
            summarizer = ReviewSummarizer(provider=ProviderFactory.get_provider())
            summary = await summarizer.summarize(
                reviews,
                competitive_benchmark=arguments.get("competitive_benchmark", 500),
                est_monthly_sales=arguments.get("est_monthly_sales", 0),
            )
            return _json_response(summary)
        except Exception as e:
            logger.error(f"analyze_reviews failed for {asin}: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Review analysis failed: {type(e).__name__}: {e}")]


    if name == "get_fulfillment":
        extractor = FulfillmentExtractor()
        asin = arguments["asin"].upper()
        result = await extractor.get_fulfillment_info(
            asin,
            host=arguments.get("host", "https://www.amazon.com"),
        )
        if isinstance(result, dict) and result.get("fulfillment_type"):
            cached = data_cache.get("amazon", asin) or {}
            cached["fulfillment_type"] = result["fulfillment_type"]
            data_cache.set("amazon", asin, cached)
        return _json_response(result)

    if name == "get_seller_feedback":
        extractor = SellerFeedbackExtractor()
        result = await extractor.get_seller_feedback_count(
            arguments["seller_id"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    if name == "get_seller_product_count":
        extractor = ProductsNumExtractor()
        result = await extractor.get_seller_and_products_count(arguments["url"])
        return _json_response(result)

    if name == "get_dimensions":
        extractor = DimensionsExtractor()
        asin = arguments["asin"].upper()
        result = await extractor.get_dimensions_and_price(
            asin,
            host=arguments.get("host", "https://www.amazon.com"),
        )
        if isinstance(result, dict):
            cached = data_cache.get("amazon", asin) or {}
            for field in ("weight_lb", "weight", "dimensions", "price", "length_in", "width_in", "height_in"):
                if result.get(field) is not None:
                    cached[field] = result[field]
            if cached:
                data_cache.set("amazon", asin, cached)
        return _json_response(result)

    if name == "get_product_images":
        extractor = ImageExtractor()
        result = await extractor.get_product_images(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    if name == "check_has_videos":
        extractor = VideoExtractor()
        result = await extractor.has_videos(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    # ── Tier 4: BSR Navigation ───────────────────────────────────────────
    if name == "get_top_bsr_categories":
        try:
            with open(BSR_URL_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            return _json_response(data.get("categories", []))
        except Exception as e:
            logger.error(f"Error reading BSR URL file: {e}")
            return [TextContent(type="text", text=f"Error reading BSR data: {str(e)}")]

    if name == "get_bsr_subcategories":
        extractor = BSRCategoryExtractor()
        results = await extractor.get_categories_from_page(arguments["url"])
        return _json_response(results)

    # ── Tier 5: Advertising API ──────────────────────────────────────────
    if name == "get_amazon_keyword_bid_recommendations":
        store_id = arguments.get("store_id")
        region = arguments.get("region", "NA")
        client = AmazonAdsClient(store_id=store_id, region=region)
        
        # Prepare keywords for API
        keywords = arguments["keyword"]
        if isinstance(keywords, str):
            keywords = [keywords]
            
        match_types = arguments.get("match_types", ["EXACT"])
        
        keywords_payload = []
        for kw in keywords:
            for m in match_types:
                keywords_payload.append({"keyword": kw, "matchType": m.upper()})
        
        result = await client.get_keyword_bid_recommendations(
            keywords=keywords_payload,
            strategy=arguments.get("strategy", "AUTO_FOR_SALES"),
            include_analysis=arguments.get("include_analysis", False),
            asins=arguments.get("asins"),
        )
        return _json_response(result)

    # ── Tier 6: Ads-API campaign / keyword / performance ─────────────────
    if name == "list_sp_campaigns":
        client = AmazonAdsClient(
            store_id=arguments.get("store_id"),
            region=arguments.get("region", "NA"),
        )
        result = await client.list_campaigns(
            states=arguments.get("states", ["ENABLED"]),
            max_results=arguments.get("max_results", 100),
        )
        return _json_response(result)

    if name == "list_sp_ad_groups":
        client = AmazonAdsClient(
            store_id=arguments.get("store_id"),
            region=arguments.get("region", "NA"),
        )
        result = await client.list_ad_groups(
            campaign_ids=arguments.get("campaign_ids"),
            states=arguments.get("states", ["ENABLED"]),
            max_results=arguments.get("max_results", 100),
        )
        return _json_response(result)

    if name == "list_sp_keywords":
        client = AmazonAdsClient(
            store_id=arguments.get("store_id"),
            region=arguments.get("region", "NA"),
        )
        result = await client.list_keywords(
            campaign_ids=arguments.get("campaign_ids"),
            ad_group_ids=arguments.get("ad_group_ids"),
            states=arguments.get("states", ["ENABLED"]),
            max_results=arguments.get("max_results", 200),
        )
        return _json_response(result)

    if name == "get_sp_performance_report":
        client = AmazonAdsClient(
            store_id=arguments.get("store_id"),
            region=arguments.get("region", "NA"),
        )
        result = await client.get_performance_report(
            report_type=arguments.get("report_type", "spCampaigns"),
            start_date=arguments.get("start_date"),
            end_date=arguments.get("end_date"),
            days=arguments.get("days", 30),
        )
        return _json_response(result)

    # ── Tier 7: SP-API (inventory / catalog) ─────────────────────────────
    if name == "get_sp_inventory":
        client = SPAPIClient(store_id=arguments.get("store_id"))
        result = await client.get_inventory(
            seller_skus=arguments.get("seller_skus"),
        )
        return _json_response(result)

    if name == "get_sp_catalog_item":
        client = SPAPIClient(store_id=arguments.get("store_id"))
        result = await client.get_catalog_item(asin=arguments["asin"])
        return _json_response(result)

    return [TextContent(type="text", text=f"Unknown Amazon tool: {name}")]


# ── Tool Definitions ─────────────────────────────────────────────────────

_HOST_PROP = {"type": "string", "default": "https://www.amazon.com", "description": "Amazon domain, must include scheme, e.g. https://www.amazon.com or https://www.amazon.co.uk"}

amazon_tools = [
    # Session
    Tool(
        name="refresh_amazon_cookies",
        description="Launch a browser to refresh Amazon session cookies.",
        inputSchema={
            "type": "object",
            "properties": {
                "headless": {"type": "boolean", "default": False, "description": "Set to false to see the browser and login manually."},
                "wait_for_login": {"type": "boolean", "default": True, "description": "If true, waits up to 60s for manual login completion."},
            },
        },
    ),
    # Tier 1: Core screening
    Tool(
        name="get_amazon_bestsellers",
        description=(
            "Scrape Amazon Best Sellers list from a category URL. "
            "Returns list of products, each with: ASIN, Rank, Title, Price, Stars, Reviews (count), Image (URL). "
            "Results are written to DataCache for downstream tools."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The Amazon Best Sellers category URL"},
                "max_pages": {"type": "integer", "default": 2, "description": "Max pages to scrape"},
            },
            "required": ["url"],
        },
    ),
    Tool(
        name="get_product_details",
        description=(
            "Fetch full product details from an Amazon listing page. "
            "Returns Product model with: asin, title, features (bullet points), description, "
            "price, sales_rank (BSR), review_count, rating (out of 5), main_image_url, "
            "category_name, category_node_id, past_month_sales, stock_level, is_fba. "
            "Result is written to DataCache under domain='amazon', key=ASIN."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "ASIN or product URL"},
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="search_products",
        description=(
            "Search Amazon by keyword and return product list from search result pages. "
            "Each product has: asin, title, price, rating, review_count, past_month_sales. "
            "Results are written to DataCache. Use search_profitability_products for richer data."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search keyword"},
                "page": {"type": "integer", "default": 1, "description": "Search result page number"},
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="search_profitability_products",
        description=(
            "Search Amazon via the Profitability Calculator API (ad-free organic results). "
            "Returns up to 16 products per page, each with: asin, title, brandName, "
            "price, currency, weight, weightUnit, length, width, height, dimensionUnit, "
            "customerReviewsCount, customerReviewsRating, category rank info. "
            "Results are written to DataCache. Preferred over search_products for precise data."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search keyword"},
                "page_offset": {"type": "integer", "default": 1, "description": "Pagination offset (1-indexed, 16 results per page)"},
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="get_bsr_rank",
        description=(
            "Fetch Best Sellers Rank (BSR) and category hierarchy for a product. "
            "Returns: ASIN, URL, PrimaryRank (int), Category (primary category name), "
            "SecondaryRanks (list of {Rank, Category} for subcategories), "
            "CategoryNodes (list of {Category, NodeId} from breadcrumb path), "
            "TopLevelNodeId (root category node ID), LeafNodeId (most specific category node ID). "
            "TopLevelNodeId is stored in DataCache and used by calc_profit for accurate fee lookup."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_batch_past_month_sales",
        description=(
            "Fetch 'X bought in past month' badge for one or more ASINs via Amazon search. "
            "Batches up to 20 ASINs per request. "
            "Returns dict of {ASIN: int|null} — null means badge not shown (low volume). "
            "Results are merged into DataCache under each ASIN's entry."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ASINs to query (e.g. [\"B08N5WRWNW\", \"B0CKY689WQ\"])",
                },
            },
            "required": ["asins"],
        },
    ),
    # Tier 2: Competitive analysis
    Tool(
        name="get_review_count",
        description=(
            "Fetch review volume and authenticity signal for a product. "
            "Returns: ASIN, GlobalRatings (total star ratings including no-text), "
            "WrittenReviews (ratings that include written text), Ratio (WrittenReviews / GlobalRatings). "
            "Natural ratio ≈ 0.10 (1 written per 10 ratings); Ratio > 0.50 is a strong fake-review signal."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_stock_estimate",
        description=(
            "Estimate remaining inventory using the add-to-cart 999 method. "
            "Returns: {value: int, status: 'Actual'|'Limit'|'Failed'}. "
            "value=999 means stock exceeds 999 units; value=-1 means estimation failed. "
            "Result is merged into DataCache under stock_estimate key."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_keyword_rank",
        description=(
            "Find the organic search position of specific ASINs for a keyword. "
            "Returns list of {asin, keyword, page, position} for each found ASIN. "
            "Useful for tracking keyword ranking and share-of-voice analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search keyword"},
                "target_asins": {"type": "array", "items": {"type": "string"}, "description": "List of ASINs to find"},
                "max_pages": {"type": "integer", "default": 3, "description": "Max search pages to scan"},
            },
            "required": ["keyword", "target_asins"],
        },
    ),
    Tool(
        name="search_return_asins",
        description=(
            "Extract raw ASINs from Amazon search results for a keyword. "
            "Returns list of {Keyword, Page, ASIN}. "
            "Use this when you only need ASINs for bulk downstream processing."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search keyword"},
                "page": {"type": "integer", "default": 1, "description": "Search result page number"},
            },
            "required": ["keyword"],
        },
    ),
    # Tier 3: Detail enrichment
    Tool(
        name="get_reviews",
        description=(
            "Fetch customer reviews from the Amazon reviews page. "
            "Returns list of review objects, each with: asin, author, rating, title, "
            "body (review text), date, verified_purchase, helpful_votes. "
            "Reviews are cached under domain='amazon', key='reviews:{ASIN}' for 24h."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "max_pages": {"type": "integer", "default": 2, "description": "Max review pages to fetch"},
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="analyze_reviews",
        description=(
            "Deeply analyze customer reviews using an LLM. Uses cached reviews if available (24h TTL). "
            "Returns structured report with: pros (list), cons (list), sentiment_score (-1 to 1), "
            "buyer_persona (description), review_velocity (recent vs. historical rate), "
            "rating_distribution ({1..5: count}), competitive_barrier (estimated reviews to compete), "
            "manipulation_risk (RCI score, template_overlap_pct, review_to_sales_ratio, risk_level). "
            "Requires LLM provider to be configured."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "max_pages": {"type": "integer", "default": 3, "description": "Max review pages to fetch"},
                "competitive_benchmark": {"type": "integer", "default": 500, "description": "Review count threshold to estimate competitive barrier"},
                "est_monthly_sales": {"type": "integer", "default": 0, "description": "Estimated monthly sales volume for review-to-sales ratio calculation (0 = unknown)"},
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_fulfillment",
        description=(
            "Determine fulfillment method for a product listing. "
            "Returns: ASIN, URL, FulfilledBy ('Amazon' for FBA, seller name for FBM). "
            "Result is merged into DataCache under fulfillment_type key."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_seller_feedback",
        description=(
            "Fetch seller feedback statistics from the seller's storefront profile. "
            "Returns feedback count and ratings breakdown for the seller."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "seller_id": {"type": "string", "description": "Amazon seller ID"},
                "host": _HOST_PROP,
            },
            "required": ["seller_id"],
        },
    ),
    Tool(
        name="get_seller_product_count",
        description=(
            "Get the total number of active product listings for a seller. "
            "Returns seller_id and product_count extracted from the storefront page."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Amazon product listing URL (seller ID will be extracted)"},
            },
            "required": ["url"],
        },
    ),
    Tool(
        name="get_dimensions",
        description=(
            "Fetch physical dimensions and current price from a product listing. "
            "Returns: ASIN, URL, Dimensions (e.g. '10 x 5 x 3 inches'), Price (string with currency). "
            "Also attempts to extract weight_lb, length_in, width_in, height_in as floats. "
            "All non-null values are merged into DataCache for use by calc_profit and calc_fba_fee."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_product_images",
        description=(
            "Fetch product image gallery from a listing page. "
            "Returns list of image objects with url and variant (MAIN, PT01, PT02, …). "
            "Useful for listing quality assessment and competitive image analysis."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="check_has_videos",
        description=(
            "Check whether a product listing contains embedded videos. "
            "Returns: {asin, has_video: bool, video_count: int}. "
            "Listings with videos tend to have higher conversion rates."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN"},
                "host": _HOST_PROP,
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_top_bsr_categories",
        description=(
            "List all top-level Amazon Best Sellers categories with their entry BSR URLs. "
            "Returns list of {name, url} objects (e.g. Electronics, Home & Kitchen, Toys & Games). "
            "Use this to discover category URLs before calling get_bsr_subcategories."
        ),
        inputSchema={"type": "object", "properties": {}}
    ),
    Tool(
        name="get_bsr_subcategories",
        description=(
            "Explore subcategories within a BSR category page. "
            "Returns list of {name, url} for each child category found on the page. "
            "Drill down recursively to find niche subcategory BSR pages."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The parent BSR URL to explore (e.g. https://www.amazon.com/Best-Sellers-Electronics/zgbs/electronics/)"}
            },
            "required": ["url"]
        }
    ),
    Tool(
        name="get_amazon_keyword_bid_recommendations",
        description="Get suggested bid and bidding ranges for a keyword from Amazon Advertising API (SP v5.0).",
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "The keyword to check (e.g. 'wireless charger')"},
                "match_types": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["EXACT", "PHRASE", "BROAD"]},
                    "default": ["EXACT"],
                    "description": "List of match types to get recommendations for"
                },
                "strategy": {
                    "oneOf": [
                        {"type": "string", "enum": ["AUTO_FOR_SALES", "LEGACY_FOR_SALES", "MANUAL"]},
                        {"type": "array", "items": {"type": "string", "enum": ["AUTO_FOR_SALES", "LEGACY_FOR_SALES", "MANUAL"]}}
                    ],
                    "default": "AUTO_FOR_SALES",
                    "description": "Bidding strategy. AUTO_FOR_SALES (Up & Down), LEGACY_FOR_SALES (Down only), MANUAL (Fixed). Can be a single string or a list."
                },
                "include_analysis": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether to include advanced impact analysis in the response (v5.0 feature)."
                },
                "store_id": {"type": "string", "description": "The store ID suffix from .env (e.g. 'US', 'UK'). If omitted, uses default."},
                "region": {"type": "string", "enum": ["NA", "EU", "FE"], "default": "NA", "description": "Amazon Ads API region."},
                "ad_group_id": {"type": "string", "description": "Optional existing ad group ID to refine recommendations based on campaign strategy."},
                "campaign_id": {"type": "string", "description": "Optional existing campaign ID."},
                "asins": {"type": "array", "items": {"type": "string"}, "description": "Optional list of owned ASINs to provide context for new ad groups."}
            },
            "required": ["keyword"]
        }
    ),
    # ── Ads-API: campaigns / ad groups / keywords / performance ───────────
    Tool(
        name="list_sp_campaigns",
        description=(
            "List Sponsored Products campaigns from Amazon Advertising API (v3). "
            "Returns: campaign_id, name, state, daily_budget, budget_type, start_date, end_date, "
            "bidding_strategy, placement_top_of_search_pct, placement_product_page_pct."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "states": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ENABLED", "PAUSED", "ARCHIVED"]},
                    "default": ["ENABLED"],
                    "description": "Filter by campaign state.",
                },
                "max_results": {"type": "integer", "default": 100, "description": "Max campaigns to return."},
                "store_id": {"type": "string", "description": "Store ID suffix (e.g. 'US'). Defaults to env default."},
                "region": {"type": "string", "enum": ["NA", "EU", "FE"], "default": "NA"},
            },
        },
    ),
    Tool(
        name="list_sp_ad_groups",
        description=(
            "List Sponsored Products ad groups from Amazon Advertising API (v3). "
            "Returns: ad_group_id, campaign_id, name, state, default_bid."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "campaign_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by campaign IDs. If omitted, returns all ad groups.",
                },
                "states": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ENABLED", "PAUSED", "ARCHIVED"]},
                    "default": ["ENABLED"],
                },
                "max_results": {"type": "integer", "default": 100},
                "store_id": {"type": "string"},
                "region": {"type": "string", "enum": ["NA", "EU", "FE"], "default": "NA"},
            },
        },
    ),
    Tool(
        name="list_sp_keywords",
        description=(
            "List Sponsored Products manual keywords from Amazon Advertising API (v3). "
            "Returns: keyword_id, ad_group_id, campaign_id, keyword_text, match_type, state, bid."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "campaign_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by campaign IDs.",
                },
                "ad_group_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Filter by ad group IDs.",
                },
                "states": {
                    "type": "array",
                    "items": {"type": "string", "enum": ["ENABLED", "PAUSED", "ARCHIVED"]},
                    "default": ["ENABLED"],
                },
                "max_results": {"type": "integer", "default": 200},
                "store_id": {"type": "string"},
                "region": {"type": "string", "enum": ["NA", "EU", "FE"], "default": "NA"},
            },
        },
    ),
    Tool(
        name="get_sp_performance_report",
        description=(
            "Request an async Amazon SP performance report and return parsed records. "
            "spCampaigns returns campaign-level metrics; spKeywords returns keyword-level metrics. "
            "Each record includes: impressions, clicks, spend, orders, sales, acos (%), ctr (%)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "report_type": {
                    "type": "string",
                    "enum": ["spCampaigns", "spKeywords"],
                    "default": "spCampaigns",
                    "description": "spCampaigns for campaign-level; spKeywords for keyword-level (includes bid).",
                },
                "days": {
                    "type": "integer",
                    "default": 30,
                    "description": "Number of past days to report on. Ignored if start_date/end_date provided.",
                },
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD (inclusive)."},
                "end_date": {"type": "string", "description": "End date YYYY-MM-DD (inclusive)."},
                "store_id": {"type": "string"},
                "region": {"type": "string", "enum": ["NA", "EU", "FE"], "default": "NA"},
            },
        },
    ),
    # ── SP-API: inventory / catalog ───────────────────────────────────────
    Tool(
        name="get_sp_inventory",
        description=(
            "Query FBA inventory from Amazon Selling Partner API. "
            "Returns per-SKU: sku, asin, fn_sku, condition, total_quantity, available_quantity, "
            "reserved_quantity, inbound_quantity, last_updated."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "seller_skus": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of seller SKUs to query. If omitted, returns all FBA inventory.",
                },
                "store_id": {"type": "string", "description": "Store ID suffix (e.g. 'US')."},
            },
        },
    ),
    Tool(
        name="get_sp_catalog_item",
        description=(
            "Fetch product metadata from Amazon Catalog Items API (2022-04-01). "
            "Returns: asin, title, brand, product_type, color, size, bullet_point_count."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Amazon ASIN to look up."},
                "store_id": {"type": "string", "description": "Store ID suffix (e.g. 'US')."},
            },
            "required": ["asin"],
        },
    ),
]

_AMAZON_META = {
    "refresh_amazon_cookies": ("DATA", "confirmation of session refresh"),
    "get_amazon_bestsellers": ("DATA", "list of bestseller products with ASIN, title, rank, price"),
    "get_product_details": ("DATA", "full product details: title, price, brand, ratings, features"),
    "search_products": ("DATA", "list of products matching keyword with ASIN, title, price"),
    "search_profitability_products": ("DATA", "list of products with rich metadata: ASIN, title, brand, dimensions, weight, price"),
    "get_bsr_rank": ("DATA", "BSR rank, category rankings and NodeIdPath"),
    "get_batch_past_month_sales": ("DATA", "dict of ASIN → past-month purchase count (batch search)"),
    "get_review_count": ("DATA", "GlobalRatings, WrittenReviews, and their Ratio — Ratio > 0.50 indicates fake-review risk"),
    "get_stock_estimate": ("DATA", "estimated remaining stock quantity"),
    "get_keyword_rank": ("DATA", "search position of ASINs for a keyword"),
    "search_return_asins": ("DATA", "ASINs from search results"),
    "get_reviews": ("DATA", "customer reviews with text, rating, date"),
    "analyze_reviews": ("COMPUTE", "structured review summary: pros/cons, sentiment, buyer persona, velocity, rating distribution, competitive barrier, manipulation risk score"),
    "get_fulfillment": ("DATA", "FBA or FBM fulfillment status"),
    "get_seller_feedback": ("DATA", "seller feedback count"),
    "get_seller_product_count": ("DATA", "total products listed by seller"),
    "get_dimensions": ("DATA", "product dimensions and weight"),
    "get_product_images": ("DATA", "image URLs"),
    "check_has_videos": ("DATA", "video presence and count"),
    "get_top_bsr_categories": ("DATA", "list of top-level BSR categories with URLs"),
    "get_bsr_subcategories": ("DATA", "list of subcategories within a BSR category"),
    "get_amazon_keyword_bid_recommendations": ("DATA", "keyword bid recommendations from Advertising API"),
    # Ads-API v3
    "list_sp_campaigns":       ("DATA", "list of SP campaigns with budget, state, bidding strategy"),
    "list_sp_ad_groups":       ("DATA", "list of SP ad groups with default bid"),
    "list_sp_keywords":        ("DATA", "list of SP manual keywords with bid and match type"),
    "get_sp_performance_report": ("DATA", "SP performance records with impressions, clicks, spend, orders, sales, ACOS, CTR"),
    # SP-API
    "get_sp_inventory":        ("DATA", "FBA inventory per SKU: available, reserved, inbound quantities"),
    "get_sp_catalog_item":     ("DATA", "product metadata from Catalog API: title, brand, size, bullet points"),
}

for tool in amazon_tools:
    cat, ret = _AMAZON_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_amazon_tool, category=cat, returns=ret)
