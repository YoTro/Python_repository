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
        return _json_response(product)

    if name == "search_products":
        extractor = SearchExtractor()
        products = await extractor.search(
            arguments["keyword"],
            page=arguments.get("page", 1),
        )
        return _json_response(products)
        
    if name == "search_profitability_products":
        extractor = ProfitabilitySearchExtractor()
        results = await extractor.search_products(
            arguments["keyword"],
            page_offset=arguments.get("page_offset", 1),
        )
        return _json_response(results)

    if name == "get_bsr_rank":
        extractor = RanksExtractor()
        result = await extractor.get_product_ranks(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    if name == "get_batch_past_month_sales":
        extractor = PastMonthSalesExtractor()
        asins = arguments["asins"]
        if isinstance(asins, str):
            asins = [asins]
        result = await extractor.get_batch_past_month_sales(asins)
        return _json_response(result)

    # ── Tier 2: Competitive analysis ─────────────────────────────────────
    if name == "get_review_count":
        extractor = ReviewRatioExtractor()
        result = await extractor.get_review_count(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    if name == "get_stock_estimate":
        extractor = CartStockExtractor()
        result = await extractor.get_stock(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
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
        result = await extractor.get_fulfillment_info(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
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
        result = await extractor.get_dimensions_and_price(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
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
        description="Scrape Amazon's Best Sellers list from a given URL.",
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
        description="Fetch detailed product data from a product listing page.",
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
        description="Search Amazon by keyword and return product list with basic data (asin, title, price, rating, review_count, past_month_sales).",
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
        description="Search Amazon using the Profitability Calculator API. Returns a clean list of organic products with rich metadata: ASIN, title, brand, dimensions, weight, price, category rank, and reviews. Excellent for precise data extraction without ads.",
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
        description="Fetch Best Sellers Rank (BSR), subcategory ranks, and breadcrumb category navigation nodes (NodeId).",
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
        description="Fetch 'X bought in past month' for one or more ASINs via Amazon search (one request per 20 ASINs). Returns {ASIN: int|null}.",
        inputSchema={
            "type": "object",
            "properties": {
                "asins": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of ASINs to query (e.g. [\"B08N5WRWNW\", \"B0CKY689WQ\"])",
                },
            },
            "required": ["asin"],
        },
    ),
    # Tier 2: Competitive analysis
    Tool(
        name="get_review_count",
        description="Fetch GlobalRatings (all star ratings) and WrittenReviews (ratings with text) for a product, plus their Ratio. Natural ratio ≈ 0.10 (1:10); Ratio > 0.50 is a strong fake-review signal.",
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
        description="Estimate remaining stock using the add-to-cart 999 method.",
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
        description="Find the search result position of specific ASINs for a keyword.",
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
        description="Extract ASINs from Amazon search results.",
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
        description="Fetch customer reviews for a product with text, rating, and metadata.",
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
            "Fetch and deeply analyze customer reviews for a product. "
            "Returns structured pros/cons, sentiment score, buyer persona, "
            "review velocity, rating distribution, competitive barrier estimate, "
            "and a manipulation risk score (RCI, template overlap, review-to-sales ratio)."
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
        description="Determine whether a product is fulfilled by Amazon (FBA) or merchant (FBM).",
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
        description="Fetch seller feedback count from the storefront profile.",
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
        description="Get the total number of products listed by a seller.",
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
        description="Fetch product dimensions and current price from a listing.",
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
        description="Fetch primary and secondary image URLs from a product listing.",
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
        description="Check if a product listing contains videos and get the count.",
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
        description="Lists all top-level Amazon Best Sellers categories (Electronics, Home, etc.) with their entry URLs. Use this as a starting point for market research.",
        inputSchema={"type": "object", "properties": {}}
    ),
    Tool(
        name="get_bsr_subcategories",
        description="Dynamically explores subcategories within a specific Amazon BSR category. Pass a parent BSR URL to find more specific niches.",
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
}

for tool in amazon_tools:
    cat, ret = _AMAZON_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_amazon_tool, category=cat, returns=ret)
