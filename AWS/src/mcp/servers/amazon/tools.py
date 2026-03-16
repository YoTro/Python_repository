from __future__ import annotations
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
from src.mcp.servers.amazon.extractors.review_count import ReviewCountExtractor
from src.mcp.servers.amazon.extractors.cart_stock import CartStockExtractor
from src.mcp.servers.amazon.extractors.keywords_rank import KeywordsRankExtractor
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
from src.mcp.servers.amazon.extractors.dimensions import DimensionsExtractor
from src.mcp.servers.amazon.extractors.images import ImageExtractor
from src.mcp.servers.amazon.extractors.videos import VideoExtractor
from src.mcp.servers.amazon.extractors.products_num import ProductsNumExtractor
from src.mcp.servers.amazon.extractors.sales import SalesExtractor
from src.core.utils.cookie_helper import AmazonCookieHelper
from src.core.data_cache import data_cache

logger = logging.getLogger("mcp-amazon")


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

    if name == "get_bsr_rank":
        extractor = RanksExtractor()
        result = await extractor.get_product_ranks(
            arguments["asin"],
            host=arguments.get("host", "https://www.amazon.com"),
        )
        return _json_response(result)

    if name == "get_past_month_sales":
        extractor = PastMonthSalesExtractor()
        result = await extractor.get_past_month_sales(arguments["asin"])
        return _json_response(result)

    # ── Tier 2: Competitive analysis ─────────────────────────────────────
    if name == "get_review_count":
        extractor = ReviewCountExtractor()
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

    if name == "search_sales_asins":
        extractor = SalesExtractor()
        result = await extractor.get_sales_data(
            arguments["keyword"],
            page=arguments.get("page", 1),
        )
        return _json_response(result)

    # ── Tier 3: Detail enrichment ────────────────────────────────────────
    if name == "get_reviews":
        extractor = CommentsExtractor()
        reviews = await extractor.get_all_comments(
            arguments["asin"],
            max_pages=arguments.get("max_pages", 2),
        )
        return _json_response(reviews)

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

    return [TextContent(type="text", text=f"Unknown Amazon tool: {name}")]


# ── Tool Definitions ─────────────────────────────────────────────────────

_HOST_PROP = {"type": "string", "default": "https://www.amazon.com", "description": "Amazon domain URL"}

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
        description="Search Amazon by keyword and return product list with basic data.",
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
        name="get_bsr_rank",
        description="Fetch Best Sellers Rank (BSR) and category rankings for a product.",
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
        name="get_past_month_sales",
        description="Extract the 'X bought in past month' figure from a product page.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "ASIN or product URL"},
            },
            "required": ["asin"],
        },
    ),
    # Tier 2: Competitive analysis
    Tool(
        name="get_review_count",
        description="Fetch total customer review count for a product.",
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
        name="search_sales_asins",
        description="Extract ASINs from Amazon search results for sales analysis.",
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
]

_AMAZON_META = {
    "refresh_amazon_cookies": ("DATA", "confirmation of session refresh"),
    "get_amazon_bestsellers": ("DATA", "list of bestseller products with ASIN, title, rank, price"),
    "get_product_details": ("DATA", "full product details: title, price, brand, ratings, features"),
    "search_products": ("DATA", "list of products matching keyword with ASIN, title, price"),
    "get_bsr_rank": ("DATA", "BSR rank and category rankings"),
    "get_past_month_sales": ("DATA", "estimated past-month purchase count"),
    "get_review_count": ("DATA", "total review count"),
    "get_stock_estimate": ("DATA", "estimated remaining stock quantity"),
    "get_keyword_rank": ("DATA", "search position of ASINs for a keyword"),
    "search_sales_asins": ("DATA", "ASINs from search results for sales analysis"),
    "get_reviews": ("DATA", "customer reviews with text, rating, date"),
    "get_fulfillment": ("DATA", "FBA or FBM fulfillment status"),
    "get_seller_feedback": ("DATA", "seller feedback count"),
    "get_seller_product_count": ("DATA", "total products listed by seller"),
    "get_dimensions": ("DATA", "product dimensions and weight"),
    "get_product_images": ("DATA", "image URLs"),
    "check_has_videos": ("DATA", "video presence and count"),
}

for tool in amazon_tools:
    cat, ret = _AMAZON_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_amazon_tool, category=cat, returns=ret)
