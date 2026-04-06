from __future__ import annotations
"""
Product Screening Workflow Definition

Implements a multi-stage funnel pipeline for selecting profitable products
to sell on Amazon US market. Uses Step primitives to compose a declarative workflow.

Stages:
  1. Market Discovery & Basic Filtering (price, weight, rating)
  2. Competition Analysis (US seller ratio in BSR)
  3. Cost & Profitability (FBA fees, profit margin)
  4. Compliance (EPA exemption, patent risk)
  5. Advertising Analysis (ad traffic ratio)
  6. Social Media Assessment (TikTok/Meta - optional)
"""

import logging
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.filter import FilterStep, RangeRule, ThresholdRule, EnumRule
from src.workflows.steps.base import ComputeTarget

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extractor wrapper functions
# ---------------------------------------------------------------------------

async def _search_products(item: dict, ctx: WorkflowContext) -> dict:
    """Search Amazon for candidate ASINs by keyword."""
    from src.mcp.servers.amazon.extractors.search import SearchExtractor
    extractor = SearchExtractor()
    keyword = item.get("keyword", "")
    page = item.get("page", 1)
    results = await extractor.search(keyword, page)
    return {"search_results": results}


async def _enrich_product_details(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch price, rating, title, features from product page."""
    from src.mcp.servers.amazon.extractors.product_details import ProductDetailsExtractor
    extractor = ProductDetailsExtractor()
    from src.core.models.product import Product
    product = Product(asin=item["asin"])
    enriched = await extractor.enrich_product(product)
    return {
        "title": enriched.title,
        "price": enriched.price,
        "rating": enriched.rating,
        "review_count": enriched.review_count,
        "features": enriched.features,
        "is_fba": enriched.is_fba,
    }


async def _enrich_via_profitability_api(item: dict, ctx: WorkflowContext) -> dict:
    """
    High-efficiency enrichment using Amazon's Profitability Calculator API.
    Fetches weight, dimensions, BSR, and price in a single request.
    """
    from src.mcp.servers.amazon.extractors.profitability_search import ProfitabilitySearchExtractor
    extractor = ProfitabilitySearchExtractor()
    asin = item.get("asin")
    if not asin:
        return {}
    
    # Searching for an ASIN usually returns the exact product match
    results = await extractor.search_products(asin, page_offset=1)
    if not results:
        return {}
    
    p = results[0]
    return {
        "title": p.get("title", item.get("title")),
        "price": p.get("price", item.get("price")),
        "weight_lb": p.get("weight"),
        "dimensions": {
            "length": p.get("length"),
            "width": p.get("width"),
            "height": p.get("height"),
            "unit": p.get("dimensionUnit")
        },
        "primary_rank": p.get("salesRank"),
        "category": p.get("salesRankContextName"),
        "review_count": p.get("customerReviewsCount"),
        "rating": p.get("customerReviewsRating"),
        "brand": p.get("brandName"),
        "fee_category": p.get("feeCategoryString")
    }


async def _enrich_past_month_sales(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch past month sales estimate."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    result = await extractor.get_past_month_sales(item["asin"])
    return {"past_month_sales": result.get("PastMonthSales")}


async def _enrich_fulfillment(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch fulfillment info (FBA/FBM)."""
    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    extractor = FulfillmentExtractor()
    result = await extractor.get_fulfillment_info(item["asin"])
    return {"fulfilled_by": result.get("FulfilledBy")}


async def _enrich_deal_history(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch off-Amazon deal history, using product title for keyword search."""
    from src.mcp.servers.market.deals.client import DealHistoryClient
    
    asin = item.get("asin")
    title = item.get("title", "")
    brand = item.get("brand", "")
    
    keyword = brand
    if title:
        title_parts = title.replace(brand, "").strip().split()
        keyword = f"{brand} {' '.join(title_parts[:3])}".strip()

    client = DealHistoryClient()
    deals = await client.get_deal_history(asin=asin, keyword=keyword)
    return {"deal_history": deals}


async def _enrich_ad_metrics_xiyou(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch ad traffic ratio from XiyouZhaoci."""
    asin = item.get("asin")
    if not asin or not ctx.mcp:
        return {}
        
    try:
        resp = await ctx.mcp.call_tool_json("xiyou_get_traffic_scores", {
            "asins": [asin],
            "country": "US"
        })
        if isinstance(resp, list) and len(resp) > 0:
            import json
            data = json.loads(resp[0].get("text", "{}"))
            if data.get("success") and data.get("data"):
                # Ratio is like 0.45 (45%)
                ratio = data["data"][0].get("advertisingTrafficScoreRatio", 0.0)
                growth = data["data"][0].get("totalTrafficScoreGrowthRate", 0.0)
                return {
                    "ad_traffic_ratio": ratio,
                    "traffic_growth_7d": growth
                }
    except Exception as e:
        logger.error(f"Failed to fetch Xiyou traffic scores for {asin}: {e}")
    return {}


# ---------------------------------------------------------------------------
# Processing functions (Pure Python or MCP)
# ---------------------------------------------------------------------------

async def _calculate_profit_mcp(items: list, ctx: WorkflowContext) -> list:
    """
    Calculate profit margin for each item using the finance MCP tool.
    This ensures we use the most up-to-date fee logic and standard data structures.
    """
    if not ctx.mcp:
        logger.error("MCP client not available in context. Skipping profit calculation.")
        return items

    for item in items:
        asin = item.get("asin")
        price = item.get("price")
        
        # Determine estimated cost (COGS)
        cost = item.get("estimated_cost")
        if cost is None and price:
            cost = price * 0.25 # Default 25% COGS estimate
            item["estimated_cost"] = cost
            item["cost_source"] = "estimated_default"

        if asin and cost:
            try:
                # Call finance MCP tool
                # The tool will enrich missing price/category from cache if needed
                resp = await ctx.mcp.call_tool_json("calc_profit", {
                    "asin": asin,
                    "estimated_cost": cost
                })
                
                if isinstance(resp, list) and len(resp) > 0:
                    import json
                    # TextContent holds the JSON response
                    profit_data = json.loads(resp[0].get("text", "{}"))
                    if profit_data.get("profitability"):
                        p = profit_data["profitability"]
                        item["profit"] = p.get("net_profit")
                        item["profit_margin"] = p.get("margin")
                        item["roi"] = p.get("roi")
                        item["fees"] = profit_data.get("fees")
            except Exception as e:
                logger.error(f"Failed to calculate profit via MCP for {asin}: {e}")

    return items


def _analyze_promotions(items: list) -> list:
    """Calculate promo frequency and risk."""
    from src.intelligence.processors.promo_analyzer import PromoAnalyzer
    analyzer = PromoAnalyzer()
    for item in items:
        current_price = item.get("price") or 0.0
        deals = item.get("deal_history", [])
        analysis = analyzer.analyze(current_price, deals)
        item["promo_frequency"] = analysis["promo_frequency"]
        item["all_time_low"] = analysis["all_time_low"]
        item["promo_dependency_score"] = analysis["promo_dependency_score"]
        item["promo_risk_level"] = analysis["risk_level"]
    return items


# ---------------------------------------------------------------------------
# Workflow Builder
# ---------------------------------------------------------------------------

@WorkflowRegistry.register("product_screening")
def build_product_screening(config: dict) -> Workflow:
    """
    Build the product screening workflow from config.
    Config values come from merge(workflow_defaults, job_override).
    """
    steps = [
        # ── Stage 1: Market Discovery & Data Enrichment ──
        # Optimization: Use Profitability API to fetch most data in one shot
        EnrichStep(
            name="enrich_via_profitability_api",
            extractor_fn=_enrich_via_profitability_api,
            parallel=True,
            concurrency=10
        ),
        EnrichStep(
            name="enrich_past_month_sales",
            extractor_fn=_enrich_past_month_sales,
            parallel=True,
        ),
        FilterStep(
            name="basic_filter",
            rules=[
                RangeRule("price", config.get("price_min", 20), config.get("price_max", 40)),
                RangeRule("rating", config.get("rating_min", 4.2), config.get("rating_max", 4.5)),
            ],
        ),

        # ── Stage 2: Competition Analysis ──
        EnrichStep(
            name="enrich_fulfillment",
            extractor_fn=_enrich_fulfillment,
            parallel=True,
        ),

        # ── Stage 3: Price Stability & Promotion Analysis ──
        EnrichStep(
            name="enrich_deal_history",
            extractor_fn=_enrich_deal_history,
            parallel=True,
        ),
        ProcessStep(
            name="analyze_promotions",
            fn=_analyze_promotions,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),
        FilterStep(
            name="promo_risk_filter",
            rules=[
                ThresholdRule("promo_dependency_score", max_val=config.get("promo_dependency_max", 70.0)),
            ],
        ),

        # ── Stage 4: Cost & Profitability ──
        ProcessStep(
            name="calculate_profit",
            fn=_calculate_profit_mcp,
            compute_target=ComputeTarget.PURE_PYTHON, # Logic is async but compute is trivial
        ),
        FilterStep(
            name="profit_filter",
            rules=[
                ThresholdRule("profit_margin", min_val=config.get("profit_margin_min", 0.30)),
            ],
        ),

        # ── Stage 5: Compliance (LLM-assisted) ──
        ProcessStep(
            name="epa_check",
            prompt_template=(
                "Based on the product title '{title}' and category '{category}', "
                "determine if this product requires EPA registration or is exempt. "
                "Respond with ONLY one of: 'exempt', 'not_required', 'required'."
            ),
            compute_target=ComputeTarget.LOCAL_LLM,
        ),
        FilterStep(
            name="compliance_filter",
            rules=[
                EnumRule("epa_status", config.get("epa_status_allowed", ["exempt", "not_required"])),
            ],
        ),

        # ── Stage 6: Advertising Analysis (Third-party) ──
        EnrichStep(
            name="enrich_ad_metrics",
            extractor_fn=_enrich_ad_metrics_xiyou,
            parallel=True,
            enabled=config.get("enable_ad_analysis_xiyou", True)
        ),
        FilterStep(
            name="ad_filter",
            rules=[
                ThresholdRule("ad_traffic_ratio", max_val=config.get("ad_traffic_ratio_max", 0.35)),
            ],
            enabled=config.get("enable_ad_analysis_xiyou", True)
        ),

        # ── Stage 7: Final Synthesis (Cloud LLM) ──
        ProcessStep(
            name="final_synthesis",
            prompt_template=(
                "Analyze these {count} candidate products for Amazon US market entry. "
                "Rank them by overall potential considering profit margin, competition, "
                "and market demand. Provide a brief recommendation for each."
            ),
            compute_target=ComputeTarget.CLOUD_LLM,
            enabled=True,
        ),
    ]

    return Workflow(name="product_screening", steps=steps)
