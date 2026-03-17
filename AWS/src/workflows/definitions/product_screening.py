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
# Each wraps an existing Extractor class into the async fn(item) -> dict
# signature expected by EnrichStep.
# ---------------------------------------------------------------------------

async def _search_products(item: dict) -> dict:
    """Search Amazon for candidate ASINs by keyword."""
    from src.mcp.servers.amazon.extractors.search import SearchExtractor
    extractor = SearchExtractor()
    keyword = item.get("keyword", "")
    page = item.get("page", 1)
    results = await extractor.search(keyword, page)
    return {"search_results": results}


async def _enrich_product_details(item: dict) -> dict:
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


async def _enrich_dimensions(item: dict) -> dict:
    """Fetch product dimensions and weight."""
    from src.mcp.servers.amazon.extractors.dimensions import DimensionsExtractor
    extractor = DimensionsExtractor()
    result = await extractor.get_dimensions_and_price(item["asin"])
    return {"dimensions": result.get("Dimensions")}


async def _enrich_ranks(item: dict) -> dict:
    """Fetch BSR ranks."""
    from src.mcp.servers.amazon.extractors.ranks import RanksExtractor
    extractor = RanksExtractor()
    result = await extractor.get_product_ranks(item["asin"])
    return {
        "primary_rank": result.get("PrimaryRank"),
        "category": result.get("Category"),
    }


async def _enrich_past_month_sales(item: dict) -> dict:
    """Fetch past month sales estimate."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    result = await extractor.get_past_month_sales(item["asin"])
    return {"past_month_sales": result.get("PastMonthSales")}


async def _enrich_fulfillment(item: dict) -> dict:
    """Fetch fulfillment info (FBA/FBM)."""
    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    extractor = FulfillmentExtractor()
    result = await extractor.get_fulfillment_info(item["asin"])
    return {"fulfilled_by": result.get("FulfilledBy")}


# ---------------------------------------------------------------------------
# Processing functions (Pure Python)
# ---------------------------------------------------------------------------

def _calculate_profit(items: list) -> list:
    """
    Calculate profit margin for each item.
    Uses FBA fee + referral fee from static resources.
    """
    import json
    import os

    resources_dir = os.path.join(
        os.path.dirname(__file__), "..", "..", "mcp", "servers", "finance"
    )

    # Load fee tables
    fba_fee_path = os.path.join(resources_dir, "fba_fee.json")
    referral_path = os.path.join(resources_dir, "referral_fee_rates.json")

    fba_fees = {}
    referral_rates = {}
    if os.path.exists(fba_fee_path):
        with open(fba_fee_path, "r") as f:
            fba_fees = json.load(f)
    if os.path.exists(referral_path):
        with open(referral_path, "r") as f:
            referral_rates = json.load(f)

    for item in items:
        price = item.get("price")
        cost = item.get("estimated_cost")

        if price and price > 0:
            # Estimate FBA fee (simplified: use default tier)
            fba_fee = fba_fees.get("default", 3.50) if isinstance(fba_fees, dict) else 3.50
            # Referral fee: default 15%
            referral_fee = price * referral_rates.get("default", 0.15) if isinstance(referral_rates, dict) else price * 0.15

            if cost is None:
                # Rough cost estimate: 25% of price as fallback
                cost = price * 0.25
                item["estimated_cost"] = cost
                item["cost_source"] = "estimated"

            profit = price - cost - fba_fee - referral_fee
            item["fba_fee"] = round(fba_fee, 2)
            item["referral_fee"] = round(referral_fee, 2)
            item["profit"] = round(profit, 2)
            item["profit_margin"] = round(profit / price, 4) if price > 0 else 0
            item["cost_ratio"] = round(cost / price, 4) if price > 0 else 1.0
        else:
            item["profit_margin"] = 0
            item["cost_ratio"] = 1.0

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
        # ── Stage 1: Market Discovery & Basic Filtering ──
        EnrichStep(
            name="enrich_product_details",
            extractor_fn=_enrich_product_details,
            parallel=True,
        ),
        EnrichStep(
            name="enrich_dimensions",
            extractor_fn=_enrich_dimensions,
            parallel=True,
        ),
        EnrichStep(
            name="enrich_ranks",
            extractor_fn=_enrich_ranks,
            parallel=True,
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
        # US seller ratio analysis would go here (requires seller_info extractor)
        # FilterStep("competition_filter", [ThresholdRule("us_seller_ratio", min_val=config.get("us_seller_ratio_min", 0.80))]),

        # ── Stage 3: Cost & Profitability ──
        ProcessStep(
            name="calculate_profit",
            fn=_calculate_profit,
            compute_target=ComputeTarget.PURE_PYTHON,
        ),
        FilterStep(
            name="profit_filter",
            rules=[
                ThresholdRule("profit_margin", min_val=config.get("profit_margin_min", 0.30)),
                ThresholdRule("cost_ratio", max_val=config.get("cost_ratio_max", 0.30)),
            ],
        ),

        # ── Stage 4: Compliance (LLM-assisted) ──
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

        # ── Stage 5: Advertising Analysis ──
        # Ad traffic data would come from SellerSprite integration
        # FilterStep("ad_filter", [ThresholdRule("ad_traffic_ratio", max_val=config.get("ad_traffic_ratio_max", 0.20))]),

        # ── Stage 6: Final Synthesis (Cloud LLM) ──
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
