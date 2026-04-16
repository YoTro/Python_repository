from __future__ import annotations
"""
Product Screening Workflow Definition

Implements a multi-stage funnel pipeline for selecting profitable products
to sell on Amazon US market. Uses Step primitives to compose a declarative workflow.

Stages:
  1. Market Discovery & Basic Filtering (price, weight, rating)
  2. Competition Analysis (US seller ratio in BSR)
  3. Cost & Profitability (FBA fees, profit margin)
  4. Compliance (Amazon restriction, EPA, certifications, CPSC recalls)
  5. Advertising Analysis (ad traffic ratio)
  6. Social Media Assessment (TikTok/Meta - optional)
"""

import logging
from typing import Dict, Any, List, Optional
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow, WorkflowContext
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
    Includes unit conversion (lb to grams) for filtering.
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
    weight_lb = p.get("weight") or 0.0
    # Convert lb to grams for workflow_defaults.yaml alignment (1 lb ≈ 453.59g)
    weight_grams = round(weight_lb * 453.59, 2)

    return {
        "title": p.get("title", item.get("title")),
        "price": p.get("price", item.get("price")),
        "weight_lb": weight_lb,
        "weight": weight_grams, # Used for filtering
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
    """Fetch past month sales and calculate daily average."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    asin = item["asin"].strip().upper()
    batch = await extractor.get_batch_past_month_sales([asin])
    past_sales = batch.get(asin) or 0
    return {
        "past_month_sales": past_sales,
        "daily_sales": round(past_sales / 30.0, 2) # Used for filtering
    }


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


async def _enrich_compliance(item: dict, ctx: WorkflowContext) -> dict:
    """
    Run all compliance checks for a product using local rule databases and CPSC.gov.

    Checks performed (in order of severity):
      1. CPSC recall    — network call to cpsc.gov; controlled by config["enable_cpsc_check"]
      2. Amazon restriction — local JSON; hard-fail if approval_required
      3. EPA regulation — local JSON; whether product is an EPA-regulated device
      4. Certification  — local JSON; lists required certs (FCC, CPC, FDA, etc.)

    Sets these fields on the item:
      compliance_status       "pass" | "warning" | "fail"
      compliance_flags        list[{type, detail, ...}] — all issues found
      epa_status              "exempt" | "not_required" | "required" (backward compat)
      amazon_restricted       bool
      cpsc_recalled           bool
      required_certifications list[str] — deduplicated list of required cert names
    """
    import json as _json
    from src.mcp.servers.compliance.tools import handle_compliance_tool

    title    = item.get("title", "") or ""
    category = item.get("category", "") or ""
    brand    = item.get("brand", "") or ""

    # Representative keyword: prefer category, fall back to first 4 words of title
    keyword = category if category else " ".join(title.split()[:4])
    if not keyword:
        return {
            "compliance_status": "pass",
            "compliance_flags": [],
            "epa_status": "not_required",
            "amazon_restricted": False,
            "cpsc_recalled": False,
            "required_certifications": [],
        }

    flags: list = []

    # ── 1. CPSC recall check (network, optional) ─────────────────────────
    cpsc_recalled = False
    if ctx.config.get("enable_cpsc_check", True):
        recall_keyword = brand if brand else keyword
        try:
            cpsc_texts = await handle_compliance_tool("check_cpsc_recall", {"keyword": recall_keyword})
            cpsc_data  = _json.loads(cpsc_texts[0].text) if cpsc_texts else {}
            if cpsc_data.get("status") == "recalled":
                cpsc_recalled = True
                flags.append({
                    "type":    "cpsc_recall",
                    "keyword": recall_keyword,
                    "count":   cpsc_data.get("count", 0),
                    "sample":  cpsc_data.get("findings", [{}])[0].get("title", ""),
                })
        except Exception as e:
            logger.warning(f"[compliance] CPSC check failed for '{recall_keyword}': {e}")

    # ── 2. Amazon restriction check (local JSON) ─────────────────────────
    amazon_restricted  = False
    approval_required  = False
    try:
        amz_texts = await handle_compliance_tool("check_amazon_restriction", {"keyword": keyword})
        amz_data  = _json.loads(amz_texts[0].text) if amz_texts else {}
        if amz_data.get("status") == "restricted_or_flagged":
            amazon_restricted = True
            for f in amz_data.get("findings", []):
                if f.get("approval_required"):
                    approval_required = True
                flags.append({
                    "type":              "amazon_restriction",
                    "category":          f.get("category"),
                    "approval_required": f.get("approval_required", False),
                    "seller_central":    f.get("seller_central_link"),
                })
    except Exception as e:
        logger.warning(f"[compliance] Amazon restriction check failed for '{keyword}': {e}")

    # ── 3. EPA check (local JSON) ─────────────────────────────────────────
    epa_required = False
    try:
        epa_texts = await handle_compliance_tool("check_epa", {"keyword": keyword})
        epa_data  = _json.loads(epa_texts[0].text) if epa_texts else {}
        if epa_data.get("status") == "warning":
            for f in epa_data.get("findings", []):
                if f.get("type") == "EPA Regulated Device":
                    epa_required = True
                flags.append({
                    "type":     "epa",
                    "detail":   f.get("type"),
                    "category": f.get("category"),
                })
    except Exception as e:
        logger.warning(f"[compliance] EPA check failed for '{keyword}': {e}")

    # ── 4. Certification check (local JSON) ───────────────────────────────
    required_certifications: list = []
    try:
        cert_texts = await handle_compliance_tool("check_certification", {"category": category or keyword})
        cert_data  = _json.loads(cert_texts[0].text) if cert_texts else {}
        if cert_data.get("status") == "matched":
            for f in cert_data.get("findings", []):
                if f.get("certification_required"):
                    certs = f.get("required_certifications", [])
                    required_certifications.extend(certs)
                    flags.append({"type": "certification", "certifications": certs})
        elif cert_data.get("certification_required"):
            certs = cert_data.get("suggested_certifications", [])
            required_certifications.extend(certs)
            if certs:
                flags.append({"type": "certification", "certifications": certs})
    except Exception as e:
        logger.warning(f"[compliance] Certification check failed for '{keyword}': {e}")

    # ── Compute overall status ─────────────────────────────────────────────
    # fail  : product is recalled, OR Amazon restriction requires pre-approval
    # warning: EPA device registration, soft restriction, or certs needed
    # pass  : no issues found
    if cpsc_recalled or approval_required:
        compliance_status = "fail"
    elif epa_required or amazon_restricted or required_certifications:
        compliance_status = "warning"
    else:
        compliance_status = "pass"

    # epa_status backward-compat field (used by downstream steps/reports)
    if epa_required:
        epa_status = "required"
    elif flags and any(f["type"] == "epa" for f in flags):
        epa_status = "warning"
    else:
        epa_status = "not_required"

    return {
        "compliance_status":       compliance_status,
        "compliance_flags":        flags,
        "epa_status":              epa_status,
        "amazon_restricted":       amazon_restricted,
        "cpsc_recalled":           cpsc_recalled,
        "required_certifications": list(dict.fromkeys(required_certifications)),  # dedup, preserve order
    }


async def _enrich_reviews(item: dict, ctx: WorkflowContext) -> dict:
    """
    Fetch top reviews for manipulation detection and quality analysis.
    Uses CommentsExtractor (AJAX + HTML fallback). Capped at 2 pages (~20 reviews)
    to stay cost-efficient while providing enough signal for ReviewSummarizer.
    """
    from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
    asin = item.get("asin")
    if not asin:
        return {}
    extractor = CommentsExtractor()
    reviews = await extractor.get_all_comments(asin, max_pages=2)
    return {"reviews": reviews}


async def _summarize_reviews(items: list, ctx: WorkflowContext) -> list:
    """
    Run ReviewSummarizer on each product's fetched reviews.

    Populates per-item fields:
      manipulation_risk_score    int 0-100
      manipulation_risk_verdict  "SAFE" | "SUSPICIOUS" | "CRITICAL" | "INSUFFICIENT_DATA"
      review_velocity            float  (reviews/month)
      review_barrier_months      float | None  (months to reach benchmark)
      review_summary             ReviewSummary object (LLM-generated insights)

    Skips products with no reviews or fewer than 5 (ReviewSummarizer minimum).
    Falls back gracefully so a single failure never drops the whole batch.
    """
    from src.intelligence.processors.review_summarizer import ReviewSummarizer
    from src.intelligence.providers.factory import ProviderFactory

    provider = ProviderFactory.get_provider()
    summarizer = ReviewSummarizer(provider=provider)
    benchmark = ctx.config.get("review_benchmark", 500)

    for item in items:
        reviews = item.get("reviews") or []
        if len(reviews) < 5:
            item.setdefault("manipulation_risk_score", 0)
            item.setdefault("manipulation_risk_verdict", "INSUFFICIENT_DATA")
            continue

        try:
            summary = await summarizer.summarize(
                reviews=reviews,
                competitive_benchmark=benchmark,
                est_monthly_sales=item.get("past_month_sales") or 0,
            )
            item["review_summary"] = summary
            item["manipulation_risk_score"]   = summary.manipulation_risk.get("score", 0)
            item["manipulation_risk_verdict"] = summary.manipulation_risk.get("verdict", "SAFE")
            item["review_velocity"]           = summary.review_velocity
            item["review_barrier_months"]     = summary.competitive_barrier_months
        except Exception as e:
            logger.error(f"Review summarization failed for {item.get('asin')}: {e}")
            item.setdefault("manipulation_risk_score", 0)
            item.setdefault("manipulation_risk_verdict", "ERROR")

    return items


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


async def _enrich_social_data(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch social media virality data (TikTok/Meta)."""
    # In a real scenario, this would call a TikTok/Meta scraper or MCP tool.
    # For now, we simulate social trend data based on category and brand.
    from src.intelligence.processors.social_virality import SocialViralityProcessor
    processor = SocialViralityProcessor()
    
    keyword = item.get("category") or item.get("title", "")[:20]
    # Simulate fetching raw social counts
    virality = processor.analyze(keyword)
    
    return {
        "social_score": virality.get("score", 0),
        "social_trend": virality.get("trend", "stable"),
        "is_trending": virality.get("score", 0) > 70
    }


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
                        # Add cost_ratio for filtering
                        if price and cost:
                            item["cost_ratio"] = round(cost / price, 4)
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
                RangeRule("weight", config.get("weight_min", 20), config.get("weight_max", 1000)),
                RangeRule("daily_sales", config.get("daily_sales_min", 30), config.get("daily_sales_max", 40)),
            ],
        ),

        # ── Stage 2: Competition Analysis ──
        EnrichStep(
            name="enrich_fulfillment",
            extractor_fn=_enrich_fulfillment,
            parallel=True,
        ),
        # Note: US seller ratio analysis could be added here if needed

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
            compute_target=ComputeTarget.PURE_PYTHON,
        ),
        FilterStep(
            name="profit_filter",
            rules=[
                ThresholdRule("profit_margin", min_val=config.get("profit_margin_min", 0.30)),
                # cost_ratio_max filtering: cost should be < 30% of price
                ThresholdRule("cost_ratio", max_val=config.get("cost_ratio_max", 0.30)),
            ],
        ),

        # ── Stage 5: Review Quality & Manipulation Detection ─────────────────
        # Fetches up to 2 pages of reviews per product (AJAX + HTML fallback).
        # ReviewSummarizer computes RCI / semantic-overlap / RSR risk score and
        # generates an LLM-backed quality summary stored in review_summary.
        # Config knobs:
        #   enable_review_analysis  (bool,  default True)  — toggle entire stage
        #   review_benchmark        (int,   default 500)   — target review count for barrier calc
        #   manipulation_risk_max   (float, default 70.0)  — max acceptable risk score (0-100)
        EnrichStep(
            name="enrich_reviews",
            extractor_fn=_enrich_reviews,
            parallel=True,
            concurrency=5,
            enabled=config.get("enable_review_analysis", True),
        ),
        ProcessStep(
            name="summarize_reviews",
            fn=_summarize_reviews,
            compute_target=ComputeTarget.PURE_PYTHON,
            enabled=config.get("enable_review_analysis", True),
        ),
        FilterStep(
            name="review_manipulation_filter",
            rules=[
                ThresholdRule("manipulation_risk_score", max_val=config.get("manipulation_risk_max", 70.0)),
            ],
            enabled=config.get("enable_review_analysis", True),
        ),

        # ── Stage 6: Compliance ──────────────────────────────────────────────
        EnrichStep(
            name="enrich_compliance",
            extractor_fn=_enrich_compliance,
            parallel=True,
            concurrency=5,
        ),
        FilterStep(
            name="compliance_filter",
            rules=[
                EnumRule(
                    "compliance_status",
                    config.get("compliance_status_allowed", ["pass", "warning"]),
                ),
                EnumRule(
                    "epa_status",
                    config.get("epa_status_allowed", ["exempt", "not_required"]),
                ),
            ],
        ),

        # ── Stage 7: Advertising Analysis (Third-party) ──
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

        # ── Stage 8: Social Media Assessment (Optional) ──
        EnrichStep(
            name="enrich_social_data",
            extractor_fn=_enrich_social_data,
            parallel=True,
            enabled=config.get("enable_social_analysis", True)
        ),
        ProcessStep(
            name="social_virality_analysis",
            fn=lambda items, ctx: items, # Placeholder for SocialViralityProcessor
            compute_target=ComputeTarget.PURE_PYTHON,
            enabled=config.get("enable_social_analysis", True)
        ),

        # ── Stage 9: Final Synthesis (Cloud LLM) ──
        ProcessStep(
            name="final_synthesis",
            prompt_template=(
                "Analyze these {count} candidate products for Amazon US market entry. "
                "Rank them by overall potential considering profit margin, competition, "
                "market demand, compliance risk (compliance_status, required_certifications), "
                "and review quality (manipulation_risk_verdict, manipulation_risk_score, review_velocity). "
                "Flag any product with manipulation_risk_verdict='SUSPICIOUS' or 'CRITICAL' and explain why. "
                "Flag any product with compliance_status='warning' and explain the cert/regulatory requirement. "
                "Provide a brief recommendation for each."
            ),
            compute_target=ComputeTarget.CLOUD_LLM,
            enabled=True,
        ),
    ]

    return Workflow(name="product_screening", steps=steps)
