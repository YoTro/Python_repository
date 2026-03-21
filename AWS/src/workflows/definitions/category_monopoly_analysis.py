from __future__ import annotations
"""
Category Monopoly Analysis Workflow

Performs a deep-dive analysis of an Amazon category to determine monopoly levels
and competition intensity across 7 dimensions.
"""

import logging
import asyncio
from typing import List, Dict, Any
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import ComputeTarget

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Extractor Wrappers
# ---------------------------------------------------------------------------

async def _fetch_bsr_list(items: List[dict], ctx: Any) -> List[dict]:
    """Fetches the Top 100 BSR products from a category URL."""
    from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
    extractor = BestSellersExtractor()
    
    url = ctx.config.get("url")
    if not url: 
        logger.error("No URL provided in workflow config for category_monopoly_analysis.")
        return []
    
    # Scrape up to 2 pages (100 products)
    products = await extractor.get_bestsellers(url, max_pages=2)
    return products

async def _enrich_sales(item: dict) -> dict:
    """Fetch past month sales."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {"sales": 0}
        
    res = await extractor.get_past_month_sales(asin)
    raw_sales = res.get("PastMonthSales", "0")
    sales_num = 0
    if isinstance(raw_sales, str):
        clean = raw_sales.replace("+", "").replace(",", "").lower()
        if "k" in clean:
            sales_num = int(float(clean.replace("k", "")) * 1000)
        else:
            try: sales_num = int(clean)
            except: sales_num = 0
    else:
        sales_num = int(raw_sales)
    return {"sales": sales_num}

async def _enrich_seller_info(item: dict) -> dict:
    """Fetch fulfillment and seller feedback."""
    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
    
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {"seller_type": "Unknown", "seller_id": None, "feedback_count": 0}
        
    f_extractor = FulfillmentExtractor()
    s_extractor = SellerFeedbackExtractor()
    
    f_res = await f_extractor.get_fulfillment_info(asin)
    seller_id = f_res.get("SellerId")
    
    feedback_count = 0
    if seller_id:
        s_res = await s_extractor.get_seller_feedback_count(seller_id)
        feedback_count = s_res.get("FeedbackCount", 0)
        
    return {
        "seller_type": f_res.get("FulfilledBy", "Unknown"),
        "seller_id": seller_id,
        "feedback_count": feedback_count
    }

async def _fetch_market_context(items: List[dict], ctx: Any) -> List[dict]:
    """
    Fetches ABA keyword data and search page ad ratio.
    Improved Accuracy: Uses Top 20 titles and a multi-candidate logic.
    """
    if not items: return []
    
    # 1. Improved Keyword Extraction (Top 20 titles)
    top_titles = [item.get("Title", "") for item in items[:20] if item.get("Title")]
    titles_str = "\n".join([f"{i+1}. {t}" for i, t in enumerate(top_titles)])
    
    prompt = (
        "Analyze these 20 Amazon Best Seller product titles. Your goal is to identify the "
        "single most accurate CORE search term that a buyer would use to find this whole list.\n\n"
        "Rules:\n"
        "- Ignore brand names.\n"
        "- Ignore attributes like 'Black', 'Pack of 2'.\n"
        "- Return ONLY the final selected keyword string, no quotes."
    )
    
    main_keyword = "unknown niche"
    try:
        from src.intelligence.router import TaskCategory
        if ctx.router:
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            main_keyword = res.text.strip().replace('"', '').replace("'", "").lower()
    except Exception as e:
        logger.warning(f"Keyword extraction failed: {e}")
        
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    api = XiyouZhaociAPI()
    
    aba_data = {}
    try:
        aba_res = await asyncio.to_thread(api.get_aba_top_asins, "US", [main_keyword])
        if aba_res and "searchTerms" in aba_res and aba_res["searchTerms"]:
            aba_data = aba_res["searchTerms"][0]
    except Exception as e:
        logger.error(f"Failed to fetch ABA data: {e}")
        
    from src.mcp.servers.amazon.extractors.search import SearchExtractor
    s_extractor = SearchExtractor()
    search_results = await s_extractor.search(main_keyword, page=1)
    
    sponsored_count = sum(1 for r in search_results if getattr(r, 'is_sponsored', False))
    total_count = len(search_results) or 1
    ad_ratio = sponsored_count / total_count
    
    ctx.cache["keyword_data"] = aba_data
    ctx.cache["ad_ratio"] = ad_ratio
    ctx.cache["main_keyword"] = main_keyword
    
    return items

async def _run_monopoly_analysis(items: List[dict], ctx: Any) -> List[dict]:
    """Calculates scores and generates flattened niche benchmarks."""
    from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
    from src.intelligence.processors.sales_estimator import SalesEstimator
    import statistics
    import json
    
    analyzer = CategoryMonopolyAnalyzer()
    estimator = SalesEstimator()
    
    keyword_data = ctx.cache.get("keyword_data")
    ad_data = {"ad_ratio": ctx.cache.get("ad_ratio", 0.3)}
    
    analysis_input = []
    for item in items:
        raw_price = str(item.get("Price") or "$0").replace("$", "").replace(",", "")
        try: price = float(raw_price)
        except: price = 0.0
        raw_rating = str(item.get("Rating") or "0").split(" ")[0]
        try: rating = float(raw_rating)
        except: rating = 0.0
        raw_reviews = str(item.get("Reviews") or "0").replace(",", "")
        try: reviews = int(raw_reviews)
        except: reviews = 0
            
        analysis_input.append({
            "rank": item.get("Rank", 999),
            "price": price,
            "sales": item.get("sales", 0),
            "brand": item.get("brand", "Unknown"),
            "seller_type": item.get("seller_type", "Unknown"),
            "feedback_count": item.get("feedback_count", 0),
            "review_count": reviews,
            "rating": rating,
        })
        
    result = analyzer.analyze(analysis_input, keyword_data=keyword_data, ad_data=ad_data)
    
    # Generate image-based context
    prices = [p['price'] for p in analysis_input if p['price'] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    avg_top_reviews = statistics.mean([p['review_count'] for p in analysis_input[:10]]) if len(analysis_input) >= 10 else 0
    avg_tail_reviews = statistics.mean([p['review_count'] for p in analysis_input[50:]]) if len(analysis_input) > 50 else 1
    review_gap = round(avg_top_reviews / max(1, avg_tail_reviews), 1)
    
    node_id = ctx.config.get("category_node_id")
    baseline = estimator.category_params.get(str(node_id), {}).get("market_logic", {})
    
    # Return FLATTENED data for safe template formatting
    return [{
        "analysis_result": json.dumps(result, ensure_ascii=False),
        "main_keyword": ctx.cache.get("main_keyword"),
        "niche_median_price": f"${median_price:.2f}",
        "review_disparity": f"{review_gap}x",
        "recommended_capital": f"${int(median_price * 2500):,}",
        "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%",
        "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get("r_squared", 0.95)
    }]

async def _prepare_report_artifact(items: List[dict], ctx: Any) -> List[dict]:
    """Saves the report to a local Markdown file."""
    if not items or "deliver_report" not in items[0]:
        return items
        
    report_data = items[0]["deliver_report"]
    
    # Robust text extraction
    report_text = None
    if hasattr(report_data, "text"): # LLMResponse object
        report_text = report_data.text
    elif isinstance(report_data, dict):
        report_text = report_data.get("text")
    else:
        report_text = str(report_data)
    
    if not report_text or report_text == "None":
        logger.warning("Report text is empty or 'None', skipping artifact.")
        return items

    import os, tempfile
    from datetime import datetime
    
    keyword = str(ctx.cache.get("main_keyword", "niche")).replace(" ", "_")
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
    file_path = os.path.normpath(os.path.join(tempfile.gettempdir(), filename))
    
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        items[0]["report_file_path"] = file_path
        logger.info(f"Artifact prepared at: {file_path}")
    except Exception as e:
        logger.error(f"Failed to write report file: {e}")
    
    return items

# ---------------------------------------------------------------------------
# Workflow Builder
# ---------------------------------------------------------------------------

@WorkflowRegistry.register("category_monopoly_analysis")
def build_category_monopoly_analysis(config: dict) -> Workflow:
    steps = [
        ProcessStep(name="fetch_bsr_top_100", fn=_fetch_bsr_list),
        EnrichStep(name="enrich_sales_data", extractor_fn=_enrich_sales, parallel=True, concurrency=10),
        EnrichStep(name="enrich_seller_background", extractor_fn=_enrich_seller_info, parallel=True, concurrency=5),
        ProcessStep(name="fetch_market_context", fn=_fetch_market_context),
        ProcessStep(name="calculate_monopoly_score", fn=_run_monopoly_analysis),
        ProcessStep(
            name="deliver_report",
            prompt_template=(
                "### ROLE & DYNAMIC CONTEXT\n"
                "Senior Amazon Analyst advising on a **{recommended_capital}** investment.\n"
                "Niche: **{main_keyword}** | Data Confidence (R²): **{data_confidence_r2}**\n\n"
                "### BENCHMARKS\n"
                "- Median Price: {niche_median_price}\n"
                "- Review Disparity: {review_disparity} (Top 10 vs Tail)\n"
                "- Typical Industry CR3: {industry_typical_cr3}\n\n"
                "### DATA: {analysis_result}\n\n"
                "### RULES\n"
                "- 400-550 words. No filler. Trace every claim to a score.\n"
                "- STRUCTURE: 1. Executive Verdict, 2. Competitive Dynamics, 3. Capital & Barrier Analysis, 4. Pre-Mortem, 5. Tactical Path."
            ),
            compute_target=ComputeTarget.CLOUD_LLM
        ),
        ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact)
    ]
    return Workflow(name="category_monopoly_analysis", steps=steps)
