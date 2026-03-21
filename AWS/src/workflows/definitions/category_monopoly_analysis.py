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
    
    # Return as new items list
    return products

async def _enrich_sales(item: dict) -> dict:
    """Fetch past month sales."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {"sales": 0}
        
    res = await extractor.get_past_month_sales(asin)
    # Clean string "1K+" to number 1000
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
    Expects items to be the BSR product list.
    Attaches context to the first item for the analyzer to pick up.
    """
    if not items: return []
    
    # 1. Determine main keyword by sending top 10 titles to local LLM
    top_titles = [item.get("Title", "") for item in items[:10] if item.get("Title")]
    titles_str = "\n".join(top_titles)
    
    prompt = (
        "Based on these 10 product titles from an Amazon Best Sellers list, "
        "identify the single most representative core search term (1-3 words) "
        f"for this entire category. \nTitles:\n{titles_str}\n"
        "Return ONLY the exact keyword string, no quotes, no extra text."
    )
    
    main_keyword = "iphone case" # Fallback
    try:
        from src.intelligence.router import TaskCategory
        if ctx.router:
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            raw_kwd = res.text.strip().replace('"', '').replace("'", "")
            if raw_kwd and len(raw_kwd) < 50:
                main_keyword = raw_kwd
    except Exception as e:
        logger.warning(f"Keyword extraction failed: {e}. Using fallback: {main_keyword}")
        
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI, XiyouAuthRequiredError
    api = XiyouZhaociAPI()
    
    # ABA Data
    aba_data = {}
    try:
        aba_res = await asyncio.to_thread(api.get_aba_top_asins, "US", [main_keyword])
        if aba_res and "searchTerms" in aba_res and aba_res["searchTerms"]:
            aba_data = aba_res["searchTerms"][0]
    except XiyouAuthRequiredError:
        logger.error("Xiyouzhaoci token expired or missing. Triggering SMS request.")
        sent = api.request_sms_code()
        if sent:
            logger.error("SMS sent successfully. Please use 'xiyou_verify_sms(xxxx)' in Feishu to re-authenticate, then re-run this workflow.")
        else:
            logger.error("Failed to auto-send SMS. Please check XIYOUZHAOCI_PHONE env var or authenticate manually.")
    except Exception as e:
        logger.error(f"Failed to fetch ABA data: {e}")
        
    # Ad Ratio (Sample Search Page)
    from src.mcp.servers.amazon.extractors.search import SearchExtractor
    s_extractor = SearchExtractor()
    search_results = await s_extractor.search(main_keyword, page=1)
    
    sponsored_count = sum(1 for r in search_results if getattr(r, 'is_sponsored', False))
    total_count = len(search_results) or 1
    ad_ratio = sponsored_count / total_count
    
    # Attach to context
    ctx.cache["keyword_data"] = aba_data
    ctx.cache["ad_ratio"] = ad_ratio
    ctx.cache["main_keyword"] = main_keyword
    
    return items

async def _run_monopoly_analysis(items: List[dict], ctx: Any) -> List[dict]:
    """Calculates final scores using the CategoryMonopolyAnalyzer."""
    from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
    analyzer = CategoryMonopolyAnalyzer()
    
    keyword_data = ctx.cache.get("keyword_data")
    ad_data = {"ad_ratio": ctx.cache.get("ad_ratio", 0.3)}
    
    # Map items to analyzer format
    analysis_input = []
    for item in items:
        # BestSellersExtractor returns Rank, Price, ASIN, Name, Reviews, Rating
        raw_price = item.get("Price") or "$0"
        raw_price = str(raw_price).replace("$", "").replace(",", "")
        try:
            price = float(raw_price)
        except:
            price = 0.0
            
        raw_rating = item.get("Rating") or "0"
        try:
            # Handle formats like "4.5 out of 5 stars"
            rating = float(str(raw_rating).split(" ")[0])
        except:
            rating = 0.0
            
        raw_reviews = item.get("Reviews") or "0"
        raw_reviews = str(raw_reviews).replace(",", "")
        try:
            reviews = int(raw_reviews)
        except:
            reviews = 0
            
        analysis_input.append({
            "rank": item.get("Rank", 999),
            "price": price,
            "sales": item.get("sales", 0),
            "brand": item.get("brand", "Unknown"), # BSR might not have brand, could extract from name
            "seller_type": item.get("seller_type", "Unknown"),
            "feedback_count": item.get("feedback_count", 0),
            "review_count": reviews,
            "rating": rating,
        })
        
    result = analyzer.analyze(analysis_input, keyword_data=keyword_data, ad_data=ad_data)
    
    import json
    return [{"analysis_result": json.dumps(result, ensure_ascii=False), "main_keyword": ctx.cache.get("main_keyword")}]

# ---------------------------------------------------------------------------
# Workflow Builder
# ---------------------------------------------------------------------------

@WorkflowRegistry.register("category_monopoly_analysis")
def build_category_monopoly_analysis(config: dict) -> Workflow:
    steps = [
        # Stage 1: Get Top 100
        ProcessStep(
            name="fetch_bsr_top_100",
            fn=_fetch_bsr_list
        ),
        
        # Stage 3: Parallel Enrichment
        EnrichStep(
            name="enrich_sales_data",
            extractor_fn=_enrich_sales,
            parallel=True,
            concurrency=10
        ),
        EnrichStep(
            name="enrich_seller_background",
            extractor_fn=_enrich_seller_info,
            parallel=True,
            concurrency=5
        ),
        
        # Stage 4: Market & Keyword Context
        ProcessStep(
            name="fetch_market_context",
            fn=_fetch_market_context
        ),
        
        # Stage 5: Final Analysis
        ProcessStep(
            name="calculate_monopoly_score",
            fn=_run_monopoly_analysis
        ),
        
        # Stage 6: Delivery
        ProcessStep(
            name="deliver_report",
            prompt_template=(
                "Generate a professional category monopoly analysis report based on these results: "
                "{analysis_result}. \nMain Keyword: {main_keyword}\n"
                "Explain the score, the competition status, and provide a strategic entry recommendation."
            ),
            compute_target=ComputeTarget.CLOUD_LLM
        )
    ]
    
    return Workflow(name="category_monopoly_analysis", steps=steps)
