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
    
    products = await extractor.get_bestsellers(url, max_pages=2)
    return products

async def _enrich_sales(item: dict, ctx: Any) -> dict:
    """Fetch past month sales."""
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    extractor = PastMonthSalesExtractor()
    asin = item.get("ASIN") or item.get("asin")
    if not asin: return {"sales": 0}
        
    res = await extractor.get_past_month_sales(asin)
    raw_sales = res.get("PastMonthSales", "0")
    try:
        clean = raw_sales.replace("+", "").replace(",", "").lower()
        if "k" in clean: sales_num = int(float(clean.replace("k", "")) * 1000)
        else: sales_num = int(clean)
    except: sales_num = 0
    return {"sales": sales_num}

async def _enrich_seller_info(item: dict, ctx: Any) -> dict:
    """Fetch fulfillment and seller feedback."""
    from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
    from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
    
    asin = item.get("ASIN") or item.get("asin")
    if not asin: return {"seller_type": "Unknown", "seller_id": None, "feedback_count": 0}
        
    f_extractor, s_extractor = FulfillmentExtractor(), SellerFeedbackExtractor()
    f_res = await f_extractor.get_fulfillment_info(asin)
    seller_id = f_res.get("SellerId")
    feedback_count = 0
    if seller_id:
        s_res = await s_extractor.get_seller_feedback_count(seller_id)
        feedback_count = s_res.get("FeedbackCount", 0)
    return {"seller_type": f_res.get("FulfilledBy", "Unknown"), "seller_id": seller_id, "feedback_count": feedback_count}

async def _fetch_market_context(items: List[dict], ctx: Any) -> List[dict]:
    """Fetches ABA keyword data and search page ad ratio for multiple core terms."""
    if not items: return []
    top_titles = [item.get("Title", "") for item in items[:20] if item.get("Title")]
    prompt = (
        """Analyze these 20 Amazon Best Seller product titles and identify the TOP 3 most accurate CORE search terms (keywords).
        Return them as a comma-separated list, most important first.
        Ignore brands and attributes. Titles:
        """
        f"{top_titles}"
    )
    
    core_keywords = ["unknown niche"]
    try:
        from src.intelligence.router import TaskCategory
        if ctx.router:
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            raw_text = res.text.strip().replace('"', '').replace("'", "").lower()
            core_keywords = [k.strip() for k in raw_text.split(",") if k.strip()][:3]
    except Exception as e: logger.warning(f"Keyword extraction failed: {e}")
        
    # ABA Data for the primary keyword
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    try:
        aba_res = await asyncio.to_thread(XiyouZhaociAPI().get_aba_top_asins, "US", [core_keywords[0]])
        ctx.cache["keyword_data"] = aba_res["searchTerms"][0] if aba_res and "searchTerms" in aba_res and aba_res["searchTerms"] else {}
    except Exception as e: logger.error(f"Failed to fetch ABA data: {e}")
        
    from src.mcp.servers.amazon.extractors.search import SearchExtractor
    search_results = await SearchExtractor().search(core_keywords[0], page=1)
    sponsored_count = sum(1 for r in search_results if getattr(r, 'is_sponsored', False))
    ctx.cache["ad_ratio"] = sponsored_count / (len(search_results) or 1)
    
    # NEW: Multi-Keyword & Multi-Strategy Bid Analysis
    detailed_bids = {}
    try:
        from src.mcp.servers.amazon.ads.client import AmazonAdsClient
        store_id = ctx.config.get("store_id")
        ads_client = AmazonAdsClient(store_id=store_id)
        
        # We fetch EXACT and PHRASE for the Top 3 keywords
        # Strategies: AUTO (Up/Down) and LEGACY (Down only)
        match_types = ["EXACT", "PHRASE"]
        strategies = ["AUTO_FOR_SALES", "LEGACY_FOR_SALES"]
        
        bid_res = await asyncio.to_thread(
            ads_client.get_keyword_bid_recommendations,
            keywords=[{"keyword": kw, "matchType": m} for kw in core_keywords for m in match_types],
            asins=[(item.get("ASIN") or item.get("asin")) for item in items[:5] if (item.get("ASIN") or item.get("asin"))],
            strategy=strategies
        )
        
        # Group results by keyword for the analyzer
        for s in strategies:
            detailed_bids[s] = bid_res.get(s, {}).get("bidRecommendations", [])
            
        ctx.cache["detailed_bid_analysis"] = detailed_bids
    except Exception as e:
        logger.error(f"Failed to fetch detailed bid recommendations: {e}")
        
    ctx.cache["core_keywords"] = core_keywords
    ctx.cache["main_keyword"] = core_keywords[0]
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

async def _run_monopoly_analysis(items: List[dict], ctx: Any) -> List[dict]:
    """Calculates scores and generates flattened niche benchmarks."""
    from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
    from src.intelligence.processors.sales_estimator import SalesEstimator
    import statistics, json
    
    analyzer = CategoryMonopolyAnalyzer()
    external_data = {"social_psi": ctx.cache.get("category_social_psi"), "deal_intensity": ctx.cache.get("category_deal_intensity")}
    analysis_input = [{"rank": item.get("Rank", 999), "price": float(str(item.get("Price") or "0").replace("$", "").replace(",", "")), "sales": item.get("sales", 0), "brand": item.get("brand", "Unknown"), "seller_type": item.get("seller_type", "Unknown"), "feedback_count": item.get("feedback_count", 0), "review_count": int(str(item.get("Reviews") or "0").replace(",", "")), "rating": float(str(item.get("Rating") or "0").split(" ")[0])} for item in items]
    
    # Combined Ad Data with Multi-Keyword CPC
    detailed_bids = ctx.cache.get("detailed_bid_analysis", {})
    ad_data = {
        "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
        "actual_bsr_ad_ratio": ctx.cache.get("actual_bsr_ad_ratio"),
        "detailed_bids": detailed_bids
    }
    
    result = analyzer.analyze(analysis_input, keyword_data=ctx.cache.get("keyword_data"), ad_data=ad_data, external_data=external_data)
    
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

    prices = [p['price'] for p in analysis_input if p['price'] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    estimator = SalesEstimator()
    node_id = ctx.config.get("category_node_id")
    baseline = estimator.category_params.get(str(node_id), {}).get("market_logic", {})
    
    return [{
        "analysis_result": json.dumps(result, ensure_ascii=False), 
        "main_keyword": ctx.cache.get("main_keyword"), 
        "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
        "niche_median_price": f"${median_price:.2f}", 
        "bid_insight": " | ".join(bid_insight[:10]),
        "review_disparity": f"{round((statistics.mean([p['review_count'] for p in analysis_input[:10]]) if len(analysis_input) >= 10 else 0) / max(1, (statistics.mean([p['review_count'] for p in analysis_input[50:]]) if len(analysis_input) > 50 else 1)), 1)}x", 
        "recommended_capital": f"${int(median_price * 2500):,}", 
        "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%", 
        "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get("r_squared", 0.95), 
        "social_psi": ctx.cache.get("category_social_psi", "N/A"), 
        "social_verdict": ctx.cache.get("category_social_verdict", "N/A"), 
        "deal_intensity": ctx.cache.get("category_deal_intensity", "N/A")
    }]

async def _prepare_report_artifact(items: List[dict], ctx: Any) -> List[dict]:
    """Saves the report to a local Markdown file."""
    if not items or "deliver_report" not in items[0]: return items
    report_data = items[0]["deliver_report"]
    report_text = report_data.text if hasattr(report_data, "text") else report_data.get("text") if isinstance(report_data, dict) else str(report_data)
    if not report_text or report_text == "None": return items
    import os, tempfile
    from datetime import datetime
    keyword = str(ctx.cache.get("main_keyword", "niche")).replace(":", "").replace("*", "").replace(" ", "_")
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now().strftime('%Y%m%d_%H%M')}.md"
    file_path = os.path.normpath(os.path.join(tempfile.gettempdir(), filename))
    try:
        with open(file_path, "w", encoding="utf-8") as f: f.write(report_text)
        items[0]["report_file_path"] = file_path
        logger.info(f"Artifact prepared at: {file_path}")
    except Exception as e: logger.error(f"Failed to write report file: {e}")
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
        EnrichStep(name="enrich_sales_data", extractor_fn=_enrich_sales, parallel=True, concurrency=10),
        EnrichStep(name="enrich_seller_background", extractor_fn=_enrich_seller_info, parallel=True, concurrency=5),
        ProcessStep(name="fetch_market_context", fn=_fetch_market_context),
        ProcessStep(name="enrich_external_intensity", fn=_enrich_external_intensity),
        ProcessStep(name="enrich_batch_traffic_scores", fn=_enrich_batch_traffic_scores),
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
                "### DATA: {analysis_result}\n\n"
                "### ADDITIONAL TACTICAL RULES\n"
                "- 400-550 words. No filler.\n"
                "- ANALYZE BID BARRIERS: Compare the suggested CPC to the median price. If CPC > 10% of median price, highlight extreme capital risk.\n"
                "- Identify which specific keywords are 'High Barrier' and if PHRASE/EXACT gaps offer opportunities."
            ),
            compute_target=ComputeTarget.CLOUD_LLM
        ),
        ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact)
    ])
