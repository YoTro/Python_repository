import os
import sys
import json
import logging
from dotenv import load_dotenv

# Ensure project root is in path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Load .env
load_dotenv(os.path.join(project_root, ".env"))

from src.mcp.servers.amazon.ads.client import AmazonAdsClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test-amazon-ads-v5")

def test_v5_bids_comprehensive(keyword: str, asin: str, strategy: str = "LEGACY_FOR_SALES"):
    """
    Test Amazon Ads v5.0 with specific strategy and includeAnalysis=True.
    """
    store_id = os.getenv("AMAZON_ADS_DEFAULT_STORE", "US")
    logger.info(f"--- COMPREHENSIVE V5 TEST ---")
    logger.info(f"Keyword: {keyword} | ASIN: {asin} | Strategy: {strategy}")
    
    try:
        client = AmazonAdsClient(store_id=store_id)
        
        # Try PHRASE match to see variation
        keywords_payload = [{"keyword": keyword, "matchType": "PHRASE"}]
        
        # Call API with analysis enabled
        result = client.get_keyword_bid_recommendations(
            keywords=keywords_payload,
            asins=[asin],
            include_analysis=True, # Enable the advanced analyzer
            strategy=strategy
        )
        
        # 1. Print Standard Recommendations
        print("\n" + "="*90)
        print(f"STANDARD RECOMMENDATIONS (Strategy: {strategy})")
        print("="*90)
        
        bid_recs = result.get("bidRecommendations", [])
        for theme_group in bid_recs:
            theme = theme_group.get("theme")
            expr_recs = theme_group.get("bidRecommendationsForTargetingExpressions", [])
            for item in expr_recs:
                bids = item.get("bidValues", [])
                low = bids[0].get("suggestedBid") if len(bids) > 0 else "-"
                sug = bids[1].get("suggestedBid") if len(bids) > 1 else "-"
                high = bids[2].get("suggestedBid") if len(bids) > 2 else "-"
                print(f"Theme: {theme:25} | Suggested: {sug:6} | Range: {low} - {high}")

        # 2. Print Bid Analysis (if available)
        print("\n" + "="*90)
        print(f"ADVANCED BID ANALYSIS (Estimated Impressions)")
        print("="*90)
        for theme_group in bid_recs:
            analyses = theme_group.get("bidAnalysesForTargetingExpressions", [])
            for a in analyses:
                points = a.get("bidAnalyses", {}).get("ALL", [])
                print(f"{'Bid Price':<10} | {'Type':<15} | {'Est. Impression (Avg)':<20}")
                print("-" * 50)
                for p in points:
                    print(f"${p.get('bid'):<9} | {p.get('type'):<15} | {p.get('impactMetrics', {}).get('estimatedImpressionAvg'):<20}")

        print("="*90 + "\n")
        
    except Exception as e:
        logger.error(f"Comprehensive Test Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    target_kw = sys.argv[1] if len(sys.argv) > 1 else "Thermacell"
    target_asin = sys.argv[2] if len(sys.argv) > 2 else "B0FXFGMD7Z"
    # Testing "Down only" strategy
    test_v5_bids_comprehensive(target_kw, target_asin, strategy="LEGACY_FOR_SALES")
