from __future__ import annotations
import logging
import statistics
from typing import List, Dict, Any, Optional
from src.workflows.config import merge_config

logger = logging.getLogger(__name__)

class CategoryMonopolyAnalyzer:
    """
    Processor to calculate monopoly and competition scores for an Amazon category
    based on Top 100 BSR data, seller details, and keyword traffic.
    """

    def __init__(self, custom_weights: Optional[Dict[str, float]] = None):
        config = merge_config("category_monopoly_analysis")
        self.weights = custom_weights or config.get("weights", {})
        self.thresholds = config.get("thresholds", {})

    def analyze(self, 
                products: List[Dict[str, Any]], 
                keyword_data: Optional[Dict[str, Any]] = None,
                ad_data: Optional[Dict[str, Any]] = None,
                external_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main analysis entry point.
        """
        if not products:
            return {"error": "No product data provided for analysis."}

        sorted_products = sorted(products, key=lambda x: x.get("rank", 999))
        
        sales_scores = self._analyze_sales_distribution(sorted_products)
        brand_score = self._analyze_brand_concentration(sorted_products)
        seller_score = self._analyze_seller_background(sorted_products)
        review_score = self._analyze_review_barrier(sorted_products)
        price_score = self._analyze_price_convergence(sorted_products)
        keyword_score = self._analyze_keyword_monopoly(keyword_data)
        ad_score = self._analyze_ad_competition(ad_data)
        social_score, deal_score = self._analyze_external_intensity(external_data)

        metrics = {
            "sales_curve_top3": sales_scores["top3_concentration"], "sales_survival_space": sales_scores["survival_space"],
            "brand_concentration": brand_score, "seller_background": seller_score, "review_curve": review_score,
            "keyword_traffic": keyword_score, "price_compression": price_score, "ad_traffic_ratio": ad_score,
            "social_promotion_intensity": social_score, "deal_promotion_intensity": deal_score
        }

        total_score, details = self._calculate_weighted_score(metrics)
        status = self._interpret_score(total_score)

        return {
            "overall_score": round(total_score, 2), "status": status, "dimension_details": details,
            "summary_metrics": {
                "cr3": sales_scores.get("cr3"), "price_cv": sales_scores.get("price_cv"),
                "avg_rating": sales_scores.get("avg_rating")
            },
            "niche_benchmarks": {
                "median_price": statistics.median([p.get("price", 0) for p in products if p.get("price", 0) > 0]) if products else 0,
                "avg_reviews_top10": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[:10]])) if len(sorted_products) >= 10 else 0,
                "avg_reviews_bottom50": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[50:]])) if len(sorted_products) > 50 else 0,
                "total_estimated_monthly_sales": int(sum(p.get("sales", 0) for p in products))
            }
        }

    def _calculate_weighted_score(self, metrics: Dict[str, float]) -> tuple[float, Dict[str, Any]]:
        total_score, details = 0.0, {}
        for key, value in metrics.items():
            weight = self.weights.get(key, 0.0)
            contribution = value * weight
            total_score += contribution
            details[key] = {"raw_score": round(value, 2), "weight": weight, "weighted_contribution": round(contribution, 2)}
        return total_score, details

    def _interpret_score(self, score: float) -> str:
        if score >= self.thresholds.get("high_monopoly_score", 75): return "High Monopoly (Red Ocean)"
        if score <= self.thresholds.get("opportunity_score", 40): return "Low Monopoly (Blue Ocean/Opportunity)"
        return "Medium Competition"

    def _analyze_sales_distribution(self, products: List[Dict[str, Any]]) -> Dict[str, float]:
        total_sales = sum(p.get("sales", 0) for p in products) or 1
        top3_sales = sum(p.get("sales", 0) for p in products[:3])
        survival_sales = sum(p.get("sales", 0) for p in products[19:50])
        cr3 = top3_sales / total_sales
        cr3_limit = self.thresholds.get("cr3_monopoly_limit", 0.60)
        conc_score = min(100, (cr3 / cr3_limit) * 100)
        survival_ratio = survival_sales / total_sales
        survival_score = max(0, 100 - (survival_ratio / 0.20) * 100)
        return {"top3_concentration": conc_score, "survival_space": survival_score, "cr3": round(cr3, 4)}

    def _analyze_brand_concentration(self, products: List[Dict[str, Any]]) -> float:
        brands = [p.get("brand") for p in products if p.get("brand")]
        if not brands: return 50
        counts = {b: brands.count(b) for b in set(brands)}
        brand_ratio = len(counts) / len(products)
        return max(0, 100 - (brand_ratio * 150))

    def _analyze_seller_background(self, products: List[Dict[str, Any]]) -> float:
        amazon_count = sum(1 for p in products if p.get("seller_type") in ["Amazon", "AMZ", "Retail"])
        mega_seller_feedback = self.thresholds.get("mega_seller_feedback", 10000)
        large_seller_count = sum(1 for p in products if p.get("feedback_count", 0) > mega_seller_feedback)
        amz_ratio = amazon_count / len(products)
        large_ratio = large_seller_count / len(products)
        return min(100, (amz_ratio * 300) + (large_ratio * 100))

    def _analyze_review_barrier(self, products: List[Dict[str, Any]]) -> float:
        if len(products) < 20: return 50
        top_10 = products[:10]
        bottom_50 = products[49:] if len(products) > 50 else products[len(products)//2:]
        avg_reviews_top = statistics.mean([p.get("review_count", 0) for p in top_10]) or 1
        avg_reviews_bottom = statistics.mean([p.get("review_count", 0) for p in bottom_50]) or 1
        review_disparity = avg_reviews_top / avg_reviews_bottom
        disparity_threshold = self.thresholds.get("review_disparity_threshold", 5.0)
        review_score = min(100, (review_disparity / disparity_threshold) * 100)
        avg_rating_top = statistics.mean([p.get("rating", 0) for p in top_10]) or 4.0
        rating_cap = self.thresholds.get("rating_hard_barrier", 4.5)
        rating_score = max(0, (avg_rating_top - 4.0) / (rating_cap - 4.0) * 100) if avg_rating_top > 4.0 else 0
        return (review_score * 0.7) + (rating_score * 0.3)

    def _analyze_price_convergence(self, products: List[Dict[str, Any]]) -> float:
        prices = [p.get("price") for p in products if p.get("price")]
        if len(prices) < 5: return 50
        avg_price, std_dev = statistics.mean(prices), statistics.stdev(prices)
        cv = std_dev / avg_price
        cv_threshold = self.thresholds.get("price_cv_compression", 0.15)
        if cv < cv_threshold: return 100
        return max(0, 100 - (cv / 0.6 * 100))

    def _analyze_keyword_monopoly(self, keyword_data: Optional[Dict[str, Any]]) -> float:
        if not keyword_data or "top_asins" not in keyword_data: return 50
        top3_shares = sum(item.get("clickShare", 0) for item in keyword_data["top_asins"][:3])
        return min(100, (top3_shares / 0.50) * 100)

    def _analyze_ad_competition(self, ad_data: Optional[Dict[str, Any]]) -> float:
        if not ad_data: return 50
        
        # 1. Ad Ratio Score (Visibility competition)
        ratio = ad_data.get("ad_ratio", 0.3)
        danger_zone = self.thresholds.get("ad_ratio_danger_zone", 0.40)
        ratio_score = min(100, (ratio / danger_zone) * 100)
        
        # 2. Detailed Bid Analysis (Capital barrier)
        detailed_bids = ad_data.get("detailed_bids", {})
        if detailed_bids:
            bid_barrier_score = self._calculate_bid_barrier_score(detailed_bids)
        else:
            # Fallback to single avg_bid if detailed data is missing
            bid = ad_data.get("avg_bid", 0)
            high_bid_threshold = self.thresholds.get("high_bid_barrier", 2.50)
            bid_barrier_score = min(100, (bid / high_bid_threshold) * 100) if bid > 0 else 50
        
        # Combined score: 40% ratio (current heat), 60% bid (capital barrier/monopoly)
        return (ratio_score * 0.4) + (bid_barrier_score * 0.6)

    def _calculate_bid_barrier_score(self, detailed_bids: Dict[str, Any]) -> float:
        """
        Calculates a barrier score based on multiple keywords and match types.
        Identifies high-barrier keywords.
        """
        all_suggested_bids = []
        high_barrier_keywords = []
        
        # Process Legacy for Sales as the most conservative/baseline strategy
        legacy_recs = detailed_bids.get("LEGACY_FOR_SALES", [])
        for rec in legacy_recs:
            for expr in rec.get("bidRecommendationsForTargetingExpressions", []):
                bid = expr.get("suggestedBid", {}).get("amount", 0)
                kw = expr.get("targetingExpression", {}).get("value", "unknown")
                m_type = expr.get("targetingExpression", {}).get("type", "unknown")
                
                if bid > 0:
                    all_suggested_bids.append(bid)
                    # Threshold for a single high-barrier keyword
                    if bid > 2.80:
                        high_barrier_keywords.append(f"{kw} ({m_type}): ${bid:.2f}")

        if not all_suggested_bids:
            return 50.0

        avg_bid = statistics.mean(all_suggested_bids)
        # 3.0 USD as a benchmark for high-competition barrier in US marketplace
        barrier_threshold = self.thresholds.get("high_bid_barrier", 3.0)
        
        score = min(100, (avg_bid / barrier_threshold) * 100)
        # Bonus penalty if multiple keywords are high-barrier
        if len(set(high_barrier_keywords)) >= 2:
            score = min(100, score + 15)
            
        return score

    def _analyze_external_intensity(self, external_data: Optional[Dict[str, Any]]) -> tuple[float, float]:
        if not external_data: return 50.0, 50.0
        social_psi = external_data.get("social_psi", 0)
        social_score = min(100, social_psi)
        deal_intensity = external_data.get("deal_intensity", 0)
        deal_score = min(100, deal_intensity * 10)
        return social_score, deal_score
