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
        # Load defaults from config
        config = merge_config("category_monopoly_analysis")
        self.weights = custom_weights or config.get("weights", {})
        self.thresholds = config.get("thresholds", {})

    def analyze(self, 
                products: List[Dict[str, Any]], 
                keyword_data: Optional[Dict[str, Any]] = None,
                ad_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main analysis entry point.
        :param products: List of Top 100 products with keys like: price, sales, brand, seller_type, feedback_count, review_count, rating.
        :param keyword_data: ABA data for the main keyword.
        :param ad_data: Data about Sponsored vs Organic ratio in search.
        """
        if not products:
            return {"error": "No product data provided for analysis."}

        # Ensure products are sorted by BSR rank
        sorted_products = sorted(products, key=lambda x: x.get("rank", 999))
        
        # 1. Sales Distribution (CR3 and Survival Space)
        sales_scores = self._analyze_sales_distribution(sorted_products)
        
        # 2. Brand Concentration
        brand_score = self._analyze_brand_concentration(sorted_products)
        
        # 3. Seller Background
        seller_score = self._analyze_seller_background(sorted_products)
        
        # 4. Review & Barrier
        review_score = self._analyze_review_barrier(sorted_products)
        
        # 5. Price Band Convergence (CV)
        price_score = self._analyze_price_convergence(sorted_products)
        
        # 6. Keyword Traffic Monopoly (ABA)
        keyword_score = self._analyze_keyword_monopoly(keyword_data)
        
        # 7. Ad Traffic Competition
        ad_score = self._analyze_ad_competition(ad_data)

        # Final Weighted Calculation
        metrics = {
            "sales_curve_top3": sales_scores["top3_concentration"],
            "sales_survival_space": sales_scores["survival_space"],
            "brand_concentration": brand_score,
            "seller_background": seller_score,
            "review_curve": review_score,
            "keyword_traffic": keyword_score,
            "price_compression": price_score,
            "ad_traffic_ratio": ad_score
        }

        total_score = 0.0
        details = {}
        
        for key, value in metrics.items():
            weight = self.weights.get(key, 0.0)
            contribution = value * weight
            total_score += contribution
            details[key] = {
                "raw_score": round(value, 2),
                "weight": weight,
                "weighted_contribution": round(contribution, 2)
            }

        # Interpret the result
        status = "Medium Competition"
        if total_score >= self.thresholds.get("high_monopoly_score", 75):
            status = "High Monopoly (Red Ocean)"
        elif total_score <= self.thresholds.get("opportunity_score", 40):
            status = "Low Monopoly (Blue Ocean/Opportunity)"

        return {
            "overall_score": round(total_score, 2),
            "status": status,
            "dimension_details": details,
            "summary_metrics": {
                "cr3": sales_scores.get("cr3"),
                "price_cv": sales_scores.get("price_cv"),
                "avg_rating": sales_scores.get("avg_rating")
            },
            "niche_benchmarks": {
                "median_price": statistics.median([p.get("price", 0) for p in products if p.get("price", 0) > 0]) if products else 0,
                "avg_reviews_top10": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[:10]])) if len(sorted_products) >= 10 else 0,
                "avg_reviews_bottom50": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[50:]])) if len(sorted_products) > 50 else 0,
                "total_estimated_monthly_sales": int(sum(p.get("sales", 0) for p in products))
            }
        }

    def _analyze_sales_distribution(self, products: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calculates CR3 (Top 3 share) and Middle Market survival."""
        total_sales = sum(p.get("sales", 0) for p in products) or 1
        top3_sales = sum(p.get("sales", 0) for p in products[:3])
        survival_sales = sum(p.get("sales", 0) for p in products[19:50])
        
        cr3 = top3_sales / total_sales
        cr3_limit = self.thresholds.get("cr3_monopoly_limit", 0.60)
        
        # Score 0-100: Higher CR3 -> Higher Monopoly Score
        # If top 3 occupy > 60%, it is highly monopolized.
        conc_score = min(100, (cr3 / cr3_limit) * 100)
        
        # Survival space: If 20-50 ranks still have > 20% total share, it is healthy.
        survival_ratio = survival_sales / total_sales
        survival_score = max(0, 100 - (survival_ratio / 0.20) * 100)

        return {
            "top3_concentration": conc_score,
            "survival_space": survival_score,
            "cr3": round(cr3, 4)
        }

    def _analyze_brand_concentration(self, products: List[Dict[str, Any]]) -> float:
        """Frequency of brands in Top 100."""
        brands = [p.get("brand") for p in products if p.get("brand")]
        if not brands: return 50
        
        counts = {}
        for b in brands:
            counts[b] = counts.get(b, 0) + 1
            
        unique_brands = len(counts)
        # Ratio of brands per product. If 100 products have 10 brands, ratio is 0.1 (High Monopoly)
        brand_ratio = unique_brands / len(products)
        
        # Lower ratio (fewer brands) -> Higher Score
        return max(0, 100 - (brand_ratio * 150)) # Weighted multiplier

    def _analyze_seller_background(self, products: List[Dict[str, Any]]) -> float:
        """Analyze Amazon Retail vs 3P and large sellers."""
        amazon_count = sum(1 for p in products if p.get("seller_type") in ["Amazon", "AMZ", "Retail"])
        
        # Pull threshold from config, fallback to 10000
        mega_seller_feedback = self.thresholds.get("mega_seller_feedback", 10000)
        large_seller_count = sum(1 for p in products if p.get("feedback_count", 0) > mega_seller_feedback)
        
        amz_ratio = amazon_count / len(products)
        large_ratio = large_seller_count / len(products)
        
        # Score influenced by AMZ presence and massive sellers
        return min(100, (amz_ratio * 300) + (large_ratio * 100))

    def _analyze_review_barrier(self, products: List[Dict[str, Any]]) -> float:
        """Barrier to entry based on relative review disparity and ratings."""
        if len(products) < 20:
            return 50 # Not enough data for relative disparity
            
        top_10 = products[:10]
        # Use products from rank 50 onwards as the "tail/new entrants" baseline
        bottom_50 = products[49:] if len(products) > 50 else products[len(products)//2:]
        
        avg_reviews_top = statistics.mean([p.get("review_count", 0) for p in top_10]) or 1
        avg_reviews_bottom = statistics.mean([p.get("review_count", 0) for p in bottom_50]) or 1
        
        # Calculate disparity multiplier (e.g., Top 10 has 10x more reviews than tail)
        review_disparity = avg_reviews_top / avg_reviews_bottom
        disparity_threshold = self.thresholds.get("review_disparity_threshold", 5.0)
        
        # If disparity is huge (e.g., > 5x), the barrier is extremely high
        review_score = min(100, (review_disparity / disparity_threshold) * 100)
        
        avg_rating_top = statistics.mean([p.get("rating", 0) for p in top_10]) or 4.0
        rating_cap = self.thresholds.get("rating_hard_barrier", 4.5)
        
        # If avg rating is > 4.5, there is no room for "quality improvement"
        # We look at Top 10 specifically because that's what a new entrant competes against
        rating_score = max(0, (avg_rating_top - 4.0) / (rating_cap - 4.0) * 100) if avg_rating_top > 4.0 else 0
        
        return (review_score * 0.7) + (rating_score * 0.3)

    def _analyze_price_convergence(self, products: List[Dict[str, Any]]) -> float:
        """Price convergence using CV (Coefficient of Variation)."""
        prices = [p.get("price") for p in products if p.get("price")]
        if len(prices) < 5: return 50
        
        avg_price = statistics.mean(prices)
        std_dev = statistics.stdev(prices)
        cv = std_dev / avg_price
        
        cv_threshold = self.thresholds.get("price_cv_compression", 0.15)
        # Lower CV (CV < 0.15) -> Intense price convergence -> Higher Score
        if cv < cv_threshold:
            return 100 # Red zone: Price war
        
        # CV > 0.6 indicates diverse niches/price bands
        return max(0, 100 - (cv / 0.6 * 100))

    def _analyze_keyword_monopoly(self, keyword_data: Optional[Dict[str, Any]]) -> float:
        """ABA Traffic share concentration."""
        if not keyword_data or "top_asins" not in keyword_data:
            return 50 # Unknown
            
        # Top 3 ASINs click share sum
        top3_shares = sum(item.get("clickShare", 0) for item in keyword_data["top_asins"][:3])
        # If Top 3 capture > 50% of clicks for main keyword
        return min(100, (top3_shares / 0.50) * 100)

    def _analyze_ad_competition(self, ad_data: Optional[Dict[str, Any]]) -> float:
        """Percentage of Sponsored products."""
        if not ad_data or "ad_ratio" not in ad_data:
            return 50 # Unknown
            
        ratio = ad_data["ad_ratio"]
        danger_zone = self.thresholds.get("ad_ratio_danger_zone", 0.40)
        
        # Ratio > 40% -> High CAC -> Higher Score
        return min(100, (ratio / danger_zone) * 100)
