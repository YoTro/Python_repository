from __future__ import annotations
import logging
import statistics
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class PromoAnalyzer:
    """
    Intelligence Processor to analyze off-Amazon deal history.
    Calculates metrics like promo frequency, median discount, and dependency score.
    """
    
    def analyze(self, current_price: float, deals: List[Dict[str, Any]], days_analyzed: int = 180) -> Dict[str, Any]:
        """
        Analyzes historical deals to evaluate price stability and promotion reliance.
        """
        if not deals:
            return {
                "promo_frequency": 0.0,
                "all_time_low": current_price,
                "median_discount_pct": 0.0,
                "promo_dependency_score": 0.0,
                "risk_level": "Low (Stable Price)",
                "total_deals_found": 0
            }

        prices = [d.get("price", current_price) for d in deals if d.get("price")]
        discounts = [d.get("discount_pct", 0) for d in deals if d.get("discount_pct")]

        all_time_low = min(prices) if prices else current_price
        median_discount = statistics.median(discounts) if discounts else 0
        
        # Rough frequency: number of deals / days analyzed.
        # Assuming each deal lasts ~3 days on average. Max frequency capped at 1.0
        promo_frequency = min(1.0, (len(deals) * 3) / days_analyzed)

        # Promo dependency score (0-100)
        # High frequency + deep discount = highly dependent on external promotions
        dependency_score = (promo_frequency * 50) + (min(median_discount, 50))
        
        risk_level = "Low (Stable Price)"
        if dependency_score >= 70:
            risk_level = "High (Price War/Clearance)"
        elif dependency_score >= 40:
            risk_level = "Medium (Regular Promotions)"

        return {
            "promo_frequency": round(promo_frequency, 3),
            "all_time_low": round(all_time_low, 2),
            "median_discount_pct": round(median_discount, 2),
            "promo_dependency_score": round(dependency_score, 2),
            "risk_level": risk_level,
            "total_deals_found": len(deals)
        }
