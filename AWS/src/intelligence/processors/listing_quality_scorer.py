from __future__ import annotations
import logging
import re
from typing import List, Dict, Any, Optional
from src.core.models.product import Product

logger = logging.getLogger(__name__)

class ListingQualityScorer:
    """
    Pure compute processor to evaluate Amazon Listing quality based on 
    standard operating procedures (SOP). 
    No LLM required; uses deterministic rules.
    """

    def score(self, product: Product) -> Dict[str, Any]:
        """
        Scores a product listing from 0 to 100.
        Returns the score and a list of specific improvement suggestions.
        """
        score = 100
        issues = []

        # 1. Title Analysis
        title = product.title or ""
        title_len = len(title)
        
        # Rule: Optimal length is 80-120 characters
        if title_len < 50:
            score -= 15
            issues.append("Title is too short (less than 50 chars). Reduces keyword indexing.")
        elif title_len > 150:
            score -= 5
            issues.append("Title is too long (over 150 chars). May be truncated on mobile.")
            
        # Rule: No all-caps words (except brand/acronyms)
        if any(word.isupper() and len(word) > 3 for word in title.split()[:5]):
            score -= 5
            issues.append("Title contains all-caps words in the first few words. Looks spammy.")

        # 2. Features (Bullet Points) Analysis
        features = product.features or []
        feature_count = len(features)
        
        if feature_count == 0:
            score -= 25
            issues.append("Missing bullet points (Features). Critical for conversion.")
        elif feature_count < 5:
            score -= 10
            issues.append(f"Only {feature_count} bullet points found. Amazon allows 5.")
            
        # Rule: Bullet points should be substantive but not huge blocks
        for i, feat in enumerate(features):
            if len(feat) < 20:
                score -= 3
                issues.append(f"Bullet point {i+1} is very short. Missed marketing opportunity.")
            if len(feat) > 500:
                score -= 2
                issues.append(f"Bullet point {i+1} is too long. Customers rarely read blocks > 500 chars.")

        # 3. Media Analysis (Images/Video)
        images = product.images or []
        image_count = len(images)
        if image_count < 7:
            score -= (7 - image_count) * 3
            issues.append(f"Only {image_count} images found. 7+ images (including video) is optimal.")
            
        if not product.videos:
            score -= 10
            issues.append("No video found in listing. Significantly impacts conversion rate.")

        # 4. Rating & Social Proof
        rating = product.rating or 0
        reviews = product.review_count or 0
        
        if rating < 4.0 and reviews > 0:
            score -= 20
            issues.append(f"Low rating ({rating}). Below 4.0 is a conversion killer.")
        elif rating < 4.3:
            score -= 5
            issues.append(f"Sub-optimal rating ({rating}). Aim for 4.3+.")
            
        if reviews < 20:
            score -= 10
            issues.append("Very low review count (less than 20). High risk for customers.")

        # 5. Inventory & Fulfillment
        if not product.is_fba:
            score -= 15
            issues.append("Product is FBM (Seller Fulfilled). Loses Prime badge advantage.")

        return {
            "asin": product.asin,
            "overall_quality_score": max(0, score),
            "status": "Excellent" if score >= 90 else "Good" if score >= 75 else "Poor" if score >= 50 else "Critical",
            "improvement_plan": issues,
            "metrics": {
                "title_length": title_len,
                "bullet_count": feature_count,
                "image_count": image_count,
                "has_video": len(product.videos or []) > 0,
                "is_fba": product.is_fba
            }
        }

    def batch_score(self, products: List[Product]) -> List[Dict[str, Any]]:
        return [self.score(p) for p in products]
