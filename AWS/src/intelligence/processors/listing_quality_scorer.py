from __future__ import annotations

import logging
from typing import Any

from src.core.models.product import Product

logger = logging.getLogger(__name__)


class ListingQualityScorer:
    """
    Pure compute processor to evaluate Amazon Listing quality based on
    standard operating procedures (SOP).
    Uses a weighted modular approach for better transparency and flexibility.
    """

    DEFAULT_WEIGHTS = {
        "title": 0.25,
        "features": 0.20,
        "media": 0.20,
        "social_proof": 0.20,
        "fulfillment_aplus": 0.15,
    }

    def score(
        self,
        product: Product,
        required_keywords: list[str] | None = None,
        keyword_config: dict[str, list[str]] | None = None,
        image_metadata: dict[str, dict[str, Any]] | None = None,
        video_metadata: dict[str, dict[str, Any]] | None = None,
        weights: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        """
        Scores a product listing from 0 to 100 based on weighted modules.
        """
        logger.info(f"Scoring product listing for ASIN: {product.asin}")
        weights = weights or self.DEFAULT_WEIGHTS

        # Calculate individual module scores (each out of 100)
        title_res = self._score_title(product, required_keywords, keyword_config)
        features_res = self._score_features(product, required_keywords, keyword_config)
        media_res = self._score_media(product, image_metadata, video_metadata)
        social_res = self._score_social_proof(product)
        fulfillment_res = self._score_fulfillment_aplus(product)

        # Weighted Sum
        final_score = (
            title_res["score"] * weights["title"]
            + features_res["score"] * weights["features"]
            + media_res["score"] * weights["media"]
            + social_res["score"] * weights["social_proof"]
            + fulfillment_res["score"] * weights["fulfillment_aplus"]
        )

        all_issues = (
            title_res["issues"]
            + features_res["issues"]
            + media_res["issues"]
            + social_res["issues"]
            + fulfillment_res["issues"]
        )

        return {
            "asin": product.asin,
            "overall_quality_score": round(max(0, final_score), 1),
            "status": "Excellent"
            if final_score >= 90
            else "Good"
            if final_score >= 75
            else "Poor"
            if final_score >= 50
            else "Critical",
            "module_scores": {
                "title": title_res["score"],
                "bullet_points": features_res["score"],
                "media": media_res["score"],
                "social_proof": social_res["score"],
                "fulfillment_aplus": fulfillment_res["score"],
            },
            "improvement_plan": all_issues,
            "metrics": {
                **title_res["metrics"],
                **features_res["metrics"],
                **media_res["metrics"],
                **social_res["metrics"],
                **fulfillment_res["metrics"],
            },
        }

    def _score_title(self, product: Product, required_keywords, keyword_config) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        title = product.title or ""
        title_len = len(title)
        brand = product.brand or ""
        words = title.split()

        # Length
        if title_len < 50:
            score -= 20
            issues.append("Title extremely short (< 50 chars).")
        elif title_len < 80:
            score -= 10
            issues.append("Title short (50-80 chars).")
        elif 80 <= title_len <= 120:
            score += 5
        elif 120 < title_len <= 150:
            score -= 5
            issues.append("Title a bit long (120-150 chars).")
        elif title_len > 150:
            score -= 15
            issues.append("Title too long (> 150 chars).")

        # Integrity (4 Pillars)
        integrity_score = 0
        if brand and brand.lower() in title.lower():
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Brand.")

        if len(words) > (len(brand.split()) if brand else 0) + 1:
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Product type.")

        feature_indicators = [
            "premium",
            "durable",
            "waterproof",
            "portable",
            "lightweight",
            "heavy-duty",
        ]
        if any(ind in title.lower() for ind in feature_indicators) or len(words) > 8:
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Core features.")

        has_specs = any(char.isdigit() for char in title) and any(
            unit in title.lower()
            for unit in [
                "oz",
                "lb",
                "pack",
                "count",
                "ml",
                "g",
                "kg",
                "inch",
                "cm",
                "mm",
                "v",
                "w",
                "ah",
            ]
        )
        if has_specs:
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Specifications.")

        metrics["title_integrity_score"] = integrity_score
        metrics["title_length"] = title_len

        # Readability & Keywords
        if any(sym in title for sym in ["!", "@", "$", "%", "*", "++", "?"]):
            score -= 5
            issues.append("Title contains spammy symbols.")
        if words and not words[0][0].isupper():
            score -= 3
            issues.append("Title should start with uppercase.")

        if keyword_config:
            for cat, pts in [("core", 10), ("modifiers", 5), ("scenes", 3)]:
                missing = [
                    kw for kw in keyword_config.get(cat, []) if kw.lower() not in title.lower()
                ]
                if missing:
                    score -= len(missing) * pts
                    issues.append(f"Title missing {cat.upper()} keywords: {', '.join(missing)}")

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_features(
        self, product: Product, required_keywords, keyword_config
    ) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        features = product.features or []
        feature_count = len(features)
        feature_text = " ".join(features).lower()
        metrics["bullet_count"] = feature_count

        if feature_count == 0:
            return {"score": 0, "issues": ["Missing all bullet points."], "metrics": metrics}
        if feature_count < 5:
            score -= 10
            issues.append(f"Only {feature_count} bullet points (Optimal: 5).")

        # Logic
        if feature_count >= 3:
            core_inds = [
                "easy",
                "high",
                "best",
                "perfect",
                "solution",
                "save",
                "efficient",
                "protect",
            ]
            if not any(ind in " ".join(features[:3]).lower() for ind in core_inds):
                score -= 5
                issues.append("First 3 bullets should highlight core functions/pain points.")

        if feature_count >= 5:
            after_inds = [
                "guarantee",
                "warranty",
                "service",
                "material",
                "made of",
                "fabric",
                "aluminum",
                "steel",
                "refund",
                "replacement",
            ]
            if not any(ind in " ".join(features[3:5]).lower() for ind in after_inds):
                score -= 5
                issues.append("Last 2 bullets should cover materials/after-sales.")

        # Keywords & Formatting
        main_kws = keyword_config.get("core", []) if keyword_config else (required_keywords or [])
        for kw in main_kws:
            count = feature_text.count(kw.lower())
            if count < 3:
                score -= 3
                issues.append(f"Keyword '{kw}' density too low (<3).")
            elif count > 6:
                score -= 5
                issues.append(f"Keyword '{kw}' density too high (>6).")

        for i, feat in enumerate(features):
            feat = feat.strip()
            if len(feat) < 20:
                score -= 3
                issues.append(f"Bullet {i + 1} too short.")
            if len(feat) > 500:
                score -= 2
                issues.append(f"Bullet {i + 1} too long.")
            if feat and not feat[0].isupper():
                score -= 2
                issues.append(f"Bullet {i + 1} not capitalized.")
            if feat and feat[-1] in [".", "!", "?", ";"]:
                score -= 1
                issues.append(f"Bullet {i + 1} has trailing punctuation.")
            if "  " in feat:
                score -= 1
                issues.append(f"Bullet {i + 1} has double spaces.")

        vague = [
            t
            for t in ["good product", "high quality", "best choice", "nice item"]
            if t in feature_text
        ]
        if vague:
            score -= len(vague) * 2
            issues.append(f"Bullets contain vague phrases: {', '.join(vague)}.")

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_media(self, product: Product, image_metadata, video_metadata) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        images = product.images or []
        image_count = len(images)
        metrics["image_count"] = image_count
        metrics["has_video"] = len(product.videos or []) > 0

        if image_count < 7:
            score -= (7 - image_count) * 3
            issues.append(f"Only {image_count} images (Optimal: 7).")

        if image_metadata:
            main_meta = image_metadata.get(product.main_image_url, {})
            if main_meta and not main_meta.get("is_pure_white_bg", True):
                score -= 15
                issues.append("Main image lacks pure white background.")
            if main_meta and main_meta.get("has_text_or_watermark", False):
                score -= 15
                issues.append("Main image has text/watermark.")

            low_res = sum(1 for url in images if image_metadata.get(url, {}).get("width", 0) < 1000)
            if low_res:
                score -= low_res * 2
                issues.append(f"{low_res} images under 1000px resolution.")

        if not product.videos:
            score -= 10
            issues.append("Missing video.")
        elif video_metadata:
            for v_url in product.videos:
                v_meta = video_metadata.get(v_url, {})
                if v_meta.get("duration_seconds", 0) < 15:
                    score -= 3
                    issues.append("Video too short (<15s).")
                if v_meta.get("resolution_height", 0) < 720:
                    score -= 5
                    issues.append("Video resolution too low (<720p).")

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_social_proof(self, product: Product) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        rating = product.rating or 0
        reviews = product.review_count or 0

        if rating < 4.0:
            score -= 20
            issues.append(f"Low rating ({rating}).")
        elif rating < 4.3:
            score -= 5
            issues.append(f"Sub-optimal rating ({rating}).")
        if reviews < 20:
            score -= 10
            issues.append("Very few reviews (<20).")

        if product.rating_breakdown:
            neg = product.rating_breakdown.get(1, 0) + product.rating_breakdown.get(2, 0)
            if neg > 15:
                score -= 10
                issues.append(f"High negative rating ratio ({neg}%).")

        if product.vp_review_ratio is not None and product.vp_review_ratio < 0.7:
            score -= 10
            issues.append("Low Verified Purchase ratio.")
        if product.recent_rating_avg is not None and product.recent_rating_avg < rating - 0.3:
            score -= 15
            issues.append("Declining recent rating trend.")
        if product.sentiment_score is not None:
            if product.sentiment_score < 0:
                score -= 10
                issues.append("Negative review sentiment.")
            elif product.sentiment_score > 0.6:
                score += 5

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_fulfillment_aplus(self, product: Product) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {"is_fba": product.is_fba, "has_a_plus": product.has_a_plus_content}

        if not product.is_fba:
            score -= 15
            issues.append("Not FBA (Seller Fulfilled).")
        if not product.has_a_plus_content:
            score -= 20
            issues.append("Missing A+ Content.")

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def batch_score(self, products: list[Product]) -> list[dict[str, Any]]:
        logger.info(f"Batch scoring {len(products)} products...")
        return [self.score(p) for p in products]
