from __future__ import annotations

import logging
import re
from collections import Counter
from typing import Any

from src.core.models.product import Product
from src.core.models.review import ReviewSummary

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (defined once, shared across all scorer instances)
# ---------------------------------------------------------------------------

LISTING_STOP_WORDS: frozenset[str] = frozenset(
    {
        "for",
        "with",
        "and",
        "or",
        "the",
        "a",
        "an",
        "to",
        "in",
        "of",
        "by",
        "on",
        "at",
        "from",
        "into",
        "your",
        "our",
        "new",
        "best",  # marketing words with no keyword-indexing value
    }
)

# Module-private alias — existing internal code uses _STOP_WORDS.
_STOP_WORDS = LISTING_STOP_WORDS

# Units that imply a meaningful numeric specification in the title
_SPEC_UNITS: frozenset[str] = frozenset(
    {
        "oz",
        "lb",
        "lbs",
        "pack",
        "count",
        "ct",
        "ml",
        "g",
        "kg",
        "inch",
        "inches",
        "in",
        "cm",
        "mm",
        "ft",
        "v",
        "w",
        "watt",
        "ah",
        "mah",
        "pcs",
        "piece",
        "pieces",
        "set",
        "pair",
        "pairs",
        "gallon",
        "gal",
        "mg",
        "fl",
        "tablet",
        "tablets",
        "capsule",
        "capsules",
        "serving",
        "servings",
    }
)

# Catches "2-Pack", "36 Count", "3-in-1", "6 Pieces", etc.
_QUANTITY_RE = re.compile(
    r"\b\d+\s*[-]?\s*(pack|count|ct|pcs|piece|pieces|set|in-1|in-one)\b",
    re.IGNORECASE,
)

# Catches numeric-plus-unit specs: "72-Inch", "6mm", "500ml", "60W", "3kg", etc.
# Digit must immediately precede the unit (with optional space/hyphen) to avoid false positives.
_SPEC_VALUE_RE = re.compile(
    r"\b\d+\.?\d*\s*[-]?\s*"
    r"(?:oz|lbs?|ml|mg|kg|g|inch(?:es)?|cm|mm|ft|watt|[wvWV]|ah|mah|fl)\b",
    re.IGNORECASE,
)

_TITLE_SPAMMY_CHARS: frozenset[str] = frozenset("!@$%*?#|^~")

_FEATURE_INDICATORS: frozenset[str] = frozenset(
    {
        "premium",
        "durable",
        "waterproof",
        "portable",
        "lightweight",
        "heavy-duty",
        "adjustable",
        "rechargeable",
        "cordless",
        "wireless",
        "organic",
        "non-toxic",
        "eco-friendly",
        "bpa-free",
        "non-slip",
        "breathable",
        "foldable",
        "collapsible",
        "rust-proof",
        "anti-slip",
        "multi-purpose",
        "multipurpose",
        "all-in-one",
        "ergonomic",
        "professional",
        "industrial",
        "commercial",
        "heavy duty",
    }
)

_CORE_BULLET_INDICATORS: frozenset[str] = frozenset(
    {
        "easy",
        "safe",
        "fast",
        "quick",
        "effective",
        "efficient",
        "powerful",
        "protect",
        "prevent",
        "reduce",
        "improve",
        "increase",
        "boost",
        "solution",
        "solve",
        "eliminate",
        "remove",
        "clean",
        "comfort",
        "support",
        "relief",
        "help",
        "ideal",
        "designed",
        "durable",
        "long-lasting",
        "sturdy",
        "reliable",
        "no more",
        "without",
    }
)

_MATERIAL_INDICATORS: frozenset[str] = frozenset(
    {
        "guarantee",
        "warranty",
        "service",
        "material",
        "made of",
        "made from",
        "fabric",
        "aluminum",
        "aluminium",
        "steel",
        "stainless",
        "silicone",
        "cotton",
        "polyester",
        "nylon",
        "leather",
        "wood",
        "bamboo",
        "plastic",
        "glass",
        "rubber",
        "refund",
        "replacement",
        "satisfaction",
        "bpa-free",
        "non-toxic",
        "food-grade",
        "fda",
        "ce",
        "rohs",
        "certified",
        "certification",
        "tested",
        "approved",
        "100%",
    }
)

_BULLET_VAGUE_PHRASES: frozenset[str] = frozenset(
    {
        "good product",
        "high quality",
        "best choice",
        "nice item",
        "great quality",
        "top quality",
        "best product",
        "amazing product",
        "excellent quality",
        "superior quality",
        "perfect product",
        "good quality",
        "premium quality",
        "very good",
        "very nice",
        "the best",
        "world class",
        "highly recommend",
        "best quality",
        "wonderful product",
        "top rated",
        "number one",
        "number 1",
    }
)


def _tokenize(text: str) -> set[str]:
    return {w for w in re.findall(r"\b[a-z]{3,}\b", text.lower()) if w not in _STOP_WORDS}


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
        "aplus": 0.15,
    }

    def score(
        self,
        product: Product,
        required_keywords: list[str] | None = None,
        keyword_config: dict[str, list[str]] | None = None,
        image_metadata: dict[str, dict[str, Any]] | None = None,
        video_metadata: dict[str, dict[str, Any]] | None = None,
        review_summary: ReviewSummary | None = None,
        weights: dict[str, float] | None = None,
        competitors: list[Product | dict] | None = None,
    ) -> dict[str, Any]:
        """
        Scores a product listing from 0 to 100 based on weighted modules.

        Pass competitors to enable brand-keyword filtering: keywords whose tokens
        appear in only one corpus source (likely a third-party brand name) are
        excluded from the improvement plan rather than flagged as missing.
        """
        logger.info(f"Scoring product listing for ASIN: {product.asin}")
        weights = weights or self.DEFAULT_WEIGHTS
        freq_map: Counter | None = (
            self._build_token_freq_map(product, competitors) if competitors else None
        )

        # Calculate individual module scores (each out of 100)
        title_res = self._score_title(product, required_keywords, keyword_config, freq_map)
        features_res = self._score_features(product, required_keywords, keyword_config, freq_map)
        media_res = self._score_media(product, image_metadata, video_metadata)
        social_res = self._score_social_proof(product, review_summary)
        aplus_res = self._score_aplus(product)

        # Weighted Sum
        final_score = (
            title_res["score"] * weights["title"]
            + features_res["score"] * weights["features"]
            + media_res["score"] * weights["media"]
            + social_res["score"] * weights["social_proof"]
            + aplus_res["score"] * weights["aplus"]
        )

        all_issues = (
            title_res["issues"]
            + features_res["issues"]
            + media_res["issues"]
            + social_res["issues"]
            + aplus_res["issues"]
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
                "aplus": aplus_res["score"],
            },
            "improvement_plan": all_issues,
            "metrics": {
                **title_res["metrics"],
                **features_res["metrics"],
                **media_res["metrics"],
                **social_res["metrics"],
                **aplus_res["metrics"],
            },
        }

    def _score_title(
        self, product: Product, required_keywords, keyword_config, freq_map: Counter | None = None
    ) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        title = product.title or ""
        if not title:
            return {
                "score": 0,
                "issues": ["Title is missing."],
                "metrics": {"title_length": 0, "title_integrity_score": 0},
            }

        title_lower = title.lower()
        title_len = len(title)
        words = title.split()
        words_lower = [w.lower() for w in words]
        brand = product.brand or ""
        brand_words_lower = brand.lower().split() if brand else []

        # ── 1. Length ──────────────────────────────────────────────────────
        # Sweet spot: 80-150 chars for keyword indexing.
        # Amazon search truncates display past ~200 chars.
        if title_len < 40:
            score -= 25
            issues.append("Title critically short (< 40 chars).")
        elif title_len < 80:
            score -= 10
            issues.append("Title short (40-79 chars).")
        elif title_len <= 150:
            score += 5  # sweet spot
        elif title_len <= 200:
            score -= 5
            issues.append("Title slightly long (151-200 chars).")
        else:
            score -= 15
            issues.append("Title too long (> 200 chars); truncated in search results.")
        metrics["title_length"] = title_len

        # ── 2. Four-Pillar Integrity ───────────────────────────────────────
        integrity_score = 0

        # Pillar 1: Brand present
        if brand and brand.lower() in title_lower:
            integrity_score += 10
            # Bonus: brand leads the title (Amazon best practice)
            if brand_words_lower and words_lower[: len(brand_words_lower)] == brand_words_lower:
                score += 3
        else:
            score -= 10
            issues.append("Title Pillar Missing: Brand.")

        # Pillar 2: Product type — require ≥ 3 non-brand, non-stop-word content words
        non_brand_content = [
            w
            for w in words[len(brand_words_lower) :]
            if w.lower() not in _STOP_WORDS and len(w) > 1
        ]
        if len(non_brand_content) >= 3:
            integrity_score += 10
        else:
            score -= 10
            issues.append(
                "Title Pillar Missing: Product type "
                f"(only {len(non_brand_content)} content words after brand; need ≥ 3)."
            )

        # Pillar 3: Key feature / differentiator
        has_feature = (
            any(ind in title_lower for ind in _FEATURE_INDICATORS) or len(non_brand_content) >= 5
        )
        if has_feature:
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Core features.")

        # Pillar 4: Specifications or quantity
        if _SPEC_VALUE_RE.search(title) or _QUANTITY_RE.search(title):
            integrity_score += 10
        else:
            score -= 10
            issues.append("Title Pillar Missing: Specifications or quantity.")
        metrics["title_integrity_score"] = integrity_score

        # ── 3. ALL_CAPS words (Amazon policy: avoid) ───────────────────────
        all_caps = [w for w in words if len(w) > 1 and w.isupper() and w.isalpha()]
        if all_caps:
            score -= min(5 * len(all_caps), 15)
            issues.append(
                f"Title contains ALL_CAPS words ({', '.join(all_caps[:3])}); "
                "violates Amazon style guide."
            )

        # ── 4. Keyword stuffing — content word repeated ≥ 3× ─────────────
        # Strip leading/trailing punctuation before counting so "mat" and "mat,"
        # are not treated as different tokens (common in comma-separated titles).
        clean = [re.sub(r"^[^\w]+|[^\w]+$", "", w) for w in words_lower]
        word_freq = Counter(w for w in clean if len(w) > 2 and w not in _STOP_WORDS)
        stuffed = [w for w, n in word_freq.items() if n >= 3]
        if stuffed:
            score -= min(5 * len(stuffed), 10)
            issues.append(
                f"Title repeats words ≥ 3×: {', '.join(stuffed)} — likely keyword stuffing."
            )

        # ── 5. Spammy symbols ─────────────────────────────────────────────
        if any(ch in _TITLE_SPAMMY_CHARS for ch in title):
            score -= 5
            issues.append("Title contains spammy symbols.")

        # ── 6. Starts with uppercase ──────────────────────────────────────
        if words and not words[0][0].isupper():
            score -= 3
            issues.append("Title should start with an uppercase letter.")

        # ── 7. Optional keyword config (penalty capped at 30) ────────────
        if keyword_config:
            kw_penalty = 0
            for cat, pts in [("core", 10), ("modifiers", 5), ("scenes", 3)]:
                missing = [
                    kw
                    for kw in keyword_config.get(cat, [])
                    if kw.lower() not in title_lower
                    and (freq_map is None or self._is_generic_keyword(kw, freq_map))
                ]
                if missing:
                    kw_penalty += len(missing) * pts
                    issues.append(f"Title missing {cat.upper()} keywords: {', '.join(missing)}")
            score -= min(kw_penalty, 30)

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_features(
        self,
        product: Product,
        required_keywords,
        keyword_config,
        freq_map: Counter | None = None,
    ) -> dict[str, Any]:
        score = 100
        issues = []
        metrics = {}

        features = product.features or []
        feature_count = len(features)
        metrics["bullet_count"] = feature_count

        if feature_count == 0:
            return {"score": 0, "issues": ["Missing all bullet points."], "metrics": metrics}
        if feature_count < 5:
            score -= 10
            issues.append(f"Only {feature_count} bullet points (optimal: 5).")

        # ── 1. Core benefit structure — 2 of first 3 bullets must lead with a benefit ──
        if feature_count >= 3:
            hits = sum(
                1 for b in features[:3] if any(ind in b.lower() for ind in _CORE_BULLET_INDICATORS)
            )
            if hits < 2:
                score -= 5
                issues.append(
                    "At least 2 of the first 3 bullets should highlight a core benefit "
                    "or pain-point solution."
                )

        # ── 2. Material / certification / after-sales in later bullets ────
        if feature_count >= 5:
            tail_text = " ".join(features[3:]).lower()
            if not any(ind in tail_text for ind in _MATERIAL_INDICATORS):
                score -= 5
                issues.append(
                    "Later bullets should cover materials, certifications, or after-sales assurance."
                )

        # ── 3. Per-bullet length and formatting ───────────────────────────
        # Sweet spot: 80-250 chars. Mobile truncates past ~400 chars.
        for i, feat in enumerate(features):
            feat = feat.strip()
            flen = len(feat)
            if flen < 50:
                score -= 5
                issues.append(f"Bullet {i + 1} too short ({flen} chars; aim for 80-250).")
            elif flen < 80:
                score -= 2
                issues.append(f"Bullet {i + 1} slightly short ({flen} chars).")
            elif flen > 400:
                score -= 3
                issues.append(f"Bullet {i + 1} too long ({flen} chars; truncated on mobile).")
            elif flen > 250:
                score -= 1
                issues.append(f"Bullet {i + 1} a bit long ({flen} chars).")

            if feat and feat[0].isalpha() and not feat[0].isupper():
                score -= 2
                issues.append(f"Bullet {i + 1} not capitalized.")
            if feat and feat[-1] in {".", "!", "?", ";"}:
                score -= 1
                issues.append(f"Bullet {i + 1} has trailing punctuation.")
            if "  " in feat:
                score -= 1
                issues.append(f"Bullet {i + 1} has double spaces.")

        # ── 4. Vague / filler phrase detection ────────────────────────────
        feature_text_lower = " ".join(features).lower()
        vague_hits = [p for p in _BULLET_VAGUE_PHRASES if p in feature_text_lower]
        if vague_hits:
            score -= min(len(vague_hits) * 3, 10)
            issues.append(f"Bullets contain vague filler phrases: {', '.join(vague_hits)}.")

        # ── 5. Keyword coverage (presence-based, capped penalty) ──────────
        # Measures how many bullets contain the keyword rather than raw occurrence count,
        # which was gameable via repetition in one bullet and punished sparse-but-correct use.
        # Brand-owned keywords (freq_map token check) are silently skipped — recommending
        # a seller adopt a competitor's brand name is both wrong and a policy violation.
        main_kws = keyword_config.get("core", []) if keyword_config else (required_keywords or [])
        kw_penalty = 0
        for kw in main_kws:
            if freq_map is not None and not self._is_generic_keyword(kw, freq_map):
                continue
            kw_lower = kw.lower()
            bullets_with_kw = sum(1 for f in features if kw_lower in f.lower())
            if bullets_with_kw == 0:
                kw_penalty += 8
                issues.append(f"Keyword '{kw}' absent from all bullet points.")
            elif bullets_with_kw == 1:
                kw_penalty += 3
                issues.append(f"Keyword '{kw}' in only 1 bullet (aim for 2-3).")
        score -= min(kw_penalty, 20)

        # ── 6. Cross-bullet duplication (first-6-word fingerprint) ────────
        openings: list[str] = [" ".join(f.lower().split()[:6]) for f in features if f.strip()]
        seen: set[str] = set()
        dupe_count = 0
        for op in openings:
            if op in seen:
                dupe_count += 1
            else:
                seen.add(op)
        if dupe_count:
            score -= min(5 * dupe_count, 10)
            issues.append(
                "Bullets have near-duplicate openings — each should cover a distinct selling point."
            )

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
            needed = 7 - image_count
            issues.append(
                f"Add {needed} more image(s) to reach the 7-image optimal (currently {image_count})"
                " — prioritise lifestyle/in-use shots, an infographic, and a size/dimension reference."
            )

        if image_metadata:
            main_meta = image_metadata.get(product.main_image_url, {})
            if main_meta and not main_meta.get("is_pure_white_bg", True):
                score -= 15
                issues.append(
                    "Retake or retouch the main image to use a pure white background"
                    " (Amazon requirement for search thumbnails)."
                )
            if main_meta and main_meta.get("has_text_or_watermark", False):
                score -= 15
                issues.append(
                    "Remove all text overlays and watermarks from the main image"
                    " — violates Amazon policy and may suppress the listing."
                )

            low_res = sum(1 for url in images if image_metadata.get(url, {}).get("width", 0) < 1000)
            if low_res:
                score -= low_res * 2
                issues.append(
                    f"Replace {low_res} low-resolution image(s) with versions ≥ 1000×1000px"
                    " to enable the zoom feature and meet Amazon's image quality guidelines."
                )

        if not product.videos:
            score -= 10
            issues.append(
                "Add a product video (15–90s, 1080p minimum) demonstrating the product in use"
                " — listings with video convert significantly higher than those without."
            )
        elif video_metadata:
            for v_url in product.videos:
                v_meta = video_metadata.get(v_url, {})
                if v_meta.get("duration_seconds", 0) < 15:
                    score -= 3
                    issues.append(
                        "Extend video to at least 15s — clips under 15s cannot adequately"
                        " demonstrate product features or build buyer confidence."
                    )
                if v_meta.get("resolution_height", 0) < 720:
                    score -= 5
                    issues.append(
                        "Re-upload video at 1080p or higher — sub-720p video appears"
                        " unprofessional on modern high-DPI screens."
                    )

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_social_proof(
        self, product: Product, review_summary: ReviewSummary | None = None
    ) -> dict[str, Any]:
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

        rb = review_summary.rating_breakdown if review_summary else {}
        if rb:
            neg = rb.get(1, 0) + rb.get(2, 0)
            if neg > 15:
                score -= 10
                issues.append(f"High negative rating ratio ({neg}%).")

        vp_ratio = review_summary.vp_review_ratio if review_summary else None
        if vp_ratio is not None and vp_ratio < 0.7:
            score -= 10
            issues.append("Low Verified Purchase ratio.")

        recent_avg = review_summary.recent_rating_avg if review_summary else None
        if recent_avg is not None and recent_avg < rating - 0.3:
            score -= 15
            issues.append("Declining recent rating trend.")

        sentiment = review_summary.sentiment_score if review_summary is not None else None
        if sentiment is not None:
            if sentiment < 0:
                score -= 10
                issues.append("Negative review sentiment.")
            elif sentiment > 0.6:
                score += 5

        # Manipulation risk from ReviewSummary — high RCI score signals fake/incentivised
        # reviews that inflate ratings, making the social proof unreliable.
        if review_summary is not None and review_summary.manipulation_risk:
            risk_score = review_summary.manipulation_risk.get("score", 0) or 0
            if risk_score >= 70:
                score -= 20
                issues.append(
                    f"High review manipulation risk (RCI {risk_score}/100) — ratings may be"
                    " inflated by incentivised or fake reviews; buyers distrust these listings."
                )
            elif risk_score >= 40:
                score -= 10
                issues.append(
                    f"Moderate review manipulation risk (RCI {risk_score}/100) — review"
                    " authenticity signals are weak; consider proactive review cleanup."
                )
            metrics["manipulation_risk_score"] = risk_score

        media_ratio = review_summary.media_review_ratio if review_summary else None
        if media_ratio is not None:
            ratio = media_ratio
            metrics["media_review_ratio"] = round(ratio, 3)

            # Estimate what fraction of reviews are low-star (1–3) to determine
            # whether media content is buyer delight or complaint/defect photos.
            if rb:
                total_rated = sum(rb.values()) or 1
                low_star = sum(rb.get(s, 0) for s in (1, 2, 3))
                low_star_ratio = low_star / total_rated
            elif product.rating is not None:
                # Linear proxy: rating 4.0 → 0 low-star, rating 1.0 → 1.0 low-star
                low_star_ratio = max(0.0, (4.0 - product.rating) / 3.0)
            else:
                low_star_ratio = 0.0

            metrics["low_star_ratio"] = round(low_star_ratio, 3)

            if low_star_ratio >= 0.30 and ratio >= 0.15:
                # High media ratio + high low-star share → complaint / defect photos
                penalty = 10 if ratio >= 0.30 else 5
                score -= penalty
                issues.append(
                    f"High proportion of reviews contain images/videos ({ratio:.0%}) but "
                    f"{low_star_ratio:.0%} of reviews are 1–3 stars — this likely indicates "
                    "complaint or defect photos; address the root product quality issue."
                )
            elif ratio >= 0.30:
                score += 10
            elif ratio >= 0.15:
                score += 5
            elif ratio < 0.05 and reviews >= 50:
                score -= 5
                issues.append(
                    f"Very few reviews include photos or videos ({ratio:.0%}) — "
                    "encourage buyers to share visual feedback via follow-up messaging."
                )

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    def _score_aplus(self, product: Product) -> dict[str, Any]:
        score = 100
        issues = []
        premium_count = len(product.aplus_images or [])
        metrics = {
            "has_a_plus": product.has_a_plus_content,
            "aplus_premium_image_count": premium_count,
        }

        if not product.has_a_plus_content:
            score -= 20
            issues.append(
                "Missing A+ Content — add Enhanced Brand Content (EBC) modules to increase"
                " conversion rate; A+ listings convert 3–10% higher on average."
            )
            return {"score": max(0, score), "issues": issues, "metrics": metrics}

        # A+ exists — evaluate richness via Brand Story premium background image count.
        # premium_count == 0 → standard EBC modules only (text/feature boxes, no Brand Story).
        # premium_count 1–3 → Brand Story started but visually thin.
        # premium_count 4+  → rich Brand Story with multiple visual modules.
        if premium_count == 0:
            score -= 10
            issues.append(
                "A+ Content contains only standard text/feature modules — no Brand Story"
                " background images detected. Add Premium A+ Brand Story modules with"
                " lifestyle imagery and a product comparison chart to maximise visual impact."
            )
        elif premium_count < 4:
            score -= 5
            issues.append(
                f"Brand Story has limited visual coverage ({premium_count} background image(s))."
                " Expand to 4+ visual modules and add a product comparison chart to help"
                " buyers evaluate your product against alternatives."
            )
        else:
            # Rich Brand Story — small bonus, but still surface comparison chart suggestion.
            score += 5
            issues.append(
                "Consider adding a product comparison chart to A+ Content if not already present"
                " — comparison modules are among the highest-converting A+ elements."
            )

        return {"score": max(0, score), "issues": issues, "metrics": metrics}

    @staticmethod
    def _build_token_freq_map(
        product: Product,
        competitors: list[Product | dict],
    ) -> Counter:
        """
        Count how many distinct sources contain each token.

        Sources: main product (title + features + description) counts as one source;
        each competitor title counts as one source.  A token that appears in N sources
        has freq N.  Tokens with freq >= min_sources in _is_generic_keyword are
        treated as ownable category terms; single-source tokens are likely brand names.
        """
        sources: list[set[str]] = []

        main_tokens: set[str] = set()
        if product.title:
            main_tokens |= _tokenize(product.title)
        for f in product.features or []:
            main_tokens |= _tokenize(f)
        if product.description:
            main_tokens |= _tokenize(product.description)
        sources.append(main_tokens)

        for c in competitors:
            title = c.title if isinstance(c, Product) else (c.get("title") or "")
            if title:
                sources.append(_tokenize(title))

        freq: Counter = Counter()
        for token_set in sources:
            for token in token_set:
                freq[token] += 1
        return freq

    @staticmethod
    def _is_generic_keyword(kw: str, freq_map: Counter, min_sources: int = 2) -> bool:
        """
        Return True if every content token in kw appears in >= min_sources corpus sources.

        A keyword whose tokens are all present across multiple product titles / the main
        listing is a generic category term and should be flagged as missing.  A keyword
        containing a token that only appears in one source (or zero) is likely a
        third-party brand identifier and must not be recommended for adoption.
        """
        tokens = [w for w in re.findall(r"\b[a-z]{3,}\b", kw.lower()) if w not in _STOP_WORDS]
        if not tokens:
            return True
        return all(freq_map.get(t, 0) >= min_sources for t in tokens)

    def batch_score(self, products: list[Product]) -> list[dict[str, Any]]:
        logger.info(f"Batch scoring {len(products)} products...")
        return [self.score(p) for p in products]
