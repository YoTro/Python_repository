from __future__ import annotations

import json
import logging
import re
from typing import Any

from src.intelligence.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)

_SCHEMA = """{
  "sentiment": {"positive": float, "negative": float, "neutral": float},
  "purchase_signals": {
    "explicit_buy_intent": int,
    "price_sensitivity": int,
    "product_inquiry": int,
    "comparison_shopping": int
  },
  "top_themes": [
    {"theme": str, "type": "product_attribute|content_style|purchase_driver|brand_comparison", "count": int}
  ],
  "top_objections": [{"objection": str, "count": int}],
  "competitor_mentions": [
    {"brand": str, "sentiment": "favorable|unfavorable|neutral", "count": int}
  ],
  "language_distribution": {"en": float, "es": float, "zh": float, "other": float},
  "confidence": {
    "total_comments_analyzed": int,
    "meaningful_comments_ratio": float,
    "sample_quality": "high|medium|low"
  },
  "summary": str
}"""

EMPTY_RESULT: dict[str, Any] = {
    "sentiment": {"positive": 0.0, "negative": 0.0, "neutral": 1.0},
    "purchase_signals": {
        "explicit_buy_intent": 0,
        "price_sensitivity": 0,
        "product_inquiry": 0,
        "comparison_shopping": 0,
    },
    "top_themes": [],
    "top_objections": [],
    "competitor_mentions": [],
    "language_distribution": {},
    "confidence": {
        "total_comments_analyzed": 0,
        "meaningful_comments_ratio": 0.0,
        "sample_quality": "low",
    },
    "summary": "Analysis failed",
}


class CommentAnalyzer:
    """
    L2 AI processor: sends a batch of raw comment texts to the LLM and returns
    structured sentiment, purchase-signal, competitor, and language analysis.

    Designed for social media comments (TikTok, etc.) — handles multilingual input,
    filters noise before counting, and returns a fixed JSON schema suitable for
    downstream use in SocialViralityProcessor or workflow ProcessSteps.
    """

    def __init__(self, provider: BaseLLMProvider) -> None:
        self.provider = provider

    async def analyze(
        self,
        comments: list[str],
        brand: str,
        product_name: str,
    ) -> dict[str, Any]:
        """
        Analyze a flat list of comment strings.
        Returns a dict matching _SCHEMA. Falls back to EMPTY_RESULT on any error.
        """
        if not comments:
            return EMPTY_RESULT.copy()

        prompt = (
            f'You are analyzing social media comments for brand "{brand}" '
            f'(product: "{product_name}").\n'
            f"Analyze all comments regardless of language. Return ONLY valid JSON.\n\n"
            f"FILTER BEFORE COUNTING — exclude:\n"
            f"- Purely emoji-only (e.g., '🔥🔥🔥')\n"
            f"- Single words with no context (e.g., 'nice', 'first', 'wow')\n"
            f"- @mention-only replies (e.g., '@john @sarah')\n"
            f"- Non-product spam or bot patterns\n\n"
            f"Count raw total vs meaningful to compute meaningful_comments_ratio.\n"
            f'sample_quality: "high" if ratio >= 0.7 AND total >= 100 | '
            f'"medium" if ratio >= 0.4 OR total >= 30 | else "low"\n\n'
            f"Return schema:\n{_SCHEMA}\n\n"
            f"Rules:\n"
            f"- sentiment values must sum to 1.0\n"
            f"- top_themes: max 4, sorted by count desc\n"
            f"- top_objections: max 2, only if count >= 2\n"
            f"- purchase_signals are raw counts of qualifying meaningful comments\n"
            f"- competitor_mentions: max 5, only brands explicitly named, sorted by count desc, "
            f"only if count >= 2\n"
            f'  sentiment: "favorable" = commenter prefers {brand} over that brand, '
            f'"unfavorable" = commenter prefers competitor, "neutral" = just mentioned\n'
            f"- language_distribution: detect language of EVERY raw comment (before filtering), "
            f"use ISO 639-1 codes, group languages below 0.03 share into 'other', "
            f"values must sum to 1.0\n"
            f"- summary: one sentence focused on commercial signal\n\n"
            f"Comments (one per line):\n" + "\n".join(comments)
        )
        try:
            response = await self.provider.generate_text(prompt)
            text = response.text.strip()
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                return json.loads(match.group())
        except Exception as e:
            logger.warning(f"LLM comment analysis failed: {e}")
        return EMPTY_RESULT.copy()
