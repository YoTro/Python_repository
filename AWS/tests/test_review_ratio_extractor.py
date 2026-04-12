from __future__ import annotations
"""
Unit tests for ReviewRatioExtractor.

All tests mock AmazonBaseScraper.fetch to avoid real network calls.
"""

import pytest
from unittest.mock import AsyncMock, patch
from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_fetch(html: str | None):
    """Patch AmazonBaseScraper.fetch to return the given HTML (or None)."""
    return patch.object(ReviewRatioExtractor, "fetch", new=AsyncMock(return_value=html))


ASIN = "B08N5WRWNW"


# ---------------------------------------------------------------------------
# Primary path: data-hook="cr-filter-info-review-rating-count"
# ---------------------------------------------------------------------------

class TestPrimaryPath:

    @pytest.mark.asyncio
    async def test_both_counts_present(self):
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          4,567 global ratings | 1,234 with reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["ASIN"] == ASIN
        assert result["GlobalRatings"] == 4567
        assert result["WrittenReviews"] == 1234
        assert result["Ratio"] == round(1234 / 4567, 3)

    @pytest.mark.asyncio
    async def test_global_ratings_only(self):
        """When there are no written reviews the 'with reviews' part is absent."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          272,876 global ratings
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 272876
        assert result["WrittenReviews"] is None
        assert result["Ratio"] is None

    @pytest.mark.asyncio
    async def test_customer_reviews_format(self):
        """Amazon also renders 'X customer reviews' instead of 'X with reviews'."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          470 global ratings | 298 customer reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 470
        assert result["WrittenReviews"] == 298
        assert result["Ratio"] == round(298 / 470, 3)

    @pytest.mark.asyncio
    async def test_ratio_suspicious_near_one(self):
        """298 written / 470 global ≈ 0.634 — well above 0.50 threshold."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          470 global ratings | 298 with reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 470
        assert result["WrittenReviews"] == 298
        assert result["Ratio"] == round(298 / 470, 3)
        assert result["Ratio"] > 0.50

    @pytest.mark.asyncio
    async def test_natural_ratio_around_ten_percent(self):
        """Healthy product: 500 written / 5000 global = 0.1."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          5,000 global ratings | 500 with reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["Ratio"] == pytest.approx(0.1, abs=0.001)

    @pytest.mark.asyncio
    async def test_commas_stripped_correctly(self):
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          1,234,567 global ratings | 123,456 with reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 1234567
        assert result["WrittenReviews"] == 123456


# ---------------------------------------------------------------------------
# Fallback A: bare regex on raw HTML
# ---------------------------------------------------------------------------

class TestFallbackA:

    @pytest.mark.asyncio
    async def test_raw_html_regex_fallback(self):
        """No data-hook element, but 'global ratings' text exists elsewhere in page."""
        html = "<div>Some other content</div><p>10,000 global ratings</p>"
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 10000


# ---------------------------------------------------------------------------
# Fallback B: cr-filter-info-section pagination text
# ---------------------------------------------------------------------------

class TestFallbackB:

    @pytest.mark.asyncio
    async def test_pagination_written_reviews(self):
        """No primary element; written count extracted from filter-info-section."""
        html = """
        <div data-hook="cr-filter-info-section">
          Showing 1-10 of 432 reviews
        </div>
        <p>99 global ratings</p>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["WrittenReviews"] == 432
        assert result["GlobalRatings"] == 99


# ---------------------------------------------------------------------------
# Fallback C: legacy acrCustomerReviewText span
# ---------------------------------------------------------------------------

class TestFallbackC:

    @pytest.mark.asyncio
    async def test_legacy_acr_span(self):
        """No other signals; global ratings parsed from legacy /dp/ span."""
        html = """
        <span id="acrCustomerReviewText">3,210 ratings</span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 3210


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_fetch_returns_none(self):
        with _patch_fetch(None):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result == {
            "ASIN": ASIN,
            "GlobalRatings": None,
            "WrittenReviews": None,
            "Ratio": None,
        }

    @pytest.mark.asyncio
    async def test_empty_html(self):
        with _patch_fetch(""):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] is None
        assert result["WrittenReviews"] is None
        assert result["Ratio"] is None

    @pytest.mark.asyncio
    async def test_zero_global_ratings_ratio_is_none(self):
        """Avoid ZeroDivisionError when GlobalRatings is 0."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          0 global ratings | 0 with reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["Ratio"] is None

    @pytest.mark.asyncio
    async def test_case_insensitive_matching(self):
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          2,500 Global Ratings | 300 With Reviews
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 2500
        assert result["WrittenReviews"] == 300

    @pytest.mark.asyncio
    async def test_custom_host(self):
        """Verify the URL is built using the custom host."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          100 global ratings | 10 with reviews
        </span>
        """
        extractor = ReviewRatioExtractor()
        mock_fetch = AsyncMock(return_value=html)
        with patch.object(extractor, "fetch", mock_fetch):
            await extractor.get_review_count(ASIN, host="https://www.amazon.co.uk")

        call_url = mock_fetch.call_args[0][0]
        assert "amazon.co.uk" in call_url
        assert ASIN in call_url

    @pytest.mark.asyncio
    async def test_singular_rating_form(self):
        """Handle '1 global rating | 1 with review' (singular)."""
        html = """
        <span data-hook="cr-filter-info-review-rating-count">
          1 global rating | 1 with review
        </span>
        """
        with _patch_fetch(html):
            result = await ReviewRatioExtractor().get_review_count(ASIN)

        assert result["GlobalRatings"] == 1
        assert result["WrittenReviews"] == 1
        assert result["Ratio"] == 1.0
