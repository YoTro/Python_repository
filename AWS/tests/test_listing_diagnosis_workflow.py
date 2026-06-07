from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.workflows.definitions  # noqa: F401 — registers all workflows
from src.core.models.product import Product
from src.core.models.review import Review, ReviewSummary
from src.workflows.engine import WorkflowContext
from src.workflows.registry import WorkflowRegistry

_MOCK_REVIEW_SUMMARY = ReviewSummary(
    pros=["Durable", "Non-slip"],
    cons=["Thin"],
    sentiment_score=0.72,
    top_complaints=["Slips on hardwood"],
    buyer_persona="Yoga enthusiasts aged 25-40",
    review_velocity=45.3,
    rating_breakdown={1: 2, 2: 3, 3: 10, 4: 30, 5: 55},
    competitive_barrier_months=8.2,
    manipulation_risk={"score": 12.0, "verdict": "SAFE", "metrics": {}},
)

_MOCK_REVIEWS = [
    Review(asin="B001", rating=5, content="Great mat!", is_verified=True),
    Review(asin="B001", rating=3, content="A bit thin.", is_verified=True),
]


@pytest.fixture
def mock_product_data():
    return Product(
        asin="B001",
        title="Super Yoga Mat",
        brand="SuperBrand",
        features=["Soft", "Non-slip", "72x24 inch", "Rubber material", "1 year warranty"],
        is_fba=True,
        has_a_plus_content=True,
        rating=4.5,
        review_count=100,
    )


@pytest.fixture
def mock_competitor_data():
    return [
        Product(asin="B002", title="Competitor Mat 1", rating=4.2, review_count=50, brand="Comp1"),
        Product(asin="B003", title="Competitor Mat 2", rating=4.8, review_count=500, brand="Comp2"),
    ]


@pytest.mark.asyncio
async def test_listing_diagnosis_workflow(mock_product_data, mock_competitor_data):
    with (
        patch(
            "src.mcp.servers.amazon.extractors.product_details.ProductDetailsExtractor.get_product_details",
            return_value=mock_product_data,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.product_details.ProductDetailsExtractor.enrich_product",
            side_effect=lambda p: p,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.search.SearchExtractor.search",
            return_value=mock_competitor_data,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.images.ImageExtractor.get_product_images",
            return_value={"Images": ["http://img.com"]},
        ),
        patch(
            "src.mcp.servers.amazon.extractors.videos.VideoExtractor.has_videos",
            return_value={"HasVideos": True, "VideoCount": 1},
        ),
        patch(
            "src.mcp.servers.amazon.extractors.comments.CommentsExtractor.get_all_comments",
            new_callable=AsyncMock,
            return_value=_MOCK_REVIEWS,
        ),
        patch(
            "src.intelligence.processors.review_summarizer.ReviewSummarizer.summarize",
            new_callable=AsyncMock,
            return_value=_MOCK_REVIEW_SUMMARY,
        ),
    ):
        mock_cloud = MagicMock()
        mock_router = MagicMock()
        mock_router.cloud = mock_cloud
        mock_router.route_and_execute = AsyncMock(return_value="LLM Diagnosis Result")

        ctx = WorkflowContext(job_id="test_diag", router=mock_router)
        params = {"asin": "B001"}

        workflow = WorkflowRegistry.build("listing_diagnosis")
        result = await workflow.execute(job_id="test_diag", params=params, ctx=ctx)

        assert result.completed is True
        assert len(result.final_items) == 1

        final_report = result.final_items[0]["final_report"]
        assert final_report["asin"] == "B001"
        assert "overall_summary" in final_report
        assert "module_performance" in final_report
        assert "comparative_analysis" in final_report
        assert final_report["qualitative_diagnosis"] == "LLM Diagnosis Result"
        assert final_report["overall_summary"]["competitor_avg_score"] > 0
        assert len(final_report["comparative_analysis"]["competitors"]) == 2

        # Review intelligence fields
        ri = final_report["review_intelligence"]
        assert ri["sentiment_score"] == pytest.approx(0.72)
        assert ri["review_velocity"] == pytest.approx(45.3)
        assert ri["competitive_barrier_months"] == pytest.approx(8.2)
        assert ri["manipulation_risk"]["verdict"] == "SAFE"
        assert "Durable" in ri["pros"]
        assert ri["buyer_persona"] == "Yoga enthusiasts aged 25-40"


if __name__ == "__main__":
    pytest.main([__file__])
