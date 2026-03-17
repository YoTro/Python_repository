import pytest
from pydantic import ValidationError
from src.core.models.product import Product
from src.core.models.review import Review, ReviewSummary
from src.core.models.market import CompetitorEntry, MarketAnalysisReport

def test_product_model():
    p = Product(asin="B01N5IB20Q", title="Test", is_fba=True)
    assert p.asin == "B01N5IB20Q"
    assert p.is_fba is True

def test_review_model():
    r = Review(asin="B01N5IB20Q", rating=5)
    assert r.asin == "B01N5IB20Q"
    assert r.rating == 5
    assert isinstance(r.image_urls, list)
    
def test_review_summary_model():
    with pytest.raises(ValidationError):
        ReviewSummary() # Missing required fields
        
    s = ReviewSummary(
        pros=["Good battery"],
        cons=["Heavy"],
        sentiment_score=0.8,
        top_complaints=["Charger breaks"],
        buyer_persona="Tech Enthusiast"
    )
    assert s.sentiment_score == 0.8

def test_market_analysis_report():
    p = Product(asin="B01N5IB20Q")
    entry = CompetitorEntry(
        product=p,
        competitive_advantage="Brand",
        weaknesses=["High price"]
    )
    report = MarketAnalysisReport(
        keyword="Test",
        avg_price=10.0,
        avg_rating=4.5,
        total_estimated_sales=1000,
        top_competitors=[entry],
        entry_barrier_score=5.0,
        swot_analysis={"Strengths": "Good"},
        summary="Enter now"
    )
    assert report.keyword == "Test"
    assert len(report.top_competitors) == 1
