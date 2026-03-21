from __future__ import annotations
import pytest
from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer

@pytest.fixture
def analyzer():
    return CategoryMonopolyAnalyzer()

def test_analyze_empty(analyzer):
    result = analyzer.analyze([])
    assert "error" in result

def test_analyze_sales_distribution(analyzer):
    products = [
        {"rank": 1, "sales": 10000},
        {"rank": 2, "sales": 8000},
        {"rank": 3, "sales": 5000},
        {"rank": 4, "sales": 1000},
        {"rank": 5, "sales": 500},
    ]
    # Add dummy products to reach > 50 for survival space logic
    for i in range(6, 60):
        products.append({"rank": i, "sales": 100})
        
    scores = analyzer._analyze_sales_distribution(products)
    
    # Total sales ~29900
    # Top 3 = 23000 -> CR3 ~ 76.9%
    # CR3 limit is 0.60. (0.769 / 0.6) * 100 > 100 -> clamped to 100
    assert scores["top3_concentration"] == 100
    assert scores["cr3"] > 0.70

def test_analyze_brand_concentration(analyzer):
    # 10 products, 10 different brands -> low monopoly
    diverse_products = [{"rank": i, "brand": f"Brand_{i}"} for i in range(10)]
    score_diverse = analyzer._analyze_brand_concentration(diverse_products)
    
    # 10 products, 2 brands -> high monopoly
    monopoly_products = [{"rank": i, "brand": f"Brand_{i%2}"} for i in range(10)]
    score_monopoly = analyzer._analyze_brand_concentration(monopoly_products)
    
    assert score_monopoly > score_diverse

def test_analyze_review_barrier(analyzer):
    # Needs at least 20 products.
    # High disparity: Top 10 avg ~ 10,000, Tail avg ~ 100
    high_disparity_products = [{"rank": i, "review_count": 10000 if i <= 10 else 100, "rating": 4.6 if i <=10 else 4.0} for i in range(1, 60)]
    score_high = analyzer._analyze_review_barrier(high_disparity_products)
    
    # Low disparity: Top 10 avg ~ 1000, Tail avg ~ 800
    low_disparity_products = [{"rank": i, "review_count": 1000 if i <= 10 else 800, "rating": 4.1} for i in range(1, 60)]
    score_low = analyzer._analyze_review_barrier(low_disparity_products)
    
    assert score_high > score_low

def test_analyze_price_convergence(analyzer):
    # High convergence (small CV) -> red ocean / price war
    converged_prices = [{"rank": i, "price": 20.0 + (i % 3)} for i in range(20)] # Prices around 20-22
    score_converged = analyzer._analyze_price_convergence(converged_prices)
    
    # Diverged prices (large CV) -> blue ocean / diverse niches
    diverged_prices = [{"rank": i, "price": (i * 10) + 10} for i in range(20)] # Prices 10, 20, 30... 200
    score_diverged = analyzer._analyze_price_convergence(diverged_prices)
    
    assert score_converged > score_diverged

def test_full_analysis_integration(analyzer):
    # Construct a balanced realistic mock
    products = []
    for i in range(1, 101):
        products.append({
            "rank": i,
            "sales": max(10, 1000 - i * 10),
            "price": 25.0 + (i % 10),
            "brand": f"Brand_{i % 30}",
            "seller_type": "FBA" if i % 5 != 0 else "Amazon",
            "feedback_count": 5000 if i > 10 else 20000,
            "review_count": 2000 if i <= 10 else 500,
            "rating": 4.5 if i <= 10 else 4.2
        })
        
    keyword_data = {
        "top_asins": [
            {"clickShare": 0.20},
            {"clickShare": 0.15},
            {"clickShare": 0.10}
        ]
    }
    
    ad_data = {"ad_ratio": 0.25}
    
    result = analyzer.analyze(products, keyword_data=keyword_data, ad_data=ad_data)
    
    assert "overall_score" in result
    assert "status" in result
    assert "dimension_details" in result
    assert "cr3" in result["summary_metrics"]
    
    # Given the balanced inputs, score should be roughly mid-range
    assert 30 < result["overall_score"] < 80
