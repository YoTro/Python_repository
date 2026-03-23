from __future__ import annotations
import pytest
from src.intelligence.processors.promo_analyzer import PromoAnalyzer

@pytest.fixture
def analyzer():
    return PromoAnalyzer()

def test_analyze_empty_deals(analyzer):
    result = analyzer.analyze(current_price=29.99, deals=[])
    assert result["promo_frequency"] == 0.0
    assert result["all_time_low"] == 29.99
    assert result["median_discount_pct"] == 0.0
    assert result["promo_dependency_score"] == 0.0
    assert result["risk_level"] == "Low (Stable Price)"
    assert result["total_deals_found"] == 0

def test_analyze_stable_price(analyzer):
    deals = [
        {"date": "2025-01-01", "price": 27.99, "discount_pct": 5},
    ]
    result = analyzer.analyze(current_price=29.99, deals=deals, days_analyzed=180)
    assert result["total_deals_found"] == 1
    assert result["all_time_low"] == 27.99
    assert result["median_discount_pct"] == 5.0
    assert result["promo_dependency_score"] < 40  # Low score
    assert result["risk_level"] == "Low (Stable Price)"

def test_analyze_medium_risk(analyzer):
    deals = [
        {"price": 19.99, "discount_pct": 30},
        {"price": 22.99, "discount_pct": 20},
    ]
    result = analyzer.analyze(current_price=29.99, deals=deals, days_analyzed=180)
    assert result["total_deals_found"] == 2
    assert result["all_time_low"] == 19.99
    assert result["median_discount_pct"] == 25.0
    
    # frequency = 2 * 3 / 180 = 6 / 180 = 0.033
    # dependency = 0.033 * 50 + min(25.0, 50) = 1.65 + 25.0 = 26.65 (so it might be low actually)
    # let's make it medium risk (>= 40)
    
    deals_medium = [{"price": 19.99, "discount_pct": 30} for _ in range(20)]
    result2 = analyzer.analyze(current_price=29.99, deals=deals_medium, days_analyzed=180)
    # frequency = 20 * 3 / 180 = 60 / 180 = 0.333
    # dependency = 0.333 * 50 + 30 = 16.65 + 30 = 46.65 (medium risk)
    assert result2["risk_level"] == "Medium (Regular Promotions)"

def test_analyze_high_risk_clearance(analyzer):
    deals = [
        {"price": 15.00, "discount_pct": 50},
        {"price": 14.00, "discount_pct": 55},
        {"price": 10.00, "discount_pct": 60},
        {"price": 12.00, "discount_pct": 50},
        {"price": 11.00, "discount_pct": 50},
        {"price": 10.00, "discount_pct": 60},
        {"price": 12.00, "discount_pct": 50},
        {"price": 11.00, "discount_pct": 50},
        {"price": 10.00, "discount_pct": 60},
        {"price": 12.00, "discount_pct": 50},
    ]
    result = analyzer.analyze(current_price=29.99, deals=deals, days_analyzed=30)
    # Frequency is 10 deals * 3 = 30 / 30 = 1.0 (capped at 1.0)
    # Dependency = 1.0 * 50 + min(median(50+), 50) = 50 + 50 = 100
    assert result["all_time_low"] == 10.00
    assert result["promo_frequency"] == 1.0
    assert result["median_discount_pct"] >= 50.0
    assert result["promo_dependency_score"] >= 70.0
    assert result["risk_level"] == "High (Price War/Clearance)"
