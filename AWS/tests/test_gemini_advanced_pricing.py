from __future__ import annotations
import pytest
from src.intelligence.providers.price_manager import PriceManager

def test_price_manager_calculation_gemini_advanced():
    """Verify Gemini pricing with cached tokens and thinking tokens."""
    pm = PriceManager(provider="gemini")
    
    # Model: gemini-2.0-flash standard_paid
    # Prices: Input $0.1, Output $0.4, Cache Read $0.025 (per 1M tokens)
    
    model = "gemini-2.0-flash"
    input_tokens = 1000000
    output_tokens = 500000
    kwargs = {
        "cached_content_token_count": 600000,
        "thoughts_token_count": 200000
    }
    
    # Expected Calculation:
    # 1. Non-cached input = 1,000,000 - 600,000 = 400,000
    # 2. Input cost = (400,000 * 0.1) + (600,000 * 0.025) = 40,000 + 15,000 = 55,000
    # 3. Total output = 500,000 + 200,000 = 700,000
    # 4. Output cost = 700,000 * 0.4 = 280,000
    # 5. Total cost = (55,000 + 280,000) / 1,000,000 = 0.335
    
    cost = pm.calculate_cost(model, input_tokens, output_tokens, **kwargs)
    assert cost == 0.335

def test_price_manager_calculation_gemini_no_advanced_fields():
    """Verify Gemini pricing still works correctly without advanced fields."""
    pm = PriceManager(provider="gemini")
    
    # gemini-2.0-flash standard_paid: Input $0.1, Output $0.4
    cost = pm.calculate_cost("gemini-2.0-flash", 1000000, 1000000)
    # (1M * 0.1 + 1M * 0.4) / 1M = 0.5
    assert cost == 0.5

def test_price_manager_calculation_gemini_tiered_pricing():
    """Verify Gemini tiered pricing (>200k tokens)."""
    pm = PriceManager(provider="gemini")
    
    # gemini-3.1-pro-preview standard_paid (fictional or check config)
    # lte_200k: Input $2.0, Output $12.0
    # gt_200k:  Input $4.0, Output $18.0
    
    # Case A: <= 200k
    cost_low = pm.calculate_cost("gemini-3.1-pro-preview", 150000, 10000)
    expected_low = (150000 * 2.0 / 1e6) + (10000 * 12.0 / 1e6) # 0.3 + 0.12 = 0.42
    assert cost_low == 0.42
    
    # Case B: > 200k (based on input tokens as per new logic)
    cost_high = pm.calculate_cost("gemini-3.1-pro-preview", 250000, 10000)
    expected_high = (250000 * 4.0 / 1e6) + (10000 * 18.0 / 1e6) # 1.0 + 0.18 = 1.18
    assert cost_high == 1.18
