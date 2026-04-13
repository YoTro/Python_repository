
import json
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.intelligence.processors.optimizer_ad_budget import AdBudgetOptimizer

def test_optimization():
    optimizer = AdBudgetOptimizer()
    
    keywords = [
        {"name": "high_cvr_premium", "avg_cpc": 3.0, "estimated_cvr": 0.15, "max_daily_clicks": 20},
        {"name": "medium_cvr_standard", "avg_cpc": 1.2, "estimated_cvr": 0.05, "max_daily_clicks": 100},
        {"name": "low_cpc_volume", "avg_cpc": 0.4, "estimated_cvr": 0.01, "max_daily_clicks": 500},
    ]
    
    budget = 100.0
    
    print(f"--- Running Ad Budget Optimization (Budget: ${budget}) ---")
    result = optimizer.optimize(keywords, budget)
    
    if result["status"] == "OPTIMAL":
        print(f"Success! Expected Orders: {result['summary']['total_expected_orders']}")
        print(f"Actual Spend: ${result['summary']['actual_spend']}")
        print("\nAllocation Details:")
        for item in result["allocation"]:
            print(f"- {item['keyword']}: {item['optimized_clicks']} clicks (Spend: ${item['estimated_spend']})")
    else:
        print(f"Failed: {result.get('message')}")

if __name__ == "__main__":
    test_optimization()
