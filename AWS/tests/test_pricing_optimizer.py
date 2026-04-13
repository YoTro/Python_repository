
import json
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from src.intelligence.processors.optimizer_pricing import PricingOptimizer

def test_pricing_optimization():
    optimizer = PricingOptimizer()
    
    # Inputs
    base_cost = 5.0
    category = "Baby Products" # < $10 is 8%, > $10 is 15%
    weight_lb = 0.5
    current_price = 15.0
    current_monthly_sales = 1000
    
    print(f"--- Running Price Optimization ---")
    print(f"Current: Price=${current_price}, Sales={current_monthly_sales}, Cost=${base_cost}")
    
    result = optimizer.optimize(
        base_cost=base_cost,
        category=category,
        weight_lb=weight_lb,
        current_price=current_price,
        current_monthly_sales=current_monthly_sales,
        elasticity=-2.5 # High sensitivity
    )
    
    if result["status"] == "OPTIMAL":
        rec = result["recommendation"]
        print(f"Optimal Price: ${rec['optimal_price']}")
        print(f"Expected Sales: {rec['expected_monthly_sales']} units")
        print(f"Expected Profit: ${rec['expected_monthly_profit']}")
        print(f"Profit Change: {result['comparison_to_current']['profit_change']}")
        print(f"Net Profit per Unit: ${rec['unit_metrics']['net_profit_per_unit']}")
    else:
        print(f"Failed: {result.get('message')}")

if __name__ == "__main__":
    test_pricing_optimization()
