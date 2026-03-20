import pytest
import numpy as np
from src.core.models.product import Product
from src.intelligence.processors.sales_estimator import SalesEstimator

def test_category_specific_estimation():
    """
    Verify that the estimator uses parameters from the JSON config 
    when a matching node_id is provided.
    """
    estimator = SalesEstimator()
    
    # Data from config: Patio, Lawn & Garden (node_id: 553632)
    # theta: 1.7886, c: 19.2222
    node_id = "553632"
    theta = 1.7886
    c = 19.2222
    rank = 1000
    
    product = Product(
        asin="B00EXAMPLE",
        sales_rank=rank,
        category_node_id=node_id
    )
    
    estimated_sales = estimator.estimate_monthly_sales(product)
    
    # Manual calculation: exp((c - ln(rank-1)) / theta)
    expected_sales = int(np.exp((c - np.log(rank - 1)) / theta))
    
    print(f"\nNode ID: {node_id} | Rank: {rank}")
    print(f"Expected: {expected_sales} | Got: {estimated_sales}")
    
    assert estimated_sales == expected_sales
    assert estimated_sales > 0

def test_fallback_to_default():
    """
    Verify that a default regressor is used when node_id is missing or unknown.
    """
    estimator = SalesEstimator()
    
    product_no_node = Product(asin="B1", sales_rank=5000)
    product_unknown_node = Product(asin="B2", sales_rank=5000, category_node_id="999999999")
    
    sales1 = estimator.estimate_monthly_sales(product_no_node)
    sales2 = estimator.estimate_monthly_sales(product_unknown_node)
    
    # Since both should use the same default regressor, results should match
    assert sales1 == sales2
    assert sales1 > 0

def test_dynamic_calibration():
    """
    Verify that we can calibrate the default model using a small sample of market data.
    """
    estimator = SalesEstimator()
    
    # Sample data for a fictional niche
    products = [
        Product(asin="A1", sales_rank=100, past_month_sales=5000),
        Product(asin="A2", sales_rank=500, past_month_sales=1200),
        Product(asin="A3", sales_rank=1000, past_month_sales=600),
        Product(asin="A4", sales_rank=5000, past_month_sales=150),
    ]
    
    # Calibrate
    estimator.calibrate_with_market_data(products, node_id="NEW_NICHE")
    
    # Test a prediction for this new niche
    test_prod = Product(asin="A5", sales_rank=300, category_node_id="NEW_NICHE")
    calibrated_sales = estimator.estimate_monthly_sales(test_prod)
    
    print(f"\nCalibrated prediction for rank 300: {calibrated_sales}")
    
    # With rank 300, it should be between rank 100 (5000) and rank 500 (1200)
    assert 1200 < calibrated_sales < 5000

if __name__ == "__main__":
    # Allow manual run
    test_category_specific_estimation()
    test_fallback_to_default()
    test_dynamic_calibration()
    print("\nAll SalesEstimator tests passed locally!")
