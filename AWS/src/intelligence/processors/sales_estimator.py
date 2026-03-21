from __future__ import annotations
import logging
import pandas as pd
import numpy as np
import re
import json
import os
from sklearn.linear_model import LinearRegression
from typing import List, Dict, Optional, Union
from src.core.models.product import Product

logger = logging.getLogger(__name__)

class SalesRankRegressor:
    """
    Advanced UCLA Sales-Rank Regressor with Category-specific calibration.
    Supports pre-calculated parameters, dynamic fitting, and social proof calibration.
    """

    def __init__(self):
        self.model = LinearRegression()
        # Defaults to a conservative general average
        self.theta = 1.5
        self.intercept_c = 15.0
        self.r_squared = 0.90
        self.is_fitted = True # Defaults to True so fallback works
        self.is_monthly_calibrated = True
        self.feature_cols = []

    def _extract_rank(self, rank_str: str) -> Optional[int]:
        if pd.isna(rank_str): return None
        if isinstance(rank_str, (int, float)): return int(rank_str)
        nums = re.findall(r'(\d[\d,]*)', str(rank_str))
        if nums:
            return int(nums[-1].replace(',', ''))
        return None

    def set_parameters(self, theta: float, c: float, r_squared: Optional[float] = None):
        """Manually set pre-calculated parameters."""
        self.theta = theta
        self.intercept_c = c
        self.r_squared = r_squared
        self.is_fitted = True
        self.is_monthly_calibrated = True # Uses the ln(Sales) = (c - ln(Rank-1))/theta formula

    def fit_with_past_month_data(self, ranks: List[int], sales: List[int], coefficient: float = 1.2) -> bool:
        """
        Calibrate model using Amazon's "Past Month Sales" (social proof) data.
        Formula: ln(Rank-1) = -theta * ln(Sales * coefficient) + c
        """
        valid_data = [(r, s) for r, s in zip(ranks, sales) if r and s and r > 1 and s > 0]
        if len(valid_data) < 3:
            logger.error("Insufficient data for past month calibration.")
            return False

        df = pd.DataFrame(valid_data, columns=['rank', 'sales'])
        X = np.log(df['sales'].values * coefficient).reshape(-1, 1)
        y = np.log(df['rank'].values - 1)

        self.model.fit(X, y)
        self.r_squared = self.model.score(X, y)
        self.theta = -float(self.model.coef_[0])
        self.intercept_c = float(self.model.intercept_)
        
        self.is_fitted = True
        self.is_monthly_calibrated = True
        logger.info(f"Calibration complete. R2: {self.r_squared:.4f}, Theta: {self.theta:.4f}, C: {self.intercept_c:.4f}")
        return True

    def predict(self, rank: int) -> float:
        """Predict monthly sales based on BSR."""
        if not self.is_fitted or rank <= 1: return 0.0
        
        # ln(Sales) = (c - ln(Rank-1)) / theta
        try:
            ln_sales = (self.intercept_c - np.log(rank - 1)) / self.theta
            return float(np.exp(ln_sales))
        except Exception as e:
            logger.error(f"Prediction error for rank {rank}: {e}")
            return 0.0

class SalesEstimator:
    """
    Intelligence Processor that manages BSR-to-Sales conversion.
    Loads category-specific parameters from a central configuration.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        self.default_regressor = SalesRankRegressor()
        self.category_params: Dict[str, Dict] = {}
        
        # Load pre-calculated parameters
        if not config_path:
            config_path = os.path.join(os.path.dirname(__file__), "config", "amazon_sales_estimator.json")
            
        self._load_config(config_path)
        self.is_ready = True

    def _load_config(self, path: str):
        if not os.path.exists(path):
            logger.warning(f"Sales estimator config not found at {path}. Using defaults.")
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Supports new nested structure
                categories = data.get("categories", [])
                for cat in categories:
                    node_id = cat.get("node_id")
                    est = cat.get("estimation", {})
                    if node_id and est:
                        # Map estimation params
                        self.category_params[str(node_id)] = {
                            "theta": est.get("theta"),
                            "c": est.get("c"),
                            "r_squared": est.get("r_squared"),
                            "unit": est.get("unit", "monthly"),
                            "market_logic": cat.get("market_logic", {}) # Carry logic forward
                        }
            logger.info(f"Loaded {len(self.category_params)} category profiles.")
        except Exception as e:
            logger.error(f"Failed to load sales estimator config: {e}")

    def _get_regressor_for_product(self, product: Product) -> SalesRankRegressor:
        """Get a specialized regressor if category info is available, else default."""
        if product.category_node_id and str(product.category_node_id) in self.category_params:
            params = self.category_params[str(product.category_node_id)]
            reg = SalesRankRegressor()
            reg.set_parameters(theta=params['theta'], c=params['c'], r_squared=params.get('r_squared'))
            
            # Adjust if unit is weekly (currently our regressor is monthly-centric)
            if params.get("unit") == "weekly":
                # ln(Sales_m) = ln(Sales_w * 4) = ln(Sales_w) + ln(4)
                # ln(Sales_m) = (c/theta - ln(rank-1)/theta) + ln(4)
                # new_c = c + theta * ln(4)
                reg.intercept_c = params['c'] + (params['theta'] * np.log(4))
                
            return reg
        return self.default_regressor

    def estimate_monthly_sales(self, product: Product) -> int:
        if not product.sales_rank or product.sales_rank <= 1:
            return 0
            
        regressor = self._get_regressor_for_product(product)
        return int(regressor.predict(product.sales_rank))

    def calibrate_with_market_data(self, products: List[Product], node_id: Optional[str] = None):
        """Dynamically calibrate parameters based on a list of products with 'past_month_sales'."""
        ranks = [p.sales_rank for p in products if p.sales_rank and p.past_month_sales]
        sales = [p.past_month_sales for p in products if p.sales_rank and p.past_month_sales]
        
        if self.default_regressor.fit_with_past_month_data(ranks, sales):
            logger.info("Default SalesEstimator calibrated with provided market data.")
            if node_id:
                # Cache for future use in this session
                summary = self.default_regressor.__dict__.copy() # Simplified
                self.category_params[str(node_id)] = {
                    "theta": self.default_regressor.theta,
                    "c": self.default_regressor.intercept_c,
                    "r_squared": self.default_regressor.r_squared
                }

    def batch_estimate(self, products: List[Product]) -> Dict[str, int]:
        return {p.asin: self.estimate_monthly_sales(p) for p in products}
