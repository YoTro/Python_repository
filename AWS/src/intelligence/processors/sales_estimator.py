from __future__ import annotations
import logging
import pandas as pd
import numpy as np
import re
from sklearn.linear_model import LinearRegression
from typing import List, Dict, Optional, Union
from src.core.models.product import Product

logger = logging.getLogger(__name__)

class SalesRankRegressor:
    """
    Advanced UCLA Sales-Rank Regressor with Seasonality Corrections.
    Moved internally to SalesEstimator to simplify module structure.
    """

    def __init__(self):
        self.model = LinearRegression()
        self.theta = None
        self.r_squared = None
        self.is_fitted = False
        self.feature_cols = []

    def _extract_rank(self, rank_str: str) -> Optional[int]:
        if pd.isna(rank_str): return None
        if isinstance(rank_str, (int, float)): return int(rank_str)
        nums = re.findall(r'(\d[\d,]*)', str(rank_str))
        if nums:
            return int(nums[-1].replace(',', ''))
        return None

    def fit(self, data_input: Union[pd.DataFrame, List[Dict], List[Product]], 
            rank_col: str = 'sales_rank', 
            sales_col: str = 'orders', 
            date_col: str = 'time') -> bool:
        
        if isinstance(data_input, pd.DataFrame):
            df = data_input.copy()
        else:
            normalized = []
            for item in data_input:
                if isinstance(item, Product):
                    normalized.append(item.model_dump())
                else:
                    normalized.append(item)
            df = pd.DataFrame(normalized)

        if rank_col not in df.columns or sales_col not in df.columns:
            logger.error(f"Required columns {rank_col} or {sales_col} not found.")
            return False

        data = df.copy()
        data['Rank_Num'] = data[rank_col].apply(self._extract_rank)
        data['Sales_Num'] = pd.to_numeric(data[sales_col], errors='coerce')
        
        if date_col in data.columns:
            data[date_col] = pd.to_datetime(data[date_col])
            data['Month'] = data[date_col].dt.month
            data['Is_Weekend'] = data[date_col].dt.dayofweek.isin([5, 6]).astype(int)
        else:
            data['Month'] = 1
            data['Is_Weekend'] = 0

        mask = (data['Sales_Num'] > 0) & (data['Rank_Num'] > 1)
        clean_data = data[mask]

        if len(clean_data) < 10:
            logger.error(f"Not enough data for regression (found {len(clean_data)} rows).")
            return False

        y = np.log(clean_data['Sales_Num']).values
        X = pd.DataFrame({
            'ln_rank': np.log(clean_data['Rank_Num'] - 1)
        })
        
        month_dummies = pd.get_dummies(clean_data['Month'], prefix='Month', drop_first=True)
        X = pd.concat([X, month_dummies], axis=1)
        X['Is_Weekend'] = clean_data['Is_Weekend']
        
        self.feature_cols = X.columns.tolist()

        self.model.fit(X, y)
        self.r_squared = self.model.score(X, y)
        
        slope_ln_rank = self.model.coef_[0]
        self.theta = -1.0 / slope_ln_rank if slope_ln_rank != 0 else 0
        
        self.is_fitted = True
        logger.info(f"Model fitted. R2: {self.r_squared:.4f}, Theta: {self.theta:.4f}")
        return True

    def predict(self, rank: int, month: int = 1, is_weekend: int = 0, multiplier: float = 7.0) -> float:
        if not self.is_fitted: raise ValueError("Model not fitted.")
        if rank <= 1: return 0.0

        feat_dict = {col: 0.0 for col in self.feature_cols}
        feat_dict['ln_rank'] = np.log(rank - 1)
        
        month_col = f'Month_{month}'
        if month_col in feat_dict:
            feat_dict[month_col] = 1.0
        
        if 'Is_Weekend' in feat_dict:
            feat_dict['Is_Weekend'] = float(is_weekend)
            
        X_pred = pd.DataFrame([feat_dict])[self.feature_cols]
        ln_q_daily = self.model.predict(X_pred)[0]
        q_daily = np.exp(ln_q_daily)
        
        return float(q_daily * multiplier)

    def get_summary(self) -> Dict:
        return {"theta_elasticity": self.theta, "r_squared": self.r_squared, "features_used": self.feature_cols}


class SalesEstimator:
    """
    Intelligence Processor that wraps the UCLA Sales-Rank Regression logic.
    Provides Agents with deterministic sales estimates based on BSR.
    """
    
    def __init__(self, training_data_path: Optional[str] = None):
        self.regressor = SalesRankRegressor()
        self.is_ready = False
        
        if training_data_path:
            self.load_and_train(training_data_path)
        else:
            self._apply_global_defaults()

    def _apply_global_defaults(self):
        self.regressor.theta = 1.5 
        self.regressor.is_fitted = True
        self.is_ready = True

    def load_and_train(self, csv_path: str):
        try:
            df = pd.read_csv(csv_path)
            success = self.regressor.fit(df)
            if success:
                self.is_ready = True
                logger.info(f"SalesEstimator trained successfully using {csv_path}")
        except Exception as e:
            logger.error(f"Failed to train SalesEstimator: {e}")

    def estimate_monthly_sales(self, product: Product) -> int:
        if not product.sales_rank or product.sales_rank <= 1:
            return 0
            
        if not self.is_ready:
            logger.warning("SalesEstimator not trained. Using heuristic.")
            return int(100000 / product.sales_rank) if product.sales_rank > 0 else 0

        try:
            weekly = self.regressor.predict(product.sales_rank)
            return int(weekly * 4)
        except Exception as e:
            logger.error(f"Error estimating sales for {product.asin}: {e}")
            return 0

    def batch_estimate(self, products: List[Product]) -> Dict[str, int]:
        results = {}
        for p in products:
            results[p.asin] = self.estimate_monthly_sales(p)
        return results
