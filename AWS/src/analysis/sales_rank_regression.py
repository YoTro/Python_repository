import pandas as pd
import numpy as np
import re
import logging
import os
from sklearn.linear_model import LinearRegression
from typing import List, Dict, Tuple, Optional, Union
from src.core.models import StandardProduct

logger = logging.getLogger(__name__)

class SalesRankRegressor:
    """
    Advanced UCLA Sales-Rank Regressor with Seasonality Corrections.
    
    Aims for R² > 0.8 by incorporating:
    1. Monthly Fixed Effects (c shifts by month for seasonal categories like Patio).
    2. Weekly Effects (Weekend vs Weekday traffic differences).
    """

    def __init__(self):
        self.model = LinearRegression()
        self.theta = None
        self.month_effects = {}
        self.weekend_effect = 0
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

    def fit(self, data_input: Union[pd.DataFrame, List[Dict], List[StandardProduct]], 
            rank_col: str = 'sales_rank', 
            sales_col: str = 'orders', 
            date_col: str = 'time') -> bool:
        """
        Fits an advanced multiple regression model.
        Supports both DataFrame and List of StandardProduct/Dict.
        """
        if isinstance(data_input, pd.DataFrame):
            df = data_input.copy()
        else:
            # Convert to DataFrame, handling StandardProduct objects
            normalized = []
            for item in data_input:
                if isinstance(item, StandardProduct):
                    normalized.append(item.to_dict())
                else:
                    normalized.append(item)
            df = pd.DataFrame(normalized)

        # Fallback for legacy column names if standard ones aren't found
        if rank_col not in df.columns and 'PrimaryRank' in df.columns:
            rank_col = 'PrimaryRank'
        if sales_col not in df.columns and 'Orders' in df.columns:
            sales_col = 'Orders'
        if date_col not in df.columns and 'Time' in df.columns:
            date_col = 'Time'

        if rank_col not in df.columns or sales_col not in df.columns:
            logger.error(f"Required columns {rank_col} or {sales_col} not found in data.")
            return False

        data = df.copy()
        data['Rank_Num'] = data[rank_col].apply(self._extract_rank)
        data['Sales_Num'] = pd.to_numeric(data[sales_col], errors='coerce')
        
        # 1. Feature Engineering
        if date_col in data.columns:
            data[date_col] = pd.to_datetime(data[date_col])
            data['Month'] = data[date_col].dt.month
            data['Is_Weekend'] = data[date_col].dt.dayofweek.isin([5, 6]).astype(int)
        else:
            logger.warning(f"Date column {date_col} not found. Seasonality will not be calculated.")
            data['Month'] = 1
            data['Is_Weekend'] = 0

        # Filter: Rank > 1 and Sales > 0
        mask = (data['Sales_Num'] > 0) & (data['Rank_Num'] > 1)
        clean_data = data[mask]

        if len(clean_data) < 10:
            logger.error(f"Not enough data for advanced regression (found {len(clean_data)} rows).")
            return False

        # 2. Prepare Features: ln(Rank-1), Month Dummies, Is_Weekend
        # Target: ln(Sales)
        y = np.log(clean_data['Sales_Num']).values
        
        # ln(Rank - 1)
        X = pd.DataFrame({
            'ln_rank': np.log(clean_data['Rank_Num'] - 1)
        })
        
        # Add Month Dummies (avoiding dummy variable trap by dropping first)
        month_dummies = pd.get_dummies(clean_data['Month'], prefix='Month', drop_first=True)
        # Ensure all Month_X columns are present even if not in this sample? 
        # Actually, for fit it's fine. For predict we'll need to handle it.
        X = pd.concat([X, month_dummies], axis=1)
        
        # Add Weekend
        X['Is_Weekend'] = clean_data['Is_Weekend']
        
        self.feature_cols = X.columns.tolist()

        # 3. Fit
        self.model.fit(X, y)
        self.r_squared = self.model.score(X, y)
        
        # 4. Extract parameters for interpretation
        # ln(Sales) = a*ln(Rank-1) + ... => theta = -1/a
        slope_ln_rank = self.model.coef_[0]
        self.theta = -1.0 / slope_ln_rank if slope_ln_rank != 0 else 0
        
        self.is_fitted = True
        logger.info(f"Advanced Model fitted. R2: {self.r_squared:.4f}, Theta: {self.theta:.4f}")
        return True

    def predict(self, rank: int, month: int = 1, is_weekend: int = 0, multiplier: float = 7.0) -> float:
        """
        Predict Weekly Sales with seasonal and weekly context.
        """
        if not self.is_fitted:
            raise ValueError("Model not fitted.")
        if rank <= 1: return 0.0

        # Build feature vector
        feat_dict = {col: 0.0 for col in self.feature_cols}
        feat_dict['ln_rank'] = np.log(rank - 1)
        
        month_col = f'Month_{month}'
        if month_col in feat_dict:
            feat_dict[month_col] = 1.0
        
        if 'Is_Weekend' in feat_dict:
            feat_dict['Is_Weekend'] = float(is_weekend)
            
        X_pred = pd.DataFrame([feat_dict])[self.feature_cols]
        
        # Predicted ln(Daily Sales)
        ln_q_daily = self.model.predict(X_pred)[0]
        q_daily = np.exp(ln_q_daily)
        
        return float(q_daily * multiplier)

    def get_summary(self) -> Dict:
        return {
            "theta_elasticity": self.theta,
            "r_squared": self.r_squared,
            "features_used": self.feature_cols,
            "note": "Intercept c is now dynamic per month to handle seasonality."
        }
