from __future__ import annotations
import logging
from typing import List, Dict, Any, Optional
from ortools.sat.python import cp_model
from src.mcp.servers.finance.tools import get_referral_rate, estimate_fba_fee_from_dims

logger = logging.getLogger(__name__)

class PricingOptimizer:
    """
    Processor using Google OR-Tools (CP-SAT) to find the optimal price point.
    It accounts for non-linear Amazon fees (referral fee tiers, FBA tiers)
    and price-demand elasticity.
    """

    def __init__(self):
        pass

    def optimize(self, 
                 base_cost: float,
                 category: str,
                 weight_lb: float,
                 current_price: float,
                 current_monthly_sales: int,
                 price_range: tuple[float, float] = (None, None),
                 elasticity: float = -2.0) -> Dict[str, Any]:
        """
        :param base_cost: Landed cost (COGS + Shipping).
        :param category: Amazon category for referral fee lookup.
        :param weight_lb: Product weight for FBA fee lookup.
        :param current_price: Current selling price.
        :param current_monthly_sales: Current sales volume at current_price.
        :param price_range: Min and Max prices to test.
        :param elasticity: Price elasticity of demand (default -2.0 means 10% price increase -> 20% sales drop).
        """
        model = cp_model.CpModel()
        
        # 1. Define Candidate Prices (Discretized into 0.1 increments for precision)
        min_p = price_range[0] or max(base_cost * 1.2, current_price * 0.5)
        max_p = price_range[1] or (current_price * 2.0)
        
        # We'll test prices in $0.1 steps. 
        # Convert to cents (integers) for CP-SAT
        candidates = []
        p = min_p
        while p <= max_p:
            candidates.append(round(p, 2))
            p += 0.5 # $0.5 steps for faster solving and realistic pricing
            
        num_candidates = len(candidates)
        
        # 2. Pre-calculate metrics for each candidate
        profits_in_cents = []
        volumes = []
        
        fba_fee = estimate_fba_fee_from_dims(weight_lb)
        
        for price in candidates:
            # Calculate Fees
            ref_rate = get_referral_rate(category, price)
            referral_fee = price * ref_rate
            
            unit_profit = price - base_cost - referral_fee - fba_fee
            
            # Simple Elasticity Model: Q = Q0 * (P / P0) ^ Elasticity
            # We use a linear approximation if P is close to P0 for simplicity in this solver
            volume_multiplier = (price / current_price) ** elasticity
            expected_volume = current_monthly_sales * volume_multiplier
            
            total_profit = unit_profit * expected_volume
            
            # CP-SAT works with integers. Scale up by 100 to handle cents.
            profits_in_cents.append(int(total_profit * 100))
            volumes.append(expected_volume)

        # 3. Decision Variable: Which candidate index to pick?
        # x[i] is 1 if candidate i is chosen, 0 otherwise.
        x = [model.NewBoolVar(f'x_{i}') for i in range(num_candidates)]

        # 4. Constraints: Only one price can be chosen
        model.Add(sum(x) == 1)

        # 5. Objective: Maximize Total Profit
        model.Maximize(sum(x[i] * profits_in_cents[i] for i in range(num_candidates)))

        # 6. Solve
        solver = cp_model.CpSolver()
        status = solver.Solve(model)

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            best_idx = -1
            for i in range(num_candidates):
                if solver.Value(x[i]):
                    best_idx = i
                    break
            
            best_price = candidates[best_idx]
            best_ref_rate = get_referral_rate(category, best_price)
            
            return {
                "status": "OPTIMAL",
                "recommendation": {
                    "optimal_price": best_price,
                    "expected_monthly_sales": round(volumes[best_idx], 1),
                    "expected_monthly_profit": round(solver.ObjectiveValue() / 100.0, 2),
                    "unit_metrics": {
                        "referral_fee": round(best_price * best_ref_rate, 2),
                        "fba_fee": round(fba_fee, 2),
                        "net_profit_per_unit": round(best_price - base_cost - (best_price * best_ref_rate) - fba_fee, 2)
                    }
                },
                "comparison_to_current": {
                    "price_change": f"{((best_price/current_price)-1)*100:+.1f}%",
                    "profit_change": f"{((solver.ObjectiveValue()/100.0)/(max(1, (current_price - base_cost - (current_price * get_referral_rate(category, current_price)) - fba_fee) * current_monthly_sales)) - 1)*100:+.1f}%"
                }
            }

        return {"status": "FAILED", "message": "Could not find an optimal price point."}
