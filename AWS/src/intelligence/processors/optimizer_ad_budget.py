from __future__ import annotations
import logging
from typing import List, Dict, Any, Optional
from ortools.linear_solver import pywraplp

logger = logging.getLogger(__name__)

class AdBudgetOptimizer:
    """
    Processor using Google OR-Tools to solve the optimal ad budget allocation problem.
    Given a set of keywords with different CPCs, CVRs, and traffic ceilings, 
    finds the allocation that maximizes total conversions within a budget.
    """

    def __init__(self):
        # Create the linear solver with the GLOP backend.
        self.solver = pywraplp.Solver.CreateSolver("GLOP")
        if not self.solver:
            logger.error("Could not create OR-Tools GLOP solver.")

    def optimize(self, 
                 keywords: List[Dict[str, Any]], 
                 total_budget: float,
                 min_clicks_per_kw: int = 0) -> Dict[str, Any]:
        """
        :param keywords: List of dicts: [
            {"name": "kw1", "avg_cpc": 1.2, "estimated_cvr": 0.05, "max_daily_clicks": 100},
            ...
        ]
        :param total_budget: Maximum daily spend in currency.
        :return: Optimized allocation plan.
        """
        if not self.solver:
            return {"error": "Solver not initialized"}
        
        self.solver.Clear()
        
        # 1. Variables: Number of clicks to buy for each keyword
        # clicks[i] >= min_clicks_per_kw AND clicks[i] <= max_daily_clicks
        num_keywords = len(keywords)
        clicks = []
        for i in range(num_keywords):
            kw = keywords[i]
            upper_bound = kw.get("max_daily_clicks", 1000)
            var = self.solver.NumVar(min_clicks_per_kw, upper_bound, kw["name"])
            clicks.append(var)

        # 2. Constraint: Total Spend <= Budget
        # Sum(clicks[i] * cpc[i]) <= total_budget
        spend_constraint = self.solver.Constraint(0, total_budget)
        for i in range(num_keywords):
            spend_constraint.SetCoefficient(clicks[i], keywords[i]["avg_cpc"])

        # 3. Objective: Maximize Conversions (Orders)
        # Convert = Sum(clicks[i] * cvr[i])
        objective = self.solver.Objective()
        for i in range(num_keywords):
            objective.SetCoefficient(clicks[i], keywords[i].get("estimated_cvr", 0.01))
        objective.SetMaximization()

        # 4. Solve
        status = self.solver.Solve()

        if status == pywraplp.Solver.OPTIMAL:
            allocation = []
            total_spend = 0
            total_clicks = 0
            
            for i in range(num_keywords):
                sol_value = clicks[i].solution_value()
                if sol_value > 0.5: # Filter out near-zero noise
                    cost = sol_value * keywords[i]["avg_cpc"]
                    allocation.append({
                        "keyword": keywords[i]["name"],
                        "optimized_clicks": round(sol_value, 1),
                        "estimated_spend": round(cost, 2),
                        "contribution_to_orders": round(sol_value * keywords[i].get("estimated_cvr", 0), 2)
                    })
                    total_spend += cost
                    total_clicks += sol_value

            return {
                "status": "OPTIMAL",
                "summary": {
                    "total_budget": total_budget,
                    "actual_spend": round(total_spend, 2),
                    "total_expected_orders": round(self.solver.Objective().Value(), 2),
                    "avg_effective_cpc": round(total_spend / total_clicks, 2) if total_clicks > 0 else 0
                },
                "allocation": sorted(allocation, key=lambda x: x["optimized_clicks"], reverse=True)
            }
        
        return {
            "status": "FAILED",
            "error_code": status,
            "message": "No optimal solution found within constraints."
        }
