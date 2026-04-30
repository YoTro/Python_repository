from __future__ import annotations
import logging
import math
from typing import Dict, List, Optional, Any

from ortools.linear_solver import pywraplp

logger = logging.getLogger(__name__)

# ── Bidding strategy → conservative CPC risk multiplier ──────────────────────
# "Dynamic bids - up and down": Amazon can raise bids up to 100% for TOS,
# up to 50% for other placements.  We use a blended conservative overhead so
# the budget constraint is not violated in practice.
_STRATEGY_CPC_MULTIPLIER: Dict[str, float] = {
    "Dynamic bids - up and down": 1.40,  # blended ~40% overhead (TOS-heavy accounts)
    "Dynamic bids - down only":   1.00,  # bids can only decrease → no overhead
    "Fixed bids":                 1.00,
    "Rule-based bidding":         1.15,  # some variance from rules
}
_DEFAULT_STRATEGY_MULTIPLIER = 1.20  # unknown strategy — moderate overhead

# CVR shrinkage: Wilson-style confidence weight.
# At CONFIDENCE_PRIOR clicks the effective CVR = raw_cvr × 1/√2 ≈ 71%.
# This penalises low-sample keywords without zeroing them out.
_CONFIDENCE_PRIOR = 30


def _pessimistic_cvr(raw_cvr: float, total_clicks: int) -> float:
    weight = math.sqrt(total_clicks / (total_clicks + _CONFIDENCE_PRIOR))
    return raw_cvr * weight


class AdBudgetOptimizer:
    """
    OR-Tools GLOP LP that maximises expected daily orders subject to:

    Constraint 1  — Global budget cap
        Σ clicks_i × eff_cpc_i ≤ total_budget

    Constraint 2  — Per-campaign budget caps  (new)
        Σ_{i ∈ campaign_c} clicks_i × eff_cpc_i ≤ budget_c    ∀ c

    Constraint 3  — Target ACOS  (new, linearised)
        Σ clicks_i × eff_cpc_i ≤ target_acos × Σ clicks_i × pessimistic_cvr_i × avg_price
        ⟺ Σ clicks_i × (eff_cpc_i − target_acos × pessimistic_cvr_i × avg_price) ≤ 0

    Constraint 4  — Inventory-linked order cap  (new)
        Σ clicks_i × pessimistic_cvr_i ≤ max_daily_orders

    Constraint 5  — Click bounds  (min floor added)
        min_daily_clicks_i ≤ clicks_i ≤ max_daily_clicks_i

    Effective CPC = avg_cpc × bidding_strategy_multiplier × placement_multiplier
    Objective     = Maximise Σ clicks_i × pessimistic_cvr_i
    """

    def __init__(self) -> None:
        self.solver = pywraplp.Solver.CreateSolver("GLOP")
        if not self.solver:
            logger.error("Could not create OR-Tools GLOP solver.")

    def optimize(
        self,
        keywords: List[Dict[str, Any]],
        total_budget: float,
        campaign_budgets: Optional[Dict[str, float]] = None,
        target_acos: Optional[float] = None,
        avg_price: Optional[float] = None,
        max_daily_orders: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Parameters
        ----------
        keywords : list of dicts with keys:
            name               str   "keyword_text|MATCH_TYPE"
            avg_cpc            float historical avg CPC
            estimated_cvr      float historical CVR (point estimate)
            max_daily_clicks   float click ceiling (headroom-adjusted)
            min_daily_clicks   float click floor (0 for most; >0 for brand/defense)
            sample_clicks      int   total historical clicks (for CVR confidence)
            campaign_id        str   owning campaign id
            bidding_strategy   str   e.g. "Dynamic bids - up and down"
            placement_multiplier float account-level placement-weighted CPC factor
        campaign_budgets : {campaign_id: daily_budget}
        target_acos      : fractional target ACOS, e.g. 0.35
        avg_price        : average selling price (for ACOS constraint)
        max_daily_orders : inventory-linked daily order cap
        """
        if not self.solver:
            return {"status": "FAILED", "message": "Solver not initialized"}

        self.solver.Clear()

        # ── Pre-compute effective CPC per keyword ─────────────────────────
        eff_cpcs: List[float] = []
        pess_cvrs: List[float] = []
        for kw in keywords:
            strategy   = kw.get("bidding_strategy", "")
            strat_mult = _STRATEGY_CPC_MULTIPLIER.get(strategy, _DEFAULT_STRATEGY_MULTIPLIER)
            place_mult = float(kw.get("placement_multiplier", 1.0))
            eff_cpc    = kw["avg_cpc"] * strat_mult * place_mult
            eff_cpcs.append(eff_cpc)

            raw_cvr  = kw.get("estimated_cvr", 0.0)
            clicks   = int(kw.get("sample_clicks", 0))
            pess_cvr = _pessimistic_cvr(raw_cvr, clicks) if clicks > 0 else raw_cvr * 0.5
            pess_cvrs.append(pess_cvr)

        # ── Variables ─────────────────────────────────────────────────────
        click_vars: List[pywraplp.Variable] = []
        for i, kw in enumerate(keywords):
            lo = float(kw.get("min_daily_clicks", 0.0))
            hi = float(kw.get("max_daily_clicks", 1000.0))
            lo = min(lo, hi)  # guard against mis-configured floors
            var = self.solver.NumVar(lo, hi, kw["name"])
            click_vars.append(var)

        n = len(keywords)

        # ── Constraint 1: Global budget ───────────────────────────────────
        c_global = self.solver.Constraint(0, total_budget, "global_budget")
        for i in range(n):
            c_global.SetCoefficient(click_vars[i], eff_cpcs[i])

        # ── Constraint 2: Per-campaign budgets ────────────────────────────
        if campaign_budgets:
            camp_idx: Dict[str, List[int]] = {}
            for i, kw in enumerate(keywords):
                cid = str(kw.get("campaign_id", ""))
                if cid:
                    camp_idx.setdefault(cid, []).append(i)

            for cid, idxs in camp_idx.items():
                cap = campaign_budgets.get(cid)
                if cap and cap > 0:
                    c = self.solver.Constraint(0, float(cap), f"camp_{cid}")
                    for i in idxs:
                        c.SetCoefficient(click_vars[i], eff_cpcs[i])

        # ── Constraint 3: Target ACOS (linearised) ───────────────────────
        # Σ clicks_i × (eff_cpc_i − target_acos × pess_cvr_i × avg_price) ≤ 0
        if target_acos and avg_price and avg_price > 0:
            c_acos = self.solver.Constraint(-self.solver.infinity(), 0.0, "target_acos")
            for i in range(n):
                coeff = eff_cpcs[i] - target_acos * pess_cvrs[i] * avg_price
                c_acos.SetCoefficient(click_vars[i], coeff)

        # ── Constraint 4: Inventory order cap ────────────────────────────
        if max_daily_orders and max_daily_orders > 0:
            c_inv = self.solver.Constraint(0, float(max_daily_orders), "inventory_cap")
            for i in range(n):
                c_inv.SetCoefficient(click_vars[i], pess_cvrs[i])

        # ── Objective: maximise Σ clicks_i × pessimistic_cvr_i ───────────
        objective = self.solver.Objective()
        for i in range(n):
            objective.SetCoefficient(click_vars[i], pess_cvrs[i])
        objective.SetMaximization()

        # ── Solve ─────────────────────────────────────────────────────────
        status = self.solver.Solve()

        if status != pywraplp.Solver.OPTIMAL:
            return {
                "status":  "FAILED",
                "error_code": status,
                "message": "No optimal solution found within constraints.",
            }

        # ── Extract solution ──────────────────────────────────────────────
        allocation: List[Dict] = []
        total_spend   = 0.0
        total_clicks  = 0.0
        total_orders  = 0.0
        camp_spend: Dict[str, float] = {}

        for i, kw in enumerate(keywords):
            sol = click_vars[i].solution_value()
            if sol < 0.5:
                continue
            spend   = sol * eff_cpcs[i]
            orders  = sol * pess_cvrs[i]
            cid     = str(kw.get("campaign_id", ""))

            allocation.append({
                "keyword":             kw["name"],
                "campaign_id":         cid,
                "optimized_clicks":    round(sol, 1),
                "estimated_spend":     round(spend, 2),
                "contribution_to_orders": round(orders, 2),
                "effective_cpc":       round(eff_cpcs[i], 4),
                "pessimistic_cvr":     round(pess_cvrs[i], 4),
            })
            total_spend  += spend
            total_clicks += sol
            total_orders += orders
            if cid:
                camp_spend[cid] = camp_spend.get(cid, 0.0) + spend

        allocation.sort(key=lambda x: x["optimized_clicks"], reverse=True)

        return {
            "status": "OPTIMAL",
            "summary": {
                "total_budget":          total_budget,
                "actual_spend":          round(total_spend, 2),
                "total_expected_orders": round(total_orders, 2),
                "avg_effective_cpc":     round(total_spend / total_clicks, 2) if total_clicks > 0 else 0,
            },
            "allocation":   allocation,
            "camp_spend":   {k: round(v, 2) for k, v in camp_spend.items()},
        }
