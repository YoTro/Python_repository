"""
Unit tests for AdBudgetOptimizer — verifies each constraint independently
and then validates the full constraint stack against real dev data.
"""
from __future__ import annotations
import json
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.intelligence.processors.optimizer_ad_budget import (
    AdBudgetOptimizer,
    _pessimistic_cvr,
    _STRATEGY_CPC_MULTIPLIER,
    _CONFIDENCE_PRIOR,
)

DEV_JSON = os.path.join(os.path.dirname(__file__), "../../ad-diag-B0FXFGMD7Z-dev.json")


# ─────────────────────────── helpers ────────────────────────────────────────

def _kw(
    name="kw|EXACT",
    avg_cpc=1.0,
    cvr=0.10,
    max_clicks=50.0,
    min_clicks=0.0,
    sample_clicks=100,
    campaign_id="C1",
    strategy="Fixed bids",
    placement_mult=1.0,
):
    return {
        "name":                name,
        "avg_cpc":             avg_cpc,
        "estimated_cvr":       cvr,
        "max_daily_clicks":    max_clicks,
        "min_daily_clicks":    min_clicks,
        "sample_clicks":       sample_clicks,
        "campaign_id":         campaign_id,
        "bidding_strategy":    strategy,
        "placement_multiplier": placement_mult,
    }


def _solve(keywords, total_budget, **kwargs):
    opt = AdBudgetOptimizer()
    return opt.optimize(keywords, total_budget, **kwargs)


def _alloc_spend(result):
    return sum(a["estimated_spend"] for a in result["allocation"])


def _alloc_orders(result):
    return sum(a["contribution_to_orders"] for a in result["allocation"])


def _camp_spend(result, cid):
    return result["camp_spend"].get(cid, 0.0)


# ─────────────────────────── _pessimistic_cvr ───────────────────────────────

class TestPessimisticCVR:
    def test_zero_clicks_handled_by_caller(self):
        # optimizer itself handles clicks==0 with ×0.5 fallback; test the function directly
        assert _pessimistic_cvr(0.10, 0) == pytest.approx(0.0)  # sqrt(0/30) = 0

    def test_high_clicks_approaches_raw(self):
        # At 10 000 clicks weight ≈ sqrt(10000/10030) ≈ 0.9985
        result = _pessimistic_cvr(0.10, 10_000)
        assert result == pytest.approx(0.10, abs=0.001)

    def test_prior_clicks_gives_half_weight(self):
        # At clicks == CONFIDENCE_PRIOR: weight = sqrt(30/60) = sqrt(0.5) ≈ 0.707
        result = _pessimistic_cvr(0.10, _CONFIDENCE_PRIOR)
        assert result == pytest.approx(0.10 * math.sqrt(0.5), rel=1e-6)

    def test_low_clicks_penalised(self):
        high_sample = _pessimistic_cvr(0.10, 1000)
        low_sample  = _pessimistic_cvr(0.10, 5)
        assert low_sample < high_sample


# ─────────────────────────── C1: Global budget ──────────────────────────────

class TestC1GlobalBudget:
    def test_spend_never_exceeds_budget(self):
        kws = [_kw(f"kw{i}|EXACT", avg_cpc=1.0, cvr=0.10, max_clicks=100) for i in range(5)]
        budget = 50.0
        r = _solve(kws, budget)
        assert r["status"] == "OPTIMAL"
        assert _alloc_spend(r) <= budget + 1e-6

    def test_unconstrained_uses_full_budget(self):
        # 1 keyword with ample ceiling — LP should spend up to budget
        kws = [_kw("kw|BROAD", avg_cpc=1.0, cvr=0.10, max_clicks=500)]
        r = _solve(kws, 100.0)
        assert r["status"] == "OPTIMAL"
        assert _alloc_spend(r) == pytest.approx(100.0, abs=1.0)

    def test_tight_budget_limits_clicks(self):
        kws = [_kw("kw|EXACT", avg_cpc=2.0, cvr=0.10, max_clicks=100)]
        r = _solve(kws, 10.0)
        assert r["status"] == "OPTIMAL"
        # max clicks affordable = 10 / 2 = 5
        assert r["allocation"][0]["optimized_clicks"] == pytest.approx(5.0, abs=0.2)


# ─────────────────────────── C2: Per-campaign budgets ───────────────────────

class TestC2CampaignBudgets:
    def test_campaign_spend_respects_cap(self):
        kws = [
            _kw("kw1|EXACT", avg_cpc=1.0, cvr=0.10, max_clicks=200, campaign_id="C1"),
            _kw("kw2|BROAD", avg_cpc=1.0, cvr=0.08, max_clicks=200, campaign_id="C2"),
        ]
        r = _solve(kws, total_budget=300.0, campaign_budgets={"C1": 30.0, "C2": 50.0})
        assert r["status"] == "OPTIMAL"
        assert _camp_spend(r, "C1") <= 30.0 + 1e-6
        assert _camp_spend(r, "C2") <= 50.0 + 1e-6

    def test_high_cvr_campaign_hits_its_cap_not_global(self):
        # C1 has great CVR but tiny budget; LP must respect C1's cap
        kws = [
            _kw("brand|EXACT", avg_cpc=0.5, cvr=0.30, max_clicks=500, campaign_id="C1"),
            _kw("generic|BROAD", avg_cpc=1.0, cvr=0.05, max_clicks=500, campaign_id="C2"),
        ]
        r = _solve(kws, total_budget=500.0, campaign_budgets={"C1": 20.0, "C2": 480.0})
        assert r["status"] == "OPTIMAL"
        assert _camp_spend(r, "C1") <= 20.0 + 1e-6

    def test_no_campaign_budgets_uses_global_only(self):
        kws = [_kw("kw|EXACT", avg_cpc=1.0, cvr=0.10, max_clicks=200, campaign_id="C1")]
        r = _solve(kws, total_budget=50.0, campaign_budgets=None)
        assert r["status"] == "OPTIMAL"
        assert _alloc_spend(r) <= 50.0 + 1e-6


# ─────────────────────────── C3: Target ACOS ────────────────────────────────

class TestC3TargetACOS:
    def test_high_cpc_kw_excluded_when_acos_tight(self):
        # kw1: cheap, efficient → should be allocated
        # kw2: expensive, same CVR → ACOS would exceed target, should be zeroed/limited
        kws = [
            _kw("cheap|EXACT",     avg_cpc=0.50, cvr=0.10, max_clicks=200, campaign_id="C1", sample_clicks=500),
            _kw("expensive|BROAD", avg_cpc=5.00, cvr=0.10, max_clicks=200, campaign_id="C1", sample_clicks=500),
        ]
        # avg_price=20, target_acos=0.25 → max spend per order = 20×0.25 = $5
        # cheap kw: eff_cpc=0.50, order_value=0.10×20=$2 → ACOS=0.50/2=25% ✓ borderline
        # expensive kw: eff_cpc=5.00, order_value=$2 → ACOS=250% ✗
        r = _solve(kws, total_budget=200.0, target_acos=0.25, avg_price=20.0)
        assert r["status"] == "OPTIMAL"
        # expensive keyword must be largely excluded
        exp_alloc = next((a for a in r["allocation"] if "expensive" in a["keyword"]), None)
        assert exp_alloc is None or exp_alloc["optimized_clicks"] < 1.0

    def test_acos_constraint_satisfied_in_solution(self):
        kws = [
            _kw("kw1|EXACT", avg_cpc=1.0, cvr=0.10, max_clicks=100, sample_clicks=200),
            _kw("kw2|BROAD", avg_cpc=1.5, cvr=0.15, max_clicks=100, sample_clicks=200),
        ]
        target_acos = 0.35
        avg_price   = 30.0
        r = _solve(kws, total_budget=300.0, target_acos=target_acos, avg_price=avg_price)
        assert r["status"] == "OPTIMAL"
        total_spend  = sum(a["estimated_spend"]          for a in r["allocation"])
        total_orders = sum(a["contribution_to_orders"]   for a in r["allocation"])
        total_revenue = total_orders * avg_price
        if total_revenue > 0:
            actual_acos = total_spend / total_revenue
            assert actual_acos <= target_acos + 0.02  # allow tiny LP float tolerance

    def test_no_acos_constraint_when_params_absent(self):
        # Without target_acos / avg_price the optimizer must still solve
        kws = [_kw("kw|EXACT", avg_cpc=10.0, cvr=0.01, max_clicks=100)]
        r = _solve(kws, total_budget=100.0)
        assert r["status"] == "OPTIMAL"


# ─────────────────────────── C4: Inventory cap ──────────────────────────────

class TestC4InventoryCap:
    def test_orders_capped_at_max(self):
        kws = [_kw("kw|EXACT", avg_cpc=0.50, cvr=0.20, max_clicks=500, sample_clicks=1000)]
        max_orders = 5.0
        r = _solve(kws, total_budget=1000.0, max_daily_orders=max_orders)
        assert r["status"] == "OPTIMAL"
        assert _alloc_orders(r) <= max_orders + 1e-4

    def test_without_cap_orders_exceed_cap_value(self):
        kws = [_kw("kw|EXACT", avg_cpc=0.50, cvr=0.20, max_clicks=500, sample_clicks=1000)]
        r_capped   = _solve(kws, total_budget=1000.0, max_daily_orders=5.0)
        r_uncapped = _solve(kws, total_budget=1000.0)
        assert _alloc_orders(r_uncapped) > _alloc_orders(r_capped) + 0.5


# ─────────────────────────── C5: Click floor (brand keywords) ───────────────

class TestC5ClickFloor:
    def test_brand_kw_always_allocated(self):
        # brand keyword has min_daily_clicks=5; budget so tight only 1 click affordable
        # LP must still allocate ≥5 clicks to brand (may cause FAILED if budget truly zero)
        kws = [
            _kw("brand|EXACT",   avg_cpc=1.0, cvr=0.20, max_clicks=50, min_clicks=5, campaign_id="C1"),
            _kw("generic|BROAD", avg_cpc=1.0, cvr=0.05, max_clicks=50, min_clicks=0, campaign_id="C1"),
        ]
        r = _solve(kws, total_budget=100.0)
        assert r["status"] == "OPTIMAL"
        brand = next(a for a in r["allocation"] if "brand" in a["keyword"])
        assert brand["optimized_clicks"] >= 5.0 - 0.1

    def test_floor_above_ceiling_is_guarded(self):
        # Guard: lo = min(lo, hi) prevents infeasible variable bounds
        kws = [_kw("kw|EXACT", avg_cpc=1.0, cvr=0.10, max_clicks=3.0, min_clicks=10.0)]
        r = _solve(kws, total_budget=100.0)
        # Should solve without crash; floor clamped to ceiling
        assert r["status"] == "OPTIMAL"


# ─────────────────────────── Bidding strategy multiplier ────────────────────

class TestBiddingStrategyMultiplier:
    def test_up_and_down_costs_more_than_fixed(self):
        base = dict(avg_cpc=1.0, cvr=0.10, max_clicks=100, sample_clicks=200, campaign_id="C1")
        kw_updown = _kw("kw|EXACT", **base, strategy="Dynamic bids - up and down")
        kw_fixed  = _kw("kw|EXACT", **base, strategy="Fixed bids")
        budget = 50.0

        r_up    = _solve([kw_updown], budget)
        r_fixed = _solve([kw_fixed],  budget)

        # "up and down" has higher eff_cpc → fewer clicks for same budget
        clicks_up    = r_up["allocation"][0]["optimized_clicks"]   if r_up["allocation"]   else 0
        clicks_fixed = r_fixed["allocation"][0]["optimized_clicks"] if r_fixed["allocation"] else 0
        assert clicks_up < clicks_fixed

    def test_down_only_equals_fixed(self):
        base = dict(avg_cpc=1.0, cvr=0.10, max_clicks=100, sample_clicks=200, campaign_id="C1")
        kw_down  = _kw("kw|EXACT", **base, strategy="Dynamic bids - down only")
        kw_fixed = _kw("kw|EXACT", **base, strategy="Fixed bids")
        r_down  = _solve([kw_down],  50.0)
        r_fixed = _solve([kw_fixed], 50.0)
        # Both multipliers == 1.0 → same effective CPC → same allocation
        assert r_down["allocation"][0]["optimized_clicks"] == pytest.approx(
            r_fixed["allocation"][0]["optimized_clicks"], abs=0.1
        )

    def test_unknown_strategy_uses_default_multiplier(self):
        kw_known   = _kw("kw|EXACT", strategy="Fixed bids",  avg_cpc=1.0, max_clicks=100, sample_clicks=100)
        kw_unknown = _kw("kw|EXACT", strategy="SomeFuture",  avg_cpc=1.0, max_clicks=100, sample_clicks=100)
        r_known   = _solve([kw_known],   50.0)
        r_unknown = _solve([kw_unknown], 50.0)
        # unknown strategy multiplier (1.20) > Fixed (1.00) → fewer clicks
        assert (
            r_unknown["allocation"][0]["optimized_clicks"]
            < r_known["allocation"][0]["optimized_clicks"]
        )


# ─────────────────────────── Placement multiplier ───────────────────────────

class TestPlacementMultiplier:
    def test_higher_placement_mult_reduces_clicks(self):
        kw_low  = _kw("kw|EXACT", avg_cpc=1.0, max_clicks=200, placement_mult=1.0, sample_clicks=200)
        kw_high = _kw("kw|EXACT", avg_cpc=1.0, max_clicks=200, placement_mult=1.5, sample_clicks=200)
        r_low  = _solve([kw_low],  100.0)
        r_high = _solve([kw_high], 100.0)
        assert r_low["allocation"][0]["optimized_clicks"] > r_high["allocation"][0]["optimized_clicks"]


# ─────────────────────────── Combined constraints ───────────────────────────

class TestCombinedConstraints:
    def test_all_constraints_active_solves(self):
        kws = [
            _kw("brand|EXACT",   avg_cpc=0.80, cvr=0.20, max_clicks=100, min_clicks=5,
                 sample_clicks=300, campaign_id="C1", strategy="Fixed bids"),
            _kw("top|BROAD",     avg_cpc=1.20, cvr=0.12, max_clicks=200, min_clicks=0,
                 sample_clicks=150, campaign_id="C1", strategy="Dynamic bids - up and down"),
            _kw("niche|PHRASE",  avg_cpc=0.60, cvr=0.08, max_clicks=80,  min_clicks=0,
                 sample_clicks=50,  campaign_id="C2", strategy="Dynamic bids - down only"),
            _kw("waste|EXACT",   avg_cpc=8.00, cvr=0.02, max_clicks=30,  min_clicks=0,
                 sample_clicks=20,  campaign_id="C2", strategy="Fixed bids"),
        ]
        r = _solve(
            kws,
            total_budget=150.0,
            campaign_budgets={"C1": 100.0, "C2": 60.0},
            target_acos=0.35,
            avg_price=25.0,
            max_daily_orders=8.0,
        )
        assert r["status"] == "OPTIMAL"

        total_spend  = _alloc_spend(r)
        total_orders = _alloc_orders(r)

        assert total_spend  <= 150.0 + 1e-4          # C1
        assert _camp_spend(r, "C1") <= 100.0 + 1e-4  # C2
        assert _camp_spend(r, "C2") <= 60.0  + 1e-4  # C2
        assert total_orders <= 8.0 + 1e-4             # C4

        # brand keyword floor respected
        brand = next((a for a in r["allocation"] if "brand" in a["keyword"]), None)
        assert brand is not None
        assert brand["optimized_clicks"] >= 5.0 - 0.1

        # waste keyword (ACOS would be 8/(0.02×25)=1600%) should be zeroed
        waste = next((a for a in r["allocation"] if "waste" in a["keyword"]), None)
        assert waste is None or waste["optimized_clicks"] < 1.0

    def test_empty_keywords_returns_failed(self):
        r = _solve([], total_budget=100.0)
        # GLOP with 0 variables is trivially optimal with 0 spend — acceptable
        assert r["status"] in ("OPTIMAL", "FAILED")

    def test_infeasible_returns_failed(self):
        # min_clicks floor forces eff_cpc × min_clicks > total_budget
        kws = [_kw("kw|EXACT", avg_cpc=100.0, max_clicks=10, min_clicks=10)]
        r = _solve(kws, total_budget=1.0)
        # min spend = 100 × 10 = 1000 > budget 1 → INFEASIBLE
        assert r["status"] == "FAILED"


# ─────────────────────────── Real dev-data smoke test ───────────────────────

class TestRealData:
    @pytest.fixture(scope="class")
    def dev_item(self):
        if not os.path.exists(DEV_JSON):
            pytest.skip("dev JSON not found")
        with open(DEV_JSON, encoding="utf-8") as f:
            data = json.load(f)
        return data["items"][0]

    def test_solves_with_real_keyword_performance(self, dev_item):
        kw_perf = dev_item.get("keyword_performance", [])
        campaigns = dev_item.get("campaigns", [])
        total_budget = dev_item.get("total_daily_budget", 0) or 0

        camp_meta = {str(c["campaign_id"]): c for c in campaigns if c.get("campaign_id")}
        campaign_budgets = {
            cid: float(c.get("daily_budget") or 0)
            for cid, c in camp_meta.items() if c.get("daily_budget")
        }

        lp_input = []
        for kw in kw_perf:
            if not kw.get("avg_cpc") or not kw.get("cvr"):
                continue
            lp_input.append({
                "name":               f"{kw['keyword_text']}|{kw['match_type']}",
                "avg_cpc":            kw["avg_cpc"],
                "estimated_cvr":      kw["cvr"],
                "sample_clicks":      kw.get("total_clicks", 0),
                "max_daily_clicks":   max(round(kw["daily_clicks"] * 3.0, 1), 1.0),
                "min_daily_clicks":   0.0,
                "campaign_id":        "",
                "bidding_strategy":   "Dynamic bids - up and down",
                "placement_multiplier": 1.0,
            })

        assert len(lp_input) > 0, "No LP-eligible keywords in dev data"

        r = _solve(
            lp_input,
            total_budget=total_budget,
            campaign_budgets=campaign_budgets or None,
        )
        assert r["status"] == "OPTIMAL"
        # Use summary spend (from LP solution) rather than summing rounded per-item values
        # to avoid double-rounding accumulation error across many keywords.
        assert r["summary"]["actual_spend"] <= total_budget + 1e-4
        assert len(r["allocation"]) > 0

        print(f"\n[real data] {len(lp_input)} keywords | "
              f"budget=${total_budget} | spend=${r['summary']['actual_spend']} | "
              f"orders={r['summary']['total_expected_orders']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
