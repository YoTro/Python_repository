"""
Stress-test suite for the LP prior + optimizer pipeline.

Covers four failure modes identified in code review:

  1. _empirical_k always falls back to K_CVR_MAX when fewer than
     _MIN_KWS_EMPIRICAL_K pairs are supplied — no dispersion signal available.

  2. _compute_cvr_prior with a single keyword collapses to a circular prior:
     global_mu == keyword's own CVR when clicks >= _MIN_CLICKS_FOR_MU (20),
     which causes _beta_cvr to return raw_cvr unchanged (zero shrinkage).
     With clicks < 20 the prior falls back to the hardcoded 0.02 sentinel.

  3. _build_lp_input silently drops keywords with zero orders (cvr == 0.0
     is falsy) even though _beta_cvr can produce a non-zero estimate from the
     prior for such keywords.

  4. C3 (ACOS) and the min_daily_clicks floor (brand keywords) can jointly
     produce an infeasible region: C3 forces clicks toward zero while C4
     keeps them above the floor — the solver returns FAILED with no relaxation
     attempted.  CVR deflation applied before the solve can trigger the same
     collapse on previously-feasible inputs.

Run with:
    PYTHONPATH=. pytest tests/test_lp_single_kw_stress.py -v
"""

from __future__ import annotations

import sys
import os

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.intelligence.processors.optimizer_ad_budget import (
    _K_CVR_MAX,
    _K_CVR_MIN,
    AdBudgetOptimizer,
    _beta_cvr,
)
from src.workflows.definitions.ad_diagnosis import (
    _MIN_CLICKS_FOR_MU,
    _MIN_KWS_EMPIRICAL_K,
    _MIN_KWS_FOR_STRATUM,
    _K_CVR_CEILING,
    _build_lp_input,
    _compute_cvr_prior,
    _empirical_k,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _kw_perf(
    keyword_text="yoga mat",
    match_type="EXACT",
    campaign_id="C1",
    total_clicks=100,
    total_orders=10,
    avg_cpc=1.0,
    daily_clicks=5.0,
):
    """Minimal kw_perf record as produced by _enrich_keyword_performance."""
    cvr = round(total_orders / total_clicks, 6) if total_clicks > 0 else 0.0
    return {
        "campaign_id": campaign_id,
        "keyword_text": keyword_text,
        "match_type": match_type,
        "total_clicks": total_clicks,
        "total_orders": total_orders,
        "avg_cpc": avg_cpc if total_clicks > 0 else None,
        "cvr": cvr,
        "daily_clicks": daily_clicks,
    }


def _camp_meta(campaign_id="C1", state="ENABLED", daily_budget=100.0, strategy="Fixed bids"):
    return {
        campaign_id: {
            "campaign_id": campaign_id,
            "state": state,
            "daily_budget": daily_budget,
            "bidding_strategy": strategy,
        }
    }


def _lp_kw(
    name="kw|EXACT",
    avg_cpc=1.0,
    cvr=0.10,
    max_clicks=50.0,
    min_clicks=0.0,
    sample_clicks=100,
    prior_mu=0.05,
    campaign_id="C1",
    strategy="Fixed bids",
    placement_mult=1.0,
    k_max=None,
):
    d = {
        "name": name,
        "avg_cpc": avg_cpc,
        "estimated_cvr": cvr,
        "max_daily_clicks": max_clicks,
        "min_daily_clicks": min_clicks,
        "sample_clicks": sample_clicks,
        "sample_orders": round(cvr * sample_clicks),
        "prior_mu": prior_mu,
        "campaign_id": campaign_id,
        "bidding_strategy": strategy,
        "placement_multiplier": placement_mult,
    }
    if k_max is not None:
        d["k_max"] = k_max
    return d


def _solve(keywords, total_budget, **kwargs):
    return AdBudgetOptimizer().optimize(keywords, total_budget, **kwargs)


# ── 1. _empirical_k fallback ──────────────────────────────────────────────────


class TestEmpiricalKFallback:
    """_empirical_k requires >= _MIN_KWS_EMPIRICAL_K (5) pairs.
    Below that threshold it always returns _K_CVR_MAX regardless of dispersion."""

    @pytest.mark.parametrize("n_pairs", [0, 1, 2, 3, 4])
    def test_below_threshold_always_returns_k_max(self, n_pairs):
        pairs = [(100, 0.10 + i * 0.02) for i in range(n_pairs)]
        mu = 0.10
        result = _empirical_k(pairs, mu)
        assert result == _K_CVR_MAX, (
            f"Expected K_CVR_MAX={_K_CVR_MAX} for {n_pairs} pairs, got {result}"
        )

    def test_at_threshold_uses_dispersion(self):
        # Exactly _MIN_KWS_EMPIRICAL_K pairs with high spread → k_hat < K_CVR_MAX
        pairs = [(200, cvr) for cvr in [0.01, 0.05, 0.10, 0.25, 0.50]]
        mu = sum(v for _, v in pairs) / len(pairs)
        result = _empirical_k(pairs, mu)
        # With wide spread σ_ratio → 1 → k_hat → K_CVR_CEILING (may exceed K_CVR_MAX)
        assert result != _K_CVR_MAX or result == _K_CVR_CEILING

    def test_identical_cvr_zero_dispersion_returns_k_max(self):
        # Zero between-keyword variance → σ²_signal ≤ 0 → fallback to K_CVR_MAX
        pairs = [(100, 0.10)] * 10
        mu = 0.10
        result = _empirical_k(pairs, mu)
        assert result == _K_CVR_MAX

    def test_high_dispersion_above_low_dispersion(self):
        # High spread → higher k_max than low spread (more shrinkage needed)
        high_spread = [(200, cvr) for cvr in [0.02, 0.05, 0.10, 0.20, 0.40, 0.60, 0.80]]
        low_spread = [(200, cvr) for cvr in [0.09, 0.10, 0.10, 0.10, 0.11, 0.10, 0.10]]
        mu_h = sum(v for _, v in high_spread) / len(high_spread)
        mu_l = sum(v for _, v in low_spread) / len(low_spread)
        k_high = _empirical_k(high_spread, mu_h)
        k_low = _empirical_k(low_spread, mu_l)
        assert k_high >= k_low


# ── 2. _compute_cvr_prior single-keyword degenerate cases ────────────────────


class TestComputeCVRPriorSingleKeyword:
    """With one keyword the prior has no cross-keyword information to pool.
    Two distinct regimes exist depending on click count relative to
    _MIN_CLICKS_FOR_MU (20)."""

    def test_single_kw_below_min_clicks_returns_sentinel(self):
        # clicks < 20 → excluded from all_pairs → global_mu = 0.02 fallback
        kw = _kw_perf(total_clicks=_MIN_CLICKS_FOR_MU - 1, total_orders=2, daily_clicks=1.0)
        _, _, global_mu, global_k = _compute_cvr_prior([kw])
        assert global_mu == pytest.approx(0.02), (
            f"Expected 0.02 hardcoded fallback, got {global_mu}"
        )
        assert global_k == _K_CVR_MAX  # < MIN_KWS_EMPIRICAL_K pairs → fallback

    def test_single_kw_above_min_clicks_uses_fallback_not_circular(self):
        # Fix: with 1 keyword (< _MIN_KWS_FOR_STRATUM=3), global_mu falls back
        # to 0.02 even when clicks >= _MIN_CLICKS_FOR_MU.  This prevents the
        # circular-prior property (prior_mu == raw_cvr → zero shrinkage).
        clicks = _MIN_CLICKS_FOR_MU * 5  # 100
        orders = 10
        raw_cvr = orders / clicks  # 0.10
        kw = _kw_perf(total_clicks=clicks, total_orders=orders, daily_clicks=5.0)
        _, _, global_mu, global_k = _compute_cvr_prior([kw])
        assert global_mu == pytest.approx(0.02), (
            f"Single keyword must fall back to 0.02 (not circular raw_cvr={raw_cvr}), got {global_mu}"
        )
        assert global_k == _K_CVR_MAX  # 1 pair < MIN_KWS_EMPIRICAL_K → fallback

    def test_stratum_falls_back_to_global_below_min_kws(self):
        # Only 2 EXACT keywords < _MIN_KWS_FOR_STRATUM (3) → stratum uses global
        kws = [
            _kw_perf("kw1", "EXACT", total_clicks=50, total_orders=5),
            _kw_perf("kw2", "EXACT", total_clicks=80, total_orders=8),
        ]
        mu_by_mt, k_by_mt, global_mu, global_k = _compute_cvr_prior(kws)
        assert mu_by_mt.get("EXACT") == pytest.approx(global_mu, rel=1e-4), (
            "EXACT stratum with 2 keywords should fall back to global_mu"
        )

    def test_zero_order_keywords_excluded_from_prior(self):
        # Keywords with cvr==0.0 are excluded from all_pairs (falsy check)
        kw_zero = _kw_perf(total_clicks=200, total_orders=0, daily_clicks=10.0)
        _, _, global_mu, _ = _compute_cvr_prior([kw_zero])
        assert global_mu == pytest.approx(0.02), (
            "Zero-order keyword must not anchor the prior; should fall back to 0.02"
        )

    def test_global_mu_click_weighted_across_keywords(self):
        # global_mu is click-weighted: high-click keyword dominates.
        # Requires >= _MIN_KWS_FOR_STRATUM (3) keywords to trust the weighted mean.
        kw_low = _kw_perf("a", total_clicks=20, total_orders=1, daily_clicks=1.0)    # cvr=0.05
        kw_high = _kw_perf("b", total_clicks=400, total_orders=80, daily_clicks=20.0) # cvr=0.20
        kw_mid = _kw_perf("c", total_clicks=50, total_orders=5, daily_clicks=2.5)    # cvr=0.10
        _, _, global_mu, _ = _compute_cvr_prior([kw_low, kw_high, kw_mid])
        # weighted: (20*0.05 + 400*0.20 + 50*0.10)/(20+400+50) = (1+80+5)/470 ≈ 0.183
        expected = (20 * 0.05 + 400 * 0.20 + 50 * 0.10) / (20 + 400 + 50)
        assert global_mu == pytest.approx(expected, rel=0.01)


# ── 3. Circular-prior property: no shrinkage when prior_mu == raw_cvr ─────────


class TestCircularPriorNoShrinkage:
    """When _compute_cvr_prior derives global_mu from the keyword's own CVR,
    _beta_cvr reduces to the identity: pess_cvr == raw_cvr exactly."""

    @pytest.mark.parametrize("raw_cvr,clicks", [
        # clicks chosen so that round(raw_cvr × clicks) == raw_cvr × clicks exactly,
        # which is required for the algebraic identity pess_cvr == raw_cvr to hold.
        (0.05, 20),    # 20 × 0.05 = 1.0 exactly
        (0.10, 100),   # 100 × 0.10 = 10.0 exactly
        (0.20, 500),   # 500 × 0.20 = 100.0 exactly
        (0.30, 1000),  # 1000 × 0.30 = 300.0 exactly
        (0.01, 100),   # 100 × 0.01 = 1.0 exactly  (not 20: round(0.01×20)=0 breaks identity)
    ])
    def test_circular_prior_returns_raw_cvr(self, raw_cvr, clicks):
        orders = round(raw_cvr * clicks)
        assert orders == raw_cvr * clicks, "Precondition: no rounding error in orders"
        # prior_mu set to the keyword's own CVR — the circular case
        result = _beta_cvr(raw_cvr=raw_cvr, clicks=clicks, orders=orders, prior_mu=raw_cvr)
        assert result == pytest.approx(raw_cvr, rel=1e-4), (
            f"Circular prior (mu={raw_cvr}) should return raw_cvr, got {result}"
        )

    def test_external_prior_does_shrink(self):
        # With a different prior_mu, the estimate is pulled toward it
        raw_cvr = 0.20
        prior_mu = 0.05  # external, lower anchor
        clicks, orders = 50, 10
        result = _beta_cvr(raw_cvr, clicks, orders, prior_mu=prior_mu)
        assert result < raw_cvr, "External prior should pull estimate below raw_cvr"
        assert result > prior_mu, "Estimate should not collapse all the way to prior"

    def test_circular_prior_vs_external_same_clicks(self):
        # External prior provides strictly more regularization than circular prior.
        # clicks=40 chosen so that round(0.15×40)=6 == 0.15×40 exactly.
        raw_cvr = 0.15
        clicks = 40
        orders = round(raw_cvr * clicks)   # 6, exact
        assert orders == raw_cvr * clicks, "Precondition: no rounding error"
        circular = _beta_cvr(raw_cvr, clicks, orders, prior_mu=raw_cvr)   # no shrinkage
        external = _beta_cvr(raw_cvr, clicks, orders, prior_mu=0.05)      # real shrinkage
        # circular == raw_cvr; external < raw_cvr
        assert circular == pytest.approx(raw_cvr, rel=1e-4)
        assert external < circular


# ── 4. Zero-order keyword exclusion gap ──────────────────────────────────────


class TestZeroOrderExclusionGap:
    """Fix: _build_lp_input previously dropped keywords with cvr==0.0 (falsy check
    `not kw.get("cvr")`).  The check is now `kw.get("cvr") is None`, so zero-order
    keywords are included and _beta_cvr produces a prior-based estimate for them."""

    def test_zero_order_kw_included_in_lp_input(self):
        # Fixed: cvr=0.0 is no longer treated as absent — keyword is now included.
        kw = _kw_perf(total_clicks=50, total_orders=0, daily_clicks=2.5)
        meta = _camp_meta()
        result = _build_lp_input([kw], meta, set(), 3.0, 1.0)
        assert len(result) == 1, "Keyword with cvr=0.0 must now be included in _build_lp_input"
        assert result[0]["estimated_cvr"] == 0.0

    def test_beta_cvr_handles_zero_orders_via_prior(self):
        # Confirms _beta_cvr returns a non-zero estimate for the excluded keyword
        raw_cvr = 0.0
        clicks, orders = 50, 0
        prior_mu = 0.05
        result = _beta_cvr(raw_cvr, clicks, orders, prior_mu=prior_mu)
        assert result > 0.0, (
            "_beta_cvr should return prior-based estimate for zero-order keyword"
        )
        assert result < prior_mu, (
            "High click count with 0 orders should shrink estimate well below prior"
        )

    def test_zero_order_only_keyword_now_included_in_lp(self):
        # Fixed: sole zero-order keyword is now included, so LP can run.
        kw = _kw_perf(total_clicks=200, total_orders=0, daily_clicks=10.0)
        meta = _camp_meta()
        result = _build_lp_input([kw], meta, set(), 3.0, 1.0)
        assert len(result) == 1

    def test_nonzero_order_kw_survives(self):
        kw = _kw_perf(total_clicks=50, total_orders=1, daily_clicks=2.5)
        meta = _camp_meta()
        result = _build_lp_input([kw], meta, set(), 3.0, 1.0)
        assert len(result) == 1
        assert result[0]["estimated_cvr"] == pytest.approx(1 / 50, rel=1e-4)

    @pytest.mark.parametrize("clicks,orders", [
        (5, 0), (20, 0), (100, 0), (500, 0), (1000, 0),
    ])
    def test_zero_orders_included_regardless_of_clicks(self, clicks, orders):
        # Fixed: previously excluded by falsy cvr=0.0; now included with estimated_cvr=0.0.
        kw = _kw_perf(total_clicks=clicks, total_orders=orders, daily_clicks=clicks / 5.0)
        meta = _camp_meta()
        result = _build_lp_input([kw], meta, set(), 3.0, 1.0)
        assert len(result) == 1, f"clicks={clicks} with 0 orders should now be included"
        assert result[0]["estimated_cvr"] == 0.0

    def test_paused_campaign_also_excluded(self):
        kw = _kw_perf(total_clicks=50, total_orders=5, daily_clicks=2.5)
        meta = _camp_meta(state="PAUSED")
        result = _build_lp_input([kw], meta, set(), 3.0, 1.0)
        assert result == []


# ── 5. C3 (ACOS) + min_daily_clicks floor infeasibility ──────────────────────


class TestC3FloorInfeasibility:
    """When target_acos is set and a brand keyword's min_daily_clicks floor > 0,
    the two constraints can jointly produce an empty feasible region:
      - C3 requires low/zero clicks from the high-CPC brand keyword
      - C4 floor forbids reducing that keyword below min_daily_clicks
    The solver returns FAILED.  Removing either constraint restores feasibility."""

    def _brand_kw(self, avg_cpc=5.0, cvr=0.05, min_clicks=10.0, max_clicks=50.0):
        return _lp_kw(
            "brand|EXACT",
            avg_cpc=avg_cpc,
            cvr=cvr,
            min_clicks=min_clicks,
            max_clicks=max_clicks,
            prior_mu=0.05,
            sample_clicks=200,
        )

    def test_tight_acos_plus_floor_recovers_via_relaxation(self):
        # Initial solve is infeasible (C3+floor conflict), but the optimizer retries
        # with floor=0 and returns OPTIMAL.  Empty allocation — no clicks allocated
        # because C3 is still active after floor relaxation.
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=10.0)
        r = _solve([kw], total_budget=500.0, target_acos=0.20, avg_price=10.0)
        assert r["status"] == "OPTIMAL"
        assert r.get("relaxed") == True
        assert r.get("relaxed_constraints") == ["floors"]
        assert r["allocation"] == []  # C3 still forbids positive clicks

    def test_same_setup_without_floor_is_feasible(self):
        # Removing the floor (min_clicks=0) lets the solver zero-out the keyword → OPTIMAL
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=0.0)
        r = _solve([kw], total_budget=500.0, target_acos=0.20, avg_price=10.0)
        assert r["status"] == "OPTIMAL"

    def test_same_setup_without_acos_constraint_is_feasible(self):
        # Removing C3 entirely lets the solver allocate clicks freely → OPTIMAL
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=10.0)
        r = _solve([kw], total_budget=500.0)  # no target_acos
        assert r["status"] == "OPTIMAL"
        assert r["allocation"][0]["optimized_clicks"] >= 10.0 - 0.1

    def test_efficient_keyword_beside_floor_is_feasible(self):
        # Adding a cheap keyword that "subsidises" the ACOS budget makes C3 satisfiable
        floor_kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=5.0, max_clicks=5.0)
        cheap_kw = _lp_kw(
            "generic|BROAD",
            avg_cpc=0.20,
            cvr=0.30,
            max_clicks=500.0,
            sample_clicks=300,
            prior_mu=0.10,
        )
        r = _solve(
            [floor_kw, cheap_kw],
            total_budget=500.0,
            target_acos=0.20,
            avg_price=10.0,
        )
        assert r["status"] == "OPTIMAL"

    @pytest.mark.parametrize("target_acos,expect_relaxed", [
        # Break-even: eff_cpc / (pess_cvr × avg_price) = 5.0 / (0.05 × 10) = 10.0
        # Below break-even → initial solve FAILED, recovered by dropping floors.
        # At or above break-even → direct OPTIMAL, no relaxation needed.
        (0.10, True),   # 10% target → floor conflict, recovered via relaxation
        (0.30, True),   # 30% target → still << break-even, recovered
        (10.0, False),  # exactly at break-even → OPTIMAL without relaxation
    ])
    def test_acos_tightness_sweep(self, target_acos, expect_relaxed):
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=10.0)
        r = _solve([kw], total_budget=500.0, target_acos=target_acos, avg_price=10.0)
        assert r["status"] == "OPTIMAL", (
            f"target_acos={target_acos}: expected OPTIMAL (with relaxation={expect_relaxed})"
        )
        assert r.get("relaxed", False) == expect_relaxed, (
            f"target_acos={target_acos}: expected relaxed={expect_relaxed}"
        )

    def test_c3_alone_without_floor_never_fails(self):
        # Critical property: C3 (ACOS) alone with min_clicks=0 cannot make the
        # problem FAILED because clicks=0 is always a valid solution that satisfies
        # all constraints with spend=0.  Infeasibility requires C3 + floor together.
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=0.0)  # no floor
        for target_acos in [0.01, 0.10, 0.30, 1.0]:
            r = _solve([kw], total_budget=500.0, target_acos=target_acos, avg_price=10.0)
            assert r["status"] == "OPTIMAL", (
                f"C3 alone at target_acos={target_acos} must not cause FAILED "
                f"(clicks=0 is always valid); got {r['status']}"
            )
            # The allocation for an "impossible" keyword must be empty (0 clicks)
            assert r["allocation"] == [], (
                f"No clicks should be allocated to an ACOS-violating keyword; "
                f"got {r['allocation']}"
            )

    @pytest.mark.parametrize("min_clicks", [0, 1, 5, 10, 20])
    def test_floor_size_sweep_tight_acos(self, min_clicks):
        # Under tight ACOS, non-zero floors trigger infeasibility; optimizer recovers
        # by dropping floors and returning OPTIMAL with relaxed=True.
        kw = self._brand_kw(avg_cpc=5.0, cvr=0.05, min_clicks=float(min_clicks))
        r = _solve([kw], total_budget=500.0, target_acos=0.20, avg_price=10.0)
        assert r["status"] == "OPTIMAL"
        if min_clicks == 0:
            assert not r.get("relaxed")
        else:
            assert r.get("relaxed") == True
            assert r.get("relaxed_constraints") == ["floors"]


# ── 6. CVR deflation boundary ─────────────────────────────────────────────────


class TestCVRDeflationBoundary:
    """CVR deflation (applied in-place to lp_input before the solve) reduces
    pess_cvr proportionally.  When deflation is large enough that
    eff_cpc > target_acos × deflated_pess_cvr × avg_price for all keywords,
    C3 collapses.  With a brand floor the region empties silently."""

    def _make_kw(self, cvr=0.20, avg_cpc=1.0, min_clicks=0.0, max_clicks=100.0):
        return _lp_kw(
            "kw|EXACT",
            avg_cpc=avg_cpc,
            cvr=cvr,
            min_clicks=min_clicks,
            max_clicks=max_clicks,
            prior_mu=cvr,  # circular prior (single-keyword case)
            sample_clicks=200,
        )

    def _deflate(self, lp_kws: list[dict], factor: float) -> list[dict]:
        import copy
        kws = copy.deepcopy(lp_kws)
        for kw in kws:
            kw["estimated_cvr"] *= factor
            kw["prior_mu"] *= factor
        return kws

    def test_no_deflation_baseline_feasible(self):
        # eff_cpc=1.0, pess_cvr≈0.20, ACOS ≈ 1/(0.20×20)=25% < 35%
        kw = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=5.0)
        r = _solve([kw], 100.0, target_acos=0.35, avg_price=20.0)
        assert r["status"] == "OPTIMAL"

    def test_c3_without_floor_never_fails_under_deflation(self):
        # Key property: without a min_clicks floor, even extreme CVR deflation
        # cannot make the problem FAILED — the solver just allocates clicks=0.
        kw = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=0.0)
        for factor in [0.10, 0.05, 0.01]:
            deflated = self._deflate([kw], factor)
            r = _solve(deflated, 100.0, target_acos=0.35, avg_price=20.0)
            assert r["status"] == "OPTIMAL", (
                f"deflation={factor}: C3 without floor should be OPTIMAL (clicks=0), "
                f"got {r['status']}"
            )
            assert r["allocation"] == [], "No clicks allocated when keyword violates ACOS"

    def test_heavy_deflation_with_floor_recovers_via_relaxation(self):
        # Heavy CVR deflation + floor causes initial infeasibility; optimizer
        # recovers by dropping the floor and returning OPTIMAL with relaxed=True.
        kw = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=5.0)
        deflated = self._deflate([kw], factor=0.10)
        r = _solve(deflated, 100.0, target_acos=0.35, avg_price=20.0)
        assert r["status"] == "OPTIMAL"
        assert r.get("relaxed") == True
        assert r.get("relaxed_constraints") == ["floors"]

    def test_deflation_boundary_search_with_floor(self):
        # With floor=5, relaxation kicks in below break-even deflation.
        # Break-even: 1.0 = 0.35 × (0.20 × factor) × 20 → factor ≈ 0.714
        # High factors → direct OPTIMAL (no relaxation); low factors → relaxed OPTIMAL.
        kw = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=5.0)
        results = {}
        for factor in [1.0, 0.80, 0.72, 0.70, 0.50, 0.25, 0.10]:
            deflated = self._deflate([kw], factor)
            results[factor] = _solve(deflated, 100.0, target_acos=0.35, avg_price=20.0)
        # All return OPTIMAL now (floor relaxation recovers infeasible cases)
        assert all(r["status"] == "OPTIMAL" for r in results.values())
        # High deflation factor → feasible without relaxation
        assert not results[1.0].get("relaxed")
        # Low deflation factor → required floor relaxation
        assert results[0.10].get("relaxed") == True

    def test_deflation_floor_collapses_earlier_than_no_floor(self):
        # Without a floor → direct OPTIMAL (clicks=0, no relaxation needed).
        # With a floor → initial infeasibility, but optimizer recovers via relaxation.
        # The key observable difference is relaxed=True on the floor variant.
        kw_no_floor = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=0.0)
        kw_with_floor = self._make_kw(cvr=0.20, avg_cpc=1.0, min_clicks=5.0)
        factor = 0.10  # heavy deflation
        r_no = _solve(self._deflate([kw_no_floor], factor), 100.0, target_acos=0.35, avg_price=20.0)
        r_fl = _solve(self._deflate([kw_with_floor], factor), 100.0, target_acos=0.35, avg_price=20.0)
        assert r_no["status"] == "OPTIMAL"
        assert not r_no.get("relaxed")       # no floor → no relaxation needed
        assert r_fl["status"] == "OPTIMAL"
        assert r_fl.get("relaxed") == True   # floor required relaxation


# ── 7. Single-keyword end-to-end LP regime ───────────────────────────────────


class TestSingleKeywordLP:
    """End-to-end LP behavior with exactly one keyword — confirms that the
    optimizer produces valid results and that the circular-prior / k_max-fallback
    regime does not cause numerical failures."""

    @pytest.mark.parametrize("clicks,orders", [
        (5, 1),     # just above min_clicks_for_cvr, sparse orders
        (20, 2),    # exactly _MIN_CLICKS_FOR_MU, prior becomes circular
        (100, 10),  # moderate data
        (1000, 200),# abundant data
    ])
    def test_single_kw_solves_across_click_regimes(self, clicks, orders):
        cvr = orders / clicks
        kw = _lp_kw("kw|EXACT", avg_cpc=1.0, cvr=cvr, sample_clicks=clicks,
                     prior_mu=cvr, max_clicks=float(clicks) * 3)
        r = _solve([kw], total_budget=float(clicks) * 1.0)
        assert r["status"] == "OPTIMAL"
        assert r["summary"]["actual_spend"] <= float(clicks) * 1.0 + 1e-4

    @pytest.mark.parametrize("budget_factor", [0.1, 0.5, 1.0, 2.0, 5.0])
    def test_single_kw_budget_sweep(self, budget_factor):
        kw = _lp_kw("kw|EXACT", avg_cpc=1.0, cvr=0.10, sample_clicks=100,
                     prior_mu=0.10, max_clicks=200.0)
        budget = 100.0 * budget_factor
        r = _solve([kw], budget)
        assert r["status"] == "OPTIMAL"
        assert r["summary"]["actual_spend"] <= budget + 1e-4

    def test_single_kw_pess_cvr_equals_raw_when_prior_circular(self):
        # Confirms the circular-prior property survives the full optimizer path
        raw_cvr = 0.10
        kw = _lp_kw("kw|EXACT", avg_cpc=1.0, cvr=raw_cvr, sample_clicks=100,
                     prior_mu=raw_cvr, max_clicks=200.0)
        r = _solve([kw], 100.0)
        assert r["status"] == "OPTIMAL"
        pess = r["allocation"][0]["pessimistic_cvr"]
        assert pess == pytest.approx(raw_cvr, rel=0.01), (
            f"Circular prior should yield pess_cvr ≈ raw_cvr={raw_cvr}, got {pess}"
        )

    def test_single_kw_external_prior_shrinks_cvr(self):
        # An external (non-circular) prior produces a lower pess_cvr
        raw_cvr = 0.20
        kw_circ = _lp_kw("kw|EXACT", avg_cpc=1.0, cvr=raw_cvr, sample_clicks=50,
                          prior_mu=raw_cvr, max_clicks=100.0)
        kw_ext = _lp_kw("kw|EXACT", avg_cpc=1.0, cvr=raw_cvr, sample_clicks=50,
                         prior_mu=0.05, max_clicks=100.0)
        r_circ = _solve([kw_circ], 50.0)
        r_ext = _solve([kw_ext], 50.0)
        assert r_circ["status"] == r_ext["status"] == "OPTIMAL"
        pess_circ = r_circ["allocation"][0]["pessimistic_cvr"]
        pess_ext = r_ext["allocation"][0]["pessimistic_cvr"]
        assert pess_ext < pess_circ, (
            "External prior should produce lower (more conservative) pess_cvr"
        )

    @pytest.mark.parametrize("avg_cpc,cvr,target_acos,avg_price,min_clicks,expect_relaxed", [
        # eff_acos = avg_cpc / (cvr * avg_price)
        # min_clicks=0 → C3 alone never causes FAILED (clicks=0 always valid, no relaxation)
        # min_clicks>0 + infeasible ACOS → floor relaxation kicks in (relaxed=True)
        (1.0, 0.20, 0.35, 20.0, 0.0, False),  # feasible: eff_acos=25% < 35%
        (1.0, 0.05, 0.35, 10.0, 0.0, False),  # OPTIMAL via 0-clicks, no relaxation
        (1.0, 0.05, 0.35, 10.0, 5.0, True),   # eff_acos=200%, floor → relaxation
        (0.50, 0.10, 0.30, 20.0, 0.0, False), # feasible: eff_acos=25% < 30%
        (2.00, 0.10, 0.30, 20.0, 0.0, False), # OPTIMAL via 0-clicks, no relaxation
        (2.00, 0.10, 0.30, 20.0, 5.0, True),  # eff_acos=100%, floor → relaxation
    ])
    def test_single_kw_acos_feasibility(
        self, avg_cpc, cvr, target_acos, avg_price, min_clicks, expect_relaxed
    ):
        kw = _lp_kw("kw|EXACT", avg_cpc=avg_cpc, cvr=cvr, prior_mu=cvr,
                     sample_clicks=200, max_clicks=200.0, min_clicks=min_clicks)
        r = _solve([kw], 200.0, target_acos=target_acos, avg_price=avg_price)
        assert r["status"] == "OPTIMAL", (
            f"avg_cpc={avg_cpc} cvr={cvr} target_acos={target_acos} "
            f"min_clicks={min_clicks}: expected OPTIMAL"
        )
        assert r.get("relaxed", False) == expect_relaxed, (
            f"avg_cpc={avg_cpc} cvr={cvr} min_clicks={min_clicks}: "
            f"expected relaxed={expect_relaxed}"
        )


# ── 8. Over-constraint with small N ──────────────────────────────────────────


class TestOverConstrainedSmallN:
    """
    When N is small and multiple constraint axes are simultaneously active the
    feasible region can collapse even though any single constraint alone would
    admit a solution.  The optimizer now recovers by retrying without floors
    (relaxed=True, relaxed_constraints=["floors"]) rather than returning FAILED.

      A. C1 vs C5  — Σ(floor_i × eff_cpc_i) > total_budget
      B. C2 vs C5  — campaign-level floor sum > campaign budget cap
      C. C4 vs C5  — floor forces min_orders > inventory cap
      D. N-scaling — one extra keyword over the breakpoint tips into infeasibility
      E. Mixed pool — one infeasible keyword with floor poisons the whole solve
    """

    # ── A. C1 vs C5: global budget vs floor sum ───────────────────────────────

    def test_floors_collectively_exceed_global_budget(self):
        """Floor sum > global budget — initially infeasible, recovered via floor relaxation."""
        kws = [
            _lp_kw("kw1|EXACT", avg_cpc=1.0, min_clicks=5.0),
            _lp_kw("kw2|EXACT", avg_cpc=1.0, min_clicks=5.0),
        ]
        # min total spend = 5+5 = 10 > budget=8
        result = _solve(kws, total_budget=8.0)
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True
        assert result.get("relaxed_constraints") == ["floors"]

    def test_floors_exactly_at_global_budget_is_optimal(self):
        """Boundary: floors exactly consume the entire budget — OPTIMAL (tight)."""
        kws = [
            _lp_kw("kw1|EXACT", avg_cpc=1.0, min_clicks=5.0),
            _lp_kw("kw2|EXACT", avg_cpc=1.0, min_clicks=5.0),
        ]
        # min total spend = 10 == budget=10
        result = _solve(kws, total_budget=10.0)
        assert result["status"] == "OPTIMAL"

    def test_single_kw_floor_alone_exceeds_global_budget(self):
        """Single keyword floor spend > budget — recovered via floor relaxation."""
        kws = [_lp_kw("kw1|EXACT", avg_cpc=2.0, min_clicks=10.0)]
        # min spend = 2.0×10 = 20 > budget=15
        result = _solve(kws, total_budget=15.0)
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True

    # ── B. C2 vs C5: per-campaign cap vs floor sum ────────────────────────────

    def test_campaign_floor_sum_exceeds_campaign_budget(self):
        """Campaign floor sum > campaign cap — recovered via floor relaxation."""
        kws = [
            _lp_kw("kw1|EXACT", avg_cpc=1.0, min_clicks=4.0, campaign_id="A"),
            _lp_kw("kw2|BROAD", avg_cpc=1.0, min_clicks=4.0, campaign_id="A"),
        ]
        # campaign min spend = 8 > campaign budget=5; global budget is ample
        result = _solve(kws, total_budget=100.0, campaign_budgets={"A": 5.0})
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True

    def test_campaign_floor_sum_exactly_at_cap_is_optimal(self):
        """Campaign floors sum exactly to the cap — OPTIMAL."""
        kws = [
            _lp_kw("kw1|EXACT", avg_cpc=1.0, min_clicks=3.0, campaign_id="A"),
            _lp_kw("kw2|BROAD", avg_cpc=1.0, min_clicks=2.0, campaign_id="A"),
        ]
        # campaign min spend = 5 == campaign budget=5
        result = _solve(kws, total_budget=100.0, campaign_budgets={"A": 5.0})
        assert result["status"] == "OPTIMAL"

    def test_campaign_floor_overflow_invisible_to_global_c1(self):
        """Campaign cap failure is invisible to C1; recovered via floor relaxation."""
        kws = [
            _lp_kw("kw1|EXACT", avg_cpc=1.0, min_clicks=6.0, campaign_id="B"),
            _lp_kw("kw2|BROAD", avg_cpc=1.0, min_clicks=6.0, campaign_id="B"),
        ]
        # global budget=100 → fine; campaign B cap=10, floor sum=12 > 10
        result = _solve(kws, total_budget=100.0, campaign_budgets={"B": 10.0})
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True

    # ── C. C4 vs C5: inventory cap vs floor ───────────────────────────────────

    def test_floor_forces_orders_over_inventory_cap(self):
        """
        floor × pess_cvr > max_daily_orders — recovered via floor relaxation.
        cvr=0.5, prior_mu=0.3 (external), floor=20 → pess_cvr≈0.49, min_orders≈9.8 >> cap=3.
        """
        kws = [
            _lp_kw(
                "kw1|EXACT",
                avg_cpc=1.0,
                cvr=0.5,
                min_clicks=20.0,
                sample_clicks=200,
                prior_mu=0.3,
            )
        ]
        result = _solve(kws, total_budget=200.0, max_daily_orders=3.0)
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True

    def test_floor_below_inventory_cap_is_optimal(self):
        """floor × pess_cvr well below max_daily_orders — stays feasible."""
        kws = [
            _lp_kw(
                "kw1|EXACT",
                avg_cpc=1.0,
                cvr=0.10,
                min_clicks=5.0,
                sample_clicks=100,
                prior_mu=0.05,
            )
        ]
        # pess_cvr ≈ 0.082 (prior pulls down); min_orders ≈ 0.41 << cap=5
        result = _solve(kws, total_budget=200.0, max_daily_orders=5.0)
        assert result["status"] == "OPTIMAL"

    # ── D. N-scaling breakpoint ───────────────────────────────────────────────

    @pytest.mark.parametrize("n", [1, 2, 4, 6])
    def test_n_floored_keywords_feasible_before_breakpoint(self, n):
        """
        N keywords × floor=3 × eff_cpc=1.0 = N×3 spend.
        Budget=20 → feasible while n ≤ 6 (max floor-spend = 18 ≤ 20).
        """
        kws = [
            _lp_kw(f"kw{i}|EXACT", avg_cpc=1.0, min_clicks=3.0)
            for i in range(n)
        ]
        result = _solve(kws, total_budget=20.0)
        assert result["status"] == "OPTIMAL", (
            f"N={n} min_spend={n*3} should be feasible with budget=20"
        )

    def test_n_floored_keywords_recovers_past_breakpoint(self):
        """N=7 × floor=3 × eff_cpc=1.0 = 21 > budget=20 — recovered via floor relaxation."""
        kws = [
            _lp_kw(f"kw{i}|EXACT", avg_cpc=1.0, min_clicks=3.0)
            for i in range(7)
        ]
        result = _solve(kws, total_budget=20.0)
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True

    # ── E. Mixed pool: one keyword poisons the solve ──────────────────────────

    def test_one_inefficient_kw_with_floor_poisons_pool(self):
        """
        Efficient keyword (eff_acos=25% < 35%) + inefficient keyword (eff_acos≈2500%,
        floor=5).  C3+floor initially infeasible; recovered via floor relaxation.
        After recovery the costly keyword receives zero clicks (C3 still active).
        """
        kws = [
            _lp_kw(
                "efficient|EXACT",
                avg_cpc=0.5,
                cvr=0.20,
                min_clicks=0.0,
                prior_mu=0.10,
            ),
            _lp_kw(
                "costly|BROAD",
                avg_cpc=5.0,
                cvr=0.02,
                min_clicks=5.0,
                prior_mu=0.02,
            ),
        ]
        result = _solve(kws, total_budget=200.0, target_acos=0.35, avg_price=10.0)
        assert result["status"] == "OPTIMAL"
        assert result.get("relaxed") == True
        # After relaxation the efficient keyword dominates the allocation.
        # The costly keyword may receive a tiny C3-subsidised infill from the efficient
        # keyword's budget surplus, but it must contribute far fewer clicks.
        alloc = {a["keyword"]: a["optimized_clicks"] for a in result["allocation"]}
        assert "efficient|EXACT" in alloc, "Efficient keyword must receive allocation"
        costly_clicks = alloc.get("costly|BROAD", 0.0)
        assert costly_clicks < alloc["efficient|EXACT"], (
            "Costly keyword must receive fewer clicks than the efficient one"
        )

    def test_inefficient_kw_without_floor_does_not_poison_pool(self):
        """Same pool, but the inefficient keyword has no floor.
        Solver zeroes it out → OPTIMAL for the efficient keyword."""
        kws = [
            _lp_kw(
                "efficient|EXACT",
                avg_cpc=0.5,
                cvr=0.20,
                min_clicks=0.0,
                prior_mu=0.10,
            ),
            _lp_kw(
                "costly|BROAD",
                avg_cpc=5.0,
                cvr=0.02,
                min_clicks=0.0,
                prior_mu=0.02,
            ),
        ]
        result = _solve(kws, total_budget=200.0, target_acos=0.35, avg_price=10.0)
        assert result["status"] == "OPTIMAL"

    def test_efficient_kw_alone_is_optimal(self):
        """Control: the efficient keyword solves fine in isolation."""
        kws = [
            _lp_kw(
                "efficient|EXACT",
                avg_cpc=0.5,
                cvr=0.20,
                min_clicks=0.0,
                prior_mu=0.10,
            )
        ]
        result = _solve(kws, total_budget=200.0, target_acos=0.35, avg_price=10.0)
        assert result["status"] == "OPTIMAL"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
