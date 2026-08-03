from __future__ import annotations

"""
Tests for the Steady-State Ad Burden block inside _run_monopoly_analysis.

Covers:
  1. No CPC at all                      → N/A fields, "unknown" verdict
  2. CPC from bid recommendations       → computed fields
  3. CPC from benchmark fallback        → same math, different source
  4. All four verdict thresholds        → Critical / High / Moderate / Low
  5. Ad profit drag: actual_bsr_ad_ratio present vs default 0.5
  6. Breakeven ACOS from FBA fee JSON   → not the hard-coded 0.18 fallback
"""

import statistics
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.workflows.definitions.category_monopoly_analysis import _run_monopoly_analysis

# ── constants copied from the production function ─────────────────────────────
_REFERRAL_FEE_PCT = 0.15
_COGS_PCT = 0.30

# FBA fee schedule constants (Large Standard 12-16 oz, 10_to_50 bracket, +3.5% surcharge)
_FBA_FEE_USD = 4.60 * 1.035  # = 4.761
_MEDIAN_PRICE = 25.0
_FBA_FEE_PCT = _FBA_FEE_USD / _MEDIAN_PRICE  # ≈ 0.19044
_BREAKEVEN = 1.0 - _COGS_PCT - _REFERRAL_FEE_PCT - _FBA_FEE_PCT  # ≈ 0.3596
_CATEGORY_CVR = 0.10


# ── helpers ────────────────────────────────────────────────────────────────────


def _make_items(n: int = 10, price: float = _MEDIAN_PRICE) -> list[dict]:
    """Minimal BSR product dicts — only the fields _run_monopoly_analysis reads."""
    return [
        {
            "ASIN": f"B{i:09d}",
            "Title": f"Test Product {i}",
            "Price": f"${price:.2f}",
            "Rank": str(i + 1),
            "Stars": "4.5",
            "Reviews": "500",
        }
        for i in range(n)
    ]


def _make_bid_rec(start_bid: float, end_bid=None) -> dict:
    """Build a single bidRecommendation object matching the real Amazon Ads API shape.

    The API returns bidValues as a list of {suggestedBid} entries (lower / mid / upper).
    statistics.median of those values is used as the per-expression CPC.
    When end_bid is given, two entries produce median == (start+end)/2.
    """
    bids = [{"suggestedBid": str(start_bid)}]
    if end_bid is not None and end_bid != start_bid:
        bids.append({"suggestedBid": str(end_bid)})
    return {
        "bidRecommendationsForTargetingExpressions": [
            {
                "bidValues": bids,
                "targetingExpression": {"type": "CLOSE_MATCH"},
            }
        ]
    }


class _MockCtx:
    """Minimal workflow context substitute."""

    def __init__(self, cache_overrides=None):
        self.config = {
            "store_id": "US",
            "category_node_id": None,
            "timezone": "America/Los_Angeles",
        }
        self.router = None  # skip all LLM sub-calls
        self.mcp = None
        self.cache: dict = {
            # keyword signals
            "core_keywords": ["pest trap"],
            "main_keyword": "pest trap",
            # market signals
            "keyword_data": {},
            "ad_ratio": 0.3,
            "detailed_bid_analysis": {"LEGACY_FOR_SALES": [], "AUTO_FOR_SALES": []},
            # CVR / CPC benchmark
            "category_cvr": _CATEGORY_CVR,
            "category_cvr_source": "amazon_ads_benchmark",
            "category_cpc_p50": None,
            # ad-traffic ratio from Xiyouzhaoci
            "actual_bsr_ad_ratio": None,
            # time-series / snapshots (empty → skip those code paths)
            "historical_data": {},
            "keyword_weekly_trends": None,
            "sellersprite_snapshots": {},
            "sellersprite_base_ym": "",
            # external signals
            "category_social_psi": 0,
            "category_social_verdict": "Unknown",
            "category_deal_intensity": 0,
            # contamination filter result
            "contamination_stats": {"status": "not_run", "n_removed": 0, "n_retained": 10},
        }
        if cache_overrides:
            self.cache.update(cache_overrides)


# ── fixtures / shared mock ────────────────────────────────────────────────────

_MOCK_ANALYZE_RESULT = {
    "overall_score": 45.0,
    "status": "Moderate Monopoly",
    "niche_benchmarks": {"total_estimated_monthly_units": 0},
    "market_churn": {
        "pattern": "stable",
        "churn_score": 20,
        "new_product_ratio": 0.1,
        "collapse_rate": 0.05,
    },
    "seasonality": {
        "pattern": "low",
        "seasonality_score": 10,
        "peak_months": [],
        "source": "bsr_daily_trends",
        "n_data_points": 0,
    },
    "bsr_churn": {
        "label": "low",
        "churn_3m": 0.1,
        "churn_6m": 0.15,
        "churn_12m": 0.2,
        "snapshots_available": [],
    },
}


@pytest.fixture(autouse=True)
def _patch_analyzer_and_estimator():
    """Stub out CategoryMonopolyAnalyzer, SalesEstimator, and ProfitabilitySearchExtractor
    for all tests. ProfitabilitySearchExtractor is patched to simulate the live fee/category
    API being unavailable (get_fees -> {}, search_products -> []) so every test deterministically
    exercises the local fee-schedule / flat-rate fallback paths, instead of depending on
    whatever a real network call to Amazon's endpoints happens to return.
    """
    mock_estimator = MagicMock()
    mock_estimator.category_params = {}

    with patch(
        "src.workflows.definitions.category_monopoly_analysis.CategoryMonopolyAnalyzer"
    ) as mock_cls:
        with patch(
            "src.workflows.definitions.category_monopoly_analysis.SalesEstimator",
            return_value=mock_estimator,
        ):
            with patch(
                "src.workflows.definitions.category_monopoly_analysis.ProfitabilitySearchExtractor"
            ) as mock_ps_cls:
                mock_ps_cls.return_value.get_fees = AsyncMock(return_value={})
                mock_ps_cls.return_value.search_products = AsyncMock(return_value=[])
                mock_cls.return_value.analyze.return_value = _MOCK_ANALYZE_RESULT
                yield


# ── helpers to call _run_monopoly_analysis synchronously ─────────────────────


def _run(ctx: _MockCtx, items: list[dict] | None = None) -> dict:
    """Run the async function synchronously and return the single result dict."""
    import asyncio

    result_list = asyncio.run(_run_monopoly_analysis(items or _make_items(), ctx))
    assert isinstance(result_list, list) and len(result_list) == 1
    return result_list[0]


# ── Test 1: No CPC data, no fallback → all N/A + "unknown" ───────────────────


def test_no_cpc_no_fallback_yields_unknown():
    ctx = _MockCtx()  # category_cpc_p50=None, detailed_bid_analysis empty
    r = _run(ctx)

    assert r["median_cpc"] == "N/A"
    assert r["estimated_steady_state_acos"] == "N/A"
    assert r["ad_profit_drag"] == "N/A"
    assert r["ad_burden_verdict"] == "unknown"
    # CVR and breakeven are always populated
    assert r["category_cvr"] == "10.0%"
    assert r["breakeven_acos"] != "N/A"


# ── Test 2: CPC from bid recommendations ─────────────────────────────────────


def test_cpc_from_bid_recommendations():
    cpc = 1.00  # ACOS = 1.00 / (25 * 0.10) = 0.40  → High
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(cpc)],
                "AUTO_FOR_SALES": [],
            }
        }
    )
    r = _run(ctx)

    assert r["median_cpc"] == f"${cpc:.2f}"
    expected_acos = cpc / (_MEDIAN_PRICE * _CATEGORY_CVR)
    assert r["estimated_steady_state_acos"] == f"{expected_acos:.0%}"
    assert r["ad_burden_verdict"] == "High"


# ── Test 3: No bid recs, fallback to benchmark cpcP50 ────────────────────────


def test_cpc_fallback_to_benchmark_cpc_p50():
    cpc = 1.00
    ctx = _MockCtx(
        cache_overrides={
            # no bid recs
            "detailed_bid_analysis": {"LEGACY_FOR_SALES": [], "AUTO_FOR_SALES": []},
            "category_cpc_p50": cpc,
        }
    )
    r = _run(ctx)

    assert r["median_cpc"] == f"${cpc:.2f}"
    expected_acos = cpc / (_MEDIAN_PRICE * _CATEGORY_CVR)
    assert r["estimated_steady_state_acos"] == f"{expected_acos:.0%}"
    # Same outcome as Test 2 — the source of CPC does not change the math
    assert r["ad_burden_verdict"] == "High"


# ── Test 4: bid recs take priority over benchmark fallback ────────────────────


def test_bid_recs_take_priority_over_cpc_p50():
    bid_cpc = 1.00
    benchmark_cpc = 3.00  # would give Critical if used
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(bid_cpc)],
                "AUTO_FOR_SALES": [],
            },
            "category_cpc_p50": benchmark_cpc,
        }
    )
    r = _run(ctx)

    assert r["median_cpc"] == f"${bid_cpc:.2f}"
    assert r["ad_burden_verdict"] == "High"  # not Critical


# ── Test 5: median CPC when multiple bid recs ─────────────────────────────────


def test_median_cpc_across_multiple_bid_recs():
    cpcs = [1.00, 2.00, 3.00]  # median = 2.00
    recs = [_make_bid_rec(c) for c in cpcs]
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {"LEGACY_FOR_SALES": recs, "AUTO_FOR_SALES": []},
        }
    )
    r = _run(ctx)
    expected_median = statistics.median(cpcs)
    assert r["median_cpc"] == f"${expected_median:.2f}"


# ── Test 6: all four verdict thresholds ──────────────────────────────────────


@pytest.mark.parametrize(
    "cpc,expected_verdict",
    [
        # ACOS = cpc / (25 * 0.10) = cpc / 2.5
        # _BREAKEVEN ≈ 0.3596
        # Critical:  ACOS >= 1.5 × _BREAKEVEN ≈ 0.5394  → cpc >= 1.349
        # High:      ACOS >= _BREAKEVEN ≈ 0.3596          → cpc >= 0.899
        # Moderate:  ACOS >= 0.7 × _BREAKEVEN ≈ 0.2517   → cpc >= 0.629
        # Low:       ACOS <  0.2517                       → cpc < 0.629
        (2.00, "Critical"),
        (1.00, "High"),
        (0.75, "Moderate"),
        (0.50, "Low"),
    ],
)
def test_verdict_thresholds(cpc, expected_verdict):
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(cpc)],
                "AUTO_FOR_SALES": [],
            }
        }
    )
    r = _run(ctx)
    assert r["ad_burden_verdict"] == expected_verdict, (
        f"CPC=${cpc} → ACOS={cpc / (_MEDIAN_PRICE * _CATEGORY_CVR):.2%} "
        f"vs breakeven≈{_BREAKEVEN:.2%}, expected {expected_verdict}, got {r['ad_burden_verdict']}"
    )


# ── Test 7: ad profit drag uses actual_bsr_ad_ratio when present ─────────────


def test_ad_profit_drag_uses_actual_bsr_ad_ratio():
    cpc = 1.00  # ACOS = 0.40
    ad_ratio = 0.60  # drag = 0.60 × 0.40 = 0.24 = 24%
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(cpc)],
                "AUTO_FOR_SALES": [],
            },
            "actual_bsr_ad_ratio": ad_ratio,
        }
    )
    r = _run(ctx)

    expected_acos = cpc / (_MEDIAN_PRICE * _CATEGORY_CVR)
    expected_drag = ad_ratio * expected_acos
    assert r["ad_profit_drag"] == f"{expected_drag:.0%}"


# ── Test 8: ad profit drag defaults to 0.5 when actual_bsr_ad_ratio is None ──


def test_ad_profit_drag_defaults_to_half_acos():
    cpc = 1.00  # ACOS = 0.40 → drag = 0.5 × 0.40 = 0.20 = 20%
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(cpc)],
                "AUTO_FOR_SALES": [],
            },
            "actual_bsr_ad_ratio": None,
        }
    )
    r = _run(ctx)

    expected_acos = cpc / (_MEDIAN_PRICE * _CATEGORY_CVR)
    expected_drag = 0.5 * expected_acos
    assert r["ad_profit_drag"] == f"{expected_drag:.0%}"


# ── Test 9: FBA fee loaded from JSON, not hard-coded fallback ─────────────────


def test_breakeven_uses_fba_fee_json_not_fallback():
    """
    The fallback _fba_fee_pct is 0.18. The JSON gives 4.60×1.035/25 ≈ 0.190.
    The two produce different breakeven values; assert we match the JSON value.
    """
    ctx = _MockCtx()
    r = _run(ctx)

    json_fba_fee_pct = _FBA_FEE_USD / _MEDIAN_PRICE
    expected_breakeven = max(0.0, 1.0 - _COGS_PCT - _REFERRAL_FEE_PCT - json_fba_fee_pct)
    fallback_breakeven = max(0.0, 1.0 - _COGS_PCT - _REFERRAL_FEE_PCT - 0.18)

    reported = r["breakeven_acos"]  # e.g. "36%"
    reported_pct = int(reported.rstrip("%")) / 100

    # Must match the JSON value (rounded to nearest integer percent)
    assert abs(reported_pct - expected_breakeven) < 0.01, (
        f"Expected breakeven≈{expected_breakeven:.2%} (JSON), "
        f"got {reported} — fallback would give {fallback_breakeven:.2%}"
    )
    # Sanity: the two differ by more than 1pp so this test is meaningful
    assert abs(json_fba_fee_pct - 0.18) > 0.005


# ── Test 10: bid rec uses startBid/endBid midpoint ───────────────────────────


def test_bid_rec_midpoint_calculation():
    start, end = 1.00, 2.00  # midpoint = 1.50
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(start, end)],
                "AUTO_FOR_SALES": [],
            }
        }
    )
    r = _run(ctx)
    midpoint = (start + end) / 2
    assert r["median_cpc"] == f"${midpoint:.2f}"


# ── Test 11: zero-price items don't break median_price calculation ────────────


def test_zero_price_items_excluded_from_median():
    items = _make_items(5, price=25.0) + _make_items(5, price=0.0)
    cpc = 1.00
    ctx = _MockCtx(
        cache_overrides={
            "detailed_bid_analysis": {
                "LEGACY_FOR_SALES": [_make_bid_rec(cpc)],
                "AUTO_FOR_SALES": [],
            }
        }
    )
    r = _run(ctx, items=items)

    # median_price should still be 25.0 (zero-price items excluded)
    expected_acos = cpc / (25.0 * _CATEGORY_CVR)
    assert r["estimated_steady_state_acos"] == f"{expected_acos:.0%}"
