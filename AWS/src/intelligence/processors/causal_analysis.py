from __future__ import annotations

"""
Causal analysis processor for ad-diagnosis change attribution.

Pipeline (single entry point: run_causal_analysis):

  Stage 1 — Window Attribution  (absorbs _correlate_changes)
       For each change event, compare [T-9,T-2] (pre) vs [T+2,T+9] (post)
       daily campaign KPIs.  Annotates with covariate context (price, promo,
       competitor price gap) and compound-change flags.

  Stage 2 — ITS  (Interrupted Time Series / segmented regression)
       Piecewise OLS on the full metric series split at the change date.
       Estimates level-shift (γ) and slope-change (δ).
       Requires only numpy/scipy — no optional dependencies.

  Stage 3 — CausalImpact  (Bayesian Structural Time Series)
       Fits a state-space model on the pre-period, forecasts the counterfactual,
       and compares to actuals.  More robust to trend/seasonality than ITS.
       Requires `causalimpact` (optional); falls back to ITS estimate if absent.

  Stage 4 — DML  (Double Machine Learning — Frisch–Waugh–Lovell)
       Residualises treatment (bid/budget step) and outcome on confounders
       (price, rank, SFR, competitor price) via RandomForest (or OLS fallback).
       Estimates the clean causal effect after removing covariate-driven variation.
       Requires `scikit-learn` (optional); falls back to OLS residualisation.

All four stages share a single aligned covariate matrix built once per item.

Entry point:
    run_causal_analysis(item, config, daily_perf) → dict
        Reads from item:  change_events, covariate_series,
                          competitor_price_summary, natural_rank_series,
                          market_trends
        Reads daily_perf: list of {campaign_id, date, spend, orders, ...}
        Returns:          {"change_attributions": [...]}
                          Each entry contains window stats AND its/causal_impact/dml/consensus.
"""

import logging
import math
from collections import Counter
from datetime import UTC, datetime, timedelta
from datetime import date as _date_cls
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np

logger = logging.getLogger(__name__)

# ── Attribution window constants (exported so ad_diagnosis can reuse for lookback) ─
ATTR_PRE_START = -9
ATTR_PRE_END = -2
ATTR_POST_START = +2
ATTR_POST_END = +9

# ── Baseline normalisation constants ───────────────────────────────────────────
YOY_OFFSET_DAYS = 364  # 52 full weeks — preserves day-of-week pattern
YOY_MIN_DAYS = 5  # min overlapping YoY days required to trust the baseline
TRAILING_START = -97  # trailing ~3M window start (relative to anchor)
TRAILING_END = -11  # trailing window end — gap before pre-window (-9)
TRAILING_MIN_DAYS = 14  # min days of trailing data required

# ── Minimum observations ────────────────────────────────────────────────────────
_ITS_MIN_PRE = 7  # pre-period rows for reliable ITS
_ITS_MIN_POST = 5  # post-period rows
_CI_MIN_PRE = 14  # pre-period rows for BSTS
# If |point_effect| exceeds this multiple of the pre-period series scale the BSTS
# state-space model diverged (degenerate counterfactual).  Treat as skipped.
_CI_OUTLIER_MULT = 100


# ── Covariate alignment ────────────────────────────────────────────────────────


def _align_covariates(
    item: dict,
    start_date: str,
    end_date: str,
) -> tuple[list[str], np.ndarray]:
    """
    Build an aligned (dates × features) covariate matrix from all available
    item-level time series.

    Columns:
      0  sale_price           own price
      1  promotion_flag       0/1
      2  competitor_median    competitor price median
      3  total_rank           organic rank (lowest totalRank across keywords)
      4  weekly_searches      ABA weekly search volume (primary keyword); positively
                              correlated with demand and linearly scalable — preferred
                              over SFR rank which is inverted and non-linear

    Missing values: forward-filled → backward-filled → zero if column entirely absent.
    Returns (dates_list, matrix) where dates_list[i] == matrix row i.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return [], np.empty((0, 5))

    dates: list[str] = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    n = len(dates)
    if n == 0:
        return [], np.empty((0, 5))

    cov_series = item.get("covariate_series") or {}
    comp_prices = item.get("competitor_price_summary") or {}
    rank_series = item.get("natural_rank_series") or {}
    mkt_trends = item.get("market_trends") or {}

    # Weekly search volume → daily (first keyword only, uniform distribution within week)
    search_by_date: dict[str, float] = {}
    for _kw, weeks in mkt_trends.items():
        for iso_week, vals in weeks.items():
            vol = vals.get("weekly_searches")
            if vol is None:
                continue
            try:
                week_start = datetime.strptime(f"{iso_week}-1", "%G-W%V-%u").date()
            except ValueError:
                continue
            daily_vol = float(vol) / 7.0
            for offset in range(7):
                d = (week_start + timedelta(days=offset)).strftime("%Y-%m-%d")
                search_by_date.setdefault(d, daily_vol)
        break

    # Best organic rank per date (smallest totalRank)
    best_rank: dict[str, float] = {}
    for _kw, days_data in rank_series.items():
        for d, pos in days_data.items():
            tr = pos.get("totalRank")
            if tr is None:
                continue
            if d not in best_rank or tr < best_rank[d]:
                best_rank[d] = float(tr)

    raw: list[list] = []
    for d in dates:
        cov = cov_series.get(d) or {}
        comp = comp_prices.get(d) or {}
        raw.append(
            [
                cov.get("sale_price"),
                float(bool(cov.get("promotion_flag", False))),
                comp.get("median"),
                best_rank.get(d),
                search_by_date.get(d),
            ]
        )

    mat = np.array(raw, dtype=float)  # None → NaN
    mat[np.isinf(mat)] = np.nan  # inf treated as missing before fill

    # Forward-fill then backward-fill; remaining NaN → 0
    for col in range(mat.shape[1]):
        last: float | None = None
        for i in range(n):
            if not np.isnan(mat[i, col]):
                last = mat[i, col]
            elif last is not None:
                mat[i, col] = last
        last = None
        for i in range(n - 1, -1, -1):
            if not np.isnan(mat[i, col]):
                last = mat[i, col]
            elif last is not None:
                mat[i, col] = last
        mat[np.isnan(mat[:, col]), col] = 0.0

    return dates, mat


# ── Stage 1: window attribution ───────────────────────────────────────────────


def _window_avg(
    daily_index: dict[tuple[str, str], dict],
    campaign_id: str,
    anchor: datetime,
    day_start: int,
    day_end: int,
) -> dict | None:
    """
    Aggregate daily KPIs over [anchor + day_start, anchor + day_end] inclusive.
    daily_index is keyed by (campaign_id, date) — spAdvertisedProduct returns
    one row per (ASIN, campaign, date) so per-campaign precision is preserved.
    Averages per day for comparability; ACOS derived from summed spend/sales.
    """
    records = []
    for offset in range(day_start, day_end + 1):
        d = (anchor + timedelta(days=offset)).strftime("%Y-%m-%d")
        rec = daily_index.get((campaign_id, d))
        if rec:
            records.append(rec)
    if not records:
        return None
    n = len(records)
    total_spend = sum(r.get("spend", 0) or 0 for r in records)
    total_sales = sum(r.get("sales", 0) or 0 for r in records)
    orders_vals = [r.get("orders", 0) or 0 for r in records]
    orders_mean = sum(orders_vals) / n
    orders_std = math.sqrt(sum((v - orders_mean) ** 2 for v in orders_vals) / n) if n > 1 else 0.0
    return {
        "spend": round(total_spend / n, 2),
        "orders": round(orders_mean, 2),
        "orders_std": round(orders_std, 4),
        "acos": round(total_spend / total_sales * 100, 2) if total_sales > 0 else None,
        "clicks": round(sum(r.get("clicks", 0) or 0 for r in records) / n, 2),
        "days": n,
    }


def _window_avg_asin(
    asin_date_index: dict[str, dict],
    anchor: datetime,
    day_start: int,
    day_end: int,
) -> dict | None:
    """
    ASIN-level fallback: aggregate KPIs over all campaigns for the window.
    asin_date_index is keyed by date, values are pre-summed across all campaigns.
    Used when the per-campaign window has no records (campaign was inactive).
    """
    records = []
    for offset in range(day_start, day_end + 1):
        d = (anchor + timedelta(days=offset)).strftime("%Y-%m-%d")
        rec = asin_date_index.get(d)
        if rec:
            records.append(rec)
    if not records:
        return None
    n = len(records)
    total_spend = sum(r.get("spend", 0) or 0 for r in records)
    total_sales = sum(r.get("sales", 0) or 0 for r in records)
    orders_vals = [r.get("orders", 0) or 0 for r in records]
    orders_mean = sum(orders_vals) / n
    orders_std = math.sqrt(sum((v - orders_mean) ** 2 for v in orders_vals) / n) if n > 1 else 0.0
    return {
        "spend": round(total_spend / n, 2),
        "orders": round(orders_mean, 2),
        "orders_std": round(orders_std, 4),
        "acos": round(total_spend / total_sales * 100, 2) if total_sales > 0 else None,
        "clicks": round(sum(r.get("clicks", 0) or 0 for r in records) / n, 2),
        "days": n,
    }


def _classify_direction(delta: float, metric: str, pre_val: float) -> str:
    if metric == "acos":
        if delta < -3:
            return "improved"
        if delta > 3:
            return "worsened"
    else:
        if pre_val > 0 and abs(delta) / pre_val >= 0.15:
            return "improved" if delta > 0 else "worsened"
    return "neutral"


def _normalized_delta_orders(
    asin_date_index: dict[str, dict],
    anchor: datetime,
    post_avg: float,
    pre_avg: float,
    yoy_date_index: dict[str, dict] | None = None,
    trailing_ext_index: dict[str, dict] | None = None,
) -> tuple[float, str]:
    """
    Compute post_avg relative to the best available seasonal baseline.

    Priority:
      P1 YoY          — same post-window 364 days ago (52 weeks, same weekday).
                        Requires ≥ YOY_MIN_DAYS of data in yoy_date_index.
      P2 Trailing 3M  — mean of [anchor-97, anchor-11] combining asin_date_index
                        (Ads API) and trailing_ext_index (ERP extension).
                        Requires ≥ TRAILING_MIN_DAYS of data.
      P3 Pre-window   — current [anchor-9, anchor-2] fallback (within-sample).

    Returns (normalized_delta, source_label).
    """
    # P1: YoY
    if yoy_date_index:
        yoy_anchor = anchor - timedelta(days=YOY_OFFSET_DAYS)
        yoy_vals = [
            float(yoy_date_index[d]["orders"])
            for i in range(ATTR_POST_START, ATTR_POST_END + 1)
            if (d := (yoy_anchor + timedelta(days=i)).strftime("%Y-%m-%d")) in yoy_date_index
        ]
        if len(yoy_vals) >= YOY_MIN_DAYS:
            return round(post_avg - sum(yoy_vals) / len(yoy_vals), 3), "yoy"

    # P2: trailing 3M — Ads API takes priority for overlapping dates
    merged: dict[str, dict] = {}
    if trailing_ext_index:
        merged.update(trailing_ext_index)
    merged.update(asin_date_index)

    trailing_vals = [
        float(merged[d]["orders"])
        for i in range(TRAILING_START, TRAILING_END + 1)
        if (d := (anchor + timedelta(days=i)).strftime("%Y-%m-%d")) in merged
    ]
    if len(trailing_vals) >= TRAILING_MIN_DAYS:
        return round(post_avg - sum(trailing_vals) / len(trailing_vals), 3), "trailing_3m"

    # P3: pre-window fallback
    return round(post_avg - pre_avg, 3), "pre_window"


def _build_attributions(
    item: dict,
    daily_perf: list[dict],
    tz: ZoneInfo,
    yoy_date_index: dict[str, dict] | None = None,
    trailing_ext_index: dict[str, dict] | None = None,
) -> list[dict]:
    """
    Stage 1: for each change event, compute before/after window KPIs and
    annotate with covariate context.  Returns the change_attributions list.
    """
    change_events = item.get("change_events") or []
    cov_series = item.get("covariate_series") or {}
    comp_summary = item.get("competitor_price_summary") or {}

    # Build per-campaign daily index: (campaign_id, date) → record.
    # spAdvertisedProduct with groupBy=advertiser returns one row per
    # (ASIN, campaignId, date), so per-campaign precision is preserved.
    daily_index: dict[tuple[str, str], dict] = {}
    # ASIN-level date index: date → aggregated KPIs across all campaigns.
    # Used as fallback when a campaign has no records in the attribution window
    # (e.g., the campaign was paused or had zero activity on those days).
    asin_date_index: dict[str, dict] = {}
    for rec in daily_perf:
        cid = str(rec.get("campaign_id") or "")
        date = rec.get("date") or ""
        if cid and date:
            daily_index[(cid, date)] = rec
        if date:
            if date not in asin_date_index:
                asin_date_index[date] = {"spend": 0.0, "orders": 0.0, "clicks": 0.0, "sales": 0.0}
            agg = asin_date_index[date]
            for k in ("spend", "orders", "clicks", "sales"):
                agg[k] = agg.get(k, 0.0) + (rec.get(k) or 0.0)

    attributions: list[dict] = []

    for ev in change_events:
        ts = ev.get("changed_at")
        if not ts:
            continue
        try:
            anchor = datetime.fromtimestamp(int(ts) / 1000, tz=UTC).astimezone(tz)
        except (TypeError, ValueError):
            continue

        cid = str(ev.get("campaign_id") or "")
        pre = _window_avg(daily_index, cid, anchor, ATTR_PRE_START, ATTR_PRE_END)
        post = _window_avg(daily_index, cid, anchor, ATTR_POST_START, ATTR_POST_END)

        kpi_level = "campaign"
        if pre is None or post is None:
            # Campaign was inactive in one or both windows; fall back to
            # ASIN-level aggregated KPIs so the event is not silently dropped.
            pre = _window_avg_asin(asin_date_index, anchor, ATTR_PRE_START, ATTR_PRE_END)
            post = _window_avg_asin(asin_date_index, anchor, ATTR_POST_START, ATTR_POST_END)
            kpi_level = "asin"

        if pre is None or post is None:
            continue

        pre_acos = pre["acos"]
        post_acos = post["acos"]
        delta_acos = (
            round(post_acos - pre_acos, 2)
            if (pre_acos is not None and post_acos is not None)
            else None
        )
        delta_orders = round(post["orders"] - pre["orders"], 2)
        delta_clicks = round(post["clicks"] - pre["clicks"], 2)

        delta_orders_normalized, baseline_source = _normalized_delta_orders(
            asin_date_index,
            anchor,
            post["orders"],
            pre["orders"],
            yoy_date_index=yoy_date_index,
            trailing_ext_index=trailing_ext_index,
        )

        direction = _classify_direction(delta_orders, "orders", pre["orders"])
        if direction == "neutral" and delta_acos is not None:
            direction = _classify_direction(delta_acos, "acos", pre_acos or 0)

        # Flag when ASIN-level KPI fallback produces a delta larger than 1.5× the
        # pre-window mean — likely dominated by seasonal trends, not the change itself.
        pre_orders_mean = pre["orders"]
        attribution_suspect = (
            kpi_level == "asin"
            and pre_orders_mean > 0
            and abs(delta_orders) > pre_orders_mean * 1.5
        )
        attribution_suspect_reason = (
            (
                f"ASIN-level KPI fallback: |Δorders|={abs(delta_orders):.1f} > 1.5× "
                f"pre-window mean ({pre_orders_mean:.1f}/day); "
                f"seasonal or account-wide trend likely dominates the change effect"
            )
            if attribution_suspect
            else None
        )

        change_date = anchor.strftime("%Y-%m-%d")
        cov = cov_series.get(change_date, {})

        # Pre/post window average price for price_delta_window
        def _avg_price(day_start: int, day_end: int, _anchor: datetime = anchor) -> float | None:
            prices = [
                cov_series.get((_anchor + timedelta(days=d)).strftime("%Y-%m-%d"), {}).get(
                    "sale_price"
                )
                for d in range(day_start, day_end + 1)
            ]
            prices = [p for p in prices if p is not None]
            return round(sum(prices) / len(prices), 2) if prices else None

        pre_price = _avg_price(ATTR_PRE_START, ATTR_PRE_END)
        post_price = _avg_price(ATTR_POST_START, ATTR_POST_END)
        price_delta = (
            round(post_price - pre_price, 2)
            if pre_price is not None and post_price is not None
            else None
        )

        comp_day = comp_summary.get(change_date, {})
        own_price = cov.get("sale_price")
        comp_median = comp_day.get("median")
        price_gap = (
            round(float(own_price) - float(comp_median), 2)
            if own_price is not None and comp_median is not None
            else None
        )

        attributions.append(
            {
                "event_id": ev.get("event_id"),
                "campaign_id": cid,
                "entity_type": ev.get("entity_type"),
                "entity_id": ev.get("entity_id"),
                "change_type": ev.get("change_type"),
                "old_value": ev.get("old_value"),
                "new_value": ev.get("new_value"),
                "changed_at": change_date,
                "priority": ev.get("priority", 0),
                "compound": ev.get("compound_change", False),
                "keyword": ev.get("keyword"),
                "keyword_type": ev.get("keyword_type"),
                "kpi_level": kpi_level,
                "pre_window": pre,
                "post_window": post,
                "delta_acos": delta_acos,
                "delta_orders": delta_orders,
                "delta_orders_normalized": delta_orders_normalized,
                "delta_baseline_source": baseline_source,
                "pre_orders_std": pre.get("orders_std", 0.0),
                "delta_clicks": delta_clicks,
                "direction": direction,
                "covariates_at_change": cov,
                "had_promotion": bool(cov.get("promotion_flag", False)),
                "price_delta_window": price_delta,
                "price_gap_to_comp_median": price_gap,
                "attribution_suspect": attribution_suspect,
                "attribution_suspect_reason": attribution_suspect_reason,
            }
        )

    # Sort: priority desc, then impact magnitude desc
    attributions.sort(
        key=lambda a: (a.get("priority", 0), abs(a["delta_orders"])),
        reverse=True,
    )
    return len(attributions), attributions[:20]


# ── Stage 2: ITS ──────────────────────────────────────────────────────────────


def _its_analyze(series: np.ndarray, intervention_idx: int) -> dict[str, Any]:
    """
    Piecewise OLS:  y = α + β·t + γ·D + δ·(t−T₀)·D + ε
    D = 0 before intervention, 1 after.
    Returns level_shift (γ), slope_change (δ), p-values, r_squared.
    """
    n = len(series)
    if intervention_idx < _ITS_MIN_PRE or (n - intervention_idx) < _ITS_MIN_POST:
        return {"skipped": True, "reason": "insufficient observations"}

    try:
        from scipy import stats as _stats
    except ImportError:
        return {"skipped": True, "reason": "scipy not installed"}

    t = np.arange(n, dtype=float)
    D = (t >= intervention_idx).astype(float)
    tD = (t - intervention_idx) * D
    X = np.column_stack([np.ones(n), t, D, tD])

    try:
        beta, _, _, _ = np.linalg.lstsq(X, series, rcond=None)
    except np.linalg.LinAlgError:
        return {"skipped": True, "reason": "singular matrix"}

    fitted = X @ beta
    resid = series - fitted
    ss_res = float(resid @ resid)
    ss_tot = float(((series - series.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    dof = n - X.shape[1]
    if dof <= 0:
        p = [1.0] * 4
    else:
        sigma2 = ss_res / dof
        try:
            cov_b = sigma2 * np.linalg.inv(X.T @ X)
            se = np.sqrt(np.diag(cov_b))
            t_stats = beta / (se + 1e-12)
            p = [2 * (1 - _stats.t.cdf(abs(ts), dof)) for ts in t_stats]
        except np.linalg.LinAlgError:
            p = [1.0] * 4

    # 95% CI on level_shift using t-distribution critical value
    z_t = float(_stats.t.ppf(0.975, max(dof, 1)))
    ls_se = float(se[2])
    level_shift_ci_lo = round(float(beta[2]) - z_t * ls_se, 4)
    level_shift_ci_hi = round(float(beta[2]) + z_t * ls_se, 4)

    return {
        "skipped": False,
        "level_shift": round(float(beta[2]), 4),
        "level_shift_ci_lo": level_shift_ci_lo,
        "level_shift_ci_hi": level_shift_ci_hi,
        "slope_change": round(float(beta[3]), 4),
        "p_level": round(float(p[2]), 4),
        "p_slope": round(float(p[3]), 4),
        "r_squared": round(r2, 4),
        "pre_mean": round(float(series[:intervention_idx].mean()), 4),
        "post_mean": round(float(series[intervention_idx:].mean()), 4),
    }


# ── Stage 3: CausalImpact (BSTS) ─────────────────────────────────────────────


def _causal_impact_analyze(
    series: np.ndarray,
    covariate_matrix: np.ndarray,
    intervention_idx: int,
) -> dict[str, Any]:
    """
    BSTS counterfactual via `causalimpact` package.
    Falls back to ITS-derived estimate if the library is not installed.
    """
    if intervention_idx < _CI_MIN_PRE:
        return {"skipped": True, "reason": "insufficient pre-period for BSTS"}

    try:
        import pandas as pd
        from causalimpact.misc import standardize as _ci_std
    except ImportError:
        return {"skipped": True, "reason": "causalimpact not installed"}

    # ── pandas compatibility patches (applied before CausalImpact is imported) ──
    # Patch 1: pandas ≥ 2.1 renamed DataFrame.applymap → DataFrame.map;
    #          causalimpact calls the old name inside _format_input_data.
    if not hasattr(pd.DataFrame, "applymap"):
        pd.DataFrame.applymap = pd.DataFrame.map  # type: ignore[attr-defined]

    # Patch 2: causalimpact 0.1.1 uses mu[0] / sig[0] (label-based) in
    #          _standardize_pre_post_data; pandas ≥ 2.0 requires .iloc[0].
    from causalimpact.main import CausalImpact as _CI

    def _patched_standardize(self: _CI) -> None:
        self.normed_pre_data, (mu, sig) = _ci_std(self.pre_data)
        self.normed_post_data = (self.post_data - mu) / sig
        self.mu_sig = (mu.iloc[0], sig.iloc[0])

    _CI._standardize_pre_post_data = _patched_standardize  # type: ignore[method-assign]

    from causalimpact import CausalImpact

    try:
        n = len(series)
        # Sanitise covariates: replace inf/nan → 0
        cov = np.where(np.isfinite(covariate_matrix), covariate_matrix, 0.0)

        # Drop zero-variance columns in the pre-period.
        # _align_covariates fills entirely-missing columns with 0, giving std=0.
        # _patched_standardize then divides by sig=0 → produces inf in exog.
        pre_std = cov[:intervention_idx].std(axis=0)
        active = np.where(pre_std > 0)[0]
        cov = cov[:, active] if len(active) > 0 else np.empty((n, 0))

        df = pd.DataFrame({"y": series})
        for i, _col_idx in enumerate(active):
            df[f"x{i}"] = cov[:, i]

        import warnings as _warnings

        with _warnings.catch_warnings():
            # causalimpact passes kwargs (nseasons, standardize, alpha) that
            # newer statsmodels versions do not accept; suppress until the
            # library is updated.
            _warnings.filterwarnings("ignore", category=FutureWarning, module="statsmodels")
            ci = CausalImpact(df, [0, intervention_idx - 1], [intervention_idx, n - 1])

        # summary_data is a DataFrame:
        #   index   = effect metrics (abs_effect, rel_effect, abs_effect_lower, ...)
        #   columns = ['average', 'cumulative']
        sd = ci.summary_data

        def _loc(row: str):
            try:
                return float(sd.loc[row, "average"])
            except (KeyError, TypeError):
                return None

        pe = _loc("abs_effect")
        # Guard: BSTS occasionally diverges (degenerate state-space) when the
        # pre-period has near-zero variance, producing a counterfactual near
        # ±millions even though actual orders are in the single digits.
        # Reject if |point_effect| > _CI_OUTLIER_MULT × pre-period scale.
        if pe is not None:
            pre_scale = max(float(np.abs(series[:intervention_idx]).max()), 1.0)
            if abs(pe) > _CI_OUTLIER_MULT * pre_scale:
                logger.warning(
                    f"CausalImpact outlier rejected: |point_effect|={pe:.1f} "
                    f"> {_CI_OUTLIER_MULT}×pre_scale={pre_scale:.1f}"
                )
                return {
                    "skipped": True,
                    "reason": f"outlier: |effect|={pe:.1f} >> pre_scale={pre_scale:.1f}",
                }
        return {
            "skipped": False,
            "point_effect": pe,
            "relative_effect": _loc("rel_effect"),
            "p_value": round(float(ci.p_value), 4) if ci.p_value is not None else None,
            "ci_lo": _loc("abs_effect_lower"),
            "ci_hi": _loc("abs_effect_upper"),
        }
    except Exception as e:
        logger.warning(f"CausalImpact failed: {e}")
        return {"skipped": True, "reason": str(e)}


# ── Stage 4: DML ─────────────────────────────────────────────────────────────


def _dml_analyze(
    treatment_series: np.ndarray,
    outcome_series: np.ndarray,
    covariate_matrix: np.ndarray,
    t0: int = 0,
) -> dict[str, Any]:
    """
    Two-stage residualisation (Frisch–Waugh–Lovell):
      1. Regress treatment on X → ν̃
      2. Regress outcome  on X → ỹ
      3. OLS of ỹ on ν̃ → θ  (clean causal estimate)
    Uses RandomForest if scikit-learn is available, OLS otherwise.
    """
    n = len(treatment_series)
    if n < _ITS_MIN_PRE + _ITS_MIN_POST:
        return {"skipped": True, "reason": "insufficient observations for DML"}
    post_obs = n - t0
    if post_obs < _ITS_MIN_POST:
        return {
            "skipped": True,
            "reason": f"insufficient post-period observations ({post_obs} < {_ITS_MIN_POST})",
        }

    X = covariate_matrix

    def _residualise(y: np.ndarray) -> np.ndarray:
        try:
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.model_selection import cross_val_predict

            rf = RandomForestRegressor(n_estimators=50, max_depth=4, random_state=0, n_jobs=1)
            hat = cross_val_predict(rf, X, y, cv=min(5, n // 4) or 2)
        except ImportError:
            try:
                Xe = np.column_stack([np.ones(n), X])
                b, _, _, _ = np.linalg.lstsq(Xe, y, rcond=None)
                hat = Xe @ b
            except np.linalg.LinAlgError:
                hat = np.zeros(n)
        return y - hat

    try:
        nu = _residualise(treatment_series)
        yt = _residualise(outcome_series)
    except Exception as e:
        return {"skipped": True, "reason": f"residualisation failed: {e}"}

    denom = float(nu @ nu)
    if denom < 1e-12:
        return {"skipped": True, "reason": "near-zero treatment variance after residualisation"}

    theta = float(nu @ yt) / denom
    fitted = nu * theta
    e = yt - fitted
    se = math.sqrt(float((nu**2 * e**2).sum()) / (denom**2))

    dof = n - X.shape[1] - 1
    t_val = theta / (se + 1e-12)
    try:
        from scipy import stats as _stats

        p_val = float(2 * (1 - _stats.t.cdf(abs(t_val), max(dof, 1))))
    except ImportError:
        p_val = float(2 * (1 - 0.5 * (1 + math.erf(abs(t_val) / math.sqrt(2)))))

    ss_res = float(e @ e)
    ss_tot = float(((yt - yt.mean()) ** 2).sum())
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    if r2 < 0:
        return {
            "skipped": True,
            "reason": f"poor model fit (R²={round(r2, 4)} < 0); theta unreliable",
        }

    # 95% CI using normal approximation (sandwich SE is asymptotically normal)
    theta_ci_lo = round(theta - 1.96 * se, 6)
    theta_ci_hi = round(theta + 1.96 * se, 6)

    return {
        "skipped": False,
        "theta": round(theta, 6),
        "se": round(se, 6),
        "theta_ci_lo": theta_ci_lo,
        "theta_ci_hi": theta_ci_hi,
        "p_value": round(p_val, 4),
        "r_squared": round(r2, 4),
    }


# ── Within-sample directional backtest ───────────────────────────────────────

_BACKTEST_SNR_MIN = 1.0  # |delta| must be ≥ 1σ of pre-window to count as signal
_BACKTEST_EFFECT_MIN = 1e-4  # avg model effect below this → direction is undefined


def _compute_backtest_stats(attributions: list[dict]) -> dict[str, Any]:
    """
    Directional calibration check: does the causal model's predicted sign match
    the observed post-window KPI direction?

    Observed direction uses delta_orders_normalized when available (YoY or trailing
    3M baseline), falling back to delta_orders (within-sample pre-window).
    backtest_baseline_dist records how many events used each baseline source.

    Quality gates (applied in order):
      1. All models skipped          → exclude (no model output)
      2. direction == "neutral"      → exclude (no reliable observed ground truth)
      3. attribution_suspect         → exclude (ASIN-level fallback with outsized Δ)
      4. compound or had_promotion   → exclude (confounded multi-cause events)
      5a. incomplete_post_window     → exclude (post-window not yet fully elapsed)
      5b. non_yoy_baseline           → exclude (trailing/pre_window ≠ CausalImpact frame)
      6. SNR < _BACKTEST_SNR_MIN     → exclude (|delta| < 1σ pre-window noise floor)
      7. |avg_effect| < ε            → exclude (model commits to no direction)
    """
    total = hits = strong = strong_hits = 0
    skipped_reasons: Counter = Counter()
    baseline_counter: Counter = Counter()

    for attr in attributions:
        its = attr.get("its") or {}
        dml = attr.get("dml") or {}
        ci = attr.get("causal_impact") or {}
        if its.get("skipped") and dml.get("skipped") and ci.get("skipped"):
            skipped_reasons["all_models_skipped"] += 1
            continue

        # Gate 2: observed direction must be non-neutral
        if attr.get("direction", "neutral") == "neutral":
            skipped_reasons["neutral_direction"] += 1
            continue

        # Gate 3: ASIN-level fallback with suspect Δ
        if attr.get("attribution_suspect"):
            skipped_reasons["attribution_suspect"] += 1
            continue

        # Gate 4: compound / promotion — confounded ground truth
        if attr.get("compound") or attr.get("had_promotion"):
            skipped_reasons["confounded"] += 1
            continue

        # Gate 5a: post-window must be fully closed (need at least ATTR_POST_END+1 days)
        changed = attr.get("changed_at", "")
        if changed:
            try:
                days_since = (_date_cls.today() - _date_cls.fromisoformat(str(changed)[:10])).days
                if days_since < abs(ATTR_POST_END) + 1:
                    skipped_reasons["incomplete_post_window"] += 1
                    continue
            except (ValueError, TypeError):
                pass

        norm = attr.get("delta_orders_normalized")
        delta = norm if norm is not None else (attr.get("delta_orders") or 0)
        if delta == 0:
            skipped_reasons["delta_zero"] += 1
            continue

        # Gate 5b: require YoY baseline — trailing/pre_window baselines use a different
        # reference frame than CausalImpact's counterfactual, causing systematic sign mismatches
        if attr.get("delta_baseline_source") != "yoy":
            skipped_reasons["non_yoy_baseline"] += 1
            continue

        # Gate 5: SNR — |delta| must clear the pre-window noise floor
        pre_std = attr.get("pre_orders_std") or 0.0
        if pre_std > 0 and abs(delta) < _BACKTEST_SNR_MIN * pre_std:
            skipped_reasons["low_snr"] += 1
            continue

        effects: list[float] = []
        if not its.get("skipped") and its.get("level_shift") is not None:
            effects.append(float(its["level_shift"]))
        if not dml.get("skipped") and dml.get("theta") is not None:
            effects.append(float(dml["theta"]))
        if not ci.get("skipped") and ci.get("point_effect") is not None:
            effects.append(float(ci["point_effect"]))

        if not effects:
            skipped_reasons["no_effects"] += 1
            continue

        # Gate 6: model must commit to a direction
        avg_effect = sum(effects) / len(effects)
        if abs(avg_effect) < _BACKTEST_EFFECT_MIN:
            skipped_reasons["near_zero_effect"] += 1
            continue

        baseline_counter[attr.get("delta_baseline_source", "pre_window")] += 1
        observed_pos = delta > 0
        predicted_pos = avg_effect > 0
        hit = predicted_pos == observed_pos
        total += 1
        hits += hit

        if "Strong evidence" in (attr.get("consensus") or ""):
            strong += 1
            strong_hits += hit

    if skipped_reasons:
        logger.debug(f"[backtest] skipped breakdown: {dict(skipped_reasons)}")

    return {
        "backtest_total": total,
        "backtest_hit_rate": round(hits / total * 100, 1) if total > 0 else None,
        "backtest_strong_n": strong,
        "backtest_strong_hit_rate": round(strong_hits / strong, 3) if strong > 0 else None,
        "backtest_baseline_dist": dict(baseline_counter),
        "backtest_skipped": dict(skipped_reasons),
    }


# ── Consensus ─────────────────────────────────────────────────────────────────

_CAUSAL_MODEL_COUNT = 3  # ITS, CausalImpact, DML — always the denominator

# Metric polarity: +1 means "positive model effect = improvement", -1 means inverted,
# 0 means ambiguous (spend/clicks direction vs orders is not directly comparable).
_METRIC_POLARITY: dict[str, int] = {
    "orders": 1,
    "sales": 1,
    "cvr": 1,
    "clicks": 0,
    "spend": 0,
    "acos": -1,
    "cpc": -1,
}


def _build_consensus(its: dict, ci: dict, dml: dict, attr: dict, metric_col: str = "orders") -> str:
    sig_flags: list[bool] = []
    votes: dict[str, int] = {"positive": 0, "negative": 0}

    def _vote(p: float | None, effect: float | None) -> None:
        if p is not None and p <= 0.10:
            sig_flags.append(True)
            if effect is not None:
                votes["positive" if effect > 0 else "negative"] += 1
        else:
            sig_flags.append(False)

    # its_fallback: CI skipped, so ITS result was copied into ci for display.
    # It is NOT an independent model vote — skip it to avoid double-counting ITS.
    ci_is_fallback = ci.get("source") == "its_fallback"

    if not its.get("skipped"):
        _vote(its.get("p_level"), its.get("level_shift"))
    if not ci.get("skipped") and not ci_is_fallback:
        _vote(ci.get("p_value"), ci.get("point_effect"))
    if not dml.get("skipped"):
        _vote(dml.get("p_value"), dml.get("theta"))

    n_sig = sum(sig_flags)
    n_ran = len(sig_flags)
    # When CI ran as its_fallback, only 2 independent models exist.
    total = _CAUSAL_MODEL_COUNT - (1 if ci_is_fallback else 0)
    if n_ran == 0:
        return "Insufficient data for causal analysis."

    dominant = "positive" if votes["positive"] >= votes["negative"] else "negative"
    direction_lbl = "increase" if dominant == "positive" else "decrease"
    change_type = attr.get("change_type", "change")

    note = ""
    if attr.get("had_promotion"):
        note = " (confounded by concurrent promotion)"
    elif attr.get("compound"):
        note = " (compound change — isolability limited)"

    if n_sig == 0:
        return f"No significant causal effect detected for {change_type}{note}."

    # Cross-check causal model direction against observed window direction.
    # Causal models estimate effect vs counterfactual trend; window attribution
    # compares pre-mean vs post-mean.  These legitimately differ when the
    # pre-period has a strong trend (model extrapolates the trend forward and
    # calls the actual outcome an "increase" relative to the trend, while
    # the simple pre/post average shows a drop).  Flag the conflict explicitly
    # rather than reporting contradictory "Strong evidence" labels.
    observed_dir = attr.get("direction", "neutral")  # "improved" | "worsened" | "neutral"
    delta_orders = attr.get("delta_orders", 0) or 0

    # Metric-polarity-aware conflict check.
    # polarity=+1: positive model effect → improved (orders, sales, cvr)
    # polarity=-1: positive model effect → worsened (acos, cpc — lower is better)
    # polarity= 0: ambiguous direction vs orders observed window (spend, clicks)
    polarity = _METRIC_POLARITY.get(metric_col, 1)
    if polarity == 1:
        model_improved = dominant == "positive"
    elif polarity == -1:
        model_improved = dominant == "negative"  # lower acos/cpc = improvement
    else:
        model_improved = None  # skip conflict check for ambiguous metrics

    obs_improved = observed_dir == "improved"
    obs_worsened = observed_dir == "worsened"
    conflict = (
        model_improved is not None
        and n_sig > 0
        and observed_dir != "neutral"
        and ((model_improved and obs_worsened) or (not model_improved and obs_improved))
    )

    if conflict:
        return (
            f"Conflicting model consensus ({n_sig}/{total} models significant): "
            f"causal models estimate {direction_lbl} vs counterfactual trend, "
            f"but observed window shows {observed_dir} "
            f"(delta_orders={delta_orders:+.2f}). "
            f"Pre-period trend likely extrapolated downward — "
            f"treat causal direction as unreliable; use window delta for priority{note}."
        )

    fallback_note = (
        " (CausalImpact unavailable — ITS used as fallback; not counted)" if ci_is_fallback else ""
    )

    # Direction agreement: all significant models must vote the same way.
    # 2 positive + 1 negative = significant but NOT in agreement — do not say "agree".
    directions_split = votes["positive"] > 0 and votes["negative"] > 0

    if directions_split:
        return (
            f"Conflicting model direction ({n_sig}/{total} models significant, "
            f"{votes['positive']} positive / {votes['negative']} negative): "
            f"{change_type} shows inconsistent directional evidence — "
            f"treat causal direction as unreliable{note}{fallback_note}."
        )

    if n_sig == n_ran == total:
        return (
            f"Strong evidence ({n_sig}/{total} models agree): "
            f"{change_type} caused a significant {direction_lbl} in the outcome metric"
            f"{note}{fallback_note}."
        )
    return (
        f"Weak evidence ({n_sig}/{total} models significant, {n_ran} ran): "
        f"{change_type} may have contributed to a {direction_lbl} in the outcome metric"
        f"{note}{fallback_note}."
    )


# ── Entry point ───────────────────────────────────────────────────────────────


def run_causal_analysis(
    item: dict,
    config: dict,
    daily_perf: list[dict] | None = None,
    yoy_date_index: dict[str, dict] | None = None,
    trailing_ext_index: dict[str, dict] | None = None,
) -> dict[str, Any]:
    """
    Run the full attribution + causal pipeline for one ASIN item.

    Stages:
      1. Window attribution  (before/after KPI comparison per change event)
      2. ITS                 (segmented regression on full metric series)
      3. CausalImpact        (BSTS counterfactual)
      4. DML                 (debiased ML, removes covariate confounding)

    ITS/CI/DML results are embedded directly in each change_attribution entry.

    Args:
        item:               enriched item dict (covariate_series, change_events, etc.)
        config:             workflow config dict (timezone, causal_metric, days, ...)
        daily_perf:         list of daily campaign performance records
                            [{campaign_id, date, spend, orders, clicks, sales}, ...]
        yoy_date_index:     {date_str: {orders, spend, clicks}} for same period last year
                            (364 days back). Used as P1 baseline in backtest normalisation.
        trailing_ext_index: {date_str: {orders, spend, clicks}} extending daily_perf
                            backwards to cover trailing ~3M. Used as P2 baseline.

    Returns:
        {"change_attributions": [...]}, or {} if insufficient input data.
    """
    change_events = item.get("change_events") or []
    cov_series = item.get("covariate_series") or {}
    if not change_events or not cov_series:
        return {}

    tz = ZoneInfo(config.get("timezone", "America/Los_Angeles"))

    # ── Stage 1: window attribution ──────────────────────────────────────────
    total_attributions_count, attributions = _build_attributions(
        item,
        daily_perf or [],
        tz,
        yoy_date_index=yoy_date_index or None,
        trailing_ext_index=trailing_ext_index or None,
    )
    if not attributions:
        return {"change_attributions": [], "change_attributions_total_count": 0}

    # ── Build shared covariate matrix and metric vector ───────────────────────
    all_dates = sorted(cov_series.keys())
    start_date = all_dates[0]
    end_date = all_dates[-1]

    dates, cov_matrix = _align_covariates(item, start_date, end_date)
    if not dates:
        return {
            "change_attributions": attributions,
            "change_attributions_total_count": total_attributions_count,
        }

    date_idx = {d: i for i, d in enumerate(dates)}

    # Metric series: prefer daily_perf; fall back to sale_price from covariate_series
    # Supported metric_col values: orders | spend | clicks | sales
    # Derived (not directly in daily_perf):  acos | cvr | cpc
    _DIRECT_METRICS = {"orders", "spend", "clicks", "sales"}
    _DERIVED_METRICS = {"acos", "cvr", "cpc"}
    metric_col = config.get("causal_metric", "orders")
    if metric_col not in _DIRECT_METRICS | _DERIVED_METRICS:
        logger.warning(
            f"[causal_analysis] Unknown causal_metric '{metric_col}'; "
            f"falling back to 'orders'. Supported: "
            f"{sorted(_DIRECT_METRICS | _DERIVED_METRICS)}"
        )
        metric_col = "orders"

    daily_perf_map: dict[str, dict] = {}
    for rec in daily_perf or []:
        d = rec.get("date")
        if d:
            # Accumulate multiple campaigns → sum for account-level series
            if d not in daily_perf_map:
                daily_perf_map[d] = {"orders": 0, "spend": 0, "clicks": 0, "sales": 0}
            for k in ("orders", "spend", "clicks", "sales"):
                daily_perf_map[d][k] = daily_perf_map[d].get(k, 0) + (rec.get(k) or 0)

    def _derive_metric(day: dict) -> float:
        """Derive acos/cvr/cpc from accumulated spend/sales/orders/clicks."""
        if metric_col == "acos":
            sales = day.get("sales", 0) or 0
            return round(day.get("spend", 0) / sales * 100, 4) if sales > 0 else 0.0
        if metric_col == "cvr":
            clicks = day.get("clicks", 0) or 0
            return round(day.get("orders", 0) / clicks, 6) if clicks > 0 else 0.0
        if metric_col == "cpc":
            clicks = day.get("clicks", 0) or 0
            return round(day.get("spend", 0) / clicks, 4) if clicks > 0 else 0.0
        return float(day.get(metric_col, 0) or 0)

    metric_vec = np.zeros(len(dates))
    for i, d in enumerate(dates):
        if daily_perf_map:
            day = daily_perf_map.get(d, {})
            metric_vec[i] = _derive_metric(day)
        else:
            val = cov_series.get(d, {}).get("sale_price")
            metric_vec[i] = float(val) if val is not None else 0.0

    # ── Stages 2–4: causal models per attribution ─────────────────────────────
    for attr in attributions:
        change_date = attr.get("changed_at")
        if not change_date or change_date not in date_idx:
            attr.update(
                {
                    "its": {"skipped": True, "reason": "date not in covariate window"},
                    "causal_impact": {"skipped": True, "reason": "date not in covariate window"},
                    "dml": {"skipped": True, "reason": "date not in covariate window"},
                    "consensus": "Change date outside covariate window.",
                }
            )
            continue

        t0 = date_idx[change_date]

        # CREATED/DELETED are structural events without a quantitative treatment
        # magnitude — causal models cannot produce meaningful estimates.
        _skip_reason = None
        if attr.get("change_type") in ("CREATED", "DELETED"):
            _skip_reason = f"{attr['change_type']} events have no quantifiable treatment magnitude"

        if _skip_reason:
            attr.update(
                {
                    "its": {"skipped": True, "reason": _skip_reason},
                    "causal_impact": {"skipped": True, "reason": _skip_reason},
                    "dml": {"skipped": True, "reason": _skip_reason},
                    "consensus": f"Causal analysis skipped: {_skip_reason}.",
                }
            )
            continue

        # Stage 2: ITS
        its = _its_analyze(metric_vec, t0)

        # Stage 3: CausalImpact
        ci = _causal_impact_analyze(metric_vec, cov_matrix, t0)
        if ci.get("skipped") and not its.get("skipped"):
            # ITS fallback so consensus has something to work with
            ci = {
                "skipped": False,
                "point_effect": its.get("level_shift"),
                "relative_effect": (
                    round(its["level_shift"] / its["pre_mean"], 4) if its.get("pre_mean") else None
                ),
                "p_value": its.get("p_level"),
                "source": "its_fallback",
            }

        # Stage 4: DML — binary step treatment (0 before, 1 from change date)
        treatment = np.zeros(len(dates))
        treatment[t0:] = 1.0
        dml = _dml_analyze(treatment, metric_vec, cov_matrix, t0=t0)

        consensus = _build_consensus(its, ci, dml, attr, metric_col=metric_col)

        # ── Per-event significance flags ──────────────────────────────────
        # ITS: p_level < 0.05 AND 95% CI does not cross zero
        its_sig = (
            not its.get("skipped")
            and (its.get("p_level") or 1.0) < 0.05
            and (its.get("level_shift_ci_lo", 0) or 0) * (its.get("level_shift_ci_hi", 0) or 0) > 0
        )
        # CausalImpact: p_value < 0.05 (skip ITS-fallback rows — same p reused)
        ci_sig = (
            not ci.get("skipped")
            and ci.get("source") != "its_fallback"
            and (ci.get("p_value") or 1.0) < 0.05
        )
        # DML: p_value < 0.05 AND theta CI does not cross zero
        dml_sig = (
            not dml.get("skipped")
            and (dml.get("p_value") or 1.0) < 0.05
            and (dml.get("theta_ci_lo", 0) or 0) * (dml.get("theta_ci_hi", 0) or 0) > 0
        )

        attr.update(
            {
                "its": its,
                "causal_impact": ci,
                "dml": dml,
                "consensus": consensus,
                "its_significant": its_sig,
                "ci_significant": ci_sig,
                "dml_significant": dml_sig,
                "event_significant": its_sig or ci_sig or dml_sig,
            }
        )

    n_asin_lvl = sum(1 for a in attributions if a.get("kpi_level") == "asin")

    # ── Item-level significance aggregate ─────────────────────────────────────
    # Only count events where at least one causal model actually ran.
    runnable = [
        a
        for a in attributions
        if not (
            a.get("its", {}).get("skipped", True)
            and a.get("causal_impact", {}).get("skipped", True)
            and a.get("dml", {}).get("skipped", True)
        )
    ]
    n_significant = sum(1 for a in runnable if a.get("event_significant"))
    events_significant_count = n_significant
    events_significant_pct = round(n_significant / len(runnable) * 100, 1) if runnable else None

    logger.info(
        f"[causal_analysis] {item.get('asin', '?')}: "
        f"{len(attributions)} change events analysed "
        f"({n_asin_lvl} ASIN-level fallback); "
        f"significant={n_significant}/{len(runnable)}"
    )
    # ── Same-day co-event annotation ─────────────────────────────────────────
    # When ≥2 attributions share the same changed_at date, their causal_impact
    # point_effect values all reflect ONE ASIN-level BSTS measurement — NOT
    # independent estimates.  Annotate so consumers never sum them.
    #   shared_effect = 'primary'   → the representative measurement for that date
    #                  'duplicate'  → same BSTS output; cite primary only
    #   same_day_event_ids          → event_ids of all other same-date entries
    #
    # Additionally, for 'duplicate' entries we null out ASIN-level delta fields
    # that would otherwise carry the same numeric value as the primary and mislead
    # the LLM into treating them as independent estimates:
    #   delta_orders_normalized  → always ASIN-level (YoY/trailing baseline); nulled
    #   delta_orders             → nulled only when kpi_level='asin' (ASIN fallback)
    #   delta_baseline_source    → set to 'shared_with_primary' to signal provenance
    from collections import defaultdict

    _by_date: dict[str, list[dict]] = defaultdict(list)
    for a in attributions:
        d = a.get("changed_at")
        if d:
            _by_date[d].append(a)
    for _date, _group in _by_date.items():
        if len(_group) < 2:
            continue
        _all_ids = [a.get("event_id") for a in _group]
        _primary_set = False
        for a in _group:
            _ci_skipped = a.get("causal_impact", {}).get("skipped", True)
            a["same_day_event_ids"] = [eid for eid in _all_ids if eid != a.get("event_id")]
            if not _ci_skipped and not _primary_set:
                a["shared_effect"] = "primary"
                _primary_set = True
            else:
                a["shared_effect"] = "duplicate"
                a["delta_orders_normalized"] = None
                a["delta_baseline_source"] = "shared_with_primary"
                if a.get("kpi_level") == "asin":
                    a["delta_orders"] = None
        if not _primary_set:
            _group[0]["shared_effect"] = "primary"

    backtest = _compute_backtest_stats(attributions)
    return {
        "change_attributions": attributions,
        "change_attributions_total_count": total_attributions_count,
        "events_significant_count": events_significant_count,
        "events_significant_pct": events_significant_pct,
        **backtest,
    }
