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
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ── Attribution window constants (exported so ad_diagnosis can reuse for lookback) ─
ATTR_PRE_START  = -9
ATTR_PRE_END    = -2
ATTR_POST_START = +2
ATTR_POST_END   = +9

# ── Minimum observations ────────────────────────────────────────────────────────
_ITS_MIN_PRE  = 7    # pre-period rows for reliable ITS
_ITS_MIN_POST = 5    # post-period rows
_CI_MIN_PRE   = 14   # pre-period rows for BSTS


# ── Covariate alignment ────────────────────────────────────────────────────────

def _align_covariates(
    item: Dict,
    start_date: str,
    end_date: str,
) -> Tuple[List[str], np.ndarray]:
    """
    Build an aligned (dates × features) covariate matrix from all available
    item-level time series.

    Columns:
      0  sale_price         own price
      1  promotion_flag     0/1
      2  competitor_median  competitor price median
      3  total_rank         organic rank (lowest totalRank across keywords)
      4  sfr                ABA search frequency rank (primary keyword)

    Missing values: forward-filled → backward-filled → zero if column entirely absent.
    Returns (dates_list, matrix) where dates_list[i] == matrix row i.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end   = datetime.strptime(end_date,   "%Y-%m-%d").date()
    except ValueError:
        return [], np.empty((0, 5))

    dates: List[str] = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    n = len(dates)
    if n == 0:
        return [], np.empty((0, 5))

    cov_series  = item.get("covariate_series") or {}
    comp_prices = item.get("competitor_price_summary") or {}
    rank_series = item.get("natural_rank_series") or {}
    mkt_trends  = item.get("market_trends") or {}

    # Weekly SFR → daily (first keyword only)
    sfr_by_date: Dict[str, float] = {}
    for kw, weeks in mkt_trends.items():
        for iso_week, vals in weeks.items():
            sfr = vals.get("sfr")
            if sfr is None:
                continue
            try:
                week_start = datetime.strptime(f"{iso_week}-1", "%G-W%V-%u").date()
            except ValueError:
                continue
            for offset in range(7):
                d = (week_start + timedelta(days=offset)).strftime("%Y-%m-%d")
                sfr_by_date.setdefault(d, float(sfr))
        break

    # Best organic rank per date (smallest totalRank)
    best_rank: Dict[str, float] = {}
    for kw, days_data in rank_series.items():
        for d, pos in days_data.items():
            tr = pos.get("totalRank")
            if tr is None:
                continue
            if d not in best_rank or tr < best_rank[d]:
                best_rank[d] = float(tr)

    raw: List[List] = []
    for d in dates:
        cov  = cov_series.get(d)  or {}
        comp = comp_prices.get(d) or {}
        raw.append([
            cov.get("sale_price"),
            float(bool(cov.get("promotion_flag", False))),
            comp.get("median"),
            best_rank.get(d),
            sfr_by_date.get(d),
        ])

    mat = np.array(raw, dtype=float)   # None → NaN

    # Forward-fill then backward-fill; remaining NaN → 0
    for col in range(mat.shape[1]):
        last: Optional[float] = None
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
    daily_index: Dict[Tuple[str, str], Dict],
    campaign_id: str,
    anchor: datetime,
    day_start: int,
    day_end: int,
) -> Optional[Dict]:
    """
    Aggregate daily KPIs over [anchor + day_start, anchor + day_end] inclusive.
    daily_index is keyed by (campaign_id, date) — spAdvertisedProduct returns
    one row per (ASIN, campaign, date) so per-campaign precision is preserved.
    Averages per day for comparability; ACOS derived from summed spend/sales.
    """
    records = []
    for offset in range(day_start, day_end + 1):
        d   = (anchor + timedelta(days=offset)).strftime("%Y-%m-%d")
        rec = daily_index.get((campaign_id, d))
        if rec:
            records.append(rec)
    if not records:
        return None
    n           = len(records)
    total_spend = sum(r.get("spend", 0) or 0 for r in records)
    total_sales = sum(r.get("sales", 0) or 0 for r in records)
    return {
        "spend":  round(total_spend / n, 2),
        "orders": round(sum(r.get("orders", 0) or 0 for r in records) / n, 2),
        "acos":   round(total_spend / total_sales * 100, 2) if total_sales > 0 else None,
        "clicks": round(sum(r.get("clicks", 0) or 0 for r in records) / n, 2),
        "days":   n,
    }


def _window_avg_asin(
    asin_date_index: Dict[str, Dict],
    anchor: datetime,
    day_start: int,
    day_end: int,
) -> Optional[Dict]:
    """
    ASIN-level fallback: aggregate KPIs over all campaigns for the window.
    asin_date_index is keyed by date, values are pre-summed across all campaigns.
    Used when the per-campaign window has no records (campaign was inactive).
    """
    records = []
    for offset in range(day_start, day_end + 1):
        d   = (anchor + timedelta(days=offset)).strftime("%Y-%m-%d")
        rec = asin_date_index.get(d)
        if rec:
            records.append(rec)
    if not records:
        return None
    n           = len(records)
    total_spend = sum(r.get("spend", 0) or 0 for r in records)
    total_sales = sum(r.get("sales", 0) or 0 for r in records)
    return {
        "spend":  round(total_spend / n, 2),
        "orders": round(sum(r.get("orders", 0) or 0 for r in records) / n, 2),
        "acos":   round(total_spend / total_sales * 100, 2) if total_sales > 0 else None,
        "clicks": round(sum(r.get("clicks", 0) or 0 for r in records) / n, 2),
        "days":   n,
    }


def _classify_direction(delta: float, metric: str, pre_val: float) -> str:
    if metric == "acos":
        if delta < -3:  return "improved"
        if delta >  3:  return "worsened"
    else:
        if pre_val > 0 and abs(delta) / pre_val >= 0.15:
            return "improved" if delta > 0 else "worsened"
    return "neutral"


def _build_attributions(
    item: Dict,
    daily_perf: List[Dict],
    tz: ZoneInfo,
) -> List[Dict]:
    """
    Stage 1: for each change event, compute before/after window KPIs and
    annotate with covariate context.  Returns the change_attributions list.
    """
    change_events    = item.get("change_events") or []
    cov_series       = item.get("covariate_series") or {}
    comp_summary     = item.get("competitor_price_summary") or {}

    # Build per-campaign daily index: (campaign_id, date) → record.
    # spAdvertisedProduct with groupBy=advertiser returns one row per
    # (ASIN, campaignId, date), so per-campaign precision is preserved.
    daily_index: Dict[Tuple[str, str], Dict] = {}
    # ASIN-level date index: date → aggregated KPIs across all campaigns.
    # Used as fallback when a campaign has no records in the attribution window
    # (e.g., the campaign was paused or had zero activity on those days).
    asin_date_index: Dict[str, Dict] = {}
    for rec in daily_perf:
        cid  = str(rec.get("campaign_id") or "")
        date = rec.get("date") or ""
        if cid and date:
            daily_index[(cid, date)] = rec
        if date:
            if date not in asin_date_index:
                asin_date_index[date] = {"spend": 0.0, "orders": 0.0, "clicks": 0.0, "sales": 0.0}
            agg = asin_date_index[date]
            for k in ("spend", "orders", "clicks", "sales"):
                agg[k] = agg.get(k, 0.0) + (rec.get(k) or 0.0)

    attributions: List[Dict] = []

    for ev in change_events:
        ts = ev.get("changed_at")
        if not ts:
            continue
        try:
            anchor = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).astimezone(tz)
        except (TypeError, ValueError):
            continue

        cid  = str(ev.get("campaign_id") or "")
        pre  = _window_avg(daily_index, cid, anchor, ATTR_PRE_START,  ATTR_PRE_END)
        post = _window_avg(daily_index, cid, anchor, ATTR_POST_START, ATTR_POST_END)

        kpi_level = "campaign"
        if pre is None or post is None:
            # Campaign was inactive in one or both windows; fall back to
            # ASIN-level aggregated KPIs so the event is not silently dropped.
            pre  = _window_avg_asin(asin_date_index, anchor, ATTR_PRE_START,  ATTR_PRE_END)
            post = _window_avg_asin(asin_date_index, anchor, ATTR_POST_START, ATTR_POST_END)
            kpi_level = "asin"

        if pre is None or post is None:
            continue

        pre_acos  = pre["acos"]
        post_acos = post["acos"]
        delta_acos   = round(post_acos - pre_acos, 2) if (pre_acos is not None and post_acos is not None) else None
        delta_orders = round(post["orders"] - pre["orders"], 2)
        delta_clicks = round(post["clicks"] - pre["clicks"], 2)

        direction = _classify_direction(delta_orders, "orders", pre["orders"])
        if direction == "neutral" and delta_acos is not None:
            direction = _classify_direction(delta_acos, "acos", pre_acos or 0)

        change_date = anchor.strftime("%Y-%m-%d")
        cov         = cov_series.get(change_date, {})

        # Pre/post window average price for price_delta_window
        def _avg_price(day_start: int, day_end: int) -> Optional[float]:
            prices = [
                cov_series.get((anchor + timedelta(days=d)).strftime("%Y-%m-%d"), {}).get("sale_price")
                for d in range(day_start, day_end + 1)
            ]
            prices = [p for p in prices if p is not None]
            return round(sum(prices) / len(prices), 2) if prices else None

        pre_price  = _avg_price(ATTR_PRE_START,  ATTR_PRE_END)
        post_price = _avg_price(ATTR_POST_START, ATTR_POST_END)
        price_delta = (
            round(post_price - pre_price, 2)
            if pre_price is not None and post_price is not None else None
        )

        comp_day = comp_summary.get(change_date, {})
        own_price = cov.get("sale_price")
        comp_median = comp_day.get("median")
        price_gap = (
            round(float(own_price) - float(comp_median), 2)
            if own_price is not None and comp_median is not None else None
        )

        attributions.append({
            "event_id":                ev.get("event_id"),
            "campaign_id":             cid,
            "entity_type":             ev.get("entity_type"),
            "entity_id":               ev.get("entity_id"),
            "change_type":             ev.get("change_type"),
            "old_value":               ev.get("old_value"),
            "new_value":               ev.get("new_value"),
            "changed_at":              change_date,
            "priority":                ev.get("priority", 0),
            "compound":                ev.get("compound_change", False),
            "keyword":                 ev.get("keyword"),
            "keyword_type":            ev.get("keyword_type"),
            "kpi_level":               kpi_level,
            "pre_window":              pre,
            "post_window":             post,
            "delta_acos":              delta_acos,
            "delta_orders":            delta_orders,
            "delta_clicks":            delta_clicks,
            "direction":               direction,
            "covariates_at_change":    cov,
            "had_promotion":           bool(cov.get("promotion_flag", False)),
            "price_delta_window":      price_delta,
            "price_gap_to_comp_median": price_gap,
        })

    # Sort: priority desc, then impact magnitude desc
    attributions.sort(
        key=lambda a: (a.get("priority", 0), abs(a["delta_orders"])),
        reverse=True,
    )
    return attributions[:20]


# ── Stage 2: ITS ──────────────────────────────────────────────────────────────

def _its_analyze(series: np.ndarray, intervention_idx: int) -> Dict[str, Any]:
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

    t  = np.arange(n, dtype=float)
    D  = (t >= intervention_idx).astype(float)
    tD = (t - intervention_idx) * D
    X  = np.column_stack([np.ones(n), t, D, tD])

    try:
        beta, _, _, _ = np.linalg.lstsq(X, series, rcond=None)
    except np.linalg.LinAlgError:
        return {"skipped": True, "reason": "singular matrix"}

    fitted  = X @ beta
    resid   = series - fitted
    ss_res  = float(resid @ resid)
    ss_tot  = float(((series - series.mean()) ** 2).sum())
    r2      = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    dof = n - X.shape[1]
    if dof <= 0:
        p = [1.0] * 4
    else:
        sigma2 = ss_res / dof
        try:
            cov_b   = sigma2 * np.linalg.inv(X.T @ X)
            se      = np.sqrt(np.diag(cov_b))
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
        "skipped":           False,
        "level_shift":       round(float(beta[2]), 4),
        "level_shift_ci_lo": level_shift_ci_lo,
        "level_shift_ci_hi": level_shift_ci_hi,
        "slope_change":      round(float(beta[3]), 4),
        "p_level":           round(float(p[2]), 4),
        "p_slope":           round(float(p[3]), 4),
        "r_squared":         round(r2, 4),
        "pre_mean":          round(float(series[:intervention_idx].mean()), 4),
        "post_mean":         round(float(series[intervention_idx:].mean()), 4),
    }


# ── Stage 3: CausalImpact (BSTS) ─────────────────────────────────────────────

def _causal_impact_analyze(
    series: np.ndarray,
    covariate_matrix: np.ndarray,
    intervention_idx: int,
) -> Dict[str, Any]:
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
    def _patched_standardize(self: "_CI") -> None:
        self.normed_pre_data, (mu, sig) = _ci_std(self.pre_data)
        self.normed_post_data = (self.post_data - mu) / sig
        self.mu_sig = (mu.iloc[0], sig.iloc[0])
    _CI._standardize_pre_post_data = _patched_standardize  # type: ignore[method-assign]

    from causalimpact import CausalImpact

    try:
        n  = len(series)
        df = pd.DataFrame({"y": series})
        for i in range(covariate_matrix.shape[1]):
            df[f"x{i}"] = covariate_matrix[:, i]

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

        return {
            "skipped":         False,
            "point_effect":    _loc("abs_effect"),
            "relative_effect": _loc("rel_effect"),
            "p_value":         round(float(ci.p_value), 4) if ci.p_value is not None else None,
            "ci_lo":           _loc("abs_effect_lower"),
            "ci_hi":           _loc("abs_effect_upper"),
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
) -> Dict[str, Any]:
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
        return {"skipped": True, "reason": f"insufficient post-period observations ({post_obs} < {_ITS_MIN_POST})"}

    X = covariate_matrix

    def _residualise(y: np.ndarray) -> np.ndarray:
        try:
            from sklearn.ensemble import RandomForestRegressor
            from sklearn.model_selection import cross_val_predict
            rf  = RandomForestRegressor(n_estimators=50, max_depth=4, random_state=0, n_jobs=1)
            hat = cross_val_predict(rf, X, y, cv=min(5, n // 4) or 2)
        except ImportError:
            try:
                Xe  = np.column_stack([np.ones(n), X])
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

    theta  = float(nu @ yt) / denom
    fitted = nu * theta
    e      = yt - fitted
    se     = math.sqrt(float((nu ** 2 * e ** 2).sum()) / (denom ** 2))

    dof   = n - X.shape[1] - 1
    t_val = theta / (se + 1e-12)
    try:
        from scipy import stats as _stats
        p_val = float(2 * (1 - _stats.t.cdf(abs(t_val), max(dof, 1))))
    except ImportError:
        p_val = float(2 * (1 - 0.5 * (1 + math.erf(abs(t_val) / math.sqrt(2)))))

    ss_res = float(e @ e)
    ss_tot = float(((yt - yt.mean()) ** 2).sum())
    r2     = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

    if r2 < 0:
        return {"skipped": True, "reason": f"poor model fit (R²={round(r2, 4)} < 0); theta unreliable"}

    # 95% CI using normal approximation (sandwich SE is asymptotically normal)
    theta_ci_lo = round(theta - 1.96 * se, 6)
    theta_ci_hi = round(theta + 1.96 * se, 6)

    return {
        "skipped":      False,
        "theta":        round(theta, 6),
        "se":           round(se, 6),
        "theta_ci_lo":  theta_ci_lo,
        "theta_ci_hi":  theta_ci_hi,
        "p_value":      round(p_val, 4),
        "r_squared":    round(r2, 4),
    }


# ── Within-sample directional backtest ───────────────────────────────────────

def _compute_backtest_stats(attributions: List[Dict]) -> Dict[str, Any]:
    """
    Directional calibration check: does the causal model's predicted sign match
    the observed post-window KPI direction?

    This is a within-sample test — both model and observation use the same
    historical data — so it measures internal consistency, not out-of-sample
    forecast accuracy.  Use it to flag when models are systematically mis-signed.
    """
    total = hits = strong = strong_hits = 0
    for attr in attributions:
        its = attr.get("its") or {}
        dml = attr.get("dml") or {}
        ci  = attr.get("causal_impact") or {}
        if its.get("skipped") and dml.get("skipped") and ci.get("skipped"):
            continue

        delta = attr.get("delta_orders") or 0
        if delta == 0:
            continue  # no directional signal to test against

        observed_pos = delta > 0

        effects: List[float] = []
        if not its.get("skipped") and its.get("level_shift") is not None:
            effects.append(float(its["level_shift"]))
        if not dml.get("skipped") and dml.get("theta") is not None:
            effects.append(float(dml["theta"]))
        if not ci.get("skipped") and ci.get("point_effect") is not None:
            effects.append(float(ci["point_effect"]))

        if not effects:
            continue

        predicted_pos = (sum(effects) / len(effects)) > 0
        hit = predicted_pos == observed_pos
        total += 1
        hits  += hit

        if "Strong evidence" in (attr.get("consensus") or ""):
            strong += 1
            strong_hits += hit

    return {
        "backtest_total":          total,
        "backtest_hit_rate":       round(hits / total, 3) if total > 0 else None,
        "backtest_strong_n":       strong,
        "backtest_strong_hit_rate": round(strong_hits / strong, 3) if strong > 0 else None,
    }


# ── Consensus ─────────────────────────────────────────────────────────────────

_CAUSAL_MODEL_COUNT = 3  # ITS, CausalImpact, DML — always the denominator

def _build_consensus(its: Dict, ci: Dict, dml: Dict, attr: Dict) -> str:
    sig_flags: List[bool] = []
    votes: Dict[str, int] = {"positive": 0, "negative": 0}

    def _vote(p: Optional[float], effect: Optional[float]) -> None:
        if p is not None and p <= 0.10:
            sig_flags.append(True)
            if effect is not None:
                votes["positive" if effect > 0 else "negative"] += 1
        else:
            sig_flags.append(False)

    if not its.get("skipped"):
        _vote(its.get("p_level"), its.get("level_shift"))
    if not ci.get("skipped"):
        _vote(ci.get("p_value"), ci.get("point_effect"))
    if not dml.get("skipped"):
        _vote(dml.get("p_value"), dml.get("theta"))

    n_sig  = sum(sig_flags)
    n_ran  = len(sig_flags)
    total  = _CAUSAL_MODEL_COUNT
    if n_ran == 0:
        return "Insufficient data for causal analysis."

    dominant       = "positive" if votes["positive"] >= votes["negative"] else "negative"
    direction_lbl  = "increase" if dominant == "positive" else "decrease"
    change_type    = attr.get("change_type", "change")

    note = ""
    if attr.get("had_promotion"):
        note = " (confounded by concurrent promotion)"
    elif attr.get("compound"):
        note = " (compound change — isolability limited)"

    if n_sig == 0:
        return f"No significant causal effect detected for {change_type}{note}."
    if n_sig == n_ran == total:
        return (
            f"Strong evidence ({n_sig}/{total} models agree): "
            f"{change_type} caused a significant {direction_lbl} in the outcome metric{note}."
        )
    return (
        f"Weak evidence ({n_sig}/{total} models significant, {n_ran} ran): "
        f"{change_type} may have contributed to a {direction_lbl} in the outcome metric{note}."
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def run_causal_analysis(
    item: Dict,
    config: Dict,
    daily_perf: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """
    Run the full attribution + causal pipeline for one ASIN item.

    Stages:
      1. Window attribution  (before/after KPI comparison per change event)
      2. ITS                 (segmented regression on full metric series)
      3. CausalImpact        (BSTS counterfactual)
      4. DML                 (debiased ML, removes covariate confounding)

    ITS/CI/DML results are embedded directly in each change_attribution entry.

    Args:
        item:        enriched item dict (covariate_series, change_events, etc.)
        config:      workflow config dict (timezone, causal_metric, days, ...)
        daily_perf:  list of daily campaign performance records
                     [{campaign_id, date, spend, orders, clicks, sales}, ...]

    Returns:
        {"change_attributions": [...]}, or {} if insufficient input data.
    """
    change_events = item.get("change_events") or []
    cov_series    = item.get("covariate_series") or {}
    if not change_events or not cov_series:
        return {}

    tz = ZoneInfo(config.get("timezone", "America/Los_Angeles"))

    # ── Stage 1: window attribution ──────────────────────────────────────────
    attributions = _build_attributions(item, daily_perf or [], tz)
    if not attributions:
        return {"change_attributions": []}

    # ── Build shared covariate matrix and metric vector ───────────────────────
    all_dates  = sorted(cov_series.keys())
    start_date = all_dates[0]
    end_date   = all_dates[-1]

    dates, cov_matrix = _align_covariates(item, start_date, end_date)
    if not dates:
        return {"change_attributions": attributions}

    date_idx = {d: i for i, d in enumerate(dates)}

    # Metric series: prefer daily_perf; fall back to sale_price from covariate_series
    metric_col = config.get("causal_metric", "orders")
    daily_perf_map: Dict[str, Dict] = {}
    for rec in (daily_perf or []):
        d = rec.get("date")
        if d:
            # Accumulate multiple campaigns → sum for account-level series
            if d not in daily_perf_map:
                daily_perf_map[d] = {"orders": 0, "spend": 0, "clicks": 0, "sales": 0}
            for k in ("orders", "spend", "clicks", "sales"):
                daily_perf_map[d][k] = daily_perf_map[d].get(k, 0) + (rec.get(k) or 0)

    metric_vec = np.zeros(len(dates))
    for i, d in enumerate(dates):
        if daily_perf_map:
            metric_vec[i] = float(daily_perf_map.get(d, {}).get(metric_col, 0) or 0)
        else:
            val = cov_series.get(d, {}).get("sale_price")
            metric_vec[i] = float(val) if val is not None else 0.0

    # ── Stages 2–4: causal models per attribution ─────────────────────────────
    for attr in attributions:
        change_date = attr.get("changed_at")
        if not change_date or change_date not in date_idx:
            attr.update({"its": {"skipped": True, "reason": "date not in covariate window"},
                         "causal_impact": {"skipped": True, "reason": "date not in covariate window"},
                         "dml": {"skipped": True, "reason": "date not in covariate window"},
                         "consensus": "Change date outside covariate window."})
            continue

        t0 = date_idx[change_date]

        # CREATED/DELETED are structural events without a quantitative treatment
        # magnitude — causal models cannot produce meaningful estimates.
        _skip_reason = None
        if attr.get("change_type") in ("CREATED", "DELETED"):
            _skip_reason = f"{attr['change_type']} events have no quantifiable treatment magnitude"

        if _skip_reason:
            attr.update({
                "its":           {"skipped": True, "reason": _skip_reason},
                "causal_impact": {"skipped": True, "reason": _skip_reason},
                "dml":           {"skipped": True, "reason": _skip_reason},
                "consensus":     f"Causal analysis skipped: {_skip_reason}.",
            })
            continue

        # Stage 2: ITS
        its = _its_analyze(metric_vec, t0)

        # Stage 3: CausalImpact
        ci = _causal_impact_analyze(metric_vec, cov_matrix, t0)
        if ci.get("skipped") and not its.get("skipped"):
            # ITS fallback so consensus has something to work with
            ci = {
                "skipped":         False,
                "point_effect":    its.get("level_shift"),
                "relative_effect": (
                    round(its["level_shift"] / its["pre_mean"], 4)
                    if its.get("pre_mean") else None
                ),
                "p_value":  its.get("p_level"),
                "source":   "its_fallback",
            }

        # Stage 4: DML — binary step treatment (0 before, 1 from change date)
        treatment = np.zeros(len(dates))
        treatment[t0:] = 1.0
        dml = _dml_analyze(treatment, metric_vec, cov_matrix, t0=t0)

        consensus = _build_consensus(its, ci, dml, attr)

        attr.update({
            "its":           its,
            "causal_impact": ci,
            "dml":           dml,
            "consensus":     consensus,
        })

    n_asin_lvl = sum(1 for a in attributions if a.get("kpi_level") == "asin")
    logger.info(
        f"[causal_analysis] {item.get('asin', '?')}: "
        f"{len(attributions)} change events analysed "
        f"({n_asin_lvl} used ASIN-level KPI fallback)"
    )
    backtest = _compute_backtest_stats(attributions)
    return {"change_attributions": attributions, **backtest}
