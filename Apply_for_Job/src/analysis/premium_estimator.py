"""
premium_estimator.py - AI skill salary premium estimation

Methods
-------
1. Raw mean difference      (fast sanity check)
2. OLS with HC3 robust SE   (controls for city / experience / company size)
   + Percentile bootstrap CI (1 000 resamples, seed-stable)
3. PSM 1:1 nearest-neighbour (optional, requires scikit-learn)
   + Covariate balance table  (SMD before / after matching)

Output columns
--------------
  ols_premium        float  — net monthly premium (CNY), HC3-robust
  ols_pvalue         float  — HC3 robust p-value
  ols_ci_95          list   — analytic 95% CI from HC3 SE
  ols_ci_95_boot     list   — percentile bootstrap 95% CI
  ols_se_robust      float  — HC3 standard error
  ols_r2             float
  ols_n              int
  ols_significant    bool   — p < 0.05
  psm_premium        float  — ATT (average treatment effect on the treated)
  psm_ci_95_boot     list   — bootstrap 95% CI on ATT
  psm_std            float
  psm_n_matched      int
  psm_balance_table  list   — [{covariate, smd_before, smd_after, balanced}, …]
"""
from __future__ import annotations

import logging
import warnings
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

MIN_GROUP_SIZE  = 8
_COVARIATES     = ["city_tier", "exp_years", "company_size_n"]
_SMD_THRESHOLD  = 0.1     # |SMD| < 0.10 → covariate is "balanced"
_N_BOOT         = 1_000   # bootstrap resamples
_BOOT_SEED      = 42


# ══════════════════════════════════════════════════════════════════════
# Internal helpers
# ══════════════════════════════════════════════════════════════════════

def _coerce_treatment(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure has_ai_skill is int 0/1 and salary_mid is float."""
    df = df.copy()
    df["salary_mid"] = pd.to_numeric(df["salary_mid"], errors="coerce")
    df = df.dropna(subset=["salary_mid", "has_ai_skill"])
    df["has_ai_skill"] = (
        df["has_ai_skill"]
        .map(lambda v: 1 if str(v).strip().lower() in ("true", "1") else 0)
        .astype(int)
    )
    return df


def _fill_covariates(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Fill missing covariate columns; return df and list of usable covariates."""
    df = df.copy()
    active: list[str] = []
    for col in _COVARIATES:
        if col not in df.columns:
            df[col] = 0
        med = df[col].median()
        df[col] = df[col].fillna(0 if pd.isna(med) else med)
        if df[col].nunique() > 1:
            active.append(col)
    return df, active


def _smd(a: np.ndarray, b: np.ndarray) -> float:
    """Standardized Mean Difference: |μ_a − μ_b| / √((σ²_a + σ²_b) / 2)."""
    denom = np.sqrt((np.var(a, ddof=1) + np.var(b, ddof=1)) / 2)
    return float(abs(a.mean() - b.mean()) / denom) if denom > 0 else 0.0


def _bootstrap_coef(
    df: pd.DataFrame,
    formula: str,
    coef_name: str,
    n_boot: int = _N_BOOT,
    seed: int = _BOOT_SEED,
) -> list[float]:
    """
    Percentile bootstrap 95% CI for a single OLS coefficient.
    Returns [lo, hi] rounded to the nearest integer.
    """
    try:
        import statsmodels.formula.api as smf
    except ImportError:
        return [float("nan"), float("nan")]

    rng   = np.random.default_rng(seed)
    coefs: list[float] = []

    for _ in range(n_boot):
        sample = df.sample(n=len(df), replace=True,
                           random_state=int(rng.integers(1_000_000)))
        try:
            m = smf.ols(formula, data=sample).fit(disp=0)
            v = m.params.get(coef_name)
            if v is not None and not np.isnan(v):
                coefs.append(float(v))
        except Exception:
            pass

    if len(coefs) < max(10, n_boot // 10):
        return [float("nan"), float("nan")]

    arr = np.array(coefs)
    return [round(float(np.percentile(arr, 2.5)), 0),
            round(float(np.percentile(arr, 97.5)), 0)]


def _bootstrap_att(
    treat_sal: np.ndarray,
    ctrl_sal: np.ndarray,
    n_boot: int = _N_BOOT,
    seed: int = _BOOT_SEED,
) -> list[float]:
    """Percentile bootstrap 95% CI for ATT (paired differences)."""
    rng  = np.random.default_rng(seed)
    atts: list[float] = []
    n    = len(treat_sal)

    for _ in range(n_boot):
        idx    = rng.integers(0, n, size=n)
        atts.append(float((treat_sal[idx] - ctrl_sal[idx]).mean()))

    arr = np.array(atts)
    return [round(float(np.percentile(arr, 2.5)), 0),
            round(float(np.percentile(arr, 97.5)), 0)]


# ══════════════════════════════════════════════════════════════════════
# OLS with HC3 robust SE + bootstrap CI
# ══════════════════════════════════════════════════════════════════════

def _ols_premium(df: pd.DataFrame) -> dict:
    """
    OLS: salary_mid ~ has_ai_skill [+ city_tier + exp_years + company_size_n]

    Standard errors: HC3 heteroskedasticity-robust (White sandwich).
    Confidence interval: reported both from HC3 SE and percentile bootstrap.
    """
    try:
        import statsmodels.formula.api as smf
    except ImportError:
        logger.warning("statsmodels not installed — skipping OLS. pip install statsmodels")
        return {}

    sub = _coerce_treatment(df)
    if sub["has_ai_skill"].nunique() < 2 or len(sub) < MIN_GROUP_SIZE * 2:
        return {}

    sub, active_controls = _fill_covariates(sub)
    formula = "salary_mid ~ has_ai_skill" + (
        " + " + " + ".join(active_controls) if active_controls else ""
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            model = smf.ols(formula, data=sub).fit(cov_type="HC3")
        except Exception as e:
            logger.warning("OLS fit failed: %s", e)
            return {}

    coef = model.params.get("has_ai_skill", np.nan)
    pval = model.pvalues.get("has_ai_skill", np.nan)
    bse  = model.bse.get("has_ai_skill", np.nan)      # HC3 SE

    ci_analytic = (
        model.conf_int().loc["has_ai_skill"].tolist()
        if "has_ai_skill" in model.conf_int().index
        else [np.nan, np.nan]
    )
    ci_boot = _bootstrap_coef(sub, formula, "has_ai_skill")

    return {
        "ols_premium":     round(float(coef), 0),
        "ols_pvalue":      round(float(pval), 4),
        "ols_se_robust":   round(float(bse),  0),
        "ols_ci_95":       [round(float(ci_analytic[0]), 0),
                            round(float(ci_analytic[1]), 0)],
        "ols_ci_95_boot":  ci_boot,
        "ols_r2":          round(float(model.rsquared), 4),
        "ols_n":           int(len(sub)),
        "ols_significant": bool(pval < 0.05),
    }


# ══════════════════════════════════════════════════════════════════════
# PSM 1:1 nearest-neighbour + balance table
# ══════════════════════════════════════════════════════════════════════

def _balance_table(
    treat_pre:  pd.DataFrame,
    ctrl_pre:   pd.DataFrame,
    treat_post: pd.DataFrame,
    ctrl_post:  pd.DataFrame,
    covariates: list[str],
) -> list[dict]:
    """
    Covariate balance table comparing treatment vs control
    before and after matching.

    Returns a list of dicts — one row per covariate:
      covariate, treat_mean, ctrl_mean_before, smd_before,
      ctrl_mean_after, smd_after, balanced
    """
    rows: list[dict] = []
    for col in covariates:
        t  = treat_pre[col].values
        c0 = ctrl_pre[col].values
        c1 = ctrl_post[col].values

        smd_before = round(_smd(t, c0), 3)
        smd_after  = round(_smd(treat_post[col].values, c1), 3)

        rows.append({
            "covariate":        col,
            "treat_mean":       round(float(t.mean()),  3),
            "ctrl_mean_before": round(float(c0.mean()), 3),
            "smd_before":       smd_before,
            "ctrl_mean_after":  round(float(c1.mean()), 3),
            "smd_after":        smd_after,
            "balanced":         smd_after < _SMD_THRESHOLD,
        })

    n_balanced = sum(r["balanced"] for r in rows)
    logger.info(
        "PSM balance: %d/%d covariates balanced (|SMD| < %.2f) after matching",
        n_balanced, len(rows), _SMD_THRESHOLD,
    )
    return rows


def _psm_premium(df: pd.DataFrame) -> dict:
    """
    PSM 1:1 nearest-neighbour with caliper + bootstrap CI on ATT.
    Returns ATT, bootstrap CI, matched-sample size, and balance table.
    """
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        from sklearn.neighbors import NearestNeighbors
    except ImportError:
        logger.warning("scikit-learn not installed — skipping PSM. pip install scikit-learn")
        return {}

    sub = _coerce_treatment(df)
    if sub["has_ai_skill"].nunique() < 2:
        return {}

    sub, _ = _fill_covariates(sub)

    X = sub[_COVARIATES].values
    y = sub["has_ai_skill"].values

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    lr = LogisticRegression(max_iter=500, random_state=_BOOT_SEED)
    lr.fit(X_scaled, y)
    ps = lr.predict_proba(X_scaled)[:, 1]
    sub = sub.copy()
    sub["propensity"] = ps

    treat_all = sub[sub["has_ai_skill"] == 1]
    ctrl_all  = sub[sub["has_ai_skill"] == 0]

    if len(treat_all) < 3 or len(ctrl_all) < 3:
        return {}

    nn = NearestNeighbors(n_neighbors=1)
    nn.fit(ctrl_all[["propensity"]].values)
    distances, indices = nn.kneighbors(treat_all[["propensity"]].values)

    caliper = 0.2 * ps.std()
    mask    = distances[:, 0] < caliper

    matched_treat = treat_all[mask]
    matched_ctrl  = ctrl_all.iloc[indices[mask, 0]]

    if len(matched_treat) < 3:
        return {}

    att_vals   = matched_treat["salary_mid"].values - matched_ctrl["salary_mid"].values
    ci_boot    = _bootstrap_att(
        matched_treat["salary_mid"].values,
        matched_ctrl["salary_mid"].values,
    )
    balance    = _balance_table(
        treat_pre  = treat_all,
        ctrl_pre   = ctrl_all,
        treat_post = matched_treat,
        ctrl_post  = matched_ctrl,
        covariates = _COVARIATES,
    )

    return {
        "psm_premium":       round(float(att_vals.mean()), 0),
        "psm_std":           round(float(att_vals.std()),  0),
        "psm_ci_95_boot":    ci_boot,
        "psm_n_matched":     int(len(matched_treat)),
        "psm_balance_table": balance,
    }


# ══════════════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════════════

def estimate_premium(
    df: pd.DataFrame,
    job_group: Optional[str] = None,
    use_psm: bool = False,
) -> dict:
    """
    Estimate AI skill salary premium for one job group.

    Parameters
    ----------
    df        : normalised + skill_extractor output DataFrame
    job_group : value of job_canonical to filter on; None = all rows
    use_psm   : also run PSM (requires scikit-learn)

    Returns
    -------
    dict with keys documented at the top of this module.
    """
    sub = df[df["job_canonical"] == job_group].copy() if job_group else df.copy()
    sub["salary_mid"] = pd.to_numeric(sub["salary_mid"], errors="coerce")
    sub = sub.dropna(subset=["salary_mid"])

    result: dict = {"job_group": job_group or "全部岗位", "n_total": len(sub)}

    if len(sub) < MIN_GROUP_SIZE:
        result["error"] = f"Insufficient sample ({len(sub)} < {MIN_GROUP_SIZE})"
        return result

    if "has_ai_skill" not in sub.columns:
        result["error"] = "Missing has_ai_skill — run skill_extractor.enrich_dataframe() first"
        return result

    ai_mask = sub["has_ai_skill"].map(
        lambda v: str(v).strip().lower() in ("true", "1")
    )
    result["n_ai"]    = int(ai_mask.sum())
    result["n_no_ai"] = int((~ai_mask).sum())

    result["mean_salary_ai"]    = round(sub[ai_mask]["salary_mid"].mean(),  0) if result["n_ai"]    > 0 else None
    result["mean_salary_no_ai"] = round(sub[~ai_mask]["salary_mid"].mean(), 0) if result["n_no_ai"] > 0 else None

    if result["mean_salary_ai"] and result["mean_salary_no_ai"]:
        raw = result["mean_salary_ai"] - result["mean_salary_no_ai"]
        result["raw_premium"]     = round(raw, 0)
        result["raw_premium_pct"] = round(raw / result["mean_salary_no_ai"] * 100, 1)
    else:
        result["raw_premium"]     = None
        result["raw_premium_pct"] = None

    result.update(_ols_premium(sub))
    if use_psm:
        result.update(_psm_premium(sub))

    return result


def estimate_all_groups(
    df: pd.DataFrame,
    min_ai_samples: int = MIN_GROUP_SIZE,
    use_psm: bool = False,
) -> pd.DataFrame:
    """
    Batch-estimate premiums across all job_canonical groups.
    Drops groups where either AI or non-AI samples fall below min_ai_samples.
    """
    groups = df["job_canonical"].dropna().unique()
    records: list[dict] = []

    for g in sorted(groups):
        sub = df[df["job_canonical"] == g]
        ai_flag = sub["has_ai_skill"].map(
            lambda v: str(v).strip().lower() in ("true", "1")
        )
        if ai_flag.sum() < min_ai_samples or (~ai_flag).sum() < min_ai_samples:
            continue
        r = estimate_premium(df, job_group=g, use_psm=use_psm)
        if "error" not in r:
            records.append(r)

    if not records:
        return pd.DataFrame()

    result_df = pd.DataFrame(records)
    sort_col  = "ols_premium" if "ols_premium" in result_df.columns else "raw_premium"
    return (
        result_df
        .sort_values(sort_col, ascending=False, na_position="last")
        .reset_index(drop=True)
    )


# ══════════════════════════════════════════════════════════════════════
# Balance table pretty-printer (for console / markdown reports)
# ══════════════════════════════════════════════════════════════════════

def format_balance_table(balance: list[dict]) -> str:
    """
    Render the balance table as a compact markdown string.

    Example output:
    | Covariate     | Treat μ | Ctrl μ (pre) | SMD pre | Ctrl μ (post) | SMD post | Balanced |
    |---------------|---------|--------------|---------|---------------|----------|----------|
    | city_tier     |    1.20 |         1.80 |   0.450 |          1.25 |    0.038 | ✓        |
    """
    if not balance:
        return "(no balance table)"

    header = (
        "| Covariate      | Treat μ | Ctrl μ (pre) | SMD pre "
        "| Ctrl μ (post) | SMD post | Balanced |\n"
        "|----------------|---------|--------------|---------|"
        "---------------|----------|----------|\n"
    )
    rows = []
    for r in balance:
        tick = "✓" if r["balanced"] else "✗"
        rows.append(
            f"| {r['covariate']:<14} "
            f"| {r['treat_mean']:>7.3f} "
            f"| {r['ctrl_mean_before']:>12.3f} "
            f"| {r['smd_before']:>7.3f} "
            f"| {r['ctrl_mean_after']:>13.3f} "
            f"| {r['smd_after']:>8.3f} "
            f"| {tick:<8} |"
        )
    return header + "\n".join(rows)
