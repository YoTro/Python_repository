"""
premium_estimator.py - AI 技能薪酬溢价估算

方法：
  1. 简单均值对比（快速）
  2. OLS 回归（控制混淆变量：城市tier / 经验年限 / 公司规模）
  3. 倾向得分匹配 PSM（可选，需 scikit-learn）

输入：标准化后的 DataFrame（来自 normalizer + skill_extractor）
输出：按 job_canonical 分组的溢价报告 dict
"""
from __future__ import annotations
import logging
import warnings
import pandas as pd
import numpy as np
from typing import Optional
from typing import Optional

logger = logging.getLogger(__name__)


# ── 最小样本量：组内少于此值跳过该岗位 ──────────────────────────────
MIN_GROUP_SIZE = 8


def _ols_premium(df: pd.DataFrame) -> dict:
    """
    OLS: salary_mid ~ has_ai_skill + city_tier + exp_years + company_size_n
    返回 has_ai_skill 的系数（月薪净溢价）及统计量。
    """
    try:
        import statsmodels.formula.api as smf
    except ImportError:
        logger.warning("statsmodels 未安装，跳过 OLS 回归。pip install statsmodels")
        return {}

    # 只保留有薪资 + AI标签的行
    sub = df.dropna(subset=["salary_mid", "has_ai_skill"]).copy()
    sub["salary_mid"] = pd.to_numeric(sub["salary_mid"], errors="coerce")
    sub = sub.dropna(subset=["salary_mid"])
    # 兼容从 CSV 读回的字符串 True/False
    sub["has_ai_skill"] = sub["has_ai_skill"].map(
        lambda v: True if str(v).strip().lower() in ("true", "1") else False
    ).astype(int)

    # 至少需要两组都有样本
    if sub["has_ai_skill"].nunique() < 2 or len(sub) < MIN_GROUP_SIZE * 2:
        return {}

    # 控制变量：有缺失则填中位数；全空列填0并从公式剔除
    formula_controls = []
    for col in ["city_tier", "exp_years", "company_size_n"]:
        if col not in sub.columns:
            sub[col] = 0
        med = sub[col].median()
        sub[col] = sub[col].fillna(0 if pd.isna(med) else med)
        if sub[col].nunique() > 1:
            formula_controls.append(col)

    formula = "salary_mid ~ has_ai_skill" + (
        " + " + " + ".join(formula_controls) if formula_controls else ""
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            model = smf.ols(formula, data=sub).fit()
        except Exception as e:
            logger.warning(f"OLS 拟合失败: {e}")
            return {}

    coef = model.params.get("has_ai_skill", np.nan)
    pval = model.pvalues.get("has_ai_skill", np.nan)
    ci   = model.conf_int().loc["has_ai_skill"].tolist() if "has_ai_skill" in model.conf_int().index else [np.nan, np.nan]

    return {
        "ols_premium":    round(float(coef), 0),
        "ols_pvalue":     round(float(pval), 4),
        "ols_ci_95":      [round(float(ci[0]), 0), round(float(ci[1]), 0)],
        "ols_r2":         round(float(model.rsquared), 4),
        "ols_n":          int(len(sub)),
        "ols_significant": bool(pval < 0.05),
    }


def _psm_premium(df: pd.DataFrame) -> dict:
    """
    倾向得分匹配（1:1 最近邻）。
    控制混淆后的平均处理效应 ATT。
    """
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        from sklearn.neighbors import NearestNeighbors
    except ImportError:
        logger.warning("scikit-learn 未安装，跳过 PSM。pip install scikit-learn")
        return {}

    sub = df.dropna(subset=["salary_mid", "has_ai_skill"]).copy()
    sub["salary_mid"] = pd.to_numeric(sub["salary_mid"], errors="coerce")
    sub = sub.dropna(subset=["salary_mid"])
    sub["has_ai_skill"] = sub["has_ai_skill"].map(
        lambda v: True if str(v).strip().lower() in ("true", "1") else False
    ).astype(int)

    if sub["has_ai_skill"].nunique() < 2:
        return {}

    for col in ["city_tier", "exp_years", "company_size_n"]:
        if col not in sub.columns:
            sub[col] = 0
        sub[col] = sub[col].fillna(sub[col].median())

    X = sub[["city_tier", "exp_years", "company_size_n"]].values
    y = sub["has_ai_skill"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    lr = LogisticRegression(max_iter=500)
    lr.fit(X_scaled, y)
    ps = lr.predict_proba(X_scaled)[:, 1]
    sub = sub.copy()
    sub["propensity"] = ps

    treat = sub[sub["has_ai_skill"] == 1]
    ctrl  = sub[sub["has_ai_skill"] == 0]

    if len(treat) < 3 or len(ctrl) < 3:
        return {}

    nn = NearestNeighbors(n_neighbors=1)
    nn.fit(ctrl[["propensity"]].values)
    distances, indices = nn.kneighbors(treat[["propensity"]].values)

    # 卡距离阈值（防止强制匹配）
    caliper = 0.2 * ps.std()
    matched_treat = treat[distances[:, 0] < caliper]
    matched_ctrl  = ctrl.iloc[indices[distances[:, 0] < caliper, 0]]

    if len(matched_treat) < 3:
        return {}

    att = matched_treat["salary_mid"].values - matched_ctrl["salary_mid"].values
    return {
        "psm_premium":   round(float(att.mean()), 0),
        "psm_std":       round(float(att.std()), 0),
        "psm_n_matched": int(len(matched_treat)),
    }


def estimate_premium(df: pd.DataFrame,
                     job_group: Optional[str] = None,
                     use_psm: bool = False) -> dict:
    """
    对单个岗位组估算 AI 技能薪酬溢价。

    Parameters
    ----------
    df         : 标准化 + skill_extractor 处理后的 DataFrame
    job_group  : job_canonical 值；None 表示不过滤（全部数据）
    use_psm    : 是否同时运行 PSM

    Returns
    -------
    {
        "job_group":        str,
        "n_total":          int,
        "n_ai":             int,
        "n_no_ai":          int,
        "mean_salary_ai":   float,
        "mean_salary_no_ai":float,
        "raw_premium":      float,
        "raw_premium_pct":  float,
        "ols_premium":      float,   # 控制混淆后净溢价（月薪元）
        "ols_pvalue":       float,
        "ols_significant":  bool,
        ...psm fields if use_psm...
    }
    """
    if job_group:
        sub = df[df["job_canonical"] == job_group].copy()
    else:
        sub = df.copy()

    sub["salary_mid"] = pd.to_numeric(sub["salary_mid"], errors="coerce")
    sub = sub.dropna(subset=["salary_mid"])

    result: dict = {"job_group": job_group or "全部岗位", "n_total": len(sub)}

    if len(sub) < MIN_GROUP_SIZE:
        result["error"] = f"样本量不足（{len(sub)} < {MIN_GROUP_SIZE}）"
        return result

    if "has_ai_skill" not in sub.columns:
        result["error"] = "缺少 has_ai_skill 列，请先运行 skill_extractor.enrich_dataframe()"
        return result

    ai_mask  = sub["has_ai_skill"] == True
    result["n_ai"]             = int(ai_mask.sum())
    result["n_no_ai"]          = int((~ai_mask).sum())
    result["mean_salary_ai"]   = round(sub[ai_mask]["salary_mid"].mean(), 0)   if result["n_ai"]    > 0 else None
    result["mean_salary_no_ai"]= round(sub[~ai_mask]["salary_mid"].mean(), 0)  if result["n_no_ai"] > 0 else None

    if result["mean_salary_ai"] and result["mean_salary_no_ai"]:
        raw = result["mean_salary_ai"] - result["mean_salary_no_ai"]
        result["raw_premium"]     = round(raw, 0)
        result["raw_premium_pct"] = round(raw / result["mean_salary_no_ai"] * 100, 1)
    else:
        result["raw_premium"] = None
        result["raw_premium_pct"] = None

    # OLS
    result.update(_ols_premium(sub))

    # PSM（可选）
    if use_psm:
        result.update(_psm_premium(sub))

    return result


def estimate_all_groups(df: pd.DataFrame,
                        min_ai_samples: int = MIN_GROUP_SIZE,
                        use_psm: bool = False) -> pd.DataFrame:
    """
    对所有 job_canonical 分组批量估算，返回汇总 DataFrame。
    过滤掉 AI 样本或非 AI 样本不足的组。
    """
    groups = df["job_canonical"].dropna().unique()
    records = []
    for g in sorted(groups):
        sub = df[df["job_canonical"] == g]
        if sub["has_ai_skill"].sum() < min_ai_samples:
            continue
        if (~sub["has_ai_skill"]).sum() < min_ai_samples:
            continue
        r = estimate_premium(df, job_group=g, use_psm=use_psm)
        if "error" not in r:
            records.append(r)

    if not records:
        return pd.DataFrame()

    result_df = pd.DataFrame(records)
    # 按 OLS溢价（降序）排列，无OLS则用 raw_premium
    sort_col = "ols_premium" if "ols_premium" in result_df.columns else "raw_premium"
    return result_df.sort_values(sort_col, ascending=False, na_position="last").reset_index(drop=True)
