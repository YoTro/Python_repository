"""
trend_tracker.py - AI 需求趋势分析

对爬取到的招聘数据做时序快照分析：
  1. 按 job_canonical 统计 AI/非AI 岗位数量及占比
  2. 按 ai_skill_tier 分布统计（tier0~tier3 各占比）
  3. 高频 AI 技能词 Top-N 排名
  4. 薪资分位数分布（P25 / P50 / P75）

注意：单次爬取无时间轴，需多次运行后追加到 data/processed/trend_snapshots.csv
      才能看到趋势变化。
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

SNAPSHOT_FILE = Path(__file__).parents[2] / "data" / "processed" / "trend_snapshots.csv"


def _skill_freq(df: pd.DataFrame, top_n: int = 20) -> dict:
    """统计 ai_skills_found 列中各技能词出现频率"""
    from collections import Counter
    counter: Counter = Counter()
    col = "ai_skills_found"
    if col not in df.columns:
        return {}
    for val in df[col].dropna():
        if isinstance(val, list):
            counter.update(val)
        elif isinstance(val, str) and val.startswith("["):
            try:
                counter.update(json.loads(val.replace("'", '"')))
            except Exception:
                pass
    return dict(counter.most_common(top_n))


def _salary_quantiles(series: pd.Series) -> dict:
    s = series.dropna()
    if len(s) < 3:
        return {}
    return {
        "p25": round(float(np.percentile(s, 25))),
        "p50": round(float(np.percentile(s, 50))),
        "p75": round(float(np.percentile(s, 75))),
        "mean": round(float(s.mean())),
        "n": int(len(s)),
    }


def build_snapshot(df: pd.DataFrame,
                   keyword: str = "",
                   snapshot_time: Optional[datetime] = None) -> dict:
    """
    从一份标准化 DataFrame 生成单次快照统计。

    Parameters
    ----------
    df            : normalizer + skill_extractor 处理后的 DataFrame
    keyword       : 本次爬取关键词（用于标记快照）
    snapshot_time : 快照时间，None 则取当前时间
    """
    ts = (snapshot_time or datetime.now()).strftime("%Y-%m-%d %H:%M")

    total = len(df)
    ai_count = int(df["has_ai_skill"].sum()) if "has_ai_skill" in df.columns else 0

    # Tier 分布
    tier_dist = {}
    if "ai_skill_tier" in df.columns:
        tier_dist = df["ai_skill_tier"].value_counts().to_dict()

    # 按岗位分组：AI占比 + 薪资
    group_stats = []
    if "job_canonical" in df.columns:
        for job, grp in df.groupby("job_canonical"):
            ai_n   = int(grp["has_ai_skill"].sum()) if "has_ai_skill" in grp.columns else 0
            total_n = len(grp)
            sal     = _salary_quantiles(grp["salary_mid"]) if "salary_mid" in grp.columns else {}
            sal_ai  = _salary_quantiles(
                grp.loc[grp["has_ai_skill"] == True, "salary_mid"]
            ) if "has_ai_skill" in grp.columns else {}
            sal_nai = _salary_quantiles(
                grp.loc[grp["has_ai_skill"] == False, "salary_mid"]
            ) if "has_ai_skill" in grp.columns else {}
            group_stats.append({
                "job":          job,
                "n_total":      total_n,
                "n_ai":         ai_n,
                "ai_ratio":     round(ai_n / total_n, 4) if total_n > 0 else 0,
                "salary_all":   sal,
                "salary_ai":    sal_ai,
                "salary_no_ai": sal_nai,
            })

    skill_freq = _skill_freq(df)

    return {
        "snapshot_time": ts,
        "keyword":       keyword,
        "total_jobs":    total,
        "ai_jobs":       ai_count,
        "ai_ratio":      round(ai_count / total, 4) if total > 0 else 0,
        "tier_dist":     tier_dist,
        "group_stats":   group_stats,
        "skill_freq":    skill_freq,
    }


def save_snapshot(snapshot: dict) -> None:
    """将快照追加到 CSV（每行一条快照，JSON 序列化嵌套字段）"""
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "snapshot_time": snapshot["snapshot_time"],
        "keyword":       snapshot["keyword"],
        "total_jobs":    snapshot["total_jobs"],
        "ai_jobs":       snapshot["ai_jobs"],
        "ai_ratio":      snapshot["ai_ratio"],
        "tier_dist":     json.dumps(snapshot["tier_dist"],   ensure_ascii=False),
        "group_stats":   json.dumps(snapshot["group_stats"], ensure_ascii=False),
        "skill_freq":    json.dumps(snapshot["skill_freq"],  ensure_ascii=False),
    }
    file_exists = SNAPSHOT_FILE.exists()
    pd.DataFrame([row]).to_csv(
        SNAPSHOT_FILE, mode="a", header=not file_exists, index=False, encoding="utf-8-sig"
    )
    logger.info(f"快照已追加到 {SNAPSHOT_FILE}")


def load_snapshots() -> pd.DataFrame:
    """加载历史快照 CSV"""
    if not SNAPSHOT_FILE.exists():
        return pd.DataFrame()
    return pd.read_csv(SNAPSHOT_FILE, encoding="utf-8-sig")


def summarize_trend(keyword_filter: Optional[str] = None) -> pd.DataFrame:
    """
    读取历史快照，返回按时间排序的趋势摘要 DataFrame。
    可按关键词过滤。
    """
    df = load_snapshots()
    if df.empty:
        return df
    if keyword_filter:
        df = df[df["keyword"] == keyword_filter]
    return df[["snapshot_time", "keyword", "total_jobs", "ai_jobs", "ai_ratio"]].sort_values("snapshot_time")
