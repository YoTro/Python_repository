"""
report.py - 分析报告生成器

输出三种格式：
  1. 控制台文本摘要（print）
  2. Markdown 文件（data/reports/）
  3. 可视化图表 PNG（需 matplotlib，可选）

入口函数：
  generate_report(df, keyword, output_dir, plot)
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


def _fmt_salary(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"¥{int(val):,}/月"


def _fmt_pct(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    return f"{val:+.1f}%"


def _fmt_pval(val) -> str:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "N/A"
    if val < 0.001:
        return "p<0.001 ***"
    if val < 0.01:
        return f"p={val:.3f} **"
    if val < 0.05:
        return f"p={val:.3f} *"
    return f"p={val:.3f} (ns)"


def _section(title: str, width: int = 60) -> str:
    return f"\n{'─' * width}\n{title}\n{'─' * width}"


# ── 控制台摘要 ──────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame,
                  premium_df: pd.DataFrame,
                  snapshot: dict,
                  keyword: str = "") -> None:
    print(_section(f"招聘市场 AI 技能溢价分析报告  [{keyword}]"))
    print(f"生成时间 : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"有效样本 : {snapshot['total_jobs']} 条  "
          f"(含 AI 要求: {snapshot['ai_jobs']} 条 / "
          f"占比: {snapshot['ai_ratio']:.1%})")

    if "salary_mid" in df.columns:
        sal = df["salary_mid"].dropna()
        if len(sal) > 0:
            print(f"整体薪资 : P25={_fmt_salary(np.percentile(sal, 25))}  "
                  f"P50={_fmt_salary(np.percentile(sal, 50))}  "
                  f"P75={_fmt_salary(np.percentile(sal, 75))}")

    print(_section("AI 技能薪酬溢价（按岗位）"))
    if premium_df.empty:
        print("  样本量不足，无法估算。")
        return

    for _, row in premium_df.iterrows():
        job   = row.get("job_group", "")
        raw_p = _fmt_salary(row.get("raw_premium"))
        raw_r = _fmt_pct(row.get("raw_premium_pct"))
        ols_p = _fmt_salary(row.get("ols_premium"))
        pval  = _fmt_pval(row.get("ols_pvalue"))
        n_ai  = int(row.get("n_ai", 0))
        n_no  = int(row.get("n_no_ai", 0))

        print(f"\n  [{job}]  AI样本={n_ai} / 非AI样本={n_no}")
        print(f"    无 AI 平均薪资 : {_fmt_salary(row.get('mean_salary_no_ai'))}")
        print(f"    有 AI 平均薪资 : {_fmt_salary(row.get('mean_salary_ai'))}")
        print(f"    原始溢价       : {raw_p} ({raw_r})")
        print(f"    OLS净溢价      : {ols_p}  {pval}")

    print(_section("高频 AI 技能词 Top-15"))
    freq = snapshot.get("skill_freq", {})
    for i, (skill, cnt) in enumerate(list(freq.items())[:15], 1):
        bar = "█" * min(cnt, 30)
        print(f"  {i:2d}. {skill:<20s} {cnt:4d}  {bar}")

    print()


# ── Markdown 报告 ───────────────────────────────────────────────────

def _md_table(headers: list, rows: list) -> str:
    lines = ["| " + " | ".join(headers) + " |",
             "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines)


def save_markdown(df: pd.DataFrame,
                  premium_df: pd.DataFrame,
                  snapshot: dict,
                  keyword: str = "",
                  output_dir: Path = Path("data/reports")) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    path = output_dir / f"report_{keyword or 'all'}_{ts}.md"

    lines = [
        f"# 招聘市场 AI 技能溢价分析报告",
        f"",
        f"- **关键词**: {keyword or '综合'}",
        f"- **生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"- **有效样本**: {snapshot['total_jobs']} 条",
        f"- **含 AI 要求**: {snapshot['ai_jobs']} 条（占比 {snapshot['ai_ratio']:.1%}）",
        f"",
        f"---",
        f"",
        f"## 一、AI 技能薪酬溢价（按岗位）",
        f"",
    ]

    if not premium_df.empty:
        headers = ["岗位", "AI样本", "非AI样本",
                   "无AI均薪", "有AI均薪", "原始溢价", "原始溢价%",
                   "OLS净溢价", "显著性"]
        rows = []
        for _, r in premium_df.iterrows():
            sig = "✅" if r.get("ols_significant") else "❌"
            rows.append([
                r.get("job_group", ""),
                int(r.get("n_ai", 0)),
                int(r.get("n_no_ai", 0)),
                _fmt_salary(r.get("mean_salary_no_ai")),
                _fmt_salary(r.get("mean_salary_ai")),
                _fmt_salary(r.get("raw_premium")),
                _fmt_pct(r.get("raw_premium_pct")),
                _fmt_salary(r.get("ols_premium")),
                f"{sig} {_fmt_pval(r.get('ols_pvalue'))}",
            ])
        lines.append(_md_table(headers, rows))
    else:
        lines.append("> 样本量不足，无法估算。")

    lines += [
        f"",
        f"> **OLS净溢价** = 控制城市/经验/公司规模后，有 AI 要求 JD 相比无 AI 要求的月薪差值。",
        f"> ✅ p<0.05 表示统计显著。",
        f"",
        f"---",
        f"",
        f"## 二、AI 技能 Tier 分布",
        f"",
    ]

    tier_desc = {
        0: "无 AI 要求",
        1: "通用 AI 工具（ChatGPT等）",
        2: "数据/自动化能力（Python/SQL等）",
        3: "核心 AI 技能（大模型/RAG/微调等）",
    }
    tier_dist = snapshot.get("tier_dist", {})
    tier_rows = []
    for tier in [3, 2, 1, 0]:
        cnt = tier_dist.get(tier, tier_dist.get(str(tier), 0))
        pct = cnt / snapshot["total_jobs"] * 100 if snapshot["total_jobs"] > 0 else 0
        tier_rows.append([f"Tier {tier}", tier_desc.get(tier, ""), cnt, f"{pct:.1f}%"])
    lines.append(_md_table(["Tier", "描述", "职位数", "占比"], tier_rows))

    lines += [
        f"",
        f"---",
        f"",
        f"## 三、高频 AI 技能词 Top-20",
        f"",
    ]
    freq = snapshot.get("skill_freq", {})
    freq_rows = [[i + 1, skill, cnt] for i, (skill, cnt) in enumerate(list(freq.items())[:20])]
    lines.append(_md_table(["排名", "技能词", "出现次数"], freq_rows))

    lines += [
        f"",
        f"---",
        f"",
        f"## 四、各岗位薪资分位数",
        f"",
    ]
    grp_stats = snapshot.get("group_stats", [])
    if grp_stats:
        sal_rows = []
        for g in sorted(grp_stats, key=lambda x: x["n_total"], reverse=True)[:15]:
            s = g.get("salary_all", {})
            sa = g.get("salary_ai", {})
            sn = g.get("salary_no_ai", {})
            sal_rows.append([
                g["job"], g["n_total"],
                f"{g['ai_ratio']:.0%}",
                _fmt_salary(s.get("p50")),
                _fmt_salary(sa.get("p50")),
                _fmt_salary(sn.get("p50")),
            ])
        lines.append(_md_table(
            ["岗位", "样本数", "AI占比", "整体P50", "有AI的P50", "无AI的P50"],
            sal_rows
        ))

    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"Markdown 报告已保存: {path}")
    return path


# ── 可视化（可选）──────────────────────────────────────────────────

def save_charts(df: pd.DataFrame,
                premium_df: pd.DataFrame,
                snapshot: dict,
                keyword: str = "",
                output_dir: Path = Path("data/reports")) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.font_manager as fm
    except ImportError:
        logger.warning("matplotlib 未安装，跳过图表生成。pip install matplotlib")
        return

    # 尝试使用系统中文字体
    zh_fonts = [f.name for f in fm.fontManager.ttflist
                if any(k in f.name for k in ["PingFang", "Heiti", "SimHei", "WenQuanYi", "Noto"])]
    if zh_fonts:
        plt.rcParams["font.family"] = zh_fonts[0]
    plt.rcParams["axes.unicode_minus"] = False

    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    # 图1：各岗位 AI 溢价对比
    if not premium_df.empty and "ols_premium" in premium_df.columns:
        sub = premium_df.dropna(subset=["ols_premium"]).head(12)
        fig, ax = plt.subplots(figsize=(10, 5))
        colors = ["#2ecc71" if s else "#e74c3c" for s in sub.get("ols_significant", [False] * len(sub))]
        bars = ax.barh(sub["job_group"], sub["ols_premium"], color=colors)
        ax.set_xlabel("OLS 月薪净溢价（元）")
        ax.set_title(f"AI 技能薪酬溢价（{keyword}）  绿=显著 红=不显著")
        ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
        for bar, val in zip(bars, sub["ols_premium"]):
            ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height() / 2,
                    f"¥{int(val):,}", va="center", fontsize=8)
        fig.tight_layout()
        p = output_dir / f"premium_{keyword}_{ts}.png"
        fig.savefig(p, dpi=150)
        plt.close(fig)
        logger.info(f"溢价图表: {p}")

    # 图2：薪资分布箱线图（有AI vs 无AI）
    if "has_ai_skill" in df.columns and "salary_mid" in df.columns:
        sub = df.dropna(subset=["salary_mid"])
        ai_sal  = sub[sub["has_ai_skill"] == True]["salary_mid"]
        nai_sal = sub[sub["has_ai_skill"] == False]["salary_mid"]
        fig, ax = plt.subplots(figsize=(6, 5))
        ax.boxplot([nai_sal.values, ai_sal.values],
                   labels=["无 AI 要求", "有 AI 要求"],
                   patch_artist=True,
                   boxprops=dict(facecolor="#aed6f1"),
                   medianprops=dict(color="red", linewidth=2))
        ax.set_ylabel("月薪（元）")
        ax.set_title(f"薪资分布对比（{keyword}）")
        fig.tight_layout()
        p = output_dir / f"salary_dist_{keyword}_{ts}.png"
        fig.savefig(p, dpi=150)
        plt.close(fig)
        logger.info(f"薪资分布图表: {p}")

    # 图3：高频 AI 技能词横向条形图
    freq = snapshot.get("skill_freq", {})
    if freq:
        top = list(freq.items())[:15]
        skills, counts = zip(*top)
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.barh(list(reversed(skills)), list(reversed(counts)), color="#8e44ad")
        ax.set_xlabel("出现次数")
        ax.set_title(f"高频 AI 技能词 Top-15（{keyword}）")
        fig.tight_layout()
        p = output_dir / f"skill_freq_{keyword}_{ts}.png"
        fig.savefig(p, dpi=150)
        plt.close(fig)
        logger.info(f"技能词图表: {p}")


# ── 统一入口 ────────────────────────────────────────────────────────

def generate_report(df: pd.DataFrame,
                    premium_df: pd.DataFrame,
                    snapshot: dict,
                    keyword: str = "",
                    output_dir: Optional[Path] = None,
                    plot: bool = True) -> Path:
    """
    一次性生成控制台输出 + Markdown 报告 + 图表（可选）。

    Returns
    -------
    Path  Markdown 报告路径
    """
    out = output_dir or Path("data/reports")
    print_summary(df, premium_df, snapshot, keyword)
    md_path = save_markdown(df, premium_df, snapshot, keyword, out)
    if plot:
        save_charts(df, premium_df, snapshot, keyword, out)
    return md_path
