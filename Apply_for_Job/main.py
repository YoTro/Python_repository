#!/usr/bin/env python3
"""
main.py - 职位采集 + AI 技能溢价分析主流程

用法:
    # 爬取 + 分析（默认）
    python3 main.py 51job   "amazon运营" 深圳 3
    python3 main.py zhipin  "amazon运营" 深圳 5
    python3 main.py both    "amazon运营" 深圳 3   # 同时抓两个平台

    # 仅分析已有 CSV（不重新爬取）
    python3 main.py analyze --51job data/raw/51job_jobs.csv --zhipin data/raw/zhipin_jobs.csv

    # 其他开关
    --no-analyze      只爬取，不分析
    --no-plot         不生成图表
    --psm             分析时使用倾向得分匹配（需 scikit-learn）
    --proxy-url URL   指定代理地址
"""
import os
import sys
import time
import random
import requests
from pathlib import Path

from src.job51 import api_scraper, drission_scraper
from src.zhipin import scraper as zhipin_scraper

# ── 目录 ──────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent
RAW_DIR  = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"
RPT_DIR  = ROOT / "data" / "reports"

ZHIPIN_CITY_MAP = {
    "深圳": "101280600",
    "广州": "101280100",
    "北京": "101010100",
    "上海": "101020100",
    "杭州": "101210100",
    "成都": "101270100",
    "武汉": "101200100",
}


# ══════════════════════════════════════════════════════════════════════
# 采集层（保持原有逻辑，仅调整输出路径到 data/raw/）
# ══════════════════════════════════════════════════════════════════════

def _resolve_proxy(proxy_url):
    if not proxy_url:
        return None, None
    from src.utils.proxy import proxies as get_proxies
    raw = get_proxies(None if proxy_url is True else proxy_url)
    http  = random.choice(raw['http'])  if isinstance(raw['http'],  list) else raw['http']
    https = random.choice(raw['https']) if isinstance(raw['https'], list) else raw['https']
    http  = http  if http.startswith('http')  else f"http://{http}"
    https = https if https.startswith('http') else f"http://{https}"
    return {"http": http, "https": https}, http


def run_51job(keyword: str, city: str, pages: int, proxy_url=None) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "51job_jobs.csv"

    if output_csv.exists():
        output_csv.unlink()
        print(f"[51job] 已清空旧数据: {output_csv}")

    session = requests.Session()
    proxies_dict, _ = _resolve_proxy(proxy_url)
    if proxies_dict:
        session.proxies.update(proxies_dict)
        print(f"[51job] 启用代理: {proxies_dict}")

    nc_params = None
    for page in range(1, pages + 1):
        print(f"\n{'='*20} 51job 第 {page}/{pages} 页 {'='*20}")
        api_success, nc_params = api_scraper.run(
            keyword=keyword,
            city_code=drission_scraper.get_city_code(city),
            page_num=page,
            output_csv_path=str(output_csv),
            session=session,
            nc_params=nc_params,
        )
        if not api_success:
            print("[51job] API 失败，启动 DrissionPage 备用...")
            try:
                dp_proxy = proxies_dict["http"] if proxies_dict else None
                drission_scraper.run_single_page(
                    keyword=keyword, city=city, page_num=page,
                    output_csv_path=str(output_csv), proxy_url=dp_proxy,
                )
            except Exception as e:
                print(f"[51job] DrissionPage 第 {page} 页失败: {e}，终止。")
                break
        if page < pages:
            t = random.uniform(2, 4)
            print(f"[51job] 休眠 {t:.1f}s...")
            time.sleep(t)

    print(f"[51job] 完成，CSV → {output_csv}")
    return output_csv


def run_zhipin(keyword: str, city: str, pages: int, proxy_url=None) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "zhipin_jobs.csv"

    if output_csv.exists():
        output_csv.unlink()
        print(f"[zhipin] 已清空旧数据: {output_csv}")

    city_code = ZHIPIN_CITY_MAP.get(city)
    if not city_code:
        print(f"[ERROR] 不支持的城市 '{city}'，支持: {list(ZHIPIN_CITY_MAP.keys())}")
        sys.exit(1)

    _, dp_proxy = _resolve_proxy(proxy_url)
    print("[zhipin] 请确保 Chrome 已在 9222 端口启动调试模式")
    zhipin_scraper.scrape_zhipin(
        query=keyword,
        city_code=city_code,
        output_filename=str(output_csv),
        max_pages=pages,
        proxy_url=dp_proxy,
    )
    print(f"[zhipin] 完成，CSV → {output_csv}")
    return output_csv


# ══════════════════════════════════════════════════════════════════════
# 分析层
# ══════════════════════════════════════════════════════════════════════

def run_analysis(path_51job: Path | None,
                 path_zhipin: Path | None,
                 keyword: str,
                 use_psm: bool = False,
                 plot: bool = True) -> None:
    """标准化 → 技能提取 → 溢价估算 → 趋势快照 → 报告"""
    from src.analysis.normalizer      import load_and_normalize
    from src.analysis.skill_extractor import enrich_dataframe
    from src.analysis.premium_estimator import estimate_all_groups
    from src.analysis.trend_tracker   import build_snapshot, save_snapshot
    from src.analysis.report          import generate_report

    # 1. 标准化合并
    p51 = str(path_51job)   if path_51job   and path_51job.exists()   else None
    pzp = str(path_zhipin)  if path_zhipin  and path_zhipin.exists()  else None
    if not p51 and not pzp:
        print("[分析] 没有可用数据文件，跳过分析。")
        return

    print(f"\n[分析] 正在标准化数据...")
    df = load_and_normalize(path_51job=p51, path_zhipin=pzp,
                            search_keyword=keyword)
    print(f"[分析] 合并后共 {len(df)} 条记录")

    # 2. 技能提取
    print("[分析] 正在提取 AI 技能信号...")
    df = enrich_dataframe(df)
    ai_cnt = df["has_ai_skill"].sum()
    print(f"[分析] 含 AI 技能要求: {ai_cnt} 条 ({ai_cnt/len(df):.1%})")

    # 3. 保存标准化数据
    PROC_DIR.mkdir(parents=True, exist_ok=True)
    processed_path = PROC_DIR / f"processed_{keyword.replace(' ', '_')}.csv"
    df.to_csv(processed_path, index=False, encoding="utf-8-sig")
    print(f"[分析] 标准化数据 → {processed_path}")

    # 4. 溢价估算
    print(f"[分析] 正在估算 AI 技能薪酬溢价 (PSM={'开启' if use_psm else '关闭'})...")
    premium_df = estimate_all_groups(df, use_psm=use_psm)

    # 5. 趋势快照
    snapshot = build_snapshot(df, keyword=keyword)
    save_snapshot(snapshot)

    # 6. 生成报告
    print("[分析] 正在生成报告...")
    md_path = generate_report(
        df=df,
        premium_df=premium_df,
        snapshot=snapshot,
        keyword=keyword,
        output_dir=RPT_DIR,
        plot=plot,
    )
    print(f"\n[分析] 报告已生成: {md_path}")


# ══════════════════════════════════════════════════════════════════════
# CLI 入口
# ══════════════════════════════════════════════════════════════════════

def _flag(name: str) -> bool:
    return name in sys.argv

def _flag_val(name: str) -> str | None:
    if name in sys.argv:
        idx = sys.argv.index(name)
        if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith("--"):
            return sys.argv[idx + 1]
    return None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]

    # ── 仅分析模式 ─────────────────────────────────────────────────────
    if args and args[0].lower() == "analyze":
        p51  = Path(_flag_val("--51job"))  if _flag_val("--51job")  else None
        pzp  = Path(_flag_val("--zhipin")) if _flag_val("--zhipin") else None
        kw   = _flag_val("--keyword") or "综合"
        run_analysis(p51, pzp, keyword=kw,
                     use_psm=_flag("--psm"),
                     plot=not _flag("--no-plot"))
        return

    # ── 爬取模式 ───────────────────────────────────────────────────────
    if len(args) < 4:
        print(__doc__)
        sys.exit(1)

    source  = args[0].lower()
    keyword = args[1]
    city    = args[2]
    pages   = int(args[3])

    proxy_url = _flag_val("--proxy-url") or (_flag("--proxy-url") or None)
    no_analyze = _flag("--no-analyze")
    use_psm    = _flag("--psm")
    no_plot    = _flag("--no-plot")

    print(f"[MAIN] source={source}  keyword={keyword}  city={city}  pages={pages}")

    path_51job = path_zhipin = None

    if source in ("51job", "both"):
        path_51job = run_51job(keyword, city, pages, proxy_url)

    if source in ("zhipin", "both"):
        path_zhipin = run_zhipin(keyword, city, pages, proxy_url)

    if source not in ("51job", "zhipin", "both"):
        print(f"[ERROR] 不支持的数据源 '{source}'，请选择 51job / zhipin / both")
        sys.exit(1)

    if not no_analyze:
        run_analysis(
            path_51job=path_51job,
            path_zhipin=path_zhipin,
            keyword=keyword,
            use_psm=use_psm,
            plot=not no_plot,
        )


if __name__ == "__main__":
    main()
