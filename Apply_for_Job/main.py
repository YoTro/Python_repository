#!/usr/bin/env python3
"""
main.py - Job scraper + AI skill premium analyser

Sub-commands
────────────
  scrape   Scrape job postings from one or more platforms
  analyze  Run analysis on existing CSVs (no new scraping)
  chat     Run the HR chat bot on a live recruitment platform

Examples
────────
  # Scrape + auto-analyse
  python3 main.py scrape 51job          "amazon运营"        深圳     3
  python3 main.py scrape zhipin         "amazon运营"        深圳     5
  python3 main.py scrape both           "amazon运营"        深圳     3
  python3 main.py scrape ziprecruiter   "amazon operations" Remote   3
  python3 main.py scrape all            "amazon"            Remote   3

  # Scrape only (skip analysis)
  python3 main.py scrape zhipin "前端开发" 北京 3 --no-analyze

  # Analyse existing CSVs
  python3 main.py analyze --zhipin data/raw/zhipin_jobs.csv --keyword "amazon运营"
  python3 main.py analyze --51job data/raw/51job_jobs.csv --zhipin data/raw/zhipin_jobs.csv

  # HR chat bot
  python3 main.py chat zhipin
  python3 main.py chat zhipin --max-turns 4 --max-chats 20 --unread-only
  python3 main.py chat zhipin --reply-timeout 120 --output data/raw/zhipin_chat.csv
"""
import argparse
import logging
import random
import time
from pathlib import Path
from typing import Optional

# ── Logging must be set up before any src imports that use it ─────────
from src.utils.logging_config import setup_logging
setup_logging()

from src.job51 import api_scraper, drission_scraper
from src.zhipin import scraper as zhipin_scraper
from src.ziprecruiter import scraper as zr_scraper
from src.indeed import scraper as indeed_scraper
from src.utils.http import build_session

logger = logging.getLogger(__name__)

# ── Directory constants ───────────────────────────────────────────────
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
# Proxy helper
# ══════════════════════════════════════════════════════════════════════

def _resolve_proxy(proxy_url: Optional[str]):
    if not proxy_url:
        return None, None
    from src.utils.proxy import proxies as get_proxies
    raw   = get_proxies(None if proxy_url is True else proxy_url)
    http  = random.choice(raw["http"])  if isinstance(raw["http"],  list) else raw["http"]
    https = random.choice(raw["https"]) if isinstance(raw["https"], list) else raw["https"]
    http  = http  if http.startswith("http")  else f"http://{http}"
    https = https if https.startswith("http") else f"http://{https}"
    return {"http": http, "https": https}, http


# ══════════════════════════════════════════════════════════════════════
# Scrapers
# ══════════════════════════════════════════════════════════════════════

def run_51job(keyword: str, city: str, pages: int, proxy_url=None) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "51job_jobs.csv"
    if output_csv.exists():
        output_csv.unlink()
        logger.info("[51job] Cleared previous data: %s", output_csv)

    proxies_dict, _ = _resolve_proxy(proxy_url)
    session = build_session(proxies=proxies_dict)
    if proxies_dict:
        logger.info("[51job] Proxy enabled: %s", proxies_dict)

    nc_params = None
    for page in range(1, pages + 1):
        logger.info("[51job] Page %d/%d", page, pages)
        api_success, nc_params = api_scraper.run(
            keyword=keyword,
            city_code=drission_scraper.get_city_code(city),
            page_num=page,
            output_csv_path=str(output_csv),
            session=session,
            nc_params=nc_params,
        )
        if not api_success:
            logger.warning("[51job] API failed — falling back to DrissionPage")
            try:
                dp_proxy = proxies_dict["http"] if proxies_dict else None
                drission_scraper.run_single_page(
                    keyword=keyword, city=city, page_num=page,
                    output_csv_path=str(output_csv), proxy_url=dp_proxy,
                )
            except Exception as e:
                logger.error("[51job] DrissionPage page %d failed: %s", page, e)
                break
        if page < pages:
            t = random.uniform(2, 4)
            logger.debug("[51job] Sleeping %.1fs", t)
            time.sleep(t)

    logger.info("[51job] Done → %s", output_csv)
    return output_csv


def run_zhipin(keyword: str, city: str, pages: int, proxy_url=None) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "zhipin_jobs.csv"
    if output_csv.exists():
        output_csv.unlink()
        logger.info("[zhipin] Cleared previous data: %s", output_csv)

    city_code = ZHIPIN_CITY_MAP.get(city)
    if not city_code:
        raise ValueError(f"Unsupported city '{city}'. Choose from: {list(ZHIPIN_CITY_MAP)}")

    _, dp_proxy = _resolve_proxy(proxy_url)
    logger.info("[zhipin] Chrome must be running with --remote-debugging-port=9222")
    zhipin_scraper.scrape_zhipin(
        query=keyword, city_code=city_code,
        output_filename=str(output_csv), max_pages=pages, proxy_url=dp_proxy,
    )
    logger.info("[zhipin] Done → %s", output_csv)
    return output_csv


def run_ziprecruiter(keyword: str, location: str, pages: int,
                     proxy_url=None, fetch_descriptions: bool = True) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "ziprecruiter_jobs.csv"
    if output_csv.exists():
        output_csv.unlink()
        logger.info("[ZipRecruiter] Cleared previous data: %s", output_csv)

    _, dp_proxy = _resolve_proxy(proxy_url)
    zr_scraper.scrape_ziprecruiter(
        query=keyword, location=location, output_filename=str(output_csv),
        max_pages=pages, proxy_url=dp_proxy, fetch_descriptions=fetch_descriptions,
    )
    logger.info("[ZipRecruiter] Done → %s", output_csv)
    return output_csv


def run_indeed(keyword: str, location: str, pages: int,
               proxy_url=None, fetch_descriptions: bool = True) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_csv = RAW_DIR / "indeed_jobs.csv"
    if output_csv.exists():
        output_csv.unlink()
        logger.info("[Indeed] Cleared previous data: %s", output_csv)

    _, dp_proxy = _resolve_proxy(proxy_url)
    indeed_scraper.scrape_indeed(
        query=keyword, location=location, output_filename=str(output_csv),
        max_pages=pages, proxy_url=dp_proxy, fetch_descriptions=fetch_descriptions,
    )
    logger.info("[Indeed] Done → %s", output_csv)
    return output_csv


# ══════════════════════════════════════════════════════════════════════
# Analysis
# ══════════════════════════════════════════════════════════════════════

def run_analysis(
    path_51job: Optional[Path],
    path_zhipin: Optional[Path],
    keyword: str,
    use_psm: bool = False,
    plot: bool = True,
    path_ziprecruiter: Optional[Path] = None,
    path_indeed: Optional[Path] = None,
) -> None:
    from src.analysis.normalizer       import load_and_normalize
    from src.analysis.skill_extractor  import enrich_dataframe
    from src.analysis.premium_estimator import estimate_all_groups
    from src.analysis.trend_tracker    import build_snapshot, save_snapshot
    from src.analysis.report           import generate_report

    p51  = str(path_51job)        if path_51job        and path_51job.exists()        else None
    pzp  = str(path_zhipin)       if path_zhipin       and path_zhipin.exists()       else None
    pzr  = str(path_ziprecruiter) if path_ziprecruiter and path_ziprecruiter.exists() else None
    pind = str(path_indeed)       if path_indeed       and path_indeed.exists()       else None

    if not any([p51, pzp, pzr, pind]):
        logger.warning("[analyze] No data files found — skipping analysis")
        return

    logger.info("[analyze] Normalising data …")
    df = load_and_normalize(path_51job=p51, path_zhipin=pzp,
                            path_ziprecruiter=pzr, path_indeed=pind,
                            search_keyword=keyword)
    logger.info("[analyze] %d records after merge", len(df))

    logger.info("[analyze] Extracting AI skill signals …")
    df = enrich_dataframe(df)
    ai_cnt = df["has_ai_skill"].sum()
    logger.info("[analyze] AI skill requirement: %d records (%.1f%%)",
                ai_cnt, 100 * ai_cnt / len(df))

    PROC_DIR.mkdir(parents=True, exist_ok=True)
    processed_path = PROC_DIR / f"processed_{keyword.replace(' ', '_')}.csv"
    df.to_csv(processed_path, index=False, encoding="utf-8-sig")
    logger.info("[analyze] Normalised data → %s", processed_path)

    logger.info("[analyze] Estimating AI skill premium (PSM=%s) …", use_psm)
    premium_df = estimate_all_groups(df, use_psm=use_psm)

    snapshot = build_snapshot(df, keyword=keyword)
    save_snapshot(snapshot)

    logger.info("[analyze] Generating report …")
    md_path = generate_report(
        df=df, premium_df=premium_df, snapshot=snapshot,
        keyword=keyword, output_dir=RPT_DIR, plot=plot,
    )
    logger.info("[analyze] Report → %s", md_path)

    # ── 历史趋势摘要（每次分析后自动打印）───────────────────────────
    from src.analysis.trend_tracker import summarize_trend
    df_trend = summarize_trend(keyword_filter=keyword)
    if len(df_trend) > 1:
        print(f"\n{'─'*60}")
        print(f"[趋势] 历史快照 {len(df_trend)} 次  关键词: {keyword}")
        print(f"{'─'*60}")
        for _, row in df_trend.iterrows():
            print(f"  {row['snapshot_time']}  {row['total_jobs']:>5}条  AI占比 {row['ai_ratio']:.1%}")
        first, last = df_trend.iloc[0], df_trend.iloc[-1]
        delta = last["ai_ratio"] - first["ai_ratio"]
        sign  = "+" if delta >= 0 else ""
        print(f"\n  整体变化: {first['ai_ratio']:.1%} → {last['ai_ratio']:.1%}  ({sign}{delta:.1%})")
        print(f"  完整快照: data/processed/trend_snapshots.csv")
        print()


# ══════════════════════════════════════════════════════════════════════
# CLI — argument parser
# ══════════════════════════════════════════════════════════════════════

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Job scraper + AI skill premium analyser",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level (default: INFO, or LOG_LEVEL env var)",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ── scrape ────────────────────────────────────────────────────────
    sp = sub.add_parser(
        "scrape",
        help="Scrape job postings from one or more platforms",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Platforms: 51job | zhipin | both | ziprecruiter | indeed | all\n\n"
            "Examples:\n"
            "  python3 main.py scrape zhipin  \"amazon运营\"        深圳   5\n"
            "  python3 main.py scrape both    \"amazon运营\"        深圳   3\n"
            "  python3 main.py scrape indeed  \"amazon operations\" Remote 3\n"
        ),
    )
    sp.add_argument("platform",
                    choices=["51job", "zhipin", "both", "ziprecruiter", "indeed", "all"],
                    help="Platform(s) to scrape")
    sp.add_argument("keyword",  help="Search keyword (e.g. 'amazon运营')")
    sp.add_argument("location", help="City (CN) or location string (EN)")
    sp.add_argument("pages",    type=int, help="Number of pages to scrape")
    sp.add_argument("--proxy-url", metavar="URL",
                    help="Proxy URL (e.g. http://127.0.0.1:7890)")
    sp.add_argument("--no-analyze", action="store_true",
                    help="Scrape only, skip analysis")
    sp.add_argument("--no-plot",    action="store_true",
                    help="Skip chart generation")
    sp.add_argument("--psm",        action="store_true",
                    help="Use Propensity Score Matching in analysis")
    sp.add_argument("--no-desc",    action="store_true",
                    help="Skip per-job description fetching (faster; ZipRecruiter/Indeed)")

    # ── analyze ───────────────────────────────────────────────────────
    ap = sub.add_parser(
        "analyze",
        help="Run analysis on existing CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Examples:\n"
            "  python3 main.py analyze --zhipin data/raw/zhipin_jobs.csv --keyword amazon运营\n"
            "  python3 main.py analyze --51job data/raw/51job_jobs.csv --zhipin data/raw/zhipin_jobs.csv\n"
        ),
    )
    ap.add_argument("--51job",        dest="path_51job",        metavar="CSV")
    ap.add_argument("--zhipin",       dest="path_zhipin",       metavar="CSV")
    ap.add_argument("--ziprecruiter", dest="path_ziprecruiter", metavar="CSV")
    ap.add_argument("--indeed",       dest="path_indeed",       metavar="CSV")
    ap.add_argument("--keyword",      default="综合", metavar="KEYWORD")
    ap.add_argument("--psm",          action="store_true")
    ap.add_argument("--no-plot",      action="store_true")

    # ── chat ──────────────────────────────────────────────────────────
    cp = sub.add_parser(
        "chat",
        help="Run the AI job-seeker chat bot on a live recruitment platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Supported platforms: zhipin | lagou | liepin | linkedin\n\n"
            "Examples:\n"
            "  python3 main.py chat zhipin\n"
            "  python3 main.py chat zhipin --max-turns 4 --unread-only\n"
            "  python3 main.py chat zhipin --reply-timeout 120 --output data/raw/zhipin_chat.csv\n"
        ),
    )
    cp.add_argument("platform",
                    choices=["zhipin", "lagou", "liepin", "linkedin"],
                    help="Recruitment platform")
    cp.add_argument("--max-turns",     type=int, default=6,   metavar="N",
                    help="Max questions per conversation (default: 6)")
    cp.add_argument("--max-chats",     type=int, default=50,  metavar="N",
                    help="Max conversations to process (default: 50)")
    cp.add_argument("--reply-timeout", type=int, default=180, metavar="SECS",
                    help="Seconds to wait for HR reply (default: 180)")
    cp.add_argument("--unread-only",   action="store_true",
                    help="Only process conversations with unread messages")
    cp.add_argument("--output",        metavar="PATH",
                    help="Output CSV path (default: data/raw/<platform>_chat.csv)")

    return parser


# ══════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    # Re-apply log level if overridden via CLI
    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info("command=%s", args.command)

    # ── chat ──────────────────────────────────────────────────────────
    if args.command == "chat":
        from src.chat_bot import run_chat_sessions
        output = args.output or str(RAW_DIR / f"{args.platform}_chat.csv")
        run_chat_sessions(
            platform=args.platform,
            output_path=output,
            max_turns=args.max_turns,
            max_chats=args.max_chats,
            reply_timeout=args.reply_timeout,
            unread_only=args.unread_only,
        )
        return

    # ── analyze ───────────────────────────────────────────────────────
    if args.command == "analyze":
        run_analysis(
            path_51job        = Path(args.path_51job)        if args.path_51job        else None,
            path_zhipin       = Path(args.path_zhipin)       if args.path_zhipin       else None,
            path_ziprecruiter = Path(args.path_ziprecruiter) if args.path_ziprecruiter else None,
            path_indeed       = Path(args.path_indeed)       if args.path_indeed       else None,
            keyword           = args.keyword,
            use_psm           = args.psm,
            plot              = not args.no_plot,
        )
        return

    # ── scrape ────────────────────────────────────────────────────────
    platform = args.platform
    keyword  = args.keyword
    location = args.location
    pages    = args.pages
    proxy    = args.proxy_url

    logger.info("platform=%s  keyword=%s  location=%s  pages=%d",
                platform, keyword, location, pages)

    path_51job = path_zhipin = path_ziprecruiter = path_indeed = None

    if platform in ("51job", "both", "all"):
        path_51job = run_51job(keyword, location, pages, proxy)

    if platform in ("zhipin", "both", "all"):
        path_zhipin = run_zhipin(keyword, location, pages, proxy)

    if platform in ("ziprecruiter", "all"):
        path_ziprecruiter = run_ziprecruiter(
            keyword, location, pages,
            proxy_url=proxy, fetch_descriptions=not args.no_desc,
        )

    if platform in ("indeed", "all"):
        path_indeed = run_indeed(
            keyword, location, pages,
            proxy_url=proxy, fetch_descriptions=not args.no_desc,
        )

    if not args.no_analyze:
        run_analysis(
            path_51job=path_51job, path_zhipin=path_zhipin,
            keyword=keyword, use_psm=args.psm, plot=not args.no_plot,
            path_ziprecruiter=path_ziprecruiter, path_indeed=path_indeed,
        )


if __name__ == "__main__":
    main()
