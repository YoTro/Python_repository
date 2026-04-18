"""
normalizer.py - 原始招聘数据标准化

负责：
  1. 统一各来源（51job / zhipin / ziprecruiter）的字段名
  2. 解析薪资字符串 → salary_min / salary_max / salary_mid（月薪，本币）
     中文平台：元/月；ZipRecruiter：USD/月
  3. 岗位名称归一化（关键词聚类）→ job_canonical
  4. 经验年限 / 城市 / 公司规模数字化

岗位归一化策略（三层，依次降级）：
  1. 从 config/job_categories.yaml 加载词典匹配
  2. 未命中时，用调用方传入的 search_keyword 作为 canonical
  3. search_keyword 也为空时，返回 "其他"
"""
from __future__ import annotations
import re
import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parents[2] / "config" / "job_categories.yaml"

# ── 公司规模映射 → 数字中位数 ──────────────────────────────────────
COMPANY_SIZE_MAP = {
    # 51job 格式
    "少于50人": 25, "50-150人": 100, "150-500人": 325,
    "500-2000人": 1250, "2000-10000人": 6000, "10000人以上": 15000,
    # zhipin 格式
    "0-20人": 10, "20-99人": 60, "100-499人": 300,
    "500-999人": 750, "1000-9999人": 5000,
}

# ── 城市 tier 映射 ──────────────────────────────────────────────
CITY_TIER = {
    "北京": 1, "上海": 1, "广州": 1, "深圳": 1,
    "杭州": 2, "成都": 2, "武汉": 2, "西安": 2, "南京": 2,
    "重庆": 2, "天津": 2, "苏州": 2, "长沙": 2, "郑州": 2,
}


# ══════════════════════════════════════════════════════════════════════
# 岗位词典加载（带缓存，yaml 修改后重启生效）
# ══════════════════════════════════════════════════════════════════════

@lru_cache(maxsize=1)
def _load_categories() -> dict[str, list[str]]:
    """
    从 config/job_categories.yaml 加载归一化词典。
    返回 {标准名: [正则列表]}。
    yaml 文件不存在时返回空 dict（全部走关键词直通）。
    """
    if not _CONFIG_PATH.exists():
        logger.warning(f"job_categories.yaml 不存在: {_CONFIG_PATH}，岗位归一化将全部使用关键词直通")
        return {}
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML 未安装，岗位归一化将全部使用关键词直通。pip install pyyaml")
        return {}
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("categories", {})


def reload_categories() -> None:
    """强制重新加载 yaml 词典（修改配置后调用）"""
    _load_categories.cache_clear()


# ══════════════════════════════════════════════════════════════════════
# 核心解析函数
# ══════════════════════════════════════════════════════════════════════

def parse_salary(s: Optional[str]) -> dict:
    """
    解析各平台薪资字符串，返回月薪（元）。
    支持格式：
      "15k-25k·13薪"  "8000-12000元/月"  "15-25K/月"
      "150-200K/年"   "面议"              "20万-30万/年"
    """
    empty = {"salary_min": None, "salary_max": None, "salary_mid": None,
             "salary_months": 12, "salary_raw": s}
    if not s or not isinstance(s, str):
        return empty

    s_clean = s.strip()

    # 提取薪资月数（13薪、14薪等）
    months = 12
    m = re.search(r'(\d+)\s*薪', s_clean)
    if m:
        months = int(m.group(1))

    # 万/年
    m = re.search(r'(\d+(?:\.\d+)?)\s*[~\-–]\s*(\d+(?:\.\d+)?)\s*万.*?年', s_clean)
    if m:
        lo = float(m.group(1)) * 10000 / 12
        hi = float(m.group(2)) * 10000 / 12
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # K/年
    m = re.search(r'(\d+(?:\.\d+)?)[kK]\s*[~\-–]\s*(\d+(?:\.\d+)?)[kK].*?年', s_clean)
    if m:
        lo = float(m.group(1)) * 1000 / 12
        hi = float(m.group(2)) * 1000 / 12
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # 千-万 混合（如 6千-1.2万、8千-1万）
    m = re.search(r'(\d+(?:\.\d+)?)\s*千\s*[~\-–·]\s*(\d+(?:\.\d+)?)\s*万', s_clean)
    if m:
        lo = float(m.group(1)) * 1000
        hi = float(m.group(2)) * 10000
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # X万-Y万/月 or X.X万-Y.Y万（不含/年）
    m = re.search(r'(\d+(?:\.\d+)?)\s*[~\-–]\s*(\d+(?:\.\d+)?)\s*万', s_clean)
    if m:
        lo = float(m.group(1)) * 10000
        hi = float(m.group(2)) * 10000
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # X千-Y千/月
    m = re.search(r'(\d+(?:\.\d+)?)\s*千\s*[~\-–·]\s*(\d+(?:\.\d+)?)\s*千', s_clean)
    if m:
        lo = float(m.group(1)) * 1000
        hi = float(m.group(2)) * 1000
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # K/月 or K·N薪
    m = re.search(r'(\d+(?:\.\d+)?)\s*[kK]\s*[~\-–·]\s*(\d+(?:\.\d+)?)\s*[kK]', s_clean)
    if m:
        lo = float(m.group(1)) * 1000
        hi = float(m.group(2)) * 1000
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    # 元/月（纯数字）
    m = re.search(r'(\d{4,6})\s*[~\-–]\s*(\d{4,6})', s_clean)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return {"salary_min": lo, "salary_max": hi,
                "salary_mid": round((lo + hi) / 2),
                "salary_months": months, "salary_raw": s}

    return empty


def parse_salary_en(s: Optional[str]) -> dict:
    """
    Parse English salary strings from ZipRecruiter into monthly USD.

    Supported formats:
      "$50,000–$70,000 a year"   "$25–$35/hr"   "$80K–$100K a year"
      "$50,000 a year"           "$30/hour"      "Up to $60,000 a year"
      "$20–$25 an hour"          "From $45,000 a year"
    """
    empty = {"salary_min": None, "salary_max": None, "salary_mid": None,
             "salary_months": 12, "salary_raw": s}
    if not s or not isinstance(s, str):
        return empty

    t = s.strip().replace(",", "").replace("\u2013", "-").replace("\u2014", "-")

    def _parse_num(raw: str) -> float:
        raw = raw.replace("$", "").replace("k", "000").replace("K", "000").strip()
        return float(raw)

    # ── Hourly ─────────────────────────────────────────────────────────
    # "$20–$35/hr"  "$25 an hour"  "$20-$35 per hour"
    m = re.search(
        r'\$?([\d.]+[kK]?)\s*[-–]\s*\$?([\d.]+[kK]?)\s*(?:/hr|/hour|an hour|per hour)',
        t, re.IGNORECASE,
    )
    if m:
        lo = _parse_num(m.group(1)) * 160   # 40 h/wk × 4 wk
        hi = _parse_num(m.group(2)) * 160
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": 12, "salary_raw": s}

    m = re.search(
        r'\$?([\d.]+[kK]?)\s*(?:/hr|/hour|an hour|per hour)',
        t, re.IGNORECASE,
    )
    if m:
        mid = _parse_num(m.group(1)) * 160
        return {"salary_min": None, "salary_max": None, "salary_mid": round(mid),
                "salary_months": 12, "salary_raw": s}

    # ── Annual range ────────────────────────────────────────────────────
    # "$50000–$70000 a year"  "$80K–$100K"
    m = re.search(
        r'\$?([\d.]+[kK]?)\s*[-–]\s*\$?([\d.]+[kK]?)\s*(?:a year|/year|per year|annually)?',
        t, re.IGNORECASE,
    )
    if m:
        lo = _parse_num(m.group(1)) / 12
        hi = _parse_num(m.group(2)) / 12
        return {"salary_min": round(lo), "salary_max": round(hi),
                "salary_mid": round((lo + hi) / 2),
                "salary_months": 12, "salary_raw": s}

    # ── Single annual figure ────────────────────────────────────────────
    # "Up to $60,000 a year"  "From $45,000 a year"
    m = re.search(
        r'(?:up to|from|starting at)?\s*\$?([\d.]+[kK]?)\s*(?:a year|/year|per year|annually)',
        t, re.IGNORECASE,
    )
    if m:
        mid = _parse_num(m.group(1)) / 12
        return {"salary_min": None, "salary_max": None, "salary_mid": round(mid),
                "salary_months": 12, "salary_raw": s}

    return empty


def parse_experience(s: Optional[str]) -> Optional[float]:
    """'3-5年经验' → 4.0（中位数）  '应届' → 0  '10年以上' → 10"""
    if not s or not isinstance(s, str):
        return None
    if any(w in s for w in ["应届", "在校", "不限"]):
        return 0.0
    m = re.search(r'(\d+)\s*[~\-–]\s*(\d+)\s*年', s)
    if m:
        return (int(m.group(1)) + int(m.group(2))) / 2
    m = re.search(r'(\d+)\s*年以上', s)
    if m:
        return float(m.group(1))
    m = re.search(r'(\d+)\s*年', s)
    if m:
        return float(m.group(1))
    return None


def canonicalize_job(title: Optional[str],
                     search_keyword: Optional[str] = None) -> str:
    """
    将职位名映射到标准类别。

    三层策略：
      1. yaml 词典正则匹配
      2. 未命中 → 返回 search_keyword（调用方的搜索词作为分组名）
      3. search_keyword 也为空 → 返回 "其他"

    Parameters
    ----------
    title          : 职位名原文
    search_keyword : 本次爬取的搜索关键词，作为未命中时的 fallback 分组
    """
    if not title or not isinstance(title, str):
        return search_keyword or "其他"

    t = title.lower()
    for canonical, patterns in _load_categories().items():
        for pat in patterns:
            try:
                if re.search(pat, t, re.IGNORECASE):
                    return canonical
            except re.error:
                logger.warning(f"job_categories.yaml 中正则无效: {pat!r}，已跳过")
                continue

    # 未命中：关键词直通
    return search_keyword or "其他"


# ══════════════════════════════════════════════════════════════════════
# 平台适配层
# ══════════════════════════════════════════════════════════════════════

def normalize_51job(df: pd.DataFrame,
                    search_keyword: Optional[str] = None) -> pd.DataFrame:
    """
    标准化 51job CSV → 统一 schema。

    Parameters
    ----------
    df             : api_scraper / drission_scraper 输出的原始 DataFrame
    search_keyword : 本次爬取关键词，用于未命中时的岗位 canonical fallback
    """
    out = pd.DataFrame({
        "source":       "51job",
        "job_title":    df.get("Job",        pd.Series([""] * len(df), index=df.index)),
        "company":      df.get("Company",    pd.Series([""] * len(df), index=df.index)),
        "location":     df.get("Location",   pd.Series([""] * len(df), index=df.index)),
        "education":    df.get("Education",  pd.Series([""] * len(df), index=df.index)),
        "experience":   df.get("Experience", pd.Series([""] * len(df), index=df.index)),
        "salary_raw":   df.get("Salary",     pd.Series([""] * len(df), index=df.index)),
        "description":  df.get("JobDetail",  pd.Series([""] * len(df), index=df.index)),
        "welfare":      df.get("Welfare",    pd.Series([""] * len(df), index=df.index)),
        "company_size": None,
        "update_date":  df.get("UpdateDate", pd.Series([""] * len(df), index=df.index)),
        "url":          df.get("Href",       pd.Series([""] * len(df), index=df.index)),
    }, index=df.index)
    return _enrich(out, search_keyword=search_keyword)


def normalize_zhipin(df: pd.DataFrame,
                     search_keyword: Optional[str] = None) -> pd.DataFrame:
    """
    标准化 zhipin CSV → 统一 schema。

    Parameters
    ----------
    df             : scraper.py 输出的原始 DataFrame
    search_keyword : 本次爬取关键词，用于未命中时的岗位 canonical fallback
    """
    location = (
        df.get("areaDistrict",     pd.Series([""] * len(df), index=df.index)).fillna("") + " " +
        df.get("businessDistrict", pd.Series([""] * len(df), index=df.index)).fillna("")
    )
    out = pd.DataFrame({
        "source":       "zhipin",
        "job_title":    df.get("jobName",        pd.Series([""] * len(df), index=df.index)),
        "company":      df.get("brandName",      pd.Series([""] * len(df), index=df.index)),
        "location":     location,
        "education":    df.get("jobDegree",      pd.Series([""] * len(df), index=df.index)),
        "experience":   df.get("jobExperience",  pd.Series([""] * len(df), index=df.index)),
        "salary_raw":   df.get("salaryDesc",     pd.Series([""] * len(df), index=df.index)),
        "description":  df.get("jobDescription", pd.Series([""] * len(df), index=df.index)),
        "welfare":      df.get("jobLabels",      pd.Series([""] * len(df), index=df.index)),
        "company_size": df.get("brandScaleName", pd.Series([None] * len(df), index=df.index)),
        "update_date":  None,
        "url":          df.get("jobDetailUrl",   pd.Series([""] * len(df), index=df.index)),
    }, index=df.index)
    return _enrich(out, search_keyword=search_keyword)


def normalize_ziprecruiter(df: pd.DataFrame,
                           search_keyword: Optional[str] = None) -> pd.DataFrame:
    """
    Normalise a ZipRecruiter CSV (output of src/ziprecruiter/scraper.py)
    into the unified schema.

    Salary values are stored in USD/month; a `salary_currency` column
    is added so downstream analysis can segment by market.
    """
    is_remote = df.get("is_remote", pd.Series([False] * len(df), index=df.index)).fillna(False)
    location = df.get("location", pd.Series([""] * len(df), index=df.index)).fillna("")
    # Append "Remote" flag to location string when applicable
    location = location.where(
        ~is_remote.astype(bool),
        location.where(location.str.contains("Remote", case=False), location + " (Remote)"),
    )

    employment_type = df.get("employment_type", pd.Series([""] * len(df), index=df.index))

    out = pd.DataFrame({
        "source":       "ziprecruiter",
        "job_title":    df.get("title",        pd.Series([""] * len(df), index=df.index)),
        "company":      df.get("company",      pd.Series([""] * len(df), index=df.index)),
        "location":     location,
        "education":    None,
        "experience":   None,
        "salary_raw":   df.get("salary_raw",   pd.Series([""] * len(df), index=df.index)),
        "description":  df.get("description",  pd.Series([""] * len(df), index=df.index)),
        "welfare":      employment_type,
        "company_size": None,
        "update_date":  df.get("posted_time",  pd.Series([""] * len(df), index=df.index)),
        "url":          df.get("url",          pd.Series([""] * len(df), index=df.index)),
    }, index=df.index)

    return _enrich(out, search_keyword=search_keyword, salary_parser=parse_salary_en,
                   currency="USD")


def normalize_indeed(df: pd.DataFrame,
                     search_keyword: Optional[str] = None) -> pd.DataFrame:
    """
    Normalise an Indeed CSV (output of src/indeed/scraper.py)
    into the unified schema.

    Salary values are stored in USD/month; salary_currency = "USD".
    """
    is_remote = df.get("is_remote", pd.Series([False] * len(df), index=df.index)).fillna(False)
    location  = df.get("location", pd.Series([""] * len(df), index=df.index)).fillna("")
    location  = location.where(
        ~is_remote.astype(bool),
        location.where(location.str.contains("Remote", case=False), location + " (Remote)"),
    )

    out = pd.DataFrame({
        "source":       "indeed",
        "job_title":    df.get("title",           pd.Series([""] * len(df), index=df.index)),
        "company":      df.get("company",          pd.Series([""] * len(df), index=df.index)),
        "location":     location,
        "education":    None,
        "experience":   None,
        "salary_raw":   df.get("salary_raw",       pd.Series([""] * len(df), index=df.index)),
        "description":  df.get("description",      pd.Series([""] * len(df), index=df.index)),
        "welfare":      df.get("employment_type",  pd.Series([""] * len(df), index=df.index)),
        "company_size": None,
        "update_date":  df.get("posted_time",      pd.Series([""] * len(df), index=df.index)),
        "url":          df.get("url",              pd.Series([""] * len(df), index=df.index)),
    }, index=df.index)

    return _enrich(out, search_keyword=search_keyword, salary_parser=parse_salary_en,
                   currency="USD")


def _enrich(df: pd.DataFrame,
            search_keyword: Optional[str] = None,
            salary_parser=None,
            currency: str = "CNY") -> pd.DataFrame:
    """
    Shared post-processing: parse salary / experience / canonicalise job /
    city tier.

    Parameters
    ----------
    salary_parser : callable that maps a raw salary string → dict.
                    Defaults to parse_salary (CNY). Pass parse_salary_en for
                    English-market sources.
    currency      : "CNY" or "USD" — stored in salary_currency column.
    """
    if salary_parser is None:
        salary_parser = parse_salary

    salary_parsed = df["salary_raw"].apply(salary_parser).apply(pd.Series)
    # Drop duplicate salary_raw column that comes out of the parser dict
    salary_parsed = salary_parsed.drop(columns=["salary_raw"], errors="ignore")
    df = pd.concat([df, salary_parsed], axis=1)
    df["salary_currency"] = currency

    df["exp_years"]      = df["experience"].apply(parse_experience)
    df["job_canonical"]  = df["job_title"].apply(
        lambda t: canonicalize_job(t, search_keyword=search_keyword)
    )
    df["company_size_n"] = df["company_size"].map(COMPANY_SIZE_MAP)

    def _tier(loc):
        if not isinstance(loc, str):
            return 3
        for city, tier in CITY_TIER.items():
            if city in loc:
                return tier
        return 3

    df["city_tier"] = df["location"].apply(_tier)
    return df


def load_and_normalize(path_51job: Optional[str] = None,
                       path_zhipin: Optional[str] = None,
                       path_ziprecruiter: Optional[str] = None,
                       path_indeed: Optional[str] = None,
                       search_keyword: Optional[str] = None) -> pd.DataFrame:
    """
    Load one or more source CSVs, normalise, and merge.
    At least one path must be provided.

    Parameters
    ----------
    search_keyword : Search keyword passed to canonicalize_job as fallback group name.
    """
    frames = []
    if path_51job:
        raw = pd.read_csv(path_51job, encoding="utf-8-sig")
        frames.append(normalize_51job(raw, search_keyword=search_keyword))
    if path_zhipin:
        raw = pd.read_csv(path_zhipin, encoding="utf-8-sig")
        frames.append(normalize_zhipin(raw, search_keyword=search_keyword))
    if path_ziprecruiter:
        raw = pd.read_csv(path_ziprecruiter, encoding="utf-8-sig")
        frames.append(normalize_ziprecruiter(raw, search_keyword=search_keyword))
    if path_indeed:
        raw = pd.read_csv(path_indeed, encoding="utf-8-sig")
        frames.append(normalize_indeed(raw, search_keyword=search_keyword))
    if not frames:
        raise ValueError("At least one data source path must be provided.")
    return pd.concat(frames, ignore_index=True)
