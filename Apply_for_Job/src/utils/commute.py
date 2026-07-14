"""
commute.py - 求职者通勤时间批量计算

从 config/applicants.yaml 读取求职者出发地址，对 raw CSV 中的每个职位
调用高德地图路径规划 API，将通勤时间写回 CSV（in-place）。

新增列命名规则：commute_{mode}_min_{name}
  驾车   → commute_drive_min_Jin
  公交   → commute_transit_min_Jin
  步行   → commute_walk_min_Jin

支持平台的地址字段映射：
  zhipin       jobAddress（精确）→ areaDistrict + city（粗略回退）
  51job        Location（城市级）
  ziprecruiter location（美国地址，高德不覆盖，标记 N/A）
  indeed       location（美国地址，高德不覆盖，标记 N/A）
"""
from __future__ import annotations

import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

_APPLICANTS_FILE = Path(__file__).parents[2] / "config" / "applicants.yaml"

_MODE_FN = {
    "driving": "route_driving",
    "transit": "route_transit",
    "walking": "route_walking",
}

# ── config loading ─────────────────────────────────────────────────────

def load_applicants() -> list[dict]:
    """
    从 config/applicants.yaml 加载求职者列表。

    Returns
    -------
    list of dict, each with keys: name, address, city, modes
    """
    if not _APPLICANTS_FILE.exists():
        logger.warning("applicants.yaml not found at %s — commute enrichment skipped", _APPLICANTS_FILE)
        return []
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not installed; run: pip install pyyaml")
        return []
    with open(_APPLICANTS_FILE, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("applicants", [])


# ── geocode cache ──────────────────────────────────────────────────────

@lru_cache(maxsize=512)
def _geocode_cached(address: str) -> Optional[str]:
    """Returns 'lng,lat' string or None. Results cached per address."""
    from src.utils.Amap import geocode
    try:
        result = geocode(address)
        loc = result.get("location", "")
        return loc if loc else None
    except Exception as e:
        logger.debug("geocode failed for '%s': %s", address, e)
        return None


# ── per-job address resolution per platform ───────────────────────────

def _clean(val) -> str:
    """Return stripped string; treat pandas NaN / None / 'nan' as empty."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    return "" if s.lower() == "nan" else s


def _resolve_job_address(row: pd.Series, platform: str, default_city: str) -> str:
    """
    Extract the best geocodeable address string from a raw CSV row.
    Returns a string suitable for passing to geocode().
    """
    if platform == "zhipin":
        addr = _clean(row.get("jobAddress"))
        if addr:
            return addr
        district = _clean(row.get("areaDistrict"))
        biz      = _clean(row.get("businessDistrict"))
        return f"{default_city}{district}{biz}" if (district or biz) else default_city

    if platform == "job51":
        loc = _clean(row.get("Location"))
        return loc if loc else default_city

    # ziprecruiter / indeed: US addresses — not supported by Amap
    return ""


# ── main enrichment function ───────────────────────────────────────────

def enrich_commute(csv_path: str, platform: str) -> None:
    """
    Add commute-time columns to a raw job CSV in-place.

    Parameters
    ----------
    csv_path : absolute or relative path to the raw CSV file
    platform : "zhipin" | "job51" | "ziprecruiter" | "indeed"
    """
    from src.utils.Amap import route_driving, route_transit, route_walking

    applicants = load_applicants()
    if not applicants:
        logger.info("[commute] No applicants configured — skipping")
        return

    if platform in ("ziprecruiter", "indeed"):
        logger.info("[commute] Platform '%s' uses US addresses; Amap not supported — skipping", platform)
        return

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if df.empty:
        return

    _route_fns = {
        "driving": route_driving,
        "transit": route_transit,
        "walking": route_walking,
    }

    for applicant in applicants:
        name    = applicant.get("name", "applicant")
        address = applicant.get("address", "")
        city    = applicant.get("city", "")
        modes   = applicant.get("modes", ["driving", "transit"])

        if not address:
            logger.warning("[commute] Applicant '%s' has no address — skipping", name)
            continue

        origin = _geocode_cached(address)
        if not origin:
            logger.warning("[commute] Could not geocode applicant '%s' address: %s", name, address)
            continue
        logger.info("[commute] Applicant '%s' origin: %s → %s", name, address, origin)

        for mode in modes:
            col = f"commute_{mode[:7]}_min_{name}"
            results: list[Optional[int]] = []

            for _, row in df.iterrows():
                job_addr_str = _resolve_job_address(row, platform, city)
                if not job_addr_str:
                    results.append(None)
                    continue

                dest = _geocode_cached(job_addr_str)
                if not dest:
                    results.append(None)
                    time.sleep(0.05)
                    continue

                fn = _route_fns[mode]
                try:
                    if mode == "transit":
                        minutes = fn(origin, dest, city)
                    else:
                        minutes = fn(origin, dest)
                except Exception as e:
                    logger.debug("[commute] %s route error: %s", mode, e)
                    minutes = None

                results.append(minutes)
                time.sleep(0.05)   # stay well within Amap free-tier QPS

            df[col] = results
            logger.info("[commute] Added column '%s' (%d/%d resolved)",
                        col, sum(v is not None for v in results), len(results))

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("[commute] Saved enriched CSV → %s", csv_path)
