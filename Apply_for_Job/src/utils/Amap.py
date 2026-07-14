"""
Amap.py - 高德地图 REST API 封装

提供：
  geocode(address)        地址/城市名 → 坐标 + 行政区信息
  regeocode(lng, lat)     坐标 → 结构化地址
  city_tier(location_str) 城市名 → 线级（1/2/3），优先查静态表，未命中时走 Amap API

API 文档：https://lbs.amap.com/api/webservice/guide/api/georegeo
"""
from __future__ import annotations

import json
import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_KEY_FILE = Path(__file__).parents[2] / "config" / "amapkey.json"
_BASE_URL = "https://restapi.amap.com/v3"

# 静态城市线级表（与 normalizer.py 保持一致）
_STATIC_TIER: dict[str, int] = {
    "北京": 1, "上海": 1, "广州": 1, "深圳": 1,
    "杭州": 2, "成都": 2, "武汉": 2, "西安": 2, "南京": 2,
    "重庆": 2, "天津": 2, "苏州": 2, "长沙": 2, "郑州": 2,
}


# ── key loading ────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_key() -> str:
    if not _KEY_FILE.exists():
        raise FileNotFoundError(
            f"Amap key file not found: {_KEY_FILE}\n"
            "Create config/amapkey.json with {\"key\": \"YOUR_KEY\"}"
        )
    with open(_KEY_FILE, encoding="utf-8") as f:
        data = json.load(f)
    key = data.get("key", "").strip()
    if not key:
        raise ValueError("Amap key is empty in config/amapkey.json")
    return key


# ── low-level request helper ───────────────────────────────────────────

def _get(endpoint: str, params: dict, retries: int = 2) -> dict:
    params["key"] = _load_key()
    url = f"{_BASE_URL}/{endpoint}"
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "1":
                raise ValueError(f"Amap API error: {data.get('info')} (infocode={data.get('infocode')})")
            return data
        except requests.RequestException as e:
            if attempt == retries:
                raise
            logger.warning("Amap request failed (attempt %d/%d): %s", attempt + 1, retries, e)
            time.sleep(1)
    return {}  # unreachable


# ── public API ────────────────────────────────────────────────────────

def geocode(address: str, city: str = "") -> dict:
    """
    地址/城市名 → 坐标 + 行政区信息。

    Parameters
    ----------
    address : 地址或城市名，例如 "深圳市南山区" 或 "深圳"
    city    : 可选，指定查询城市以提高精度，例如 "深圳"

    Returns
    -------
    dict 包含以下字段（未找到时为空字符串）：
      location   : "经度,纬度"，例如 "114.085947,22.547"
      province   : 省份，例如 "广东省"
      city       : 城市，例如 "深圳市"
      district   : 区县，例如 "南山区"
      adcode     : 行政区划代码，例如 "440305"
      level      : 匹配级别，例如 "市" / "区县" / "兴趣点"
      formatted_address : 完整格式化地址
    """
    params: dict = {"address": address, "output": "json"}
    if city:
        params["city"] = city

    data = _get("geocode/geo", params)
    geocodes = data.get("geocodes") or []
    if not geocodes:
        logger.warning("Amap geocode: no result for '%s'", address)
        return {
            "location": "", "province": "", "city": "", "district": "",
            "adcode": "", "level": "", "formatted_address": "",
        }

    g = geocodes[0]
    return {
        "location":          g.get("location", ""),
        "province":          g.get("province", ""),
        "city":              g.get("city", ""),
        "district":          g.get("district", ""),
        "adcode":            g.get("adcode", ""),
        "level":             g.get("level", ""),
        "formatted_address": g.get("formatted_address", ""),
    }


def regeocode(lng: float, lat: float, radius: int = 1000) -> dict:
    """
    坐标 → 结构化地址（逆地理编码）。

    Parameters
    ----------
    lng    : 经度
    lat    : 纬度
    radius : 搜索半径（米），默认 1000

    Returns
    -------
    dict 包含以下字段（未找到时为空字符串）：
      formatted_address : 完整格式化地址
      province          : 省份
      city              : 城市
      district          : 区县
      adcode            : 行政区划代码
      township          : 街道/乡镇
    """
    params = {
        "location": f"{lng},{lat}",
        "radius":   radius,
        "output":   "json",
        "extensions": "base",
    }
    data = _get("geocode/regeo", params)
    info = data.get("regeocode", {})
    comp = info.get("addressComponent", {})
    return {
        "formatted_address": info.get("formatted_address", ""),
        "province":          comp.get("province", ""),
        "city":              comp.get("city", ""),
        "district":          comp.get("district", ""),
        "adcode":            comp.get("adcode", ""),
        "township":          comp.get("township", ""),
    }


# ── route planning ────────────────────────────────────────────────────

def route_driving(origin: str, destination: str) -> Optional[int]:
    """
    驾车路径规划 → 最优方案行驶时长（分钟）。

    Parameters
    ----------
    origin      : "经度,纬度"，例如 "113.930478,22.533191"
    destination : "经度,纬度"

    Returns
    -------
    int | None : 行驶时长（分钟），失败时返回 None
    """
    try:
        data = _get("direction/driving", {"origin": origin, "destination": destination})
        paths = data.get("route", {}).get("paths") or []
        if not paths:
            return None
        return round(int(paths[0]["duration"]) / 60)
    except Exception as e:
        logger.debug("route_driving failed (%s → %s): %s", origin, destination, e)
        return None


def route_transit(origin: str, destination: str, city: str) -> Optional[int]:
    """
    公共交通（地铁 + 公交）路径规划 → 最优方案行程时长（分钟）。

    Parameters
    ----------
    origin      : "经度,纬度"
    destination : "经度,纬度"
    city        : 城市名，例如 "深圳"（公交换乘 API 必填）

    Returns
    -------
    int | None : 行程时长（分钟），失败时返回 None
    """
    try:
        data = _get("direction/transit/integrated", {
            "origin": origin, "destination": destination,
            "city": city, "output": "json",
        })
        transits = data.get("route", {}).get("transits") or []
        if not transits:
            return None
        return round(int(transits[0]["duration"]) / 60)
    except Exception as e:
        logger.debug("route_transit failed (%s → %s): %s", origin, destination, e)
        return None


def route_walking(origin: str, destination: str) -> Optional[int]:
    """
    步行路径规划 → 行走时长（分钟）。

    Parameters
    ----------
    origin      : "经度,纬度"
    destination : "经度,纬度"

    Returns
    -------
    int | None : 行走时长（分钟），失败时返回 None
    """
    try:
        data = _get("direction/walking", {"origin": origin, "destination": destination})
        paths = data.get("route", {}).get("paths") or []
        if not paths:
            return None
        return round(int(paths[0]["duration"]) / 60)
    except Exception as e:
        logger.debug("route_walking failed (%s → %s): %s", origin, destination, e)
        return None


@lru_cache(maxsize=256)
def city_tier(location_str: str) -> int:
    """
    城市线级解析（1=一线, 2=二线, 3=其他/三线）。

    优先查询静态表；未命中时调用 Amap geocode API 获取城市名再查表；
    仍未命中则返回 3。结果按城市名缓存，避免重复请求。

    Parameters
    ----------
    location_str : 位置字符串，例如 "深圳" / "广东省深圳市南山区" / "深圳市·南山区"

    Returns
    -------
    int : 1, 2, 或 3
    """
    if not isinstance(location_str, str) or not location_str.strip():
        return 3

    # 1. 静态表快速匹配
    for city, tier in _STATIC_TIER.items():
        if city in location_str:
            return tier

    # 2. Amap API 解析城市名
    try:
        result = geocode(location_str)
        api_city = result.get("city", "").replace("市", "")
        if api_city:
            for city, tier in _STATIC_TIER.items():
                if city in api_city or api_city in city:
                    return tier
    except Exception as e:
        logger.debug("Amap city_tier fallback failed for '%s': %s", location_str, e)

    return 3
