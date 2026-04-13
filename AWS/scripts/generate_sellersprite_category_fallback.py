"""
Generate Sellersprite market research fallback JSON.

Fetches subcategory data (return_rate, search_to_buy_ratio, avg_return_rate)
for every US top-level category node via get_market_research(), then writes
the result to config/data/sellersprite_market_research_fallback.json.

Run:
    python3 scripts/generate_sellersprite_category_fallback.py

Output structure:
    {
      "meta": {
        "title": "Amazon US Category Return Rate & Search-to-Buy Ratio",
        "marketplace": "Amazon.com (US)",
        "source": "Sellersprite (卖家精灵) Market Research",
        "month": "bsr_sales_nearly",
        "market_id": 1,
        "generated_date": "2026-04-13",
        "notes": "return_rate_pct and avg_return_rate_pct are percentages (%). search_to_buy_ratio_pm is per-mille (‰)."
      },
      "categories": {
        "<node_id>": {
          "label": "...",
          "locale": "...",
          "total_products": 123,
          "items": [
            {
              "node_id": "...",
              "category_name": "...",
              "search_to_buy_ratio_pm": 12.0,
              "return_rate_pct": 3.5,
              "avg_return_rate_pct": 2.1
            },
            ...
          ]
        },
        ...
      }
    }
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_ROOT, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
for lib in ("urllib3", "requests", "charset_normalizer"):
    logging.getLogger(lib).setLevel(logging.WARNING)

logger = logging.getLogger("fallback_gen")

OUTPUT_PATH = os.path.join(_ROOT, "src", "mcp", "servers", "finance", "sellersprite_market_research_fallback.json")
MARKET_ID   = 1          # US
MONTH_NAME  = "bsr_sales_nearly"
PAGE_SIZE   = 20          # 20 rows/page; server-side max observed
RPM         = 40          # requests per minute limit
SLEEP_BETWEEN = 60 / RPM  # 1.5s between requests

TOP_LEVEL_NODES = [
    {"id": "2619525011",  "label": "Appliances",                              "locale": "家电"},
    {"id": "2617941011",  "label": "Arts, Crafts & Sewing",                   "locale": "艺术、手工艺"},
    {"id": "15684181",    "label": "Automotive",                               "locale": "汽车"},
    {"id": "165796011",   "label": "Baby Products",                            "locale": "婴儿产品"},
    {"id": "3760911",     "label": "Beauty & Personal Care",                   "locale": "美容与护理"},
    {"id": "283155",      "label": "Books",                                    "locale": "图书"},
    {"id": "2335752011",  "label": "Cell Phones & Accessories",                "locale": "手机"},
    {"id": "7141123011",  "label": "Clothing, Shoes & Jewelry",               "locale": "服装、鞋履和珠宝"},
    {"id": "172282",      "label": "Electronics",                              "locale": "电子产品"},
    {"id": "16310101",    "label": "Grocery & Gourmet Food",                   "locale": "杂货店"},
    {"id": "3760901",     "label": "Health & Household",                       "locale": "健康与家居"},
    {"id": "1055398",     "label": "Home & Kitchen",                           "locale": "家居用品"},
    {"id": "706813011",   "label": "Hunting & Fishing",                        "locale": "狩猎&渔具"},
    {"id": "16310091",    "label": "Industrial & Scientific",                  "locale": "工业类"},
    {"id": "15736321",    "label": "Lights, Bulbs & Indicators",               "locale": "灯具&配件"},
    {"id": "11091801",    "label": "Musical Instruments",                      "locale": "乐器"},
    {"id": "1064954",     "label": "Office Products",                          "locale": "办公产品"},
    {"id": "2972638011",  "label": "Patio, Lawn & Garden",                    "locale": "庭院、草坪和园艺"},
    {"id": "2619533011",  "label": "Pet Supplies",                             "locale": "宠物用品"},
    {"id": "328182011",   "label": "Power & Hand Tools",                       "locale": "电动和手动工具"},
    {"id": "1267449011",  "label": "Small Appliance Parts & Accessories",      "locale": "小家电配件"},
    {"id": "3375251",     "label": "Sports & Outdoors",                        "locale": "运动与户外"},
    {"id": "228013",      "label": "Tools & Home Improvement",                 "locale": "工具"},
    {"id": "165793011",   "label": "Toys & Games",                             "locale": "玩具"},
    {"id": "468642",      "label": "Video Games",                              "locale": "视频游戏"},
]


def fetch_all_pages(api, node_id: str) -> tuple[int, list[dict]]:
    """Fetch all pages for a node, return (total_products, items)."""
    all_items: list[dict] = []
    page = 1
    total = None

    while True:
        result = api.get_market_research(
            market_id=MARKET_ID,
            node_id_path=node_id,
            month_name=MONTH_NAME,
            size=PAGE_SIZE,
            page=page,
        )
        if not result:
            break

        if total is None:
            total = result.get("total_products", 0)

        items = result.get("items", [])
        all_items.extend(items)

        logger.info(f"  page {page}: +{len(items)} items (total_products={total}, fetched={len(all_items)})")

        # Stop if we've fetched everything or got an empty page
        if not items or len(all_items) >= (total or 0):
            break

        page += 1
        time.sleep(SLEEP_BETWEEN)

    return total or 0, all_items


def main() -> None:
    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
    api = SellerspriteAPI()

    # Load existing file to allow resuming interrupted runs
    existing: dict = {}
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH) as f:
            existing = json.load(f)
        logger.info(f"Resuming — {len(existing.get('categories', {}))} categories already fetched")

    categories = existing.get("categories", {})
    errors: list[str] = []

    for i, node in enumerate(TOP_LEVEL_NODES, 1):
        node_id = node["id"]
        label   = node["label"]

        if node_id in categories:
            logger.info(f"[{i:02d}/{len(TOP_LEVEL_NODES)}] {label} — skipping (already fetched)")
            continue

        logger.info(f"[{i:02d}/{len(TOP_LEVEL_NODES)}] Fetching: {label} (node={node_id})")
        try:
            total, items = fetch_all_pages(api, node_id)
            categories[node_id] = {
                "label":          label,
                "locale":         node["locale"],
                "total_products": total,
                "items":          items,
            }
            logger.info(f"  ✓ {label}: {len(items)} subcategories")
        except Exception as e:
            logger.error(f"  ✗ {label}: {e}")
            errors.append(f"{label} ({node_id}): {e}")

        # Checkpoint after every category
        _write(categories)
        time.sleep(SLEEP_BETWEEN)

    _write(categories)

    print(f"\n{'=' * 56}")
    print(f"  Done: {len(categories)}/{len(TOP_LEVEL_NODES)} categories")
    print(f"  Errors: {len(errors)}")
    for e in errors:
        print(f"    - {e}")
    print(f"  Output: {OUTPUT_PATH}")
    print(f"{'=' * 56}")


def _write(categories: dict) -> None:
    payload = {
        "meta": {
            "title":          "Amazon US Category Return Rate & Search-to-Buy Ratio",
            "marketplace":    "Amazon.com (US)",
            "source":         "Sellersprite (卖家精灵) Market Research",
            "month":          MONTH_NAME,
            "market_id":      MARKET_ID,
            "generated_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "notes": (
                "return_rate_pct and avg_return_rate_pct are percentages (%). "
                "search_to_buy_ratio_pm is per-mille (‰). "
                "Each item represents one subcategory; values are Sellersprite-reported averages for that subcategory."
            ),
        },
        "categories": categories,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
