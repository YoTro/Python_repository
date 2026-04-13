import os
import sys
import json
import time
import logging
import math

# Add project root to sys.path
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _ROOT)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
logger = logging.getLogger("collect_market_data")

from src.mcp.servers.market.sellersprite.client import SellerspriteAPI

def collect_market_data():
    api = SellerspriteAPI(tenant_id="default")
    
    # 25 Major categories
    major_categories = [
        {"id": "2619525011", "label": "Appliances", "locale": "家电"},
        {"id": "2617941011", "label": "Arts, Crafts & Sewing", "locale": "艺术、手工艺"},
        {"id": "15684181", "label": "Automotive", "locale": "汽车"},
        {"id": "165796011", "label": "Baby Products", "locale": "婴儿产品"},
        {"id": "3760911", "label": "Beauty & Personal Care", "locale": "美容与护理"},
        {"id": "283155", "label": "Books", "locale": "图书"},
        {"id": "2335752011", "label": "Cell Phones & Accessories", "locale": "手机"},
        {"id": "7141123011", "label": "Clothing, Shoes & Jewelry", "locale": "服装、鞋履和珠宝"},
        {"id": "172282", "label": "Electronics", "locale": "电子产品"},
        {"id": "16310101", "label": "Grocery & Gourmet Food", "locale": "杂货店"},
        {"id": "3760901", "label": "Health & Household", "locale": "健康与家居"},
        {"id": "1055398", "label": "Home & Kitchen", "locale": "家居用品"},
        {"id": "706813011", "label": "Hunting & Fishing", "locale": "狩猎&渔具"},
        {"id": "16310091", "label": "Industrial & Scientific", "locale": "工业类"},
        {"id": "15736321", "label": "Lights, Bulbs & Indicators", "locale": "灯具&配件"},
        {"id": "11091801", "label": "Musical Instruments", "locale": "乐器"},
        {"id": "1064954", "label": "Office Products", "locale": "办公产品"},
        {"id": "2972638011", "label": "Patio, Lawn & Garden", "locale": "庭院、草坪和园艺"},
        {"id": "2619533011", "label": "Pet Supplies", "locale": "宠物用品"},
        {"id": "328182011", "label": "Power & Hand Tools", "locale": "电动和手动工具"},
        {"id": "1267449011", "label": "Small Appliance Parts & Accessories", "locale": "小家电配件"},
        {"id": "3375251", "label": "Sports & Outdoors", "locale": "运动与户外"},
        {"id": "228013", "label": "Tools & Home Improvement", "locale": "工具"},
        {"id": "165793011", "label": "Toys & Games", "locale": "玩具"},
        {"id": "468642", "label": "Video Games", "locale": "视频游戏"}
    ]
    
    all_data = {}
    market_id = 1
    month_name = "bsr_sales_nearly"
    size = 500
    
    for major in major_categories:
        major_id = major["id"]
        major_label = major["label"]
        logger.info(f">>> Fetching sub-category reports for: {major_label} ({major_id})")
        
        # Initial request to get total
        res = api.get_market_research(market_id, major_id, month_name=month_name, size=size, page=1)
        
        total_products = res.get("total_products", 0)
        items = res.get("items", [])
        
        logger.info(f"[{major_label}] Total sub-categories found: {total_products}")
        logger.info(f"[{major_label}] Page 1: Fetched {len(items)} items")
        
        if total_products > size:
            total_pages = math.ceil(total_products / size)
            for p in range(2, total_pages + 1):
                logger.info(f"[{major_label}] Fetching page {p}/{total_pages}...")
                page_res = api.get_market_research(market_id, major_id, month_name=month_name, size=size, page=p)
                new_items = page_res.get("items", [])
                items.extend(new_items)
                logger.info(f"[{major_label}] Page {p}: Fetched {len(new_items)} items")
                time.sleep(0.5)
        
        all_data[major_id] = {
            "major_category": f"{major_label} ({major['locale']})",
            "sub_categories_count": len(items),
            "data": items
        }
        
        logger.info(f"[{major_label}] Finished. Collected {len(items)} sub-categories.")

    # Save Phase
    output_path = os.path.join(_ROOT, "src", "mcp", "servers", "finance", "amazon_sub_category_market_research.json")
    final_output = {
        "meta": {
            "title": "Amazon Sub-Category Market Research Data (Financial Metrics)",
            "description": "NodeID, Name, Search-to-buy ratio, Return rate, Avg Return rate for all sub-categories of the 25 major categories.",
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "market": "US"
        },
        "reports": all_data
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_output, f, indent=4, ensure_ascii=False)
    
    logger.info(f"SUCCESS. Data saved to {output_path}")

if __name__ == "__main__":
    collect_market_data()
