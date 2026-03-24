
import sys
import os
import asyncio
import json

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.mcp.servers.market.deals.client import DealHistoryClient

async def debug_deal_history():
    client = DealHistoryClient()
    
    # 测试案例 1: 热门品牌 (验证基础抓取能力)
    print("\n--- [Test 1] Searching for 'Zevo' (Expected: Some results) ---")
    zevo_deals = await client.get_deal_history(asin="", keyword="zevo", max_pages=1)
    print(f"Total deals found for 'zevo': {len(zevo_deals)}")
    if zevo_deals:
        print(f"Sample deal: {zevo_deals[0]['title']} at {zevo_deals[0]['site']}")
    
    # 测试案例 2: 问题品牌 'TIENBE' (验证 403 修复)
    print("\n--- [Test 2] Searching for 'TIENBE fly trap' (Expected: 200 OK, empty or results) ---")
    tienbe_deals = await client.get_deal_history(asin="B0CZNQ53H4", keyword="TIENBE fly trap", max_pages=1)
    print(f"Total deals found for 'TIENBE fly trap': {len(tienbe_deals)}")
    if tienbe_deals:
        print(f"Sample deal: {tienbe_deals[0]['title']} at {tienbe_deals[0]['site']}")
    else:
        print("No deals found, but check logs to see if 403 occurred.")

    # 测试案例 3: 通用大众电子产品 (验证 Slickdeals 高频词)
    print("\n--- [Test 3] Searching for 'Sony' (Expected: High volume) ---")
    sony_deals = await client.get_deal_history(asin="", keyword="Sony", max_pages=1)
    print(f"Total deals found for 'Sony': {len(sony_deals)}")

if __name__ == "__main__":
    asyncio.run(debug_deal_history())
