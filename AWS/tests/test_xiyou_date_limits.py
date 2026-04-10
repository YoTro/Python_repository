
import json
import logging
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.getcwd())

from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

logging.basicConfig(level=logging.INFO)

def test_dates():
    api = XiyouZhaociAPI()
    if not api.auth_token:
        print("Error: No token found. Please ensure you are logged in.")
        return

    asin = "B0DZFGTCLR" # User provided ASIN
    country = "US"

    test_cases = [
        # 1. 基础查询 (1个月)
        {"start": "2025-01-01", "end": "2025-01-31", "desc": "Standard 1-month range"},
        
        # 2. 边界测试: 刚好 25 个月 (2023-02-01 到 2025-03-01 约 25 个月)
        {"start": "2023-02-01", "end": "2025-03-01", "desc": "Max range check (~25 months)"},
        
        # 3. 超出测试: 26 个月
        {"start": "2023-02-01", "end": "2025-04-01", "desc": "Exceed max range (26 months)"},
        
        # 4. 最早日期之前测试
        {"start": "2023-01-01", "end": "2023-01-31", "desc": "Before earliest allowed date (Jan 2023)"},
    ]

    for case in test_cases:
        print(f"\n--- Testing: {case['desc']} ({case['start']} to {case['end']}) ---")
        try:
            res = api.get_asin_daily_trends(country, asin, case['start'], case['end'])
            
            # Check if entities exist and have trends
            entities = res.get("entities", [])
            if entities and entities[0].get("trends"):
                trends = entities[0]["trends"]
                print(f"Result: SUCCESS. Received {len(trends)} days of data.")
                print(f"First day: {trends[0].get('localDate')}")
                print(f"Last day: {trends[-1].get('localDate')}")
            else:
                print(f"Result: EMPTY/NO DATA. Response: {json.dumps(res, ensure_ascii=False)[:200]}...")
        except Exception as e:
            print(f"Result: FAILED with error: {e}")

if __name__ == "__main__":
    test_dates()
