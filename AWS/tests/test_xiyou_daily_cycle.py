
import logging
import os
import sys
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("xiyou_daily_test")

def test_daily_cycle(api, asin, country, start_date, end_date):
    url = f"{api.base_url}/v3/asins/research/list"
    
    # Use the payload structure suggested by the user
    payload = {
        "resource": {"country": country, "asin": asin},
        "biz": {
            "asin":       asin,
            "country":    country,
            "page":       1,
            "pageSize":   10,
            "query":      "",
            "orders":     [{"field": "follow", "order": "desc"}],
            "filters":    [{"field": "asinResearchType", "filter": ["all"]}],
            "rangeFilters": [],
            "cycleFilter": {
                "cycle":      "daily",
                "period":     "",
                "startCycle": {"startDate": start_date, "endDate": start_date},
                "endCycle":   {"startDate": end_date,   "endDate": end_date},
            },
        },
    }

    headers = api.common_headers.copy()
    headers["request-url"] = f"/detail/asin/look_up/{country}/{asin}?listType=dataList"
    headers["krs-ver"]     = api._krs_ver()

    logger.info(f"Testing DAILY cycle: {start_date} -> {end_date}")
    try:
        response = api._request("POST", url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        kw_list = data.get("list") or []
        avail = data.get("availableDate") or {}
        logger.info(f"  SUCCESS: Found {len(kw_list)} keywords.")
        logger.info(f"  Available Date in Response: {avail}")
        if kw_list:
            logger.info(f"  Sample: {kw_list[0].get('searchTerm')}")
        return True
    except Exception as e:
        logger.error(f"  FAILED: {e}")
        if hasattr(e, 'response') and e.response:
             logger.error(f"  Response content: {e.response.text}")
        return False

def main():
    api = XiyouZhaociAPI()
    asin = "B0FVLPXRNY"
    country = "US"
    
    # Test a mid-month range
    test_daily_cycle(api, asin, country, "2026-02-01", "2026-02-19")

if __name__ == "__main__":
    main()
