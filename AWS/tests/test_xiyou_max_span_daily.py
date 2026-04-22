
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("xiyou_daily_precision")

def test_exact_days(api, asin, country, days):
    end_date = "2026-04-21"
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    # days=7 means 7 days total: (end - 6) to end
    start_dt = end_dt - timedelta(days=days-1)
    start_date = start_dt.strftime("%Y-%m-%d")
    
    logger.info(f"Testing {days}-DAY span: {start_date} -> {end_date}")
    try:
        raw = api.get_asin_keywords(
            country=country,
            asin=asin,
            start_date=start_date,
            end_date=end_date,
            page_size=5,
        )
        if raw and "list" in raw:
            logger.info(f"  SUCCESS: Found {len(raw.get('list', []))} keywords.")
            return True
        else:
            logger.warning(f"  EMPTY/INVALID: {raw}")
            return False
    except Exception as e:
        logger.error(f"  FAILED: {e}")
        return False

def main():
    api = XiyouZhaociAPI()
    asin = "B0FVLPXRNY"
    country = "US"
    
    # Precision testing around the 30-day mark
    test_cases = [7, 14, 28, 29, 30, 31]
    results = {}
    
    for days in test_cases:
        ok = test_exact_days(api, asin, country, days)
        results[days] = ok

    print("\nSummary of Precise DAILY span tests:")
    for days, ok in results.items():
        status = "PASS" if ok else "FAIL"
        print(f"  {days:2d} days: {status}")

if __name__ == "__main__":
    main()
