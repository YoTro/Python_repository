
import logging
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("xiyou_span_test")

def test_span(api, asin, country, months):
    end_date = "2026-04-21"
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=30 * months)
    start_date = start_dt.strftime("%Y-%m-%d")
    
    logger.info(f"Testing span: {months} months ({start_date} -> {end_date})")
    try:
        raw = api.get_asin_keywords(
            country=country,
            asin=asin,
            start_date=start_date,
            end_date=end_date,
            page_size=5,
        )
        kw_list = raw.get("list") or []
        if kw_list:
            logger.info(f"  SUCCESS: Found {len(kw_list)} keywords for {months} months span.")
            return True
        else:
            logger.warning(f"  EMPTY: No keywords found for {months} months span.")
            logger.warning(f"  Response: {raw}")
            return False
    except Exception as e:
        logger.error(f"  FAILED: Error for {months} months span: {e}")
        return False

def main():
    api = XiyouZhaociAPI()
    asin = "B0FVLPXRNY"
    country = "US"
    
    spans = [6, 12, 18, 24, 36]
    results = {}
    
    for months in spans:
        ok = test_span(api, asin, country, months)
        results[months] = ok
        if not ok:
            # If it fails, maybe we hit a limit, but let's continue to be sure
            pass

    print("\nSummary of maximum span tests:")
    for months, ok in results.items():
        status = "PASS" if ok else "FAIL/EMPTY"
        print(f"  {months:2d} months: {status}")

if __name__ == "__main__":
    main()
