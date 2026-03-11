import logging
from typing import List, Dict, Any
from .base_task import BaseTask
from src.extractors.product_details import ProductDetailsExtractor
from src.extractors.fulfillment import FulfillmentExtractor
from src.extractors.dimensions import DimensionsExtractor
from src.extractors.ranks import RanksExtractor
from src.extractors.images import ImageExtractor
from src.extractors.videos import VideoExtractor
from src.extractors.cart_stock import CartStockExtractor
from src.extractors.review_count import ReviewCountExtractor
from src.extractors.past_month_sales import PastMonthSalesExtractor

logger = logging.getLogger(__name__)

class FullAsinDetailsTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        
        proxies = context.get('proxies')
        ex_details = ProductDetailsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_fulfillment = FulfillmentExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_dimensions = DimensionsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_ranks = RanksExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_images = ImageExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_videos = VideoExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_stock = CartStockExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_review_count = ReviewCountExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_past_sales = PastMonthSalesExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)

        all_results = []
        for asin in asins:
            logger.info(f"Aggregating full data for ASIN: {asin}")
            combined_data = {"ASIN": asin}
            
            try:
                url = asin if "http" in asin else f"https://www.amazon.com/dp/{asin}"
                details = ex_details.get_product_details(url)
                if details: combined_data.update(details)
            except Exception as e: logger.error(f"Error details {asin}: {e}")

            try:
                f = ex_fulfillment.get_fulfillment_info(asin)
                if f: combined_data.update(f)
            except Exception as e: logger.error(f"Error fulfillment {asin}: {e}")

            try:
                d = ex_dimensions.get_dimensions_and_price(asin)
                if d: combined_data.update(d)
            except Exception as e: logger.error(f"Error dimensions {asin}: {e}")

            try:
                r = ex_ranks.get_product_ranks(asin)
                if r: 
                    r["SecondaryRanks"] = str(r.get("SecondaryRanks", []))
                    combined_data.update(r)
            except Exception as e: logger.error(f"Error ranks {asin}: {e}")

            try:
                img = ex_images.get_product_images(asin)
                if img and "Images" in img: combined_data["Images"] = ", ".join(img["Images"])
            except Exception as e: logger.error(f"Error images {asin}: {e}")

            try:
                v = ex_videos.has_videos(asin)
                if v: combined_data.update(v)
            except Exception as e: logger.error(f"Error videos {asin}: {e}")

            try:
                s = ex_stock.get_stock(asin)
                combined_data["Stock"] = s["Stock"]
                combined_data["StockStatus"] = s["StockStatus"]
            except Exception as e: logger.error(f"Error stock {asin}: {e}")

            try:
                rc = ex_review_count.get_review_count(asin)
                if rc: combined_data.update(rc)
            except Exception as e: logger.error(f"Error review_count {asin}: {e}")

            try:
                ps = ex_past_sales.get_past_month_sales(asin)
                if ps and "PastMonthSales" in ps: combined_data["PastMonthSales"] = ps["PastMonthSales"]
            except Exception as e: logger.error(f"Error past_sales {asin}: {e}")

            all_results.append(combined_data)
        return all_results
