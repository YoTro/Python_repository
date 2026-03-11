import logging
from typing import List, Dict, Any
from .base_task import BaseTask
from src.extractors.comments import CommentsExtractor
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

class ReviewsTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs for the 'reviews' task.")
            return []
        extractor = CommentsExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for asin in asins:
            data = extractor.get_all_comments(asin, max_pages=args.pages)
            results.extend(data)
        return results

class DetailsTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input (with URLs or ASINs) is required.")
            return []
        extractor = ProductDetailsExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for item in asins:
            url = item if "http" in item else f"https://www.amazon.com/dp/{item}"
            data = extractor.get_product_details(url)
            results.append(data)
        return results

class FulfillmentTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = FulfillmentExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.get_fulfillment_info(asin) for asin in asins]

class DimensionsTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = DimensionsExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.get_dimensions_and_price(asin) for asin in asins]

class RanksTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = RanksExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for asin in asins:
            data = extractor.get_product_ranks(asin)
            data["SecondaryRanks"] = str(data.get("SecondaryRanks", []))
            results.append(data)
        return results

class ImagesTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = ImageExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for asin in asins:
            data = extractor.get_product_images(asin)
            if "Images" in data:
                data["Images"] = ", ".join(data["Images"])
            results.append(data)
        return results

class VideosTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = VideoExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.has_videos(asin) for asin in asins]

class StockTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = CartStockExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for asin in asins:
            res = extractor.get_stock(asin)
            results.append({"ASIN": asin, "Stock": res["Stock"], "StockStatus": res["StockStatus"]})
        return results

class ReviewCountTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = ReviewCountExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.get_review_count(asin) for asin in asins]

class PastMonthSalesTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return []
        extractor = PastMonthSalesExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.get_past_month_sales(asin) for asin in asins]
