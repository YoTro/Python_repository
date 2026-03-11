import logging
from typing import List, Dict, Any
from .base_task import BaseTask
from src.extractors.products_num import ProductsNumExtractor
from src.extractors.feedback import SellerFeedbackExtractor

logger = logging.getLogger(__name__)

class ProductNumTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not args.url and not asins:
            logger.error("Either --url or --input (with URLs) is required for 'product_num' task.")
            return []
        extractor = ProductsNumExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        urls_to_process = [args.url] if args.url else asins
        return [extractor.get_seller_and_products_count(url) for url in urls_to_process]

class FeedbackTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not asins:
            logger.error("--input is required and must contain Seller IDs.")
            return []
        extractor = SellerFeedbackExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return [extractor.get_seller_feedback_count(seller_id) for seller_id in asins]
