import logging
from typing import List, Dict, Any
from .base_task import BaseTask
from src.extractors.sales import SalesExtractor
from src.extractors.keywords_rank import KeywordsRankExtractor
from src.extractors.bestsellers import BestSellersExtractor

logger = logging.getLogger(__name__)

class SalesTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        if not args.keyword:
            logger.error("--keyword is required for the 'sales' task.")
            return []
        extractor = SalesExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        results = []
        for p in range(1, args.pages + 1):
            data = extractor.get_sales_data(args.keyword, p)
            results.extend(data)
        return results

class KeywordsRankTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        asins = context.get('asins', [])
        if not args.keyword or not asins:
            logger.error("--keyword and --input (with target ASINs) are required.")
            return []
        extractor = KeywordsRankExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return extractor.get_asin_ranks_for_keyword(args.keyword, asins, max_pages=args.pages or 3)

class BestsellersTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        if not args.url:
            logger.error("--url is required for the 'bestsellers' task.")
            return []
        extractor = BestSellersExtractor(use_proxy=args.use_proxy, proxies_dict=context.get('proxies'))
        return extractor.get_bestsellers(args.url)
