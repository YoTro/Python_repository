import logging
from typing import List, Dict, Any
from src.tasks.base_task import BaseTask
from src.extractors.products_num import ProductsNumExtractor
from src.extractors.bestsellers import BestSellersExtractor

logger = logging.getLogger("AWS_UrlTasks")

class ProductNumTask(BaseTask):
    def execute(self, args: Any, context: Dict[str, Any]) -> List[Dict]:
        asins = context.get('asins', [])
        if not args.url and not asins:
            logger.error("Either --url or --input (with URLs) is required for 'product_num' task.")
            return []
            
        proxies = context.get('proxies')
        extractor = ProductsNumExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        urls_to_process = [args.url] if args.url else asins
        all_results = []
        for url in urls_to_process:
            data = extractor.get_seller_and_products_count(url)
            if data:
                all_results.append(data)
        return all_results

class BestsellersTask(BaseTask):
    def execute(self, args: Any, context: Dict[str, Any]) -> List[Dict]:
        if not args.url:
            logger.error("--url is required for the 'bestsellers' task.")
            return []
            
        proxies = context.get('proxies')
        extractor = BestSellersExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        return extractor.get_bestsellers(args.url) or []
