import argparse
import logging
from typing import List, Dict

# Import the core and utils
from src.utils.csv_helper import CSVHelper
from src.utils.config_helper import ConfigHelper
from src.core.proxy import ProxyManager
# Import all extractors
from src.extractors.sales import SalesExtractor
from src.extractors.comments import CommentsExtractor
from src.extractors.products_num import ProductsNumExtractor
from src.extractors.product_details import ProductDetailsExtractor
from src.extractors.product_details import ProductDetailsExtractor
from src.extractors.fulfillment import FulfillmentExtractor
from src.extractors.dimensions import DimensionsExtractor
from src.extractors.bestsellers import BestSellersExtractor
from src.extractors.keywords_rank import KeywordsRankExtractor
from src.extractors.ranks import RanksExtractor
from src.extractors.feedback import SellerFeedbackExtractor
from src.extractors.images import ImageExtractor
from src.extractors.videos import VideoExtractor
from src.extractors.cart_stock import CartStockExtractor
from src.extractors.review_count import ReviewCountExtractor
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AWS_Main")

def main():
    ConfigHelper.load_config()
    
    parser = argparse.ArgumentParser(description="AWS (Amazon Web Scraper) V2")
    
    # Available tasks corresponding to the extractors
    tasks = [
        "sales", "reviews", "product_num", "details", 
        "fulfillment", "dimensions", "bestsellers", "keywords_rank", 
        "ranks", "feedback", "images", "videos", "stock", "review_count", "full_asin_details"
    ]
    
    parser.add_argument("task", choices=tasks, help="Task to perform")
    parser.add_argument("--keyword", type=str, help="Keyword for search tasks (e.g., sales, keywords_rank)")
    parser.add_argument("--url", type=str, help="Specific URL for tasks like bestsellers or product_num")
    parser.add_argument("--input", type=str, help="Input CSV file containing ASINs or URLs")
    parser.add_argument("--output", type=str, required=True, help="Output CSV file path")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch (for tasks that support pagination)")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy support")
    
    args = parser.parse_args()

    # Proxy handling
    proxies = None
    if args.use_proxy:
        proxy_file = ConfigHelper.get("network.proxy_file", "config/proxies.txt")
        pm = ProxyManager(proxy_file)

        # Optionally, you can verify proxies here before using them:
        # working_proxies = pm.get_verified_proxies()
        # if working_proxies:
        #     pm.proxies["http"] = working_proxies

        proxies = pm.get_random_proxy()
        logger.info(f"Using proxy: {proxies}")

    # Load ASINs if input file is provided
    asins = []
    if args.input:
        # We try to load "ASIN" column by default, but some tasks might use "URL"
        asins = CSVHelper.read_asins_from_csv(args.input, column_name="ASIN")
        if not asins:
            # Fallback to reading a URL column if ASINs are empty
            asins = CSVHelper.read_asins_from_csv(args.input, column_name="URL")
        logger.info(f"Loaded {len(asins)} items from {args.input}")

    all_results: List[Dict] = []

    # Route to the appropriate extractor based on the selected task
    if args.task == "sales":
        if not args.keyword:
            logger.error("--keyword is required for the 'sales' task.")
            return
        extractor = SalesExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for p in range(1, args.pages + 1):
            data = extractor.get_sales_data(args.keyword, p)
            all_results.extend(data)
            
    elif args.task == "reviews":
        if not asins:
            logger.error("--input is required and must contain ASINs for the 'reviews' task.")
            return
        extractor = CommentsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_all_comments(asin, max_pages=args.pages)
            all_results.extend(data)
            
    elif args.task == "product_num":
        if not args.url and not asins:
            logger.error("Either --url or --input (with URLs) is required for 'product_num' task.")
            return
        extractor = ProductsNumExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        urls_to_process = [args.url] if args.url else asins
        for url in urls_to_process:
            data = extractor.get_seller_and_products_count(url)
            all_results.append(data)
            
    elif args.task == "details":
        if not asins:
            logger.error("--input (with URLs or ASINs formatted as URLs) is required.")
            return
        extractor = ProductDetailsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for item in asins:
            url = item if "http" in item else f"https://www.amazon.com/dp/{item}"
            data = extractor.get_product_details(url)
            all_results.append(data)
            
    elif args.task == "qa":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = QAExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_qa_data(asin)
            all_results.extend(data)
            
    elif args.task == "fulfillment":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = FulfillmentExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_fulfillment_info(asin)
            all_results.append(data)

    elif args.task == "dimensions":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = DimensionsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_dimensions_and_price(asin)
            all_results.append(data)

    elif args.task == "bestsellers":
        if not args.url:
            logger.error("--url is required for the 'bestsellers' task.")
            return
        extractor = BestSellersExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        all_results = extractor.get_bestsellers(args.url)

    elif args.task == "keywords_rank":
        if not args.keyword or not asins:
            logger.error("--keyword and --input (with target ASINs) are required.")
            return
        extractor = KeywordsRankExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        # Scan multiple pages (default 3) to find the ASINs
        all_results = extractor.get_asin_ranks_for_keyword(args.keyword, asins, max_pages=args.pages or 3)

    elif args.task == "ranks":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = RanksExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_product_ranks(asin)
            # Flatten secondary ranks for CSV output if needed, or just convert to string
            data["SecondaryRanks"] = str(data["SecondaryRanks"])
            all_results.append(data)

    elif args.task == "feedback":
        if not asins:
            logger.error("--input is required and must contain Seller IDs.")
            return
        extractor = SellerFeedbackExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for seller_id in asins:
            data = extractor.get_seller_feedback_count(seller_id)
            all_results.append(data)

    elif args.task == "images":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = ImageExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_product_images(asin)
            # Join list of URLs into a single string for CSV compatibility
            data["Images"] = ", ".join(data["Images"])
            all_results.append(data)

    elif args.task == "videos":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = VideoExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.has_videos(asin)
            all_results.append(data)

    elif args.task == "stock":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = CartStockExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            res = extractor.get_stock(asin)
            all_results.append({
                "ASIN": asin, 
                "Stock": res["Stock"], 
                "StockStatus": res["StockStatus"]
            })

    elif args.task == "review_count":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        extractor = ReviewCountExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        for asin in asins:
            data = extractor.get_review_count(asin)
            all_results.append(data)

    elif args.task == "full_asin_details":
        if not asins:
            logger.error("--input is required and must contain ASINs.")
            return
        logger.info(f"Starting 'full_asin_details' for {len(asins)} ASINs.")
        ex_details = ProductDetailsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_fulfillment = FulfillmentExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_dimensions = DimensionsExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_ranks = RanksExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_images = ImageExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_videos = VideoExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_stock = CartStockExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)
        ex_review_count = ReviewCountExtractor(use_proxy=args.use_proxy, proxies_dict=proxies)

        for asin in asins:
            logger.info(f"Aggregating full data for ASIN: {asin}")
            combined_data = {"ASIN": asin}
            
            # 1. Product Details
            try:
                url = asin if "http" in asin else f"https://www.amazon.com/dp/{asin}"
                details = ex_details.get_product_details(url)
                if details: combined_data.update(details)
            except Exception as e: logger.error(f"Error fetching details for {asin}: {e}")

            # 2. Fulfillment Info
            try:
                fulfillment = ex_fulfillment.get_fulfillment_info(asin)
                if fulfillment: combined_data.update(fulfillment)
            except Exception as e: logger.error(f"Error fetching fulfillment for {asin}: {e}")

            # 3. Dimensions & Weight
            try:
                dimensions = ex_dimensions.get_dimensions_and_price(asin)
                if dimensions: combined_data.update(dimensions)
            except Exception as e: logger.error(f"Error fetching dimensions for {asin}: {e}")

            # 4. Ranks
            try:
                ranks = ex_ranks.get_product_ranks(asin)
                if ranks: 
                    ranks["SecondaryRanks"] = str(ranks.get("SecondaryRanks", []))
                    combined_data.update(ranks)
            except Exception as e: logger.error(f"Error fetching ranks for {asin}: {e}")

            # 5. Images
            try:
                images = ex_images.get_product_images(asin)
                if images and "Images" in images:
                    combined_data["Images"] = ", ".join(images["Images"])
            except Exception as e: logger.error(f"Error fetching images for {asin}: {e}")

            # 6. Videos
            try:
                videos = ex_videos.has_videos(asin)
                if videos: combined_data.update(videos)
            except Exception as e: logger.error(f"Error fetching videos for {asin}: {e}")

            # 7. Stock
            try:
                stock_data = ex_stock.get_stock(asin)
                combined_data["Stock"] = stock_data["Stock"]
                combined_data["StockStatus"] = stock_data["StockStatus"]
            except Exception as e: logger.error(f"Error fetching stock for {asin}: {e}")

            # 8. Review Count
            try:
                review_count = ex_review_count.get_review_count(asin)
                if review_count: combined_data.update(review_count)
            except Exception as e: logger.error(f"Error fetching review count for {asin}: {e}")

            all_results.append(combined_data)

    else:
        logger.error(f"Task '{args.task}' is not recognized.")
        return

    # Save results
    if all_results:
        CSVHelper.save_to_csv(all_results, args.output)
    else:
        logger.warning("No data was extracted. Output file will not be created.")

if __name__ == "__main__":
    main()
