import sys
import os
import logging
import time

# Add the root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.sales import SalesExtractor
from src.extractors.product_details import ProductDetailsExtractor
from src.analysis.similarity import ProductSimilarityAnalysis
from src.utils.csv_helper import CSVHelper
from src.utils.config_helper import ConfigHelper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestSimilarityFlow")

def test_full_flow():
    ConfigHelper.load_config()
    
    keyword = "mosquito repellent outdoor patio"
    output_dir = "tests/test_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Scrape ASINs
    logger.info(f"--- Step 1: Scraping ASINs for '{keyword}' ---")
    sales_ex = SalesExtractor()
    asins = sales_ex.extract_sales_from_search(keyword, page=1)
    
    if not asins:
        logger.error("No ASINs found. Skipping rest of the test.")
        return

    # Take top 5 for a quick test
    test_asins = asins[:5]
    logger.info(f"Found {len(asins)} ASINs. Testing with first {len(test_asins)} items.")

    # 2. Scrape Details
    logger.info("--- Step 2: Fetching Product Details ---")
    details_ex = ProductDetailsExtractor()
    all_details = []
    for asin in test_asins:
        url = f"https://www.amazon.com/dp/{asin}"
        data = details_ex.get_product_details(url)
        if data:
            data['ASIN'] = asin
            all_details.append(data)
        time.sleep(2) # Be polite

    if not all_details:
        logger.error("No product details extracted. Skipping analysis.")
        return

    # 3. Analyze Similarity
    logger.info("--- Step 3: Performing Similarity Analysis ---")
    analyzer = ProductSimilarityAnalysis(all_details)
    if analyzer.fit():
        # Perform clustering with 2 groups for test
        analyzer.cluster_products(n_clusters=2)
        analyzed_data = analyzer.get_analyzed_data()
        
        # 4. Save and Verify
        result_path = os.path.join(output_dir, "test_similarity_results.csv")
        CSVHelper.save_to_csv(analyzed_data, result_path)
        logger.info(f"Test complete! Results saved to {result_path}")
    else:
        logger.error("Analysis fitting failed.")

if __name__ == "__main__":
    test_full_flow()
