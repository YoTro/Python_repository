import sys
import os
import logging
import pandas as pd
import time

# Add the root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extractors.sales import SalesExtractor
from src.extractors.product_details import ProductDetailsExtractor
from src.analysis.similarity import ProductSimilarityAnalysis
from src.utils.csv_helper import CSVHelper
from src.utils.config_helper import ConfigHelper

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("RealClusteringComparison")

def run_real_comparison():
    ConfigHelper.load_config()
    keyword = "mosquito repellent outdoor patio"
    
    # 1. Scrape ASINs
    logger.info(f"--- Step 1: Scraping ASINs for '{keyword}' ---")
    sales_ex = SalesExtractor()
    asins = sales_ex.extract_sales_from_search(keyword, page=1)
    
    if not asins:
        logger.error("No ASINs found. Scraper might be blocked or keyword invalid.")
        return

    # Take top 15 for a meaningful clustering test
    test_asins = asins[:15]
    logger.info(f"Found {len(asins)} ASINs. Fetching details for top {len(test_asins)}...")

    # 2. Scrape Details
    logger.info("--- Step 2: Fetching Product Details ---")
    details_ex = ProductDetailsExtractor()
    all_details = []
    for asin in test_asins:
        url = f"https://www.amazon.com/dp/{asin}"
        data = details_ex.get_product_details(url)
        if data and data.get('Title'):
            data['ASIN'] = asin
            all_details.append(data)
            logger.info(f"Fetched: {data['Title'][:50]}...")
        else:
            logger.warning(f"Failed to fetch details for ASIN: {asin}")
        time.sleep(2) # Politeness delay

    if len(all_details) < 3:
        logger.error("Not enough products fetched for meaningful clustering.")
        return

    # 3. Analyze Similarity
    logger.info("--- Step 3: Performing Similarity Analysis ---")
    analyzer = ProductSimilarityAnalysis(all_details)
    if not analyzer.fit():
        logger.error("Failed to fit TF-IDF.")
        return

    # A. Run KMeans
    n_kmeans = max(3, len(all_details) // 4)
    logger.info(f"Running KMeans (n_clusters={n_kmeans})...")
    analyzer.cluster_products(n_clusters=n_kmeans, method='kmeans')
    kmeans_data = analyzer.get_analyzed_data()
    kmeans_clusters = [item['Cluster'] for item in kmeans_data]

    # B. Run DBSCAN
    logger.info("Running DBSCAN (eps=0.7)...")
    analyzer.cluster_products(method='dbscan') 
    dbscan_data = analyzer.get_analyzed_data()
    dbscan_clusters = [item['Cluster'] for item in dbscan_data]

    # Combine for comparison
    comparison_list = []
    for i in range(len(all_details)):
        comparison_list.append({
            "ASIN": all_details[i]['ASIN'],
            "Title": all_details[i]['Title'][:50] + "...",
            "KMeans": kmeans_clusters[i],
            "DBSCAN": dbscan_clusters[i]
        })

    df_comp = pd.DataFrame(comparison_list)
    
    print("\n" + "="*90)
    print(f"REAL DATA CLUSTERING COMPARISON (N={len(all_details)})")
    print("="*90)
    print(df_comp.to_string(index=False))
    print("="*90)
    
    # Save to data folder
    os.makedirs("data", exist_ok=True)
    output_path = "data/real_clustering_comparison.csv"
    df_comp.to_csv(output_path, index=False)
    logger.info(f"Comparison results saved to {output_path}")

if __name__ == "__main__":
    run_real_comparison()
