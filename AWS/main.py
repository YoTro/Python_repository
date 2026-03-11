import argparse
import logging
import pandas as pd
from typing import List, Dict

# Import the core and utils
from src.utils.csv_helper import CSVHelper
from src.utils.config_helper import ConfigHelper
from src.core.proxy import ProxyManager
from src.tasks.factory import TaskFactory

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AWS_Main")

def main():
    ConfigHelper.load_config()
    
    parser = argparse.ArgumentParser(description="AWS (Amazon Web Scraper) V2")
    
    # Available tasks derived from the factory
    available_tasks = TaskFactory.get_available_tasks()
    
    parser.add_argument("task", choices=available_tasks, help="Task to perform")
    parser.add_argument("--keyword", type=str, help="Keyword for search tasks (e.g., sales, keywords_rank)")
    parser.add_argument("--url", type=str, help="Specific URL for tasks like bestsellers or product_num")
    parser.add_argument("--input", type=str, help="Input CSV file containing ASINs or URLs")
    parser.add_argument("--output", type=str, required=True, help="Output CSV file path")
    parser.add_argument("--pages", type=int, default=1, help="Number of pages to fetch (for tasks that support pagination)")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy support")
    parser.add_argument("--clusters", type=int, default=None, help="Number of clusters for analyze_similarity task")
    parser.add_argument("--cluster-method", choices=["kmeans", "dbscan"], default="kmeans", help="Clustering method to use")
    parser.add_argument("--rank-col", type=str, default="PrimaryRank", help="Column name for Rank in analyze_sales_rank task")
    parser.add_argument("--sales-col", type=str, default="Orders", help="Column name for Sales in analyze_sales_rank task")
    parser.add_argument("--date-col", type=str, default="Time", help="Column name for Date in analyze_sales_rank task")
    
    args = parser.parse_args()

    # Proxy handling
    proxies = None
    if args.use_proxy:
        proxy_file = ConfigHelper.get("network.proxy_file", "config/proxies.txt")
        pm = ProxyManager(proxy_file)
        proxies = pm.get_random_proxy()
        logger.info(f"Using proxy: {proxies}")

    # Load ASINs if input file is provided
    asins = []
    if args.input:
        asins = CSVHelper.read_asins_from_csv(args.input, column_name="ASIN")
        if not asins:
            asins = CSVHelper.read_asins_from_csv(args.input, column_name="URL")
        logger.info(f"Loaded {len(asins)} items from {args.input}")

    # Prepare context for the task
    context = {
        "proxies": proxies,
        "asins": asins
    }

    try:
        # 1. Get task instance via factory
        task_runner = TaskFactory.get_task(args.task)
        
        # 2. Execute the task
        logger.info(f"Executing task: {args.task}")
        all_results = task_runner.execute(args, context)
        
        # 3. Save results
        if all_results:
            CSVHelper.save_to_csv(all_results, args.output)
            logger.info(f"Task '{args.task}' completed. Results saved to {args.output}")
        else:
            logger.warning(f"No data was extracted for task '{args.task}'.")
            
    except ValueError as e:
        logger.error(e)
    except Exception as e:
        logger.exception(f"An unexpected error occurred during task execution: {e}")

if __name__ == "__main__":
    main()
