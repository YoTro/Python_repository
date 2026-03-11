import logging
from typing import List, Dict, Any
from .base_task import BaseTask
from src.analysis.similarity import ProductSimilarityAnalysis
from src.utils.csv_helper import CSVHelper

logger = logging.getLogger(__name__)

class AnalyzeSimilarityTask(BaseTask):
    def execute(self, args, context) -> List[Dict]:
        if not args.input:
            logger.error("--input (CSV file with product data) is required for 'analyze_similarity' task.")
            return []
        
        logger.info(f"Loading data from {args.input} for similarity analysis.")
        data = CSVHelper.read_csv(args.input)
        if not data:
            logger.error("No data found in input file.")
            return []

        analyzer = ProductSimilarityAnalysis(data)
        if analyzer.fit():
            analyzer.cluster_products(n_clusters=args.clusters, method=args.cluster_method)
            results = analyzer.get_analyzed_data()
            logger.info(f"Analysis complete using {args.cluster_method}. Found {len(results)} analyzed rows.")
            return results
        else:
            logger.error("Failed to fit the analyzer.")
            return []
