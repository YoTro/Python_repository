import logging
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans, DBSCAN
from typing import List, Dict, Optional, Union
from src.core.models import StandardProduct

logger = logging.getLogger(__name__)

class ProductSimilarityAnalysis:
    """
    Utility to analyze product similarity based on textual data.
    Uses TF-IDF for vectorization and Scikit-learn for similarity/clustering.
    """

    def __init__(self, data: Union[List[Dict], List[StandardProduct]]):
        """
        :param data: List of dictionaries or StandardProduct objects.
        """
        # Convert StandardProduct objects to dicts
        normalized_data = []
        for item in data:
            if isinstance(item, StandardProduct):
                normalized_data.append(item.to_dict())
            else:
                # Handle legacy uppercase keys and convert to lowercase standard
                norm_item = {}
                norm_item['asin'] = item.get('asin', item.get('ASIN', ''))
                norm_item['title'] = item.get('title', item.get('Title', ''))
                norm_item['features'] = item.get('features', item.get('Features', []))
                # keep other original keys just in case
                for k, v in item.items():
                    if k not in ['ASIN', 'Title', 'Features', 'asin', 'title', 'features']:
                        norm_item[k] = v
                normalized_data.append(norm_item)

        self.df = pd.DataFrame(normalized_data)
        
        if 'title' not in self.df.columns or self.df['title'].str.strip().empty:
            logger.warning("Data does not contain 'title' column. Analysis might be limited.")
            self.df['title'] = ""
            
        self.df['title'] = self.df['title'].fillna("")
        
        if 'features' in self.df.columns:
            self.df['Features_Str'] = self.df['features'].apply(lambda x: " ".join(x) if isinstance(x, list) else str(x))
        else:
            self.df['Features_Str'] = ""

        # Combine title and features for a richer text representation
        self.df['CombinedText'] = (self.df['title'] + " " + self.df['Features_Str']).str.lower()
        
        self.vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
        self.tfidf_matrix = None

    def fit(self):
        """Fit the TF-IDF vectorizer on the combined text."""
        if self.df['CombinedText'].str.strip().empty:
            logger.error("No text data available for vectorization.")
            return False
        
        logger.info(f"Vectorizing {len(self.df)} products...")
        self.tfidf_matrix = self.vectorizer.fit_transform(self.df['CombinedText'])
        return True

    def get_similarity_matrix(self) -> np.ndarray:
        """Calculate the pairwise cosine similarity matrix."""
        if self.tfidf_matrix is None:
            self.fit()
        return cosine_similarity(self.tfidf_matrix)

    def cluster_products(self, n_clusters: Optional[int] = None, method: str = 'kmeans') -> pd.DataFrame:
        """
        Group products into clusters using K-Means or DBSCAN.
        :param n_clusters: Number of clusters (for kmeans).
        :param method: 'kmeans' or 'dbscan'.
        """
        if self.tfidf_matrix is None:
            self.fit()

        num_samples = self.tfidf_matrix.shape[0]
        if num_samples < 2:
            logger.warning("Not enough samples for clustering.")
            self.df['Cluster'] = 0
            return self.df

        if method == 'kmeans':
            if n_clusters is None:
                # Simple heuristic for cluster size: roughly 1 cluster per 5-10 items
                n_clusters = max(2, num_samples // 5)
                n_clusters = min(n_clusters, 20) # Cap at 20 clusters for CLI tasks
                logger.info(f"Automatically choosing {n_clusters} clusters for KMeans.")

            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
            self.df['Cluster'] = kmeans.fit_predict(self.tfidf_matrix)
        
        elif method == 'dbscan':
            # eps is the maximum distance between two samples for one to be considered as in the neighborhood of the other.
            # For TF-IDF (normalized), cosine distance is typically used. 
            # We use 1 - cosine_similarity as metric.
            logger.info("Using DBSCAN for density-based clustering.")
            dbscan = DBSCAN(eps=0.5, min_samples=2, metric='cosine')
            self.df['Cluster'] = dbscan.fit_predict(self.tfidf_matrix)
            
        logger.info(f"Clustering complete using {method}. Found {self.df['Cluster'].nunique()} groups.")
        return self.df

    def find_top_similar(self, target_asin: str, top_n: int = 5) -> pd.DataFrame:
        """Find the most similar products to a given ASIN."""
        if self.tfidf_matrix is None:
            self.fit()

        if 'asin' not in self.df.columns:
            logger.error("asin column not found in data.")
            return pd.DataFrame()

        idx_matches = self.df.index[self.df['asin'] == target_asin].tolist()
        if not idx_matches:
            logger.error(f"ASIN {target_asin} not found in the dataset.")
            return pd.DataFrame()

        target_idx = idx_matches[0]
        sim_scores = cosine_similarity(self.tfidf_matrix[target_idx], self.tfidf_matrix).flatten()
        
        # Get indices of top_n matches (excluding itself)
        related_indices = sim_scores.argsort()[-(top_n+1):-1][::-1]
        
        results = self.df.iloc[related_indices].copy()
        results['SimilarityScore'] = sim_scores[related_indices]
        
        return results[['asin', 'title', 'SimilarityScore']]

    def get_analyzed_data(self) -> List[Dict]:
        """Return the dataframe as a list of dictionaries for saving."""
        # Clean up temporary columns before returning
        output_df = self.df.copy()
        if 'Features_Str' in output_df.columns:
            output_df.drop(columns=['Features_Str'], inplace=True)
        if 'CombinedText' in output_df.columns:
            output_df.drop(columns=['CombinedText'], inplace=True)
            
        return output_df.to_dict(orient='records')
