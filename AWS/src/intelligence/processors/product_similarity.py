from __future__ import annotations
import logging
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans, DBSCAN
from typing import List, Dict, Optional, Union
from src.core.models import Product

logger = logging.getLogger(__name__)

class ProductSimilarityProcessor:
    """
    Intelligence Processor to analyze product similarity based on textual data.
    Uses TF-IDF for vectorization and Scikit-learn for similarity/clustering.
    """

    def __init__(self, products: List[Product]):
        self.products = products
        self.df = pd.DataFrame([p.model_dump() for p in products])
        
        # Ensure 'title' and 'features' exist even if empty
        self.df['title'] = self.df['title'].fillna("")
        if 'features' in self.df.columns:
            self.df['Features_Str'] = self.df['features'].apply(lambda x: " ".join(x) if isinstance(x, list) else str(x))
        else:
            self.df['Features_Str'] = ""

        # Combine title and features for a richer text representation
        self.df['CombinedText'] = (self.df['title'] + " " + self.df['Features_Str']).str.lower()
        
        self.vectorizer = TfidfVectorizer(stop_words='english', max_features=5000)
        self.tfidf_matrix = None
        self.is_fitted = False

    def fit(self) -> bool:
        """Fit the TF-IDF vectorizer on the combined text."""
        if self.df['CombinedText'].str.strip().empty:
            logger.error("No text data available for vectorization. Cannot fit similarity model.")
            return False
        
        logger.info(f"Vectorizing {len(self.df)} products for similarity analysis...")
        self.tfidf_matrix = self.vectorizer.fit_transform(self.df['CombinedText'])
        self.is_fitted = True
        return True

    def get_similarity_matrix(self) -> Optional[np.ndarray]:
        """Calculate the pairwise cosine similarity matrix."""
        if not self.is_fitted: self.fit()
        if self.tfidf_matrix is None: return None
        return cosine_similarity(self.tfidf_matrix)

    def cluster_products(self, n_clusters: Optional[int] = None, method: str = 'kmeans') -> List[Dict]:
        """
        Group products into clusters using K-Means or DBSCAN.
        Returns a list of dictionaries with product ASIN and assigned Cluster ID.
        """
        if not self.is_fitted: self.fit()
        if self.tfidf_matrix is None or self.tfidf_matrix.shape[0] < 2:
            logger.warning("Not enough samples for clustering. Returning unclustered data.")
            return [{**p.model_dump(), "cluster_id": 0} for p in self.products]

        num_samples = self.tfidf_matrix.shape[0]

        if method == 'kmeans':
            if n_clusters is None: n_clusters = max(2, num_samples // 5)
            n_clusters = min(n_clusters, 20) # Cap for practical use
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init='auto')
            self.df['cluster_id'] = kmeans.fit_predict(self.tfidf_matrix)
        
        elif method == 'dbscan':
            dbscan = DBSCAN(eps=0.5, min_samples=2, metric='cosine')
            self.df['cluster_id'] = dbscan.fit_predict(self.tfidf_matrix)
            
        logger.info(f"Clustering complete using {method}. Found {self.df['cluster_id'].nunique()} groups.")
        
        # Merge cluster_id back to original product data for output
        results = self.df[['asin', 'cluster_id']].to_dict(orient='records')
        product_map = {p.asin: p for p in self.products}
        
        output = []
        for res in results:
            p = product_map.get(res['asin'])
            if p:
                output.append({**p.model_dump(), "cluster_id": res['cluster_id']})
        return output

    def find_top_similar(self, target_asin: str, top_n: int = 5) -> List[Dict]:
        """
        Find the most similar products to a given ASIN within the dataset.
        Returns a list of dictionaries with similar products and their scores.
        """
        if not self.is_fitted: self.fit()
        if self.tfidf_matrix is None: return []

        idx_matches = self.df.index[self.df['asin'] == target_asin].tolist()
        if not idx_matches:
            logger.error(f"Target ASIN {target_asin} not found for similarity analysis.")
            return []

        target_idx = idx_matches[0]
        sim_scores = cosine_similarity(self.tfidf_matrix[target_idx], self.tfidf_matrix).flatten()
        
        # Get indices of top_n matches (excluding itself)
        related_indices = sim_scores.argsort()[-(top_n+1):-1][::-1]
        
        results_df = self.df.iloc[related_indices].copy()
        results_df['similarity_score'] = sim_scores[related_indices]
        
        # Prepare output with product details and similarity score
        output = []
        for _, row in results_df.iterrows():
            p = Product.model_validate(row.drop('similarity_score').to_dict())
            output.append({**p.model_dump(), "similarity_score": row['similarity_score']})
            
        return output
