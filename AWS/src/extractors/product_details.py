import logging
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class ProductDetailsExtractor(AmazonBaseScraper):
    """
    Extractor to fetch product details (5 features/bullets and description) from an Amazon listing.
    """

    def get_product_details(self, url: str) -> dict:
        """
        Fetch the product page and extract features and description.
        
        :param url: The Amazon product URL.
        :return: A dictionary containing URL, Features (list), and Description (string).
        """
        logger.info(f"Fetching product details for: {url}")
        html_content = self.fetch(url)
        if not html_content:
            logger.warning(f"Failed to fetch content for {url}")
            return {"URL": url, "Features": [], "Description": ""}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 1. Extract 5 Features (bullet points)
        features = []
        feature_bullets_div = soup.find('div', id='feature-bullets')
        if feature_bullets_div:
            # Find all list items inside the bullets section
            list_items = feature_bullets_div.find_all('li')
            for li in list_items:
                # Features are typically wrapped in span with class 'a-list-item'
                span = li.find('span', class_='a-list-item')
                if span:
                    text = span.get_text(strip=True)
                    if text:
                        features.append(text)
        
        # 2. Extract Product Description
        description = ""
        desc_div = soup.find('div', id='productDescription')
        if desc_div:
            # We can extract all text inside, using newline separator for paragraphs
            description = desc_div.get_text(separator='\n', strip=True)
            
        return {
            "URL": url,
            "Features": features,
            "Description": description
        }
