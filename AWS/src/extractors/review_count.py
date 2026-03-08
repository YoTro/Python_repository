import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class ReviewCountExtractor(AmazonBaseScraper):
    """
    Extractor to fetch the total customer review count for a product.
    """

    def get_review_count(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and extract the total review count.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing ASIN and ReviewCount (int).
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching review count for ASIN: {asin}")
        
        html = self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for ASIN {asin}")
            return {"ASIN": asin, "ReviewCount": 0}
            
        soup = BeautifulSoup(html, 'html.parser')
        review_count = 0
        
        # Method A: Look for the specific element ID that Amazon often uses for total ratings/reviews
        count_span = soup.find('span', id='acrCustomerReviewText')
        if count_span:
            text = count_span.get_text(strip=True)
            # Usually says "1,234 ratings" or "1,234 customer reviews"
            match = re.search(r'([\d,]+)\s+rating', text, re.IGNORECASE)
            if not match:
                match = re.search(r'([\d,]+)\s+customer review', text, re.IGNORECASE)
                
            if match:
                review_count = int(match.group(1).replace(',', ''))
            else:
                # If the string doesn't match standard patterns, try to just pull the first number
                match_num = re.search(r'([\d,]+)', text)
                if match_num:
                    review_count = int(match_num.group(1).replace(',', ''))
                    
        # Method B: Legacy Regex Fallback
        if review_count == 0:
            match = re.search(r'<span id="acrCustomerReviewText" class="[^"]*">(.*?)\s+customer reviews', html)
            if match:
                review_count = int(match.group(1).replace(',', ''))
                
        return {
            "ASIN": asin,
            "ReviewCount": review_count
        }
