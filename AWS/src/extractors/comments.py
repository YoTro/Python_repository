import logging
import math
import random
import time
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class CommentsExtractor(AmazonBaseScraper):
    """
    Extractor to fetch comments/reviews for a given Amazon ASIN.
    """

    def fetch_comments_page(self, asin: str, page: int = 1) -> list:
        """
        Fetch a single page of reviews for an ASIN via the Amazon Reviews AJAX endpoint.
        Returns a list of extracted review texts.
        """
        url = "https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_viewopt_sr"
        
        headers = self._get_default_headers()
        # Specific headers required for the AJAX endpoint
        headers.update({
            "accept": "text/html,*/*",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "origin": "https://www.amazon.com",
            "referer": f"https://www.amazon.com/product-reviews/{asin}/ref=acr_dp_hist_1?ie=UTF8&filterByStar=one_star&reviewerType=all_reviews",
            "x-requested-with": "XMLHttpRequest"
        })

        payload = {
            "sortBy": "",
            "reviewerType": "all_reviews",
            "formatType": "",
            "mediaType": "",
            "filterByStar": "all_stars",
            "pageNumber": str(page),
            "filterByLanguage": "",
            "filterByKeyword": "",
            "shouldAppend": "undefined",
            "deviceType": "desktop",
            "canShowIntHeader": "undefined",
            "reftag": "cm_cr_arp_d_viewopt_sr",
            "pageSize": "10",
            "asin": asin,
            "scope": "reviewsAjax0"
        }

        logger.info(f"Fetching reviews for ASIN: {asin}, Page: {page}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.post(url, headers=headers, data=payload, timeout=15)
                response.raise_for_status()
                
                # The response from this endpoint is typically a series of HTML fragments separated by `&&&`
                # We can use BeautifulSoup to parse the HTML and find the review spans.
                # This is a much more robust approach than the legacy regex.
                html_content = response.text
                
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Amazon typically stores review text in elements with class `review-text-content`
                review_elements = soup.find_all('span', class_='review-text-content')
                
                reviews = []
                for el in review_elements:
                    # Extract text and clean up whitespace
                    text = el.get_text(strip=True)
                    if text:
                        reviews.append(text)
                
                logger.info(f"Found {len(reviews)} reviews on page {page} for ASIN {asin}")
                return reviews
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to fetch comments for {asin}: {e}")
                time.sleep(random.uniform(2, 5))
                headers["User-Agent"] = self._get_random_ua()
                
        logger.error(f"Failed to fetch comments for {asin} after {max_retries} attempts.")
        return []

    def get_all_comments(self, asin: str, max_pages: int = 2) -> list:
        """
        Fetch reviews across multiple pages.
        Returns structured data suitable for CSV export.
        """
        all_reviews = []
        for page in range(1, max_pages + 1):
            reviews = self.fetch_comments_page(asin, page)
            if not reviews:
                # If a page returns no reviews, it's likely we've hit the end
                break
                
            for review in reviews:
                all_reviews.append({
                    "ASIN": asin,
                    "Page": page,
                    "Review": review
                })
            
            # Polite delay between pages
            if page < max_pages:
                time.sleep(random.uniform(1.5, 4.0))
                
        return all_reviews
