from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class SellerFeedbackExtractor(AmazonBaseScraper):
    """
    Extractor to fetch a seller's feedback count (e.g., last 30 days) from their storefront profile.
    """

    async def get_seller_feedback_count(self, seller_id: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the 30-day feedback count for a given Amazon Seller ID.
        
        :param seller_id: The seller's Merchant ID.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing SellerID and FeedbackCount (for the last 30 days).
        """
        # Endpoint to view seller profile and feedback
        url = f"{host}/sp?seller={seller_id}"
        logger.info(f"Fetching feedback count for seller: {seller_id}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for seller {seller_id}")
            return {"SellerID": seller_id, "FeedbackCount": None}
            
        soup = BeautifulSoup(html, 'html.parser')
        
        feedback_count = None
        
        # Look for the feedback table
        feedback_table = soup.find('table', id='feedback-summary-table')
        if feedback_table:
            # Usually the first row of data (after headers) is for 30 days, the last column might be "Count"
            # Alternatively, find the specific row header for "Count"
            count_th = feedback_table.find('th', string=re.compile(r'Count', re.IGNORECASE))
            if count_th:
                # Get the parent tr, then look at its td elements
                row = count_th.find_parent('tr')
                if row:
                    # Usually the first td after the th is for 30 days
                    td = row.find('td')
                    if td:
                        span = td.find('span')
                        if span:
                            feedback_count = span.get_text(strip=True).replace(',', '')
                        else:
                            feedback_count = td.get_text(strip=True).replace(',', '')
                            
        # Fallback to legacy regex
        if not feedback_count:
            # Matches: Count</td><td class="a-text-right"><span>123</span>
            match = re.search(r'Count<\/td>\s*<td[^>]*>\s*<span>(.*?)<\/span>', html)
            if match:
                feedback_count = match.group(1).replace(',', '')
                
        return {
            "SellerID": seller_id,
            "FeedbackCount": feedback_count
        }
