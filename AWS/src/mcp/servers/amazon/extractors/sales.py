from __future__ import annotations
import re
import logging
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class SalesExtractor(AmazonBaseScraper):
    """
    Extractor to find the sales ASINs in Amazon search results.
    """
    
    async def extract_sales_from_search(self, keyword: str, page: int = 1) -> list:
        """
        Search for a keyword and extract ASINs from the results page.
        
        :param keyword: The search term (e.g. "outdoor rug")
        :param page: The page number to fetch
        :return: A list of ASINs found on the page.
        """
        # Format the URL
        url = f'https://www.amazon.com/s?k={keyword.replace(" ", "+")}&page={page}'
        logger.info(f"Fetching search page: {url}")
        
        html = await self.fetch(url)
        if not html:
            logger.error(f"Failed to retrieve HTML for keyword: {keyword}, page: {page}")
            return []
            
        # Regex based on the original legacy implementation
        # Note: Amazon's HTML structure changes frequently, consider using BeautifulSoup here in the future.
        matches = re.findall(r'dp\/(B[A-Z0-9]{9}).*?a-row a-size-base\"><span class=\"a-size-base a-color-secondary\">(.*?)<\/span>', html)
        
        # matches is expected to be a list of tuples like [('B01XXXXXXX', 'Some text'), ...]
        # We might just want the ASINs
        asins = [m[0] for m in matches]
        logger.info(f"Found {len(asins)} ASINs for keyword: {keyword} on page {page}")
        
        return asins

    async def get_sales_data(self, keyword: str, page: int = 1) -> list:
        """
        Higher level function returning structured data for CSV saving.
        """
        asins = await self.extract_sales_from_search(keyword, page)
        
        results = []
        for asin in asins:
            results.append({
                "Keyword": keyword,
                "Page": page,
                "ASIN": asin
            })
            
        return results
