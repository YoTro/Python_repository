from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class KeywordsRankExtractor(AmazonBaseScraper):
    """
    Extractor to find the rank (position) of specific ASINs for a given keyword search.
    """

    async def get_keyword_results_count(self, keyword: str) -> int:
        """
        Fetch the first search page to determine the total number of results.
        
        :param keyword: The search keyword.
        :return: Total number of results (int).
        """
        url = f"https://www.amazon.com/s?k={keyword.replace(' ', '+')}&page=1"
        logger.info(f"Fetching keyword results count for: {keyword}")
        
        html = await self.fetch(url)
        if not html:
            return 0
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Amazon search result count is usually in a span containing text like "1-48 of over 10,000 results for"
        count_text = soup.get_text()
        
        # Regex to find the total number of results
        match = re.search(r'of (?:over )?([\d,]+) results for', count_text)
        if match:
            try:
                # Remove commas and convert to int
                return int(match.group(1).replace(',', ''))
            except ValueError:
                pass
                
        # Legacy fallback
        match_legacy = re.search(r'a-section a-spacing-small a-spacing-top-small\">\s+<span>.*?over (.*?)results for', html)
        if match_legacy:
            try:
                return int(match_legacy.group(1).replace(',', '').strip())
            except ValueError:
                pass

        logger.warning(f"Could not parse results count for keyword: {keyword}")
        return 0

    async def get_asin_ranks_for_keyword(self, keyword: str, target_asins: list, max_pages: int = 3) -> list:
        """
        Find the specific position (rank) of target ASINs across multiple search pages.
        
        :param keyword: The search keyword.
        :param target_asins: List of ASINs to look for.
        :param max_pages: Maximum number of pages to scan (default 3).
        :return: A list of dicts with rank info for each found ASIN.
        """
        all_ranks = []
        found_asins = set()
        
        for page in range(1, max_pages + 1):
            url = f"https://www.amazon.com/s?k={keyword.replace(' ', '+')}&page={page}"
            logger.info(f"Scanning page {page} for keyword '{keyword}'")
            
            html = await self.fetch(url)
            if not html:
                continue
                
            soup = BeautifulSoup(html, 'html.parser')
            
            # Find results via data-asin and data-index
            search_results = soup.find_all('div', attrs={'data-asin': True, 'data-index': True})
            
            # If no results found, might be a different layout, try fallback
            if not search_results:
                logger.debug(f"No results with data-index found on page {page}. Trying fallback.")
                for asin in target_asins:
                    if asin in found_asins: continue
                    match = re.search(f'data-asin="{asin}" data-index="(\d+)"', html)
                    if match:
                        rank_val = int(match.group(1))
                        all_ranks.append({
                            "ASIN": asin,
                            "Keyword": keyword,
                            "Page": page,
                            "Rank": rank_val
                        })
                        found_asins.add(asin)
            else:
                for result in search_results:
                    asin = result.get('data-asin')
                    if asin and asin in target_asins and asin not in found_asins:
                        try:
                            index = int(result.get('data-index'))
                            all_ranks.append({
                                "ASIN": asin,
                                "Keyword": keyword,
                                "Page": page,
                                "Rank": index
                            })
                            found_asins.add(asin)
                        except ValueError:
                            pass
            
            # Optional: if all target ASINs found, break early
            if len(found_asins) == len(target_asins):
                break
                    
        return all_ranks
