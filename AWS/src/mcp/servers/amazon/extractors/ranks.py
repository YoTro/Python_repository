from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class RanksExtractor(AmazonBaseScraper):
    """
    Extractor to fetch the Best Sellers Rank (BSR) from a product listing.
    """

    async def get_product_ranks(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and extract its Best Sellers Rank.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing ASIN, Primary Rank, and Category.
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching BSR for ASIN: {asin}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for ASIN {asin}")
            return {"ASIN": asin, "URL": url, "PrimaryRank": None, "Category": None, "SecondaryRanks": []}
            
        soup = BeautifulSoup(html, 'html.parser')
        
        primary_rank = None
        category = None
        secondary_ranks = []
        
        # Method A: Extract from the product details table
        details_table = soup.find('table', id='productDetails_techSpec_section_1')
        if not details_table:
            # Sometimes it's in a different details section
            details_table = soup.find('div', id='detailBulletsWrapper_feature_div')

        if details_table:
            # Look for text mentioning "Best Sellers Rank"
            text_content = details_table.get_text(separator=' ', strip=True)
            
            # The structure is usually "#123 in Category"
            # Using Regex to parse the extracted text
            bsr_matches = re.findall(r'#([0-9,]+)\s+in\s+([^(\n]+)', text_content)
            
            if bsr_matches:
                primary_rank = bsr_matches[0][0].replace(',', '')
                category = bsr_matches[0][1].strip()
                
                # If there are sub-category ranks
                if len(bsr_matches) > 1:
                    for match in bsr_matches[1:]:
                        secondary_ranks.append({
                            "Rank": match[0].replace(',', ''),
                            "Category": match[1].strip()
                        })

        # Method B: Fallback to legacy regexes if the DOM structure wasn't parsed correctly
        if not primary_rank:
            # Matches strings like "#1,234 in Home & Kitchen ("
            match1 = re.search(r'(#\d+,\d{3}.*?)\(', html)
            if not match1:
                match1 = re.search(r'(#\d{1,3}.*?)\(<a href', html)
                
            if match1:
                # E.g. "#1,234 in Home"
                raw = match1.group(1).strip()
                parts = raw.split(' in ', 1)
                if len(parts) == 2:
                    primary_rank = parts[0].replace('#', '').replace(',', '')
                    category = parts[1].strip()
                    
            # Matches secondary ranks inside spans
            # This is less reliable but keeps parity with the legacy script
            match2 = re.findall(r'>(#\d+|#\d+,\d{3})\sin\s<', html)
            if match2:
                # If we couldn't parse primary, this might be it
                if not primary_rank:
                    primary_rank = match2[0].replace('#', '').replace(',', '')
                else:
                    for r in match2:
                        secondary_ranks.append({
                            "Rank": r.replace('#', '').replace(',', ''),
                            "Category": "Unknown Subcategory"
                        })
                        
        return {
            "ASIN": asin,
            "URL": url,
            "PrimaryRank": primary_rank,
            "Category": category,
            "SecondaryRanks": secondary_ranks
        }
