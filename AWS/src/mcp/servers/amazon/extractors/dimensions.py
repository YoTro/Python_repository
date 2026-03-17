from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class DimensionsExtractor(AmazonBaseScraper):
    """
    Extractor to fetch product dimensions and price.
    """

    async def get_dimensions_and_price(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and extract physical dimensions and current price.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing ASIN, Dimensions, and Price.
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching dimensions and price for ASIN: {asin}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for ASIN {asin}")
            return {"ASIN": asin, "URL": url, "Dimensions": None, "Price": None}
            
        soup = BeautifulSoup(html, 'html.parser')
        
        dimensions = None
        price = None
        
        # 1. Extract Dimensions
        # Method A: Usually in the product details table
        details_table = soup.find('table', id='productDetails_techSpec_section_1')
        if details_table:
            for row in details_table.find_all('tr'):
                th = row.find('th')
                if th and ('Product Dimensions' in th.text or 'Item Dimensions' in th.text):
                    td = row.find('td')
                    if td:
                        dimensions = td.get_text(strip=True).replace('\u200e', '')
                        break
                        
        # Method B: Sometimes it's in a bulleted list of details
        if not dimensions:
            details_bullets = soup.find('div', id='detailBullets_feature_div')
            if details_bullets:
                for li in details_bullets.find_all('li'):
                    text = li.get_text(strip=True)
                    if 'Product Dimensions' in text or 'Package Dimensions' in text:
                        # Format is often "Product Dimensions : 10 x 5 x 2 inches"
                        parts = text.split(':')
                        if len(parts) > 1:
                            dimensions = parts[1].strip().replace('\u200e', '')
                            break

        # Method C: Fallback to legacy regexes
        if not dimensions:
            match1 = re.search(r'<td class="a-size-base">\s+(.*?)\sinches', html)
            match2 = re.search(r'Product Dimensions:\s+</b>\s+(.*?)\s+inches', html)
            if match1:
                dimensions = f"{match1.group(1).strip()} inches"
            elif match2:
                dimensions = f"{match2.group(1).strip()} inches"
                
        # 2. Extract Price
        # Method A: Modern price block
        price_span = soup.find('span', class_='a-price')
        if price_span:
            offscreen = price_span.find('span', class_='a-offscreen')
            if offscreen:
                price = offscreen.get_text(strip=True)
                
        # Method B: Fallback to legacy regex
        if not price:
            match_price = re.search(r'class="a-size-medium a-color-price">\$?(.*?)</span>', html)
            if match_price:
                price = f"${match_price.group(1).strip()}"
                
        return {
            "ASIN": asin,
            "URL": url,
            "Dimensions": dimensions,
            "Price": price
        }
