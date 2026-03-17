from __future__ import annotations
import logging
import re
import json
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class ImageExtractor(AmazonBaseScraper):
    """
    Extractor to fetch the primary and secondary image URLs from an Amazon product listing.
    """

    async def get_product_images(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and extract the high-resolution image URLs.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing ASIN and a list of ImageURLs.
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Fetching images for ASIN: {asin}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for ASIN {asin}")
            return {"ASIN": asin, "Images": []}
            
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        
        # Method A: Look for the landingImage element which contains a dictionary of image sizes
        img_element = soup.find('img', id='landingImage')
        if img_element and img_element.get('data-a-dynamic-image'):
            try:
                # It's stored as a JSON-like string dictionary mapping URL -> [width, height]
                img_data_str = img_element.get('data-a-dynamic-image')
                img_data = json.loads(img_data_str)
                # Sort by resolution (width * height) descending and get the largest
                sorted_urls = sorted(img_data.keys(), key=lambda k: img_data[k][0] * img_data[k][1], reverse=True)
                images = sorted_urls
            except Exception as e:
                logger.warning(f"Failed to parse JSON for landing image: {e}")
                
        # Method B: If landingImage is missing, try looking for the main image block
        if not images:
            img_wrapper = soup.find('div', id='imgTagWrapperId')
            if img_wrapper:
                img = img_wrapper.find('img')
                if img and img.get('src'):
                    images.append(img.get('src'))
                    
        # Method C: Legacy regex fallback
        if not images:
            match = re.search(r'id="landingImage"[^>]*data-a-dynamic-image="(?:{&quot;|{")([^"&]+)', html)
            if match:
                images.append(match.group(1))
                
        return {
            "ASIN": asin,
            "Images": images
        }
