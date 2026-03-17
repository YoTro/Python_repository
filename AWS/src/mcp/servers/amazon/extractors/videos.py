from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class VideoExtractor(AmazonBaseScraper):
    """
    Extractor to determine if an Amazon product listing contains videos.
    """

    async def has_videos(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Fetch the product page and check if it has videos.
        
        :param asin: The product ASIN.
        :param host: The Amazon marketplace host.
        :return: A dictionary containing ASIN, HasVideos (bool), and VideoCount (int).
        """
        url = f"{host}/dp/{asin}"
        logger.info(f"Checking for videos on ASIN: {asin}")
        
        html = await self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for ASIN {asin}")
            return {"ASIN": asin, "HasVideos": False, "VideoCount": 0}
            
        soup = BeautifulSoup(html, 'html.parser')
        
        has_videos = False
        video_count = 0
        
        # Look for elements that typically indicate video presence in the image block
        # The class 'video-count' is often used
        video_count_span = soup.find('span', class_='video-count')
        if video_count_span:
            has_videos = True
            text = video_count_span.get_text(strip=True)
            # Usually text is like "1 VIDEO" or "2 VIDEOS"
            match = re.search(r'(\d+)', text)
            if match:
                video_count = int(match.group(1))
            else:
                video_count = 1  # Fallback
                
        # Alternative checks if the span is missing
        if not has_videos:
            # Check for video block identifiers
            if soup.find('div', id='video-block') or soup.find('div', class_='video-container'):
                has_videos = True
                video_count = 1
                
        # Legacy regex fallback
        if not has_videos:
            match = re.search(r'<span class="[^"]*video-count[^"]*">(.*?)<\/span>', html)
            if match:
                has_videos = True
                num_match = re.search(r'(\d+)', match.group(1))
                video_count = int(num_match.group(1)) if num_match else 1
                
        return {
            "ASIN": asin,
            "HasVideos": has_videos,
            "VideoCount": video_count
        }
