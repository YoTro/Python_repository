import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class BestSellersExtractor(AmazonBaseScraper):
    """
    Extractor to fetch data from Amazon Best Sellers pages.
    """

    def get_bestsellers(self, url: str) -> list:
        """
        Fetch a Best Sellers page and extract the ranked products.
        
        :param url: The Amazon Best Sellers URL.
        :return: A list of dictionaries containing Rank, ASIN, Title, Image, Stars, Reviews, and Price.
        """
        logger.info(f"Fetching best sellers from: {url}")
        html = self.fetch(url)
        if not html:
            logger.warning(f"Failed to fetch content for {url}")
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        
        # Amazon Best Sellers are typically laid out in a grid.
        # We find all grid items. The class names change often, but they usually contain 'zg-grid-general-faceout'
        # or similar, or we can look for items with the 'zg-bdg-text' class which holds the rank.
        
        results = []
        
        # Method A: Try to find product cards directly
        cards = soup.find_all('div', id=re.compile(r'^gridItemRoot'))
        
        if cards:
            for card in cards:
                item_data = {
                    "Rank": None,
                    "ASIN": None,
                    "Title": None,
                    "Image": None,
                    "Stars": None,
                    "Reviews": None,
                    "Price": None
                }
                
                # Rank
                rank_span = card.find('span', class_='zg-bdg-text')
                if rank_span:
                    item_data["Rank"] = rank_span.get_text(strip=True).replace('#', '')
                    
                # ASIN and Title
                # Links usually look like /Title/dp/ASIN/ref=...
                link = card.find('a', class_='a-link-normal')
                if link:
                    href = link.get('href', '')
                    match = re.search(r'/dp/([A-Z0-9]{10})', href)
                    if match:
                        item_data["ASIN"] = match.group(1)
                    
                    # Title is often in a specific div
                    title_div = card.find('div', class_=re.compile(r'_cDEzb_p13n-sc-css-line-clamp'))
                    if title_div:
                        item_data["Title"] = title_div.get_text(strip=True)
                    else:
                        # Fallback to image alt text for title
                        img = card.find('img')
                        if img and img.get('alt'):
                            item_data["Title"] = img.get('alt')
                            
                # Image
                img = card.find('img')
                if img:
                    item_data["Image"] = img.get('src')
                    
                # Stars and Reviews
                # Usually contained in an 'a-icon-row'
                icon_row = card.find('div', class_='a-icon-row')
                if icon_row:
                    star_span = icon_row.find('a', title=re.compile(r'out of 5 stars'))
                    if star_span:
                        item_data["Stars"] = star_span.get('title').replace(' out of 5 stars', '')
                        
                    review_span = icon_row.find('span', class_='a-size-small')
                    if review_span:
                        item_data["Reviews"] = review_span.get_text(strip=True)
                        
                # Price
                price_span = card.find('span', class_='a-color-price')
                if price_span:
                    item_data["Price"] = price_span.get_text(strip=True)
                    
                if item_data["ASIN"]:
                    results.append(item_data)
                    
        else:
            # Method B: Fallback to the legacy regex method if the DOM completely changed
            logger.info("Falling back to regex parsing for Best Sellers.")
            ranks = re.findall(r'<span class="zg-bdg-text">#(\d+)<\/span>', html)
            asins = re.findall(r'<a class="a-link-normal" tabindex="-1" href=".*?\/dp\/(.*?)\/ref', html)
            titles = re.findall(r'<div class="_cDEzb_p13n-sc-css-line-clamp-3_g3dy1">(.*?)<\/div>', html)
            imgs = re.findall(r'a-section a-spacing-mini _cDEzb_noop_3Xbw5.*?src="(.*?)\"', html)
            stars = re.findall(r'a-icon-alt">(.*?) out of 5 stars<\/span>', html)
            reviews = re.findall(r'a-size-small">(.*?)<\/span>', html) 
            prices = re.findall(r'<span class="a-size-base a-color-price"><span class="_cDEzb_p13n-sc-price_3mJ9Z">(.*?)<\/span>', html)
            
            # Combine them safely, avoiding out-of-bounds errors
            min_len = min(len(ranks), len(asins))
            for i in range(min_len):
                results.append({
                    "Rank": ranks[i] if i < len(ranks) else None,
                    "ASIN": asins[i] if i < len(asins) else None,
                    "Title": titles[i] if i < len(titles) else None,
                    "Image": imgs[i] if i < len(imgs) else None,
                    "Stars": stars[i] if i < len(stars) else None,
                    "Reviews": reviews[i] if i < len(reviews) else None,
                    "Price": prices[i] if i < len(prices) else None,
                })

        return results
