from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper
from src.core.models.product import Product
from src.core.utils.parser_helper import parse_price, parse_rating, parse_integer

logger = logging.getLogger(__name__)

class SearchExtractor(AmazonBaseScraper):
    """
    Broad-scan extractor for Amazon search results.
    Provides fast, shallow data for multiple products.
    """

    async def search(self, keyword: str, page: int = 1) -> list[Product]:
        url = f'https://www.amazon.com/s?k={keyword.replace(" ", "+")}&page={page}'
        logger.info(f"Searching for '{keyword}' on page {page}...")
        
        html_content = await self.fetch(url)
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        search_results = soup.find_all('div', {'data-component-type': 's-search-result'})
        
        products = []
        for result in search_results:
            try:
                asin = result.get('data-asin')
                if not asin: continue

                title_h2 = result.find('h2')
                title = title_h2.get_text(strip=True) if title_h2 else None

                price_whole = result.find('span', class_='a-price-whole')
                price = parse_price(price_whole.get_text(strip=True)) if price_whole else None

                rating_el = result.find('i', class_=re.compile(r'a-star-small-\d'))
                rating = parse_rating(rating_el.get_text(strip=True)) if rating_el else None

                review_count_el = result.find('span', {'aria-label': re.compile(r'[\d,]+ ratings')})
                review_count = parse_integer(review_count_el.get_text(strip=True)) if review_count_el else None

                sales_el = result.find('span', class_='a-size-base a-color-secondary', string=re.compile(r'bought in past month'))
                past_month_sales = parse_integer(sales_el.get_text(strip=True)) if sales_el else None

                products.append(Product(
                    asin=asin,
                    title=title,
                    price=price,
                    rating=rating,
                    review_count=review_count,
                    past_month_sales=past_month_sales
                ))
            except Exception as e:
                logger.error(f"Error parsing search result item: {e}")
                continue

        return products
