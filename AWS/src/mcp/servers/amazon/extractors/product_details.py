from __future__ import annotations
import logging
import re
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper
from src.core.models.product import Product
from src.core.utils.parser_helper import parse_price, parse_rating, parse_integer

logger = logging.getLogger(__name__)

class ProductDetailsExtractor(AmazonBaseScraper):
    """
    Deep-dive extractor for Amazon product pages.
    Enriches Product models with high-fidelity data like bullets and descriptions.
    """

    async def get_product_details(self, url_or_asin: str) -> Product:
        """Fetch a new Product model from a detail page."""
        product = Product(asin=self._extract_asin(url_or_asin))
        return await self.enrich_product(product)

    async def enrich_product(self, product: Product) -> Product:
        """
        Takes an existing Product model (e.g., from search) and fills in missing details.
        Reduces redundant logic by only updating if fields are missing.
        """
        url = f"https://www.amazon.com/dp/{product.asin}"
        logger.info(f"Enriching product details for: {product.asin}")
        
        html_content = await self.fetch(url)
        if not html_content:
            return product
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Always update deep-dive only fields
        # 1. Title (if missing or too short)
        if not product.title or len(product.title) < 20:
            title_span = soup.find('span', id='productTitle')
            if title_span:
                product.title = title_span.get_text(strip=True)

        # 2. Features & Description (Always deep-dive)
        feature_bullets_div = soup.find('div', id='feature-bullets')
        if feature_bullets_div:
            product.features = [span.get_text(strip=True) for span in feature_bullets_div.select('li span.a-list-item') if span.get_text(strip=True)]
        
        desc_div = soup.find('div', id='productDescription')
        if desc_div:
            product.description = desc_div.get_text(separator='\n', strip=True)

        # 3. Price/Rating/Reviews (Only update if missing from search)
        if product.price is None:
            price_span = soup.find('span', class_='a-price-whole')
            product.price = parse_price(price_span.get_text(strip=True)) if price_span else None

        if product.review_count is None:
            review_span = soup.find('span', id='acrCustomerReviewText')
            product.review_count = parse_integer(review_span.get_text(strip=True)) if review_span else None

        if product.rating is None:
            rating_span = soup.select_one('i.a-icon-star span.a-icon-alt')
            product.rating = parse_rating(rating_span.get_text(strip=True)) if rating_span else None
            
        # 4. Past Month Sales
        if product.past_month_sales is None:
            # Try social proofing span first
            sales_span = soup.find('span', id='social-proofing-faceout-title-tk_bought')
            if not sales_span:
                # Fallback to broader search in text
                sales_text = soup.find(string=re.compile(r'bought in past month', re.I))
                if sales_text:
                    product.past_month_sales = parse_integer(sales_text)
            else:
                product.past_month_sales = parse_integer(sales_span.get_text(strip=True))

        # 5. Fulfillment
        fba_span = soup.find('span', id='tabular-buybox-truncate-0')
        if fba_span and "Amazon" in fba_span.get_text():
            product.is_fba = True

        return product

    def _extract_asin(self, text: str) -> str:
        asin_match = re.search(r'/dp/([A-Z0-9]{10})', text)
        return asin_match.group(1) if asin_match else text
