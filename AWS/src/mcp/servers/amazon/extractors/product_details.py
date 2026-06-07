from __future__ import annotations

import json
import logging
import re

from bs4 import BeautifulSoup

from src.core.models.product import Product
from src.core.scraper import AmazonBaseScraper
from src.core.utils.parser_helper import parse_integer, parse_price, parse_rating

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

        soup = BeautifulSoup(html_content, "html.parser")

        # Always update deep-dive only fields
        # 1. Title (if missing or too short)
        if not product.title or len(product.title) < 20:
            title_span = soup.find("span", id="productTitle")
            if title_span:
                product.title = title_span.get_text(strip=True)

        # 2. Features & Description (Always deep-dive)
        feature_bullets_div = soup.find("div", id="feature-bullets")
        if feature_bullets_div:
            product.features = [
                span.get_text(strip=True)
                for span in feature_bullets_div.select("li span.a-list-item")
                if span.get_text(strip=True)
            ]

        desc_div = soup.find("div", id="productDescription")
        if desc_div:
            product.description = desc_div.get_text(separator="\n", strip=True)

        # 3. Price/Rating/Reviews (Only update if missing from search)
        if product.price is None:
            price_span = soup.find("span", class_="a-price-whole")
            product.price = parse_price(price_span.get_text(strip=True)) if price_span else None

        if product.review_count is None:
            review_span = soup.find("span", id="acrCustomerReviewText")
            product.review_count = (
                parse_integer(review_span.get_text(strip=True)) if review_span else None
            )

        if product.rating is None:
            rating_span = soup.select_one("i.a-icon-star span.a-icon-alt")
            product.rating = parse_rating(rating_span.get_text(strip=True)) if rating_span else None

        # 4. Past Month Sales
        if product.past_month_sales is None:
            # Try social proofing span first
            sales_span = soup.find("span", id="social-proofing-faceout-title-tk_bought")
            if not sales_span:
                # Fallback to broader search in text
                sales_text = soup.find(string=re.compile(r"bought in past month", re.I))
                if sales_text:
                    product.past_month_sales = parse_integer(sales_text)
            else:
                product.past_month_sales = parse_integer(sales_span.get_text(strip=True))

        # 5. Fulfillment
        fba_span = soup.find("span", id="tabular-buybox-truncate-0")
        if fba_span and "Amazon" in fba_span.get_text():
            product.is_fba = True

        # 6. A+ Content
        if product.has_a_plus_content is None:
            product.has_a_plus_content = bool(soup.find("div", class_="aplus-content-wrapper"))

        if product.has_a_plus_content:
            product.aplus_images = self._extract_aplus_images(soup)
            logger.info(f"Found {len(product.aplus_images)} A+ images for {product.asin}")

        # 7. Main images — extract URLs and resolution metadata from the same soup pass.
        if not product.images:
            product.images, product.images_metadata = self._extract_main_images(soup, html_content)
            if product.images and not product.main_image_url:
                product.main_image_url = product.images[0]
            logger.info(f"Extracted {len(product.images)} image(s) for {product.asin}")

        # 8. Videos
        if not product.videos:
            video_urls, video_count = self._extract_video_meta(soup, html_content)
            if video_urls:
                product.videos = video_urls
            elif video_count:
                product.videos = ["has_video_placeholder"] * video_count
            if product.videos:
                logger.info(f"Found {len(product.videos)} video(s) for {product.asin}")

        return product

    @staticmethod
    def _extract_main_images(soup: BeautifulSoup, html: str) -> tuple[list[str], dict[str, dict]]:
        """
        Extract main image URLs and their {width, height} metadata from the same parse pass.
        Returns (urls, metadata) — no extra HTTP request needed.
        """
        # Method A: data-a-dynamic-image — JSON map of URL → [width, height]
        img_el = soup.find("img", id="landingImage")
        if img_el and img_el.get("data-a-dynamic-image"):
            try:
                img_data: dict[str, list[int]] = json.loads(img_el["data-a-dynamic-image"])
                urls = sorted(img_data, key=lambda k: img_data[k][0] * img_data[k][1], reverse=True)
                metadata = {
                    url: {"width": img_data[url][0], "height": img_data[url][1]} for url in img_data
                }
                return urls, metadata
            except Exception:
                pass

        # Method B: imgTagWrapperId wrapper (no dimension data available)
        wrapper = soup.find("div", id="imgTagWrapperId")
        if wrapper:
            img = wrapper.find("img")
            if img and img.get("src"):
                return [img["src"]], {}

        # Method C: hiRes key in embedded JS blob (no dimension data available)
        m = re.search(r'"hiRes"\s*:\s*"(https://[^"]+)"', html)
        if m:
            return [m.group(1)], {}

        return [], {}

    @staticmethod
    def _extract_video_meta(soup: BeautifulSoup, html: str) -> tuple[list[str], int]:
        """
        Return (video_urls, count).
        video_urls contains real MP4/HLS URLs when detectable from embedded JS;
        count is the fallback used when only presence/quantity can be determined.
        """
        url_re = re.compile(
            r'"(?:videoUrl|contentUrl|url)"\s*:\s*"(https://[^"]+\.(?:mp4|m3u8)[^"]*)"'
        )
        video_urls = list(dict.fromkeys(m.group(1) for m in url_re.finditer(html)))
        if video_urls:
            return video_urls, len(video_urls)

        # Fallback: detect presence / count only
        count = 0
        count_span = soup.find("span", class_="video-count")
        if count_span:
            m = re.search(r"(\d+)", count_span.get_text(strip=True))
            count = int(m.group(1)) if m else 1
        elif soup.find("div", id="video-block") or soup.find("div", class_="video-container"):
            count = 1
        else:
            m = re.search(r'class="[^"]*video-count[^"]*"[^>]*>(.*?)</span>', html, re.S)
            if m:
                n = re.search(r"(\d+)", m.group(1))
                count = int(n.group(1)) if n else 1

        return [], count

    @staticmethod
    def _extract_aplus_images(soup: BeautifulSoup) -> list[str]:
        """Extract image URLs from A+ premium background sections."""
        urls: list[str] = []
        for img in soup.select("div.premium-background-wrapper div.background-image img[src]"):
            src = img.get("src", "").strip()
            if src and src.startswith("http"):
                urls.append(src)
        return urls

    def _extract_asin(self, text: str) -> str:
        asin_match = re.search(r"/dp/([A-Z0-9]{10})", text)
        return asin_match.group(1) if asin_match else text
