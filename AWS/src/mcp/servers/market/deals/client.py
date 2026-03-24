from __future__ import annotations
import logging
import asyncio
import re
import urllib.parse
import json
import base64
from bs4 import BeautifulSoup
from curl_cffi import requests
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DealHistoryClient:
    """
    Client to fetch off-Amazon deal history.
    Targets top-level deal sites like Slickdeals and DealNews.
    """
    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        # Base browser fingerprint (safe for most sites)
        self.base_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="146", "Chromium";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
        }

    async def get_deal_history(self, asin: str, keyword: str = "", max_pages: int = 3) -> List[Dict[str, Any]]:
        """
        Fetch deal history from multiple external deal sites in parallel.
        """
        search_term = keyword if keyword else asin
        logger.info(f"Fetching deal history for: {search_term} (up to {max_pages} pages)")
        
        # Run scrapers for different sites in parallel
        tasks = [
            self._fetch_slickdeals(search_term, max_pages),
            self._fetch_dealnews(search_term, max_pages)
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Flatten the list of lists into a single list of deals
        all_deals = [deal for sublist in results for deal in sublist]
        
        # Deduplicate deals based on a unique key (site, title, price)
        seen = set()
        unique_deals = []
        for d in all_deals:
            key = f"{d['site']}:{d['title']}:{d.get('price', 0)}"
            if key not in seen:
                seen.add(key)
                unique_deals.append(d)
                
        return unique_deals

    async def _fetch_slickdeals(self, search_term: str, max_pages: int) -> List[Dict[str, Any]]:
        encoded_term = urllib.parse.quote(search_term)
        all_deals = []
        
        # Site-specific headers with dynamic referer
        headers = {
            **self.base_headers,
            'referer': 'https://slickdeals.net/',
            'origin': 'https://slickdeals.net',
            'cache-control': 'max-age=0',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1'
        }
        
        for page in range(1, max_pages + 1):
            url = (
                f"https://slickdeals.net/search?q={encoded_term}&searchtype=normal"
                f"&filters%5Bforum%5D%5B%5D=&sort=relevance&filters%5Brating%5D%5B%5D=all"
                f"&filters%5Bdate%5D%5B%5D=1095&filters%5Bprice%5D%5Bmin%5D="
                f"&filters%5Bprice%5D%5Bmax%5D=&filters%5Bstore%5D%5B%5D=1&page={page}"
            )
            
            try:
                response = await asyncio.to_thread(self.session.get, url, headers=headers, timeout=15)
                if response.status_code != 200:
                    logger.warning(f"Slickdeals returned status {response.status_code} on page {page}. Stopping.")
                    break
                
                page_deals = self._parse_slickdeals(response.text)
                if not page_deals:
                    logger.info(f"No more deals found on Slickdeals page {page}.")
                    break
                all_deals.extend(page_deals)
                
                if page < max_pages:
                    await asyncio.sleep(1.0) # Politeness delay
            except Exception as e:
                logger.error(f"Slickdeals error on page {page}: {e}")
                break
        return all_deals

    async def _fetch_dealnews(self, search_term: str, max_pages: int) -> List[Dict[str, Any]]:
        encoded_term = urllib.parse.quote(search_term)
        all_deals = []
        
        # Site-specific headers with dynamic referer
        headers = {
            **self.base_headers,
            'referer': 'https://www.dealnews.com/',
            'origin': 'https://www.dealnews.com'
        }
        
        # 1. Fetch the initial search page to get deal IDs
        url = f"https://www.dealnews.com/s313/Amazon/?search={encoded_term}&sort=featured"
        try:
            response = await asyncio.to_thread(self.session.get, url, headers=headers, timeout=15)
            if response.status_code != 200:
                return []
            
            html = response.text
            # Parse deals already rendered on the page
            initial_deals = self._parse_dealnews(html)
            all_deals.extend(initial_deals)
            
            # 2. Extract remaining deal IDs for async pagination
            ids_match = re.search(r'data-ids="([^"]+)"', html)
            if not ids_match or max_pages <= 1:
                return all_deals
                
            all_ids = ids_match.group(1).split(',')
            remaining_ids = all_ids[len(initial_deals):]
            
            # 3. Fetch remaining pages via async grid API
            chunk_size = 20
            num_extra_pages = min(max_pages - 1, (len(remaining_ids) + chunk_size - 1) // chunk_size)
            
            for i in range(num_extra_pages):
                offset = i * chunk_size
                chunk = remaining_ids[offset : offset + chunk_size]
                if not chunk:
                    break
                    
                payload = {"i": ",".join(chunk), "e": 0, "c": len(chunk), "g": "ContentCard", "w": 1, "gutter": False, "x": "eyJmb3JjZV9pbWFnZSI6ZmFsc2UsInRpdGxlX2xpbWl0X3NtYWxsIjoyfQ=="}
                h_param = base64.b64encode(json.dumps(payload).replace(" ", "").encode()).decode()
                
                async_url = f"https://www.dealnews.com/async/grids/?h={h_param}"
                async_headers = {**headers, 'accept': 'dealnews/json, */*; q=0.1'}
                
                resp = await asyncio.to_thread(self.session.get, async_url, headers=async_headers, timeout=15)
                if resp.status_code == 200 and 'html' in resp.json():
                    all_deals.extend(self._parse_dealnews(resp.json()['html']))
                
                if i < num_extra_pages - 1:
                    await asyncio.sleep(1.0)
                    
        except Exception as e:
            logger.error(f"DealNews error: {e}")
            
        return all_deals

    def _parse_slickdeals(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        deals = []
        for card in soup.select('.dealCardListView'):
            try:
                deals.append({
                    "date": card.select_one('.slickdealsTimestamp').get('title', ''),
                    "price": self._extract_price(card.select_one('.dealCardListView__finalPrice').get_text(strip=True)),
                    "discount_pct": self._extract_percentage(card.select_one('.dealCardListView__savings').get_text(strip=True)),
                    "title": card.select_one('.dealCardListView__title').get_text(strip=True),
                    "site": "slickdeals.net", "type": "Search Result"
                })
            except Exception: continue
        return deals

    def _parse_dealnews(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        deals = []
        for card in soup.select('.content-view, .content-card'):
            try:
                price = self._extract_price(card.select_one('.callout').get_text(strip=True))
                comp_price = self._extract_price(card.select_one('.callout-comparison').get_text(strip=True))
                discount_pct = round(((comp_price - price) / comp_price) * 100, 1) if comp_price and price else 0.0
                
                date_text = ""
                script = card.find('script', type='application/ld+json')
                if script:
                    data = json.loads(script.string)
                    offers = data.get('offers', [])
                    date_text = (offers[0] if isinstance(offers, list) else offers).get('validFrom', '')

                deals.append({
                    "date": date_text, "price": price, "discount_pct": discount_pct,
                    "title": card.select_one('.title-link, .title').get_text(strip=True),
                    "site": "dealnews.com", "type": "Search Result"
                })
            except Exception: continue
        return deals

    def _extract_price(self, text: str) -> float:
        match = re.search(r'\$([\d,]+(?:\.\d+)?)', text.replace(',', ''))
        return float(match.group(1)) if match else 0.0

    def _extract_percentage(self, text: str) -> float:
        match = re.search(r'([\d\.]+)%', text)
        return float(match.group(1)) if match else 0.0
