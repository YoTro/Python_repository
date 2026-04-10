from __future__ import annotations
import logging
import random
import time
import requests
from .auth import SellerspriteAuth
from src.gateway.rate_limit import RateLimiter
from src.core.errors.exceptions import RetryableError

logger = logging.getLogger(__name__)

class SellerspriteAPI:
    """
    API client for Sellersprite (卖家精灵).
    Fetches Keepa/Traffic data.
    """

    def __init__(self):
        self.session = requests.Session()
        self.auth = SellerspriteAuth()

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """HTTP request with Layer 3 token-bucket and 429 exponential backoff."""
        limiter = RateLimiter()
        for attempt in range(3):
            if not limiter.acquire_source("sellersprite"):
                raise RetryableError("sellersprite source rate limit timeout", retry_after_seconds=60)

            response = self.session.request(method, url, **kwargs)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 2 ** (attempt + 1))) + random.uniform(0, 1)
                logger.warning(f"[sellersprite] 429 rate limited — waiting {wait:.1f}s (attempt {attempt + 1}/3)")
                time.sleep(wait)
                continue

            return response

        raise RetryableError("sellersprite still rate limited after 3 retries", retry_after_seconds=120)

    def get_keepa_data(self, auth_token: str, asin: str) -> dict:
        """
        Fetch Keepa ranking data for an ASIN.
        """
        tk = self.auth.generate_tk("", asin)
        url = f"https://www.sellersprite.com/v2/extension/keepa?station=US&asin={asin}&tk={tk}&version=3.4.2&language=zh_CN&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome"
        
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json",
            "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
            "Auth-Token": auth_token,
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        }
        
        logger.info(f"Fetching Keepa data for {asin}")
        res = self._request("GET", url, headers=headers)
        response_data = {'times': [], 'bsr': [], 'subRanks': []}
        
        if res.status_code == 200:
            data = res.json()
            if 'data' in data and 'keepa' in data['data']:
                keepa = data['data']['keepa']
                response_data['bsr'] = keepa.get('bsr', [])
                response_data['times'] = data['data'].get('times', [])
                sub_ranks = keepa.get('subRanks', {})
                if sub_ranks:
                    response_data['subRanks'] = list(sub_ranks.values())[0]
        else:
            logger.error(f"Failed to fetch Keepa data: {res.text}")
            
        return response_data
