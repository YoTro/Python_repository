from __future__ import annotations
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError
import asyncio
import random
import logging
from typing import Optional, Dict, Any, Callable
from src.core.utils.cookie_helper import AmazonCookieHelper

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


class AmazonBaseScraper:
    def __init__(self, use_proxy: bool = False, proxies_dict: Optional[Dict] = None):
        self.proxies = proxies_dict if use_proxy else None
        self.cookie_helper = AmazonCookieHelper()
        self.session = None
        self._headers: Dict = {}
        self._initialize_session()

    def _initialize_session(self, force_refresh: bool = False):
        cookie_data = self.cookie_helper.get_cookie_data(force_refresh=force_refresh)
        if not cookie_data:
            logger.warning("Failed to get cookie data.")
            return

        ua = cookie_data.get("user_agent", _USER_AGENTS[0])
        cookies = cookie_data.get("cookies", {})

        self._headers = {
            "User-Agent": ua,
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }
        self.session = requests.AsyncSession(
            headers=self._headers,
            cookies=cookies,
            impersonate="chrome110",
            proxies=self.proxies,
        )

    def _get_default_headers(self) -> Dict:
        """Return a copy of the base session headers for use in custom requests."""
        return self._headers.copy()

    def _get_random_ua(self) -> str:
        """Return a random User-Agent string."""
        return random.choice(_USER_AGENTS)

    async def fetch(
        self,
        url: str,
        method: str = "GET",
        headers: Optional[Dict] = None,
        data: Any = None,
        validator: Optional[Callable[[str], bool]] = None,
        max_retries: int = 3,
    ) -> Optional[str]:
        """
        Async fetch with content validation and retries.
        :param validator: Optional function returning True if content is valid.
        """
        for attempt in range(max_retries):
            try:
                if method.upper() == "POST":
                    response = await self.session.post(url, headers=headers, data=data, timeout=30)
                else:
                    response = await self.session.get(url, headers=headers, timeout=30)

                response.raise_for_status()
                response_text = response.text

                if validator and not validator(response_text):
                    raise ValueError("Content validation failed (soft block detected).")

                return response_text

            except (RequestsError, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 5))

        logger.error(f"Failed to fetch valid content from {url} after {max_retries} attempts.")
        return None
