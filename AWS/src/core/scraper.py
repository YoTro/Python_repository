from __future__ import annotations

import asyncio
import logging
import random
import re
from collections.abc import Callable
from typing import Any

from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

from src.core.errors import ErrorCode, ScraperError
from src.core.utils.cookie_helper import AMAZON_UA, AmazonCookieHelper, _nearest_cffi_target

logger = logging.getLogger(__name__)


class AmazonBaseScraper:
    def __init__(self, use_proxy: bool = False, proxies_dict: dict | None = None):
        self.proxies: Any = proxies_dict if use_proxy else None  # curl_cffi.ProxySpec has no stubs
        self.cookie_helper = AmazonCookieHelper()
        self.session: requests.AsyncSession | None = None
        self._headers: dict = {}
        self._initialize_session()

    def _initialize_session(self, force_refresh: bool = False):
        cookie_data = self.cookie_helper.get_cookie_data(force_refresh=force_refresh)
        if not cookie_data:
            logger.warning("Failed to get cookie data.")
            return

        ua = cookie_data.get("user_agent", AMAZON_UA)
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
        chrome_ver = re.search(r"Chrome/(\d+)", ua)
        major = int(chrome_ver.group(1)) if chrome_ver else 146
        impersonate = f"chrome{_nearest_cffi_target(major)}"

        self.session = requests.AsyncSession(
            headers=self._headers,
            cookies=cookies,
            impersonate=impersonate,
            proxies=self.proxies,
        )

    def _get_default_headers(self) -> dict:
        """Return a copy of the base session headers for use in custom requests."""
        return self._headers.copy()

    def _get_random_ua(self) -> str:
        return AMAZON_UA

    async def fetch(
        self,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        data: Any = None,
        validator: Callable[[str], bool] | None = None,
        max_retries: int = 3,
    ) -> str | None:
        """
        Async fetch with content validation and retries.
        Layer 3 rate limiting: acquires one token from the 'crawler' bucket before
        each request. All Amazon extractor subclasses inherit this automatically.
        :param validator: Optional function returning True if content is valid.
        """
        from src.gateway.rate_limit import RateLimiter  # lazy import — avoids circular deps

        if not await RateLimiter().async_acquire_source("amazon_scraper"):
            logger.warning(f"[scraper] amazon_scraper token-bucket timeout, skipping: {url}")
            return None

        if self.session is None:
            logger.warning(f"[scraper] session not initialised, skipping fetch: {url}")
            return None

        for attempt in range(max_retries):
            try:
                if method.upper() == "POST":
                    response = await self.session.post(url, headers=headers, data=data, timeout=30)
                else:
                    response = await self.session.get(url, headers=headers, timeout=30)

                response.raise_for_status()
                response_text = response.text

                if validator and not validator(response_text):
                    raise ScraperError(
                        "Content validation failed (soft block detected).",
                        code=ErrorCode.SOFT_BLOCKED,
                    )

                # Inter-request jitter — randomises timing to avoid detectable regular patterns.
                await asyncio.sleep(random.uniform(1.0, 3.0))
                return response_text

            except (RequestsError, ScraperError) as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {e}")
                # 4xx errors are definitive — retrying won't change the outcome and
                # burns WAF budget, potentially causing subsequent requests to be blocked.
                if "403" in str(e) or "404" in str(e) or "401" in str(e):
                    break
                if attempt < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 5))

        logger.error(f"Failed to fetch valid content from {url} after {max_retries} attempts.")
        return None
