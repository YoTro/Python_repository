from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError
import random
import time
import logging
import re
from typing import Optional, Dict
from src.utils.config_helper import ConfigHelper
from src.utils.cookie_helper import AmazonCookieHelper

logger = logging.getLogger(__name__)

class AmazonBaseScraper:
    """
    Base scraper class for Amazon. 
    Handles session management, common headers, user-agents, and retry logic.
    """
    
    def __init__(self, use_proxy: bool = False, proxies_dict: Optional[Dict[str, str]] = None):
        self.session = requests.Session(impersonate="chrome")
        self.use_proxy = use_proxy
        self.ua = self._get_random_ua()  # Initialize with a random UA first
        if use_proxy and proxies_dict:
            self.session.proxies.update(proxies_dict)
        
        # Initialize Cookie Helper
        self.cookie_helper = AmazonCookieHelper()
        self._load_session_cookies()

    def _load_session_cookies(self, force_refresh: bool = False):
        """Load cookies and match User-Agent into the current session."""
        data = self.cookie_helper.get_cookie_data(force_refresh=force_refresh)
        if data:
            self.session.cookies.update(data.get("cookies", {}))
            # Use the UA that got the cookies to ensure session consistency
            self.ua = data.get("user_agent", self.ua)
            logger.info(f"Session cookies and User-Agent updated. UA: {self.ua[:30]}...")
        else:
            logger.warning("No cookie data found, using default User-Agent.")
            
    def _get_random_ua(self) -> str:
        default_uas = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
        ]
        ua_list = ConfigHelper.get("user_agents", default_uas)
        return random.choice(ua_list)

    def _get_default_headers(self) -> dict:
        return {
            "Host": "www.amazon.com",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": self.ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.9"
        }

    def _is_ttd(self, html: str) -> bool:
        """
        Check if the page was blocked by Amazon's anti-bot system ("dogs" page).
        """
        if "_TTD_.jpg" in html or "api-services-support@amazon.com" in html:
            return True
        return False

    def fetch(self, url: str, max_retries: Optional[int] = None) -> Optional[str]:
        """
        Fetch a URL with automatic retries for HTTP errors and Bot detection.
        """
        if max_retries is None:
            max_retries = ConfigHelper.get("scraper.max_retries", 5)
            
        timeout = ConfigHelper.get("scraper.timeout", 15)
        delay_min = ConfigHelper.get("scraper.retry_delay_min", 2)
        delay_max = ConfigHelper.get("scraper.retry_delay_max", 5)
        
        headers = self._get_default_headers()
        
        for attempt in range(max_retries):
            try:
                # Use custom headers and curl_cffi's TLS impersonation
                response = self.session.get(url, headers=headers, timeout=timeout)
                # Check for anti-bot
                if self._is_ttd(response.text):
                    logger.warning(f"Attempt {attempt + 1}/{max_retries}: Blocked by Amazon TTD (Bot Detection). Refreshing cookies and retrying...")
                    time.sleep(random.uniform(delay_min, delay_max))
                    
                    # Refresh cookies and rotate UA on TTD block
                    self._load_session_cookies(force_refresh=True)
                    headers["User-Agent"] = self._get_random_ua()
                    continue
                
                # If status code is valid
                response.raise_for_status()
                return response.text
                
            except RequestsError as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Request failed: {e}")
                time.sleep(random.uniform(delay_min, delay_max))
                
        logger.error(f"Failed to fetch {url} after {max_retries} attempts.")
        return None
