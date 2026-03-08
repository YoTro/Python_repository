from DrissionPage import ChromiumPage, ChromiumOptions
import logging
import time
import os
import json
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class AmazonCookieHelper:
    """
    Helper to fetch and manage Amazon cookies using DrissionPage.
    """
    
    def __init__(self, cache_file: str = "config/cookies.json", headless: bool = True):
        self.cache_file = cache_file
        self.headless = headless
        
    def fetch_fresh_cookies(self) -> Dict[str, str]:
        """
        Launch a browser to fetch fresh cookies from Amazon.
        """
        logger.info("Launching browser to fetch fresh Amazon cookies...")
        co = ChromiumOptions()
        if self.headless:
            co.headless(True)
        co.incognito()
        
        # Use a fixed modern UA for consistency
        ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        co.set_user_agent(ua)
        
        # Add some common arguments to avoid detection
        co.set_argument('--no-sandbox')
        co.set_argument('--disable-gpu')
        
        page = ChromiumPage(co)
        cookies_dict = {}
        
        try:
            page.get('https://www.amazon.com/')
            time.sleep(3) # Wait for initial load
            
            # Handle "Continue shopping" button if it appears
            continue_btn = page.ele('text:Continue shopping', timeout=3)
            if continue_btn:
                logger.info("Clicking 'Continue shopping' button...")
                continue_btn.click()
                time.sleep(2)
                
            raw_cookies = page.cookies()
            for cookie in raw_cookies:
                cookies_dict[cookie.get('name')] = cookie.get('value')
                
            if 'session-id' in cookies_dict:
                # Force US preferences (Currency, Language)
                cookies_dict['i18n-prefs'] = 'USD'
                cookies_dict['lc-main'] = 'en_US'
                cookies_dict['sp-cdn'] = '"L5Z9:US"'
                
                # Store the UA used so scraper can match it
                data_to_save = {
                    "cookies": cookies_dict,
                    "user_agent": ua
                }
                
                logger.info(f"Successfully fetched {len(cookies_dict)} cookies. Forced US preferences (USD, en_US).")
                self._save_to_cache(data_to_save)
            else:
                logger.warning("Fetched cookies but 'session-id' is missing.")
                
        except Exception as e:
            logger.error(f"Error fetching cookies with DrissionPage: {e}")
        finally:
            page.quit()
            
        return cookies_dict

    def get_cookie_data(self, force_refresh: bool = False) -> Dict:
        """
        Get both cookies and the UA used to fetch them.
        """
        if not force_refresh:
            cached = self._load_from_cache()
            if cached:
                return cached
                
        self.fetch_fresh_cookies()
        return self._load_from_cache()

    def get_cookies(self, force_refresh: bool = False) -> Dict[str, str]:
        data = self.get_cookie_data(force_refresh)
        return data.get("cookies", {}) if data else {}

    def _save_to_cache(self, data: Dict):
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save cookies to cache: {e}")

    def _load_from_cache(self) -> Optional[Dict]:
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load cookies from cache: {e}")
        return None
