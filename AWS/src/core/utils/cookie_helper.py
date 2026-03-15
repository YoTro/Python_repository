from __future__ import annotations
from DrissionPage import ChromiumPage, ChromiumOptions
import logging
import time
import os
import json
from typing import Dict, Optional
import random

logger = logging.getLogger(__name__)

class AmazonCookieHelper:
    """
    Helper to fetch and manage Amazon cookies using DrissionPage.
    Smartly switches between Anonymous and Authenticated (logged-in) sessions.
    """
    
    def __init__(self, cache_file: str = "config/cookies.json", headless: bool = False):
        self.cache_file = cache_file
        self.headless = headless
        
    def fetch_fresh_cookies(self, wait_for_manual: bool = False) -> Dict[str, str]:
        """
        Launch a browser to fetch fresh cookies.
        :param wait_for_manual: 
            If True: Go to Login Page and wait for user (for Reviews).
            If False: Go to Homepage (for general search/details).
        """
        target_url = "https://www.amazon.com/"
        if wait_for_manual:
            # Deep link to sign-in page
            target_url = "https://www.amazon.com/ap/signin?openid.pape.max_auth_age=0&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_signin&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.assoc_handle=usflex&openid.mode=checkid_setup&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
            logger.info("🔑 LOGIN MODE: Navigating to Sign-in page...")
        else:
            logger.info("🌐 ANONYMOUS MODE: Navigating to Amazon Homepage...")

        co = ChromiumOptions()
        random_port = random.randint(10000, 60000)
        co.set_local_port(random_port)
        
        # In manual mode, we MUST show the browser
        effective_headless = False if wait_for_manual else self.headless
        co.headless(effective_headless)
            
        co.incognito()
        co.set_argument('--proxy-server-bypass-list', '<-loopback>')
        co.set_argument('--disable-gpu')
        co.set_argument('--no-sandbox')
        
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        co.set_user_agent(ua)
        
        page = None
        cookies_dict = {}
        
        try:
            page = ChromiumPage(co)
            page.set.load_mode.eager() 
            page.get(target_url, timeout=30)
            
            if wait_for_manual:
                logger.info("🕒 WAITING FOR MANUAL LOGIN (60s)... Please finish login in the opened window.")
                # Loop to detect login success
                for _ in range(60):
                    # Check for logout link or account name which indicates login
                    if page.ele('#nav-item-signout') or page.ele('text:Account & Lists'):
                        logger.info("✅ Login detected!")
                        break
                    time.sleep(1)
            else:
                # Basic anonymous wait
                time.sleep(5)
                # Handle potential simple captchas or "Continue shopping" automatically
                continue_btn = page.ele('text:Continue shopping', timeout=2)
                if continue_btn: continue_btn.click(); time.sleep(2)
            
            raw_cookies = page.cookies()
            cookies_dict = {c.get('name'): c.get('value') for c in raw_cookies}
            
            if 'session-id' in cookies_dict:
                # Add regional defaults
                cookies_dict['i18n-prefs'] = 'USD'
                cookies_dict['lc-main'] = 'en_US'
                
                data_to_save = {
                    "cookies": cookies_dict,
                    "user_agent": ua,
                    "is_logged_in": wait_for_manual # Mark if this session is authenticated
                }
                
                self._save_to_cache(data_to_save)
                logger.info(f"💾 Captured {len(cookies_dict)} cookies. Session saved to {self.cache_file}.")
            else:
                logger.warning("⚠️ Failed to capture 'session-id'. Cookies might be invalid.")
                
        except Exception as e:
            logger.error(f"❌ CookieHelper Error: {e}")
        finally:
            if page: page.quit()
            
        return cookies_dict

    def get_cookie_data(self, force_refresh: bool = False) -> Dict:
        if not force_refresh:
            cached = self._load_from_cache()
            if cached: return cached
        return self.fetch_fresh_cookies(wait_for_manual=False) # Default to anonymous refresh

    def _save_to_cache(self, data: Dict):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

    def _load_from_cache(self) -> Optional[Dict]:
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
