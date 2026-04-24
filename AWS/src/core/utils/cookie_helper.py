from __future__ import annotations
from DrissionPage import ChromiumPage, ChromiumOptions
import logging
import sys
import time
import os
import json
from typing import Dict, Optional
import random

logger = logging.getLogger(__name__)

# Remote debug port used when running on a headless Linux server in login mode.
# User connects via:  ssh -L 9222:localhost:9222 user@server
# Then open:          http://localhost:9222  in their local Chrome.
_REMOTE_DEBUG_PORT = 9222


def _is_headless_linux() -> bool:
    """True when running on Linux without a graphical display."""
    return sys.platform.startswith("linux") and not os.environ.get("DISPLAY")


class AmazonCookieHelper:
    """
    Helper to fetch and manage Amazon cookies using DrissionPage.
    Smartly switches between Anonymous and Authenticated (logged-in) sessions.

    On a headless Linux server (no DISPLAY):
      - Anonymous mode: runs Chrome with --headless=new automatically.
      - Manual login mode: launches Chrome with --remote-debugging-port=9222
        and prints SSH tunnel instructions so the user can interact with the
        browser from their local machine.
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
        headless_server = _is_headless_linux()

        target_url = "https://www.amazon.com/"
        if wait_for_manual:
            target_url = (
                "https://www.amazon.com/ap/signin"
                "?openid.pape.max_auth_age=0"
                "&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_signin"
                "&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
                "&openid.assoc_handle=usflex"
                "&openid.mode=checkid_setup"
                "&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select"
                "&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0"
            )
            logger.info("🔑 LOGIN MODE: Navigating to Sign-in page...")
        else:
            logger.info("🌐 ANONYMOUS MODE: Navigating to Amazon Homepage...")

        co = ChromiumOptions()
        random_port = random.randint(10000, 60000)
        co.set_local_port(random_port)

        co.incognito()
        co.set_argument("--proxy-server-bypass-list", "<-loopback>")
        co.set_argument("--disable-gpu")
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-dev-shm-usage")  # prevents /dev/shm OOM on Linux

        if wait_for_manual and headless_server:
            # Cannot show a window on a headless server.  Launch Chrome with a
            # remote debugging port so the user can interact from their local
            # machine via an SSH tunnel.
            co.set_argument(f"--remote-debugging-port={_REMOTE_DEBUG_PORT}")
            co.set_argument("--remote-debugging-address=127.0.0.1")
            # headless=False so the page actually renders (needed for CDP session)
            co.headless(False)
            logger.warning(
                "\n"
                "╔══════════════════════════════════════════════════════════════╗\n"
                "║  HEADLESS SERVER DETECTED — manual login via SSH tunnel      ║\n"
                "╠══════════════════════════════════════════════════════════════╣\n"
                "║  1. On your LOCAL machine, run:                              ║\n"
                f"║     ssh -L {_REMOTE_DEBUG_PORT}:localhost:{_REMOTE_DEBUG_PORT} <user>@<server-ip>             ║\n"
                "║  2. Open Chrome on your local machine and go to:            ║\n"
                f"║     http://localhost:{_REMOTE_DEBUG_PORT}                                  ║\n"
                "║  3. Click the Amazon sign-in page link and log in.          ║\n"
                "║  4. The script will detect login and save cookies.          ║\n"
                "╚══════════════════════════════════════════════════════════════╝"
            )
        else:
            # In manual mode on a desktop, show the browser window.
            # In anonymous mode, respect self.headless; force headless on a
            # headless Linux server so Chrome doesn't crash looking for a display.
            if wait_for_manual:
                effective_headless = False
            elif headless_server:
                effective_headless = True
            else:
                effective_headless = self.headless

            co.headless(effective_headless)
            if effective_headless:
                co.set_argument("--headless=new")

        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        co.set_user_agent(ua)

        page = None
        cookies_dict = {}

        try:
            page = ChromiumPage(co)
            page.set.load_mode.eager()
            page.get(target_url, timeout=30)

            if wait_for_manual:
                timeout_sec = 300 if headless_server else 120
                logger.info(
                    f"🕒 WAITING FOR MANUAL LOGIN ({timeout_sec}s)... "
                    "Please finish login in the opened window."
                )
                for _ in range(timeout_sec):
                    if page.ele("#nav-item-signout") or page.ele("text:Account & Lists"):
                        logger.info("✅ Login detected!")
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError(
                        f"Login timeout — no login detected within {timeout_sec} seconds."
                    )
            else:
                time.sleep(5)
                continue_btn = page.ele("text:Continue shopping", timeout=2)
                if continue_btn:
                    continue_btn.click()
                    time.sleep(2)

            raw_cookies = page.cookies()
            cookies_dict = {c.get("name"): c.get("value") for c in raw_cookies}

            if "session-id" not in cookies_dict:
                raise RuntimeError(
                    "Failed to capture 'session-id' — Amazon may have blocked "
                    "the request or shown a CAPTCHA."
                )

            cookies_dict["i18n-prefs"] = "USD"
            cookies_dict["lc-main"] = "en_US"
            self._save_to_cache({
                "cookies": cookies_dict,
                "user_agent": ua,
                "is_logged_in": wait_for_manual,
            })
            logger.info(
                f"💾 Captured {len(cookies_dict)} cookies. "
                f"Session saved to {self.cache_file}."
            )

        except Exception as e:
            logger.error(f"❌ CookieHelper Error: {e}")
            raise
        finally:
            if page:
                page.quit()

        return cookies_dict

    def get_cookie_data(self, force_refresh: bool = False) -> Dict:
        if not force_refresh:
            cached = self._load_from_cache()
            if cached:
                return cached
        return self.fetch_fresh_cookies(wait_for_manual=False)

    def _save_to_cache(self, data: Dict):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def _load_from_cache(self) -> Optional[Dict]:
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return None
