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
            # ── Headless Linux login flow ────────────────────────────────────
            # Problem: Chrome cannot open a GUI window without a display.
            # Solution: Start Chrome in --headless=new mode (no display needed)
            #   and expose Chrome DevTools Protocol on a fixed TCP port.
            #   The operator SSH-tunnels that port to their local machine, then
            #   operates the Amazon login page through Chrome's DevTools remote
            #   view (chrome://inspect).
            #
            # Verified login steps (tested on Ubuntu 22.04 → macOS):
            #
            #   Step 1 — Open SSH tunnel (new local terminal, keep open):
            #     ssh -L 9222:localhost:9222 <user>@<server-ip>
            #
            #   Step 2 — Open DevTools remote view in LOCAL Chrome:
            #     chrome://inspect  →  Configure  →  add localhost:9222
            #     The Amazon sign-in page appears under "Remote Target".
            #     Click "inspect" to open the DevTools window.
            #
            #   Step 3 — Enter email in the DevTools remote view:
            #     Keyboard input works normally for the email/username field.
            #     Click "Continue".
            #
            #   Step 4 — Password field workaround (Amazon blocks CDP key events):
            #     Amazon's password field intercepts CDP-injected keystrokes, so
            #     direct typing does NOT work.  Use the DevTools Console instead
            #     (copy-paste the exact snippet below — verified working):
            #
            #     var pwd = document.getElementById('ap_password');
            #     pwd.focus();
            #     var nativeInputValueSetter = Object.getOwnPropertyDescriptor(
            #       window.HTMLInputElement.prototype, 'value'
            #     ).set;
            #     nativeInputValueSetter.call(pwd, 'YOUR_PASSWORD_HERE');
            #     pwd.dispatchEvent(new Event('input',  { bubbles: true }));
            #     pwd.dispatchEvent(new Event('change', { bubbles: true }));
            #     document.getElementById('signInSubmit').click();
            #
            #     Why this works: React/Amazon wraps the password input with
            #     synthetic event guards that block CDP keydown/keyup injection.
            #     Using window.HTMLInputElement.prototype's native value setter
            #     writes the value at the DOM level, bypassing those guards.
            #     The explicit input+change dispatch then triggers Amazon's
            #     form-validation logic so the Sign-In button activates.
            #
            #   Step 5 — Script detects login automatically and saves cookies.
            #
            # Note: set_local_port must match --remote-debugging-port so
            #   DrissionPage connects to the same Chrome instance the user sees.
            co.headless(True)
            co.set_argument("--headless=new")
            co.set_argument(f"--remote-debugging-port={_REMOTE_DEBUG_PORT}")
            co.set_argument("--remote-debugging-address=127.0.0.1")
            co.set_local_port(_REMOTE_DEBUG_PORT)
            server_ip   = os.environ.get("SERVER_IP",   "<server-ip>")
            server_user = os.environ.get("SERVER_USER", "root")
            logger.warning(
                "\n"
                "╔══════════════════════════════════════════════════════════════════╗\n"
                "║  HEADLESS SERVER — Amazon login via Chrome DevTools SSH tunnel   ║\n"
                "╠══════════════════════════════════════════════════════════════════╣\n"
                "║  1. Open SSH tunnel (new local terminal, keep open):             ║\n"
                f"║     ssh -L 9222:localhost:9222 {server_user}@{server_ip}                 ║\n"
                "║  2. LOCAL Chrome → chrome://inspect → Configure → localhost:9222 ║\n"
                "║     Click 'inspect' on the Amazon tab                            ║\n"
                "║  3. Enter email in the remote view (keyboard works normally)     ║\n"
                "║  4. PASSWORD — paste into DevTools Console (direct typing blocked): ║\n"
                "║     var pwd=document.getElementById('ap_password');              ║\n"
                "║     pwd.focus();                                                 ║\n"
                "║     var s=Object.getOwnPropertyDescriptor(                       ║\n"
                "║       window.HTMLInputElement.prototype,'value').set;            ║\n"
                "║     s.call(pwd,'YOUR_PASSWORD_HERE');                            ║\n"
                "║     pwd.dispatchEvent(new Event('input',{bubbles:true}));        ║\n"
                "║     pwd.dispatchEvent(new Event('change',{bubbles:true}));       ║\n"
                "║     document.getElementById('signInSubmit').click();             ║\n"
                "║  5. Script detects login and saves cookies automatically         ║\n"
                "╚══════════════════════════════════════════════════════════════════╝"
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
            page.get(target_url, timeout=60)

            if wait_for_manual:
                timeout_sec = 600 if headless_server else 120
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

    def import_from_browser_export(self, export_file: str) -> Dict[str, str]:
        """
        Import cookies from a browser extension export (Cookie-Editor JSON format).

        Cookie-Editor exports an array:
          [{"name": "session-id", "value": "...", "domain": ".amazon.com", ...}, ...]

        Usage:
          1. Install Cookie-Editor in your LOCAL Chrome.
          2. Log in to amazon.com on your local machine.
          3. Click Cookie-Editor → Export → Export as JSON → save file.
          4. scp the file to the server, then call this method.

        Example:
          helper = AmazonCookieHelper()
          helper.import_from_browser_export("/tmp/amazon_cookies.json")
        """
        with open(export_file, "r", encoding="utf-8") as f:
            raw = json.load(f)

        # Cookie-Editor exports a list; our cache stores a name→value dict.
        if isinstance(raw, list):
            cookies_dict = {c["name"]: c["value"] for c in raw if "name" in c and "value" in c}
        elif isinstance(raw, dict) and "cookies" in raw:
            # Already in our own cache format — just re-save.
            cookies_dict = raw["cookies"]
        else:
            raise ValueError(f"Unrecognised cookie export format in {export_file}")

        if "session-id" not in cookies_dict:
            raise ValueError("Imported cookies missing 'session-id' — make sure you exported from amazon.com while logged in.")

        cookies_dict.setdefault("i18n-prefs", "USD")
        cookies_dict.setdefault("lc-main", "en_US")

        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        self._save_to_cache({
            "cookies": cookies_dict,
            "user_agent": ua,
            "is_logged_in": True,
        })
        logger.info(f"💾 Imported {len(cookies_dict)} cookies from {export_file} → {self.cache_file}")
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
