from __future__ import annotations

import json
import logging
import os
import random
import re
import subprocess
import sys
import time

from DrissionPage import ChromiumOptions, ChromiumPage

logger = logging.getLogger(__name__)

# Remote debug port used when running on a headless Linux server in login mode.
# User connects via:  ssh -L 9222:localhost:9222 user@server
# Then open:          http://localhost:9222  in their local Chrome.
_REMOTE_DEBUG_PORT = 9222

# ASIN used for WAF warmup after login: navigate to its reviews page so the
# WAF JS challenge fires (issuing aws-waf-token) and click "Show 10 more" to
# capture anti-csrftoken-a2z via the network listener.
_WARMUP_ASIN = "B0CPJ37XZH"

# curl_cffi built-in Chrome impersonation targets (highest → lowest).
# The JA3 fingerprint is only correct for these exact major versions.
# Versions between targets (e.g. Chrome 126 between 124 and 131) will have
# a JA3 mismatch between DrissionPage and curl_cffi — AJAX tier will get 403.
_CFFI_TARGETS: list[int] = [146, 142, 136, 131, 124, 120, 119]


def _detect_chrome_major() -> int | None:
    """Return the installed Chrome major version, or None if undetectable."""
    candidates = (
        [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]
        if sys.platform == "darwin"
        else ["google-chrome", "chromium-browser", "chromium"]
    )
    for bin_path in candidates:
        try:
            out = subprocess.check_output(
                [bin_path, "--version"], stderr=subprocess.DEVNULL, timeout=5
            ).decode()
            m = re.search(r"Chrome[/\s](\d+)", out)
            if m:
                return int(m.group(1))
        except Exception:
            continue
    return None


def _nearest_cffi_target(major: int) -> int:
    """Return the highest curl_cffi target version that is <= *major*."""
    for t in _CFFI_TARGETS:
        if major >= t:
            return t
    return _CFFI_TARGETS[-1]


def _build_ua(chrome_major: int) -> str:
    return (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{chrome_major}.0.0.0 Safari/537.36"
    )


def _resolve_amazon_ua() -> tuple[str, bool]:
    """Return (ua_string, has_mismatch).

    ua_string is built from the nearest curl_cffi target so that the HTTP
    User-Agent header and the TLS JA3 fingerprint always agree.
    has_mismatch is True when the installed Chrome version is between targets,
    meaning DrissionPage's real JA3 won't match curl_cffi's impersonated JA3.
    """
    actual = _detect_chrome_major()
    if actual is None:
        return _build_ua(146), False  # fallback — assume Chrome 146
    target = _nearest_cffi_target(actual)
    mismatch = actual != target
    return _build_ua(target), mismatch


# Canonical UA for all Amazon requests: auto-derived from the installed Chrome
# version, mapped to the nearest curl_cffi target so JA3 and UA always agree.
# No manual update needed — reinstall/upgrade Chrome and restart the service.
AMAZON_UA, _CHROME_TARGET_MISMATCH = _resolve_amazon_ua()


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

    def fetch_fresh_cookies(
        self, wait_for_manual: bool = False, warmup_asin: str = _WARMUP_ASIN
    ) -> dict[str, str]:
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

        # Resolve Chrome binary — macOS hides it inside an .app bundle
        if sys.platform == "darwin":
            _MAC_CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
            _MAC_CHROMIUM = "/Applications/Chromium.app/Contents/MacOS/Chromium"
            for _candidate in (_MAC_CHROME, _MAC_CHROMIUM):
                if os.path.isfile(_candidate):
                    co.set_browser_path(_candidate)
                    break

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
            server_ip = os.environ.get("SERVER_IP", "<server-ip>")
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

        ua = AMAZON_UA
        co.set_user_agent(ua)

        page = None
        cookies_dict = {}
        anti_csrf_token: str | None = None

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
                if _CHROME_TARGET_MISMATCH:
                    actual = _detect_chrome_major()
                    target = _nearest_cffi_target(actual)
                    logger.warning(
                        f"⚠️  Chrome {actual} is installed but the nearest curl_cffi target "
                        f"is chrome{target}. DrissionPage will present Chrome {actual} JA3; "
                        f"curl_cffi will present Chrome {target} JA3. "
                        f"The aws-waf-token issued here won't work for AJAX requests — "
                        f"install Chrome {_CFFI_TARGETS[0]} to eliminate the mismatch."
                    )

                # Amazon auto-redirects to the homepage after login.
                # Wait for the homepage JS (including WAF challenge) to finish.
                time.sleep(20)

                # ── WAF warmup ──────────────────────────────────────────────
                # aws-waf-token is only issued after the WAF JS challenge runs
                # on a product/review page (not on the sign-in page or homepage).
                # anti-csrftoken-a2z is generated by Amazon's JS — not a cookie —
                # and can only be captured by intercepting the "Show 10 more"
                # AJAX request via a network listener.
                if warmup_asin:
                    reviews_url = (
                        f"https://www.amazon.com/product-reviews/{warmup_asin}"
                        f"?reviewerType=all_reviews&pageNumber=1"
                    )
                    logger.info(f"🔥 WAF warmup: navigating to reviews/{warmup_asin}...")
                    # Switch to normal load mode so the full page JS executes —
                    # including the WAF challenge that issues aws-waf-token.
                    # eager mode stops at DOM-ready, before WAF JS has run.
                    page.set.load_mode.normal()
                    page.get(reviews_url, timeout=60)
                    page.set.load_mode.eager()
                    time.sleep(5)

                    warmup_html = page.html
                    has_reviews = 'data-hook="review"' in warmup_html
                    try:
                        _mid = page.run_cdp("Network.getAllCookies")
                        _mid_names = {c["name"] for c in _mid.get("cookies", [])}
                    except Exception:
                        _mid_names = set()
                    logger.info(
                        f"  warmup page URL: {page.url[:80]} | has_reviews={has_reviews} | "
                        f"aws-waf-token: {'PRESENT' if 'aws-waf-token' in _mid_names else 'MISSING'}"
                    )

                    try:
                        page.listen.start("portal/customer-reviews/ajax")
                        show_more = (
                            page.ele("@@data-hook=show-more-button", timeout=5)
                            or page.ele('xpath://a[@data-hook="show-more-button"]', timeout=3)
                            or page.ele(".cm-cr-show-more", timeout=3)
                        )
                        if show_more:
                            show_more.click()
                            packet = page.listen.wait(timeout=15)
                            page.listen.stop()
                            if packet:
                                req_headers = (
                                    getattr(getattr(packet, "request", None), "headers", {}) or {}
                                )
                                anti_csrf_token = req_headers.get(
                                    "anti-csrftoken-a2z"
                                ) or req_headers.get("Anti-Csrftoken-A2z")
                                if anti_csrf_token:
                                    logger.info(
                                        f"✅ Captured anti-csrftoken-a2z ({len(anti_csrf_token)} chars)"
                                    )
                                else:
                                    logger.warning(
                                        "  AJAX intercepted but anti-csrftoken-a2z header absent."
                                    )
                        else:
                            page.listen.stop()
                            logger.warning(
                                "  No show-more-button found on warmup reviews page — "
                                "anti-csrftoken-a2z will be captured on first CommentsExtractor run."
                            )
                    except Exception as warmup_exc:
                        logger.warning(f"WAF warmup AJAX interception failed: {warmup_exc}")
                        try:
                            page.listen.stop()
                        except Exception:
                            pass
                    # WAF JS sets aws-waf-token asynchronously after the AJAX call —
                    # wait for it to complete before the CDP cookie snapshot.
                    time.sleep(5)
            else:
                time.sleep(5)
                continue_btn = page.ele("text:Continue shopping", timeout=2)
                if continue_btn:
                    continue_btn.click()
                    time.sleep(2)

            # Use CDP Network.getAllCookies to capture every cookie the browser holds,
            # including HttpOnly and WAF-challenge cookies (e.g. aws-waf-token) that
            # page.cookies() silently omits due to domain/HttpOnly filtering.
            try:
                cdp_result = page.run_cdp("Network.getAllCookies")
                raw_cookies = cdp_result.get("cookies", [])
            except Exception:
                raw_cookies = page.cookies()
            cookies_dict = {c.get("name"): c.get("value") for c in raw_cookies}

            if "session-id" not in cookies_dict:
                raise RuntimeError(
                    "Failed to capture 'session-id' — Amazon may have blocked "
                    "the request or shown a CAPTCHA."
                )

            cookies_dict["i18n-prefs"] = "USD"
            cookies_dict["lc-main"] = "en_US"
            if anti_csrf_token:
                cookies_dict["anti-csrftoken-a2z"] = anti_csrf_token
            self._save_to_cache(
                {
                    "cookies": cookies_dict,
                    "user_agent": ua,
                    "is_logged_in": wait_for_manual,
                }
            )
            logger.info(
                f"💾 Captured {len(cookies_dict)} cookies. Session saved to {self.cache_file}. "
                f"aws-waf-token: {'PRESENT' if 'aws-waf-token' in cookies_dict else 'MISSING'} | "
                f"anti-csrftoken-a2z: {'PRESENT' if 'anti-csrftoken-a2z' in cookies_dict else 'MISSING'}"
            )

        except Exception as e:
            logger.error(f"❌ CookieHelper Error: {e}")
            raise
        finally:
            if page:
                page.quit()

        return cookies_dict

    def import_from_browser_export(self, export_file: str) -> dict[str, str]:
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
        with open(export_file, encoding="utf-8") as f:
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
            raise ValueError(
                "Imported cookies missing 'session-id' — make sure you exported from amazon.com while logged in."
            )

        cookies_dict.setdefault("i18n-prefs", "USD")
        cookies_dict.setdefault("lc-main", "en_US")

        ua = AMAZON_UA
        self._save_to_cache(
            {
                "cookies": cookies_dict,
                "user_agent": ua,
                "is_logged_in": True,
            }
        )
        logger.info(
            f"💾 Imported {len(cookies_dict)} cookies from {export_file} → {self.cache_file}"
        )
        return cookies_dict

    def import_from_chrome(self) -> dict[str, str]:
        """
        Capture Amazon cookies from the user's real Chrome profile.
        Launches a second Chrome instance on a private CDP port using the existing
        profile directory so it inherits the logged-in session. Captures ALL cookies
        (including session-only aws-waf-token) via CDP Network.getAllCookies.
        """
        chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        profile_dir = os.path.expanduser("~/Library/Application Support/Google/Chrome")

        # Use AMAZON_UA — already resolved to the nearest curl_cffi target for
        # the installed Chrome, so JA3 and UA stay aligned.
        ua = AMAZON_UA

        # Copy just the Cookies SQLite file into a temp profile so Chrome starts
        # logged-in without conflicting with the user's running Chrome instance.
        import shutil
        import tempfile

        src_cookies = os.path.join(profile_dir, "Default", "Cookies")
        tmp_dir = tempfile.mkdtemp(prefix="chrome_cookie_capture_")
        tmp_default = os.path.join(tmp_dir, "Default")
        os.makedirs(tmp_default, exist_ok=True)
        if os.path.exists(src_cookies):
            shutil.copy2(src_cookies, os.path.join(tmp_default, "Cookies"))

        co = ChromiumOptions()
        co.set_browser_path(chrome_bin)
        co.set_user_data_path(tmp_dir)
        co.set_local_port(random.randint(10000, 60000))
        co.set_argument("--no-sandbox")
        co.set_argument("--disable-gpu")
        co.set_argument("--disable-dev-shm-usage")
        # No incognito — real cookies needed for WAF to recognise the session.

        page = None
        try:
            page = ChromiumPage(co)
            page.set.load_mode.eager()
            logger.info(
                "🌐 Navigating to Amazon with real session cookies to trigger WAF challenge..."
            )
            page.get("https://www.amazon.com/", timeout=30)
            time.sleep(10)  # WAF JS challenge needs time to complete and set aws-waf-token

            try:
                cdp_result = page.run_cdp("Network.getAllCookies")
                raw_cookies = cdp_result.get("cookies", [])
            except Exception:
                raw_cookies = page.cookies()

            cookies_dict = {
                c.get("name"): c.get("value")
                for c in raw_cookies
                if c.get("domain", "").endswith("amazon.com")
            }

            if "session-id" not in cookies_dict:
                raise RuntimeError(
                    "No Amazon session found — make sure you are logged in to amazon.com in Chrome."
                )

            cookies_dict.setdefault("i18n-prefs", "USD")
            cookies_dict.setdefault("lc-main", "en_US")
            self._save_to_cache({"cookies": cookies_dict, "user_agent": ua, "is_logged_in": True})
            logger.info(f"💾 Captured {len(cookies_dict)} cookies → {self.cache_file}")
            logger.info(
                f"   aws-waf-token: {'PRESENT' if 'aws-waf-token' in cookies_dict else 'MISSING'}"
            )
            return cookies_dict
        finally:
            if page:
                page.quit()
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def get_cookie_data(self, force_refresh: bool = False) -> dict:
        if not force_refresh:
            cached = self._load_from_cache()
            if cached:
                return cached
        return self.fetch_fresh_cookies(wait_for_manual=False)

    def _save_to_cache(self, data: dict):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def _load_from_cache(self) -> dict | None:
        if os.path.exists(self.cache_file):
            with open(self.cache_file, encoding="utf-8") as f:
                return json.load(f)
        return None
