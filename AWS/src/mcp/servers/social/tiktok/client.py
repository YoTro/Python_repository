from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import urllib.parse
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from DrissionPage import ChromiumPage

from curl_cffi import requests

from src.core.errors.exceptions import RetryableError
from src.gateway.rate_limit import RateLimiter

from .auth import TikTokSigner

logger = logging.getLogger(__name__)

# --- ARCHITECTURAL NOTE & WAF BYPASS DISCOVERY ---
# During the reverse engineering phase, a critical vulnerability in TikTok's
# Web WAF was discovered regarding 'msToken' privilege levels.
#
# 1. THE PROBLEM: Dynamically generated 'msToken' from standard page loads
#    are often assigned 'Level 0' privileges (Public Data only), causing
#    interaction APIs like /api/comment/list/ to return silent empty responses.
#
# 2. THE DISCOVERY: Sending a "naked" (unauthenticated & unscreened) GET request
#    to /api/recommend/item_list/?aid=1988 triggers the backend to issue a
#    'Level 1' (Interaction-privileged) msToken via Set-Cookie.
#
# 3. THE SOLUTION: This client implements a 'seed-on-demand' strategy via the
#    _seed_ms_token() method, effectively "tricking" the WAF into granting
#    high-privilege session credentials without requiring a full JS environment
#    or manual login.
# --------------------------------------------------


class TikTokClient:
    """
    L1 Data Extractor for TikTok using curl_cffi, dynamic X-Bogus generation,
    and automatic session initialization (ttwid/webid extraction).
    """

    @staticmethod
    def _build_sec_ch_ua(user_agent: str) -> str:
        """Derive sec-ch-ua from the Chrome major version in the UA string."""
        m = re.search(r"Chrome/(\d+)", user_agent)
        v = m.group(1) if m else "120"
        return f'"Google Chrome";v="{v}", "Chromium";v="{v}", "Not)A;Brand";v="24"'

    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
        self.session = requests.Session(impersonate="chrome")

        # Default msToken that bypassed WAF for unauthenticated requests
        self._default_ms_token = "5CXDD9eri9K2V5yFV8FqLWdlGZ60UTQ3f6Io_vtOV6FOVkn19nviaABiUPIj4o8UOgy7KvwMJ1lQy6FiiWx7J_R5wBuD8CIvtSdJM65O_bG0GGBPY6fQKkrwVF7X-2D7KncdPUqKuHv7enl5zegrCig="

        self.base_headers = {
            "accept": "*/*",
            "accept-language": "en",
            "priority": "u=1, i",
            "sec-ch-ua": self._build_sec_ch_ua(self.user_agent),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agent,
        }

        # Params common to every TikTok API call.
        # browser_version is excluded — it derives from self.user_agent which may
        # be patched after __init__, so callers inject it fresh per request.
        self.base_params: dict[str, str] = {
            "aid": "1988",
            "app_language": "en",
            "app_name": "tiktok_web",
            "browser_language": "en",
            "browser_name": "Mozilla",
            "browser_online": "true",
            "browser_platform": "MacIntel",
            "channel": "tiktok_web",
            "cookie_enabled": "true",
            "data_collection_enabled": "false",
            "device_platform": "web_pc",
            "focus_state": "true",
            "history_len": "2",
            "is_fullscreen": "false",
            "is_page_visible": "true",
            "os": "mac",
            "priority_region": "",
            "referer": "",
            "region": "US",
            "screen_height": "900",
            "screen_width": "1440",
            "tz_name": "America/New_York",
            "user_is_login": "false",
            "webcast_language": "en",
        }

    # Fallback clientABVersions — used when the hydration JSON doesn't contain
    # abTestVersion.versionName.  Extracted from a real browser session; prefer
    # the live value from the page whenever possible.
    _CLIENT_AB_VERSIONS = (
        "70508271,73720540,75638230,75694226,75843653,76034400,76055828,76065197,"
        "76088343,76143645,76146170,76146379,76184862,76191886,76212861,76248967,"
        "76251328,76252684,76276622,76299613,76308135,76314877,76360146,76365577,"
        "76378433,76383374,76403729,70405643,71057832,71200802,72361743,73171280,"
        "73208420,74276218,74413136,74844724,75330961"
    )

    def _seed_ms_token(self, video_referer: str = "https://www.tiktok.com/@tiktok") -> str:
        """
        Retrieves a Level 1 msToken by:
        1. Fetching the video/profile page to collect ttwid + extract odinId, device_id, itemID.
        2. Calling /api/related/item_list/ with signed params — reads the Level 1 token from
           the x-ms-token response header (also present in Set-Cookie as msToken).

        Key fixes vs prior version:
        - Adds clientABVersions (required by WAF to return data).
        - browser_language="en" (not "en-US") — matches what browser sends.
        - Signs the seed request with X-Gnarly + X-Bogus.
        - Builds URL manually so '/' in X-Gnarly is never encoded as '%2F'.
        - Reads token from x-ms-token response header, not just Set-Cookie.
        """
        try:
            # Step 1: fetch the page to collect session cookies and extract IDs from hydration JSON
            page_headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "en",
                "cache-control": "no-cache",
                "pragma": "no-cache",
                "priority": "u=0, i",
                "sec-ch-ua": self.base_headers["sec-ch-ua"],
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": self.user_agent,
            }
            response = self.session.get(video_referer, headers=page_headers, timeout=10)
            cookies_dict = self.session.cookies.get_dict()
            ttwid = cookies_dict.get("ttwid", "")

            odin_id = "7654397973388346399"
            device_id = "7654397980141078047"
            item_id = ""
            ab_versions = self._CLIENT_AB_VERSIONS
            category_type = "113"

            render_data_text = re.compile(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">(.*?)</script>'
            ).findall(response.text)
            if render_data_text:
                try:
                    render_data = json.loads(
                        urllib.parse.unquote(render_data_text[0]), strict=False
                    )
                    scope = render_data.get("__DEFAULT_SCOPE__", {})
                    app_ctx = scope.get("webapp.app-context", {})
                    odin_id = app_ctx.get("odinId", odin_id)
                    device_id = app_ctx.get("wid", device_id)
                    category_type = str(app_ctx.get("categoryType", category_type))
                    ab_versions = (
                        scope.get("abTestVersion", {}).get("versionName", "")
                        or self._CLIENT_AB_VERSIONS
                    )
                    video_detail = scope.get("webapp.video-detail", {})
                    item_id = video_detail.get("itemInfo", {}).get("itemStruct", {}).get("id", "")
                except Exception as e:
                    logger.warning(f"Failed to parse hydration data: {e}")
                    ab_versions = self._CLIENT_AB_VERSIONS

            # Step 2: call /api/related/item_list/ to obtain a Level 1 msToken
            params = {
                **self.base_params,
                "CategoryType": category_type,
                "WebIdLastTime": str(int(time.time())),
                "browser_version": self.user_agent.replace("Mozilla/", ""),
                "clientABVersions": ab_versions,
                "count": "16",
                "coverFormat": "2",
                "cursor": "0",
                "device_id": str(device_id),
                "from_page": "video",
                "isNonPersonalized": "false",
                "itemID": item_id,
                "language": "en",
                "launch_mode": "direct",
                "odinId": str(odin_id),
                "video_encoding": "dash",
                "msToken": "",
            }
            if ttwid:
                self.session.cookies.set("ttwid", ttwid, domain=".tiktok.com")

            # Signing order matters: X-Gnarly is computed first (over params without
            # any signature), then X-Bogus is computed over the query string that
            # already contains X-Gnarly.  Reversing the order invalidates both.
            qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            x_gnarly = TikTokSigner.generate_x_gnarly(qs, self.user_agent)
            params["X-Gnarly"] = x_gnarly
            qs2 = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            x_bogus = TikTokSigner.generate_x_bogus(qs2, self.user_agent, int(time.time()))
            params["X-Bogus"] = x_bogus
            # Move signatures to the end — TikTok's WAF expects them there
            final = {k: v for k, v in params.items() if k not in ("X-Bogus", "X-Gnarly")}
            final["X-Bogus"] = x_bogus
            final["X-Gnarly"] = x_gnarly

            api_headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "priority": "u=1, i",
                "sec-ch-ua": self.base_headers["sec-ch-ua"],
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "referer": video_referer,
                "user-agent": self.user_agent,
            }
            # Build URL manually — prevents curl_cffi from re-encoding '/' in X-Gnarly as '%2F'
            full_url = "https://www.tiktok.com/api/related/item_list/?" + urllib.parse.urlencode(
                final, quote_via=urllib.parse.quote
            )
            seed_resp = self.session.get(full_url, headers=api_headers, timeout=10)
            # Read token from x-ms-token header (canonical) or Set-Cookie fallback
            token = seed_resp.headers.get("x-ms-token", "")
            if not token:
                sc = seed_resp.headers.get("set-cookie", "")
                if "msToken=" in sc:
                    token = sc.split("msToken=")[1].split(";")[0]
            if not token:
                token = self.session.cookies.get_dict().get("msToken", "")
            if token:
                logger.debug(f"Server-seeded msToken: {token[:20]}...")
            return token
        except Exception as e:
            logger.warning(f"Failed to seed msToken from server: {e}")
            return ""

    def _generate_ms_token(self, randomlength: int = 107) -> str:
        """
        Returns a server-issued msToken. Falls back to a random string if the server is unreachable.
        """
        token = self._seed_ms_token()
        if token:
            return token
        random_str = ""
        base_str = "ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_="
        length = len(base_str) - 1
        for _ in range(randomlength):
            random_str += base_str[random.randint(0, length)]
        return random_str

    def _get_ttwid_webid(self, req_url: str) -> tuple[str, str, str]:
        """
        Visits a TikTok page to extract fresh ttwid cookie, webid (odinId), and deviceId (wid).
        """
        for _ in range(3):
            try:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "en",
                    "cache-control": "no-cache",
                    "pragma": "no-cache",
                    "priority": "u=0, i",
                    "sec-ch-ua": self.base_headers["sec-ch-ua"],
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"macOS"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "none",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                    "user-agent": self.user_agent,
                }

                response = self.session.request("GET", req_url, headers=headers, timeout=10)

                # Fetch from session cookies (which accumulate) rather than just response cookies
                cookies_dict = self.session.cookies.get_dict()
                ttwid_str = cookies_dict.get("ttwid", "")

                # Extract JSON hydration data
                render_data_text = re.compile(
                    r"\<script id=\"__UNIVERSAL_DATA_FOR_REHYDRATION__\" type\=\"application\/json\"\>(.*?)\<\/script\>"
                ).findall(response.text)
                if not render_data_text:
                    render_data_text = re.compile(
                        r"\<script id=\"RENDER_DATA\" type\=\"application\/json\"\>(.*?)\<\/script\>"
                    ).findall(response.text)

                odin_id = "7619886743638033430"  # Fallback
                device_id = "7619886743638033430"

                if render_data_text:
                    render_data_text = urllib.parse.unquote(render_data_text[0])
                    try:
                        render_data_json = json.loads(render_data_text, strict=False)

                        # Path 1: New UNIVERSAL_DATA structure
                        app_ctx = render_data_json.get("__DEFAULT_SCOPE__", {}).get(
                            "webapp.app-context", {}
                        )
                        odin_id = app_ctx.get("odinId", odin_id)
                        device_id = app_ctx.get("wid", device_id)

                        # Path 2: Legacy or App-based structure
                        if str(odin_id) == "7619886743638033430":
                            odin_id = (
                                render_data_json.get("app", {})
                                .get("odin", {})
                                .get("user_unique_id", odin_id)
                            )

                    except Exception as e:
                        logger.warning(f"JSON parse error for TikTok render data: {e}")

                if ttwid_str:
                    return ttwid_str, str(odin_id), str(device_id)
            except Exception as e:
                logger.warning(f"Error fetching ttwid/webid from {req_url}: {e}")
                time.sleep(1)

        return "", "7619886743638033430", "7619886743638033430"

    def _request(self, endpoint: str, params: dict, referer: str, ttwid: str) -> dict[str, Any]:
        """Signs the request with X-Bogus + X-Gnarly and executes it."""
        url = f"https://www.tiktok.com{endpoint}"

        params = {
            **self.base_params,
            "WebIdLastTime": str(int(time.time())),
            "browser_version": self.user_agent.replace("Mozilla/", ""),
            "current_region": "US",
            "enter_from": "tiktok_web",
            "fromWeb": "1",
            "is_non_personalized": "false",
            **params,
        }

        ms_token = self._generate_ms_token(107)
        params["msToken"] = ms_token

        # X-Gnarly first (no signatures in params yet), then X-Bogus over the
        # string that already contains X-Gnarly — order is non-negotiable.
        qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        x_gnarly = TikTokSigner.generate_x_gnarly(qs, self.user_agent)
        params["X-Gnarly"] = x_gnarly
        qs2 = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        timestamp = int(time.time())
        x_bogus = TikTokSigner.generate_x_bogus(qs2, self.user_agent, timestamp)
        params["X-Bogus"] = x_bogus
        # Signatures must appear at the end of the query string
        final_params = {k: v for k, v in params.items() if k not in ("X-Bogus", "X-Gnarly")}
        final_params["X-Bogus"] = x_bogus
        final_params["X-Gnarly"] = x_gnarly

        headers = self.base_headers.copy()
        headers["referer"] = referer

        self.session.cookies.set("msToken", ms_token, domain=".tiktok.com")
        if ttwid:
            self.session.cookies.set("ttwid", ttwid, domain=".tiktok.com")

        # Build URL manually — prevents curl_cffi from re-encoding '/' in X-Gnarly as '%2F'
        full_url = url + "?" + urllib.parse.urlencode(final_params, quote_via=urllib.parse.quote)

        limiter = RateLimiter()
        for attempt in range(3):
            if not limiter.acquire_source("tiktok"):
                raise RetryableError("tiktok source rate limit timeout", retry_after_seconds=60)
            try:
                response = self.session.get(full_url, headers=headers)
                if response.status_code == 200:
                    try:
                        return response.json()
                    except Exception as e:
                        logger.error(
                            f"Failed to parse TikTok JSON response: {e}. Snippet: {response.text[:100]}"
                        )
                        return {}
                elif response.status_code == 429:
                    wait = int(
                        response.headers.get("Retry-After", 2 ** (attempt + 1))
                    ) + random.uniform(0, 1)
                    logger.warning(
                        f"TikTok 429 rate limited, waiting {wait:.1f}s (attempt {attempt + 1}/3)"
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.error(f"TikTok request failed with status: {response.status_code}")
                    return {}
            except Exception as e:
                logger.error(f"TikTok connection error: {e}")
                return {}
        raise RetryableError("TikTok still rate limited after 3 retries", retry_after_seconds=60)

    def get_tag_info(self, tag_name: str) -> dict[str, Any]:
        """
        Fetch metadata for a specific hashtag (tag_name).
        Returns counts like videoCount and viewCount.
        """
        # A valid session and msToken is needed even for detail API now
        referer = f"https://www.tiktok.com/tag/{tag_name}"
        logger.info(f"Fetching metadata for hashtag: #{tag_name}")
        ttwid, odin_id, device_id = self._get_ttwid_webid(referer)

        params = {
            "challengeName": tag_name,
            "device_id": device_id,
            "odinId": odin_id,
        }

        data = self._request("/api/challenge/detail/", params, referer, ttwid)

        chal_info = data.get("challengeInfo", {})
        stats = chal_info.get("statsV2", chal_info.get("stats", {}))

        return {
            "id": chal_info.get("challenge", {}).get("id"),
            "title": chal_info.get("challenge", {}).get("title"),
            "desc": chal_info.get("challenge", {}).get("desc"),
            "video_count": int(stats.get("videoCount", 0)),
            "view_count": int(stats.get("viewCount", 0)),
        }

    def get_hashtag_videos(
        self, challenge_id: str, tag_name: str, count: int = 0
    ) -> list[dict[str, Any]]:
        """
        Fetch videos for a specific hashtag (challengeID), supporting pagination.
        If count=0, it will fetch ALL available videos until the API indicates no more data.
        Automatically handles session initialization and cursor updates.
        """
        referer = f"https://www.tiktok.com/tag/{tag_name}"
        logger.info(f"Initializing TikTok session for tag [{tag_name}]...")
        ttwid, odin_id, device_id = self._get_ttwid_webid(referer)

        if not ttwid:
            logger.error("Failed to acquire ttwid. TikTok API request may fail.")

        all_videos = []
        cursor = "0"

        while count == 0 or len(all_videos) < count:
            # Calculate how many more we need, cap single request at 30
            request_count = 30 if count == 0 else min(count - len(all_videos), 30)

            params = {
                **self.base_params,
                "browser_version": self.user_agent.replace("Mozilla/", ""),
                "challengeID": challenge_id,
                "count": str(request_count),
                "cursor": cursor,
                "device_id": device_id,
                "from_page": "hashtag",
                "language": "en",
                "odinId": odin_id,
                # hashtag feed uses larger viewport than default
                "screen_height": "1440",
                "screen_width": "2560",
            }

            logger.info(
                f"Requesting {request_count} videos at cursor {cursor} (Collected: {len(all_videos)}/{count})"
            )
            data = self._request("/api/challenge/item_list/", params, referer, ttwid)

            items = data.get("itemList", [])
            if not items:
                logger.warning("No more items returned from TikTok API.")
                break

            all_videos.extend(items)

            # Check if there is more data according to the API
            if not data.get("hasMore"):
                logger.info("TikTok API indicates no more videos available for this hashtag.")
                break

            # Update cursor for next page
            cursor = str(data.get("cursor", len(all_videos)))

            # Add a small human-like delay between pages to avoid rate limiting
            if len(all_videos) < count:
                time.sleep(random.uniform(1.0, 2.5))

        logger.info(f"Completed! Total videos collected for [{tag_name}]: {len(all_videos)}")
        return all_videos[:count]  # Trim to exact requested amount

    def search_videos(self, keyword: str, count: int = 20) -> list[dict[str, Any]]:
        """
        Search for videos by keyword.
        Dynamically fetches the challenge ID for the keyword as a tag.
        """
        tag_name = keyword.replace(" ", "").replace("#", "")
        tag_info = self.get_tag_info(tag_name)

        challenge_id = tag_info.get("id")
        if not challenge_id:
            logger.warning(
                f"Could not resolve challenge ID for tag [{tag_name}]. Falling back to baseline search."
            )
            challenge_id = "9789"  # Nike fallback for demo

        return self.get_hashtag_videos(challenge_id, tag_name, count)

    # Dedicated debug port for DrissionPage so we never conflict with a user's
    # Chrome on the default 9222.
    _DRISSION_PORT = 9223
    # Persistent profile for DrissionPage — cookies accumulate here across runs.
    _DRISSION_PROFILE = os.path.expanduser("~/.cache/drission_tiktok")

    @classmethod
    def _drission_page_with_cookies(cls) -> ChromiumPage | None:
        """
        Return a ChromiumPage that is guaranteed to have a TikTok session.

        Priority order:
        1. Existing Chrome on port 9222 (user started Chrome with --remote-debugging-port=9222).
        2. Re-use our own DrissionPage Chrome on port 9223 (already running from a prior call).
        3. Launch a fresh Chrome on port 9223 using the real Chrome profile directory
           (safe only when no Chrome is running — profile is unlocked).
        4. Launch a fresh Chrome on port 9223 with a dedicated profile
           (~/.cache/drission_tiktok/) seeded by copying the real Chrome cookie DB.
           macOS Chrome stores the encryption key in the Keychain by binary, so any
           Chrome process on the same machine can decrypt those cookies.
        5. Return None — caller should fall back to the signed-API path.
        """
        import os
        import shutil
        import socket
        import subprocess
        import sys

        try:
            from DrissionPage import ChromiumOptions, ChromiumPage
        except ImportError:
            return None

        port = cls._DRISSION_PORT

        def _port_open(p: int) -> bool:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                return s.connect_ex(("127.0.0.1", p)) == 0

        def _chrome_running() -> bool:
            try:
                name = "Google Chrome" if sys.platform == "darwin" else "chrome"
                return (
                    subprocess.run(["pgrep", "-x", name], capture_output=True, timeout=3).returncode
                    == 0
                )
            except Exception:
                return False

        # 1. User-managed Chrome with debug port
        if _port_open(9222):
            return ChromiumPage()

        # 2. Our own DrissionPage Chrome already running on dedicated port
        if _port_open(port):
            opts = ChromiumOptions().set_local_port(port)
            return ChromiumPage(addr_or_opts=opts)

        # Locate real Chrome binary and profile
        if sys.platform == "darwin":
            real_profile = os.path.expanduser("~/Library/Application Support/Google/Chrome")
            chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
        else:
            real_profile = os.path.expanduser("~/.config/google-chrome")
            chrome_bin = "/usr/bin/google-chrome"

        opts = ChromiumOptions()
        opts.set_local_port(port)
        if os.path.exists(chrome_bin):
            opts.set_paths(browser_path=chrome_bin)

        # 3. Real profile (only safe when Chrome is not already running)
        if os.path.exists(real_profile) and not _chrome_running():
            opts.set_paths(user_data_path=real_profile)
            try:
                return ChromiumPage(addr_or_opts=opts)
            except Exception as e:
                logger.warning(f"[browser] Real-profile launch failed: {e}")

        # 4. Dedicated profile seeded with real Chrome cookies
        drission_default = os.path.join(cls._DRISSION_PROFILE, "Default")
        os.makedirs(drission_default, exist_ok=True)

        # Seed cookies from the real Chrome Default profile (best-effort copy).
        # Chrome's macOS cookie encryption key lives in the Keychain under the
        # Chrome binary name, not the profile — so any Chrome on this machine
        # can decrypt cookies regardless of which profile dir they came from.
        real_default = os.path.join(real_profile, "Default")
        for fname in ("Cookies", "Web Data", "Preferences"):
            src = os.path.join(real_default, fname)
            dst = os.path.join(drission_default, fname)
            if os.path.exists(src) and not os.path.exists(dst):
                try:
                    shutil.copy2(src, dst)
                    logger.info(f"[browser] Seeded {fname} from real Chrome profile")
                except Exception as e:
                    logger.debug(f"[browser] Could not copy {fname}: {e}")

        opts.set_paths(user_data_path=cls._DRISSION_PROFILE)
        try:
            return ChromiumPage(addr_or_opts=opts)
        except Exception as e:
            logger.warning(f"[browser] Dedicated-profile launch failed: {e}")
            return None

    @staticmethod
    def _page_fetch(page: ChromiumPage, url: str) -> dict | None:
        """
        Execute a fetch() inside the browser page and return the parsed JSON.

        TikTok's SDK patches window.fetch to add X-Bogus + X-Gnarly automatically,
        so any URL fetched this way is properly signed without us doing any signing.
        The browser also sends all session cookies (credentials: 'include').
        """
        page.run_js(
            f"""
            window.__tt_fetch_done = false;
            window.__tt_fetch_data = null;
            (async () => {{
                try {{
                    const r = await window.fetch(
                        {json.dumps(url)},
                        {{credentials: 'include'}}
                    );
                    window.__tt_fetch_data = await r.json();
                }} catch(e) {{
                    window.__tt_fetch_data = {{error: e.message}};
                }}
                window.__tt_fetch_done = true;
            }})();
            """
        )
        for _ in range(12):
            time.sleep(1)
            if page.run_js("return !!window.__tt_fetch_done"):
                break
        return page.run_js("return window.__tt_fetch_data")

    def _get_comments_via_browser(
        self, video_id: str, author_id: str | None, count: int
    ) -> list[dict[str, Any]] | None:
        """
        Primary comment path: navigate to the video page in the user's existing
        Chrome session (has real TikTok cookies) and capture /api/comment/list/
        without any manual signing.  Returns None on failure so the caller can
        fall back to the signed-API path.

        Strategy A — XHR intercept: call window.fetch() from inside the page.
          TikTok's SDK has already patched window.fetch to inject X-Bogus +
          X-Gnarly, so the request is auto-signed and carries real session cookies.

        Strategy B — click + listen (fallback): click the comment button and
          capture the API packet via CDP network interception.
        """
        if author_id:
            author_id = author_id if author_id.startswith("@") else f"@{author_id}"
            video_url = f"https://www.tiktok.com/{author_id}/video/{video_id}"
        else:
            video_url = f"https://www.tiktok.com/video/{video_id}"

        logger.info(f"[browser] Fetching comments via DrissionPage: {video_url}")
        page = self._drission_page_with_cookies()
        if page is None:
            return None

        try:
            page.get(video_url)
            time.sleep(5)  # let TikTok SDK initialize and patch window.fetch

            all_comments: list[dict] = []

            # ── Strategy A: XHR intercept ────────────────────────────────────
            base_api = (
                f"/api/comment/list/?aweme_id={video_id}"
                "&count=20&cursor=0&aid=1988&app_name=tiktok_web"
            )
            first_data = self._page_fetch(page, base_api)

            if first_data and first_data.get("status_code") == 0:
                logger.info("[browser] XHR intercept succeeded")
                all_comments.extend(first_data.get("comments") or [])
                cursor = first_data.get("cursor", len(all_comments))
                has_more = bool(first_data.get("has_more"))

                while has_more and (count == 0 or len(all_comments) < count):
                    next_url = (
                        f"/api/comment/list/?aweme_id={video_id}"
                        f"&count=20&cursor={cursor}&aid=1988&app_name=tiktok_web"
                    )
                    page_data = self._page_fetch(page, next_url)
                    if not page_data or page_data.get("status_code", -1) != 0:
                        break
                    all_comments.extend(page_data.get("comments") or [])
                    cursor = page_data.get("cursor", len(all_comments))
                    has_more = bool(page_data.get("has_more"))
                    time.sleep(random.uniform(0.8, 1.5))

            else:
                # ── Strategy B: click comment button + CDP packet capture ────
                logger.info(
                    f"[browser] XHR intercept gave status_code="
                    f"{first_data.get('status_code') if first_data else 'None'}, "
                    "falling back to click+listen"
                )
                page.listen.start("/api/comment/list/")
                for sel in [
                    'xpath://div[@data-e2e="comment-icon"]//button[@data-testid="tux-web-icon-button"]',
                    'xpath://div[@role="button"][contains(@aria-label,"comment")]',
                    '[data-e2e="comment-icon"]',
                ]:
                    try:
                        el = page.ele(sel, timeout=5)
                        if el:
                            el.click()
                            break
                    except Exception:
                        pass

                time.sleep(2)
                packet = page.listen.wait(timeout=15)
                if not packet:
                    page.quit()
                    return None

                body = packet.response.body
                data = json.loads(body) if isinstance(body, str) else body
                all_comments.extend(data.get("comments") or [])
                cursor = str(data.get("cursor", len(all_comments)))
                has_more = bool(data.get("has_more"))
                base_url = packet.response.url

                while has_more and (count == 0 or len(all_comments) < count):
                    next_url = re.sub(r"cursor=([^&]+)", f"cursor={cursor}", base_url)
                    next_url = re.sub(r"count=\d+", "count=20", next_url)
                    page_data = self._page_fetch(page, next_url)
                    if not page_data or page_data.get("status_code", -1) != 0:
                        break
                    all_comments.extend(page_data.get("comments") or [])
                    cursor = str(page_data.get("cursor", len(all_comments)))
                    has_more = bool(page_data.get("has_more"))
                    time.sleep(random.uniform(0.8, 1.5))

            page.quit()
            result = all_comments[:count] if count > 0 else all_comments
            logger.info(f"[browser] Got {len(result)} comments for video {video_id}")
            return result

        except Exception as e:
            logger.warning(f"[browser] DrissionPage comment fetch failed: {e}")
            try:
                page.quit()
            except Exception:
                pass
            return None

    def get_video_comments(
        self, video_id: str, count: int = 20, author_id: str = None
    ) -> list[dict[str, Any]]:
        """
        Fetch comments for a specific video.

        Primary path: signed API (_get_video_comments_signed) — fast, headless,
        no browser required.  Falls back to DrissionPage if the signed path
        returns nothing (e.g. video requires a logged-in session).
        """
        result = self._get_video_comments_signed(video_id, count, author_id)
        if result:
            return result

        logger.info(
            f"[comments] Signed-API returned empty, falling back to DrissionPage for {video_id}"
        )
        browser_result = self._get_comments_via_browser(video_id, author_id, count)
        return browser_result if browser_result is not None else []

    def get_user_recent_video_stats(
        self, author_id: str = "", days: int = 30
    ) -> list[dict[str, Any]]:
        """
        Fetch stats for a user's videos posted within the last N days.

        Calls /api/post/item_list/ with pagination and stops as soon as a video
        older than the cutoff is encountered (feed is newest-first).

        Returns a list of dicts, one per qualifying video:
            id, createTime, desc, playCount, likeCount, commentCount, shareCount, collectCount

        Falls back to @tiktok if author_id is empty or secUid cannot be resolved.
        """
        FALLBACK = "@tiktok"
        if not author_id:
            author_id = FALLBACK

        author_id = author_id if author_id.startswith("@") else f"@{author_id}"
        profile_url = f"https://www.tiktok.com/{author_id}"
        cutoff = int(time.time()) - days * 86400

        # ── Step 1: single page visit → cookies + ttwid + odinId + secUid ────
        page_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": self.base_headers["sec-ch-ua"],
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self.user_agent,
        }
        ttwid = ""
        odin_id = "7620022218310960159"
        device_id = "7620022218281616927"
        sec_uid = ""
        ab_versions = self._CLIENT_AB_VERSIONS
        try:
            resp = self.session.get(profile_url, headers=page_headers, timeout=10)
            cookies = self.session.cookies.get_dict()
            ttwid = cookies.get("ttwid", "")
            m = re.compile(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">(.*?)</script>'
            ).search(resp.text)
            if m:
                hydration = json.loads(urllib.parse.unquote(m.group(1)), strict=False)
                scope = hydration.get("__DEFAULT_SCOPE__", {})
                app_ctx = scope.get("webapp.app-context", {})
                odin_id = app_ctx.get("odinId", odin_id)
                device_id = app_ctx.get("wid", device_id)
                ab_versions = (
                    scope.get("abTestVersion", {}).get("versionName", "")
                    or self._CLIENT_AB_VERSIONS
                )
                user_info = scope.get("webapp.user-detail", {}).get("userInfo", {}).get("user", {})
                sec_uid = user_info.get("secUid", "")
        except Exception as e:
            logger.warning(f"[post_list] Failed to extract secUid for {author_id}: {e}")

        if not sec_uid:
            if author_id == FALLBACK:
                logger.error("[post_list] Could not resolve secUid even for @tiktok fallback")
                return []
            # secUid is required by the API and cannot be guessed from the handle.
            # Re-running with @tiktok at least exercises the pagination code path
            # rather than erroring out silently.
            logger.warning(f"[post_list] secUid not found for {author_id}, retrying with @tiktok")
            return self.get_user_recent_video_stats("", days)

        # ── Step 2: seed msToken ────────────────────────────────────────────
        ms_token = self._seed_ms_token(profile_url)
        if ms_token:
            self.session.cookies.set("msToken", ms_token, domain=".tiktok.com")

        # ── Step 3: paginate /api/post/item_list/ ───────────────────────────
        referer = profile_url
        all_stats: list[dict] = []
        cursor = "0"

        while True:
            params = {
                **self.base_params,
                "WebIdLastTime": str(int(time.time())),
                "browser_version": self.user_agent.replace("Mozilla/", ""),
                "clientABVersions": ab_versions,
                "count": "16",
                "coverFormat": "2",
                "cursor": cursor,
                "device_id": str(device_id),
                "enable_cache": "false",
                "from_page": "user",
                "language": "en",
                "needPinnedItemIds": "true",
                "odinId": str(odin_id),
                "post_item_list_request_type": "0",
                "secUid": sec_uid,
                "video_encoding": "dash",
                "msToken": ms_token,
            }
            if ttwid:
                self.session.cookies.set("ttwid", ttwid, domain=".tiktok.com")

            qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            xg = TikTokSigner.generate_x_gnarly(qs, self.user_agent)
            params["X-Gnarly"] = xg
            qs2 = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            xb = TikTokSigner.generate_x_bogus(qs2, self.user_agent, int(time.time()))
            params["X-Bogus"] = xb
            final = {k: v for k, v in params.items() if k not in ("X-Bogus", "X-Gnarly")}
            final["X-Bogus"] = xb
            final["X-Gnarly"] = xg

            headers = {
                **self.base_headers,
                "referer": referer,
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }

            full_url = "https://www.tiktok.com/api/post/item_list/?" + urllib.parse.urlencode(
                final, quote_via=urllib.parse.quote
            )

            try:
                resp = self.session.get(full_url, headers=headers, timeout=15)
            except Exception as e:
                logger.warning(f"[post_list] Request error: {e}")
                break

            if not resp.text.strip():
                logger.warning("[post_list] Empty response body")
                break

            try:
                data = resp.json()
            except Exception as e:
                logger.warning(f"[post_list] JSON parse error: {e}")
                break

            items = data.get("itemList", [])
            if not items:
                break

            page_done = False
            for item in items:
                create_time = item.get("createTime", 0)
                # Feed is sorted newest-first, so the first item older than cutoff
                # means all remaining items are also out of range — stop paginating.
                if int(create_time) < cutoff:
                    page_done = True
                    break
                stats_raw = item.get("statsV2") or item.get("stats", {})
                all_stats.append(
                    {
                        "id": item.get("id"),
                        "createTime": create_time,
                        "desc": item.get("desc", ""),
                        "playCount": int(stats_raw.get("playCount", 0)),
                        "likeCount": int(stats_raw.get("diggCount", 0)),
                        "commentCount": int(stats_raw.get("commentCount", 0)),
                        "shareCount": int(stats_raw.get("shareCount", 0)),
                        "collectCount": int(stats_raw.get("collectCount", 0)),
                    }
                )

            if page_done or not data.get("hasMore"):
                break

            cursor = str(data.get("cursor", ""))
            if not cursor:
                break
            time.sleep(random.uniform(0.8, 1.5))

        logger.info(f"[post_list] {author_id}: {len(all_stats)} videos in last {days} days")
        return all_stats

    def _get_video_comments_signed(
        self, video_id: str, count: int = 20, author_id: str = None
    ) -> list[dict[str, Any]]:
        """
        Fallback: fetch comments via manually signed /api/comment/list/ requests.
        Uses X-Bogus + X-Gnarly signatures (xgnarly.mjs algorithm).
        """
        if author_id:
            author_id = author_id if author_id.startswith("@") else f"@{author_id}"
            referer = f"https://www.tiktok.com/{author_id}/video/{video_id}"
            profile_url = f"https://www.tiktok.com/{author_id}"
        else:
            referer = f"https://www.tiktok.com/video/{video_id}"
            profile_url = "https://www.tiktok.com/@tiktok"

        # Use profile URL (not video URL) — profile pages are lighter and don't time out
        ttwid, odin_id, device_id = self._get_ttwid_webid(profile_url)

        # Ensure we have valid IDs, fallback to high-trust ones if extraction returned generic defaults
        if str(odin_id) == "7619886743638033430":
            odin_id = "7620022218310960159"
        if str(device_id) == "7619886743638033430":
            device_id = "7620022218281616927"

        # Dynamically seed a Level 1 msToken via /api/related/item_list/
        ms_token = self._seed_ms_token(referer)
        if not ms_token:
            # High-trust msToken associated with the fallback IDs above
            ms_token = "_2Rt-OjroXlfNDODlRjBG9mvNjg5SDGmuvV_gGdZ_z_3zWaeMspWSbyUm5Rx3x5NTJESK6MAaB1AmI-MGGnrKcYScouq9OCw9cI7OffmJdp88qR7EXxrvZir6FODu-KIV1bYoA3QwzibI3bmCKltMFc="

        all_comments = []
        cursor = "0"

        url = "https://www.tiktok.com/api/comment/list/"
        headers = {**self.base_headers, "referer": referer}

        logger.info(
            f"Fetching comments for video: {author_id}/{video_id} (Unauthenticated Bypass Mode)"
        )

        while count == 0 or len(all_comments) < count:
            request_count = 20 if count == 0 else min(count - len(all_comments), 20)
            params = {
                **self.base_params,
                "WebIdLastTime": str(int(time.time())),
                "aweme_id": video_id,
                "browser_version": self.user_agent.replace("Mozilla/", ""),
                "count": str(request_count),
                "cursor": cursor,
                "device_id": str(device_id),
                "from_page": "video",
                "odinId": str(odin_id),
                "msToken": ms_token,
            }

            # 1. Generate X-Gnarly from the current query string
            # Use quote (not default quote_plus) so spaces encode as %20, matching the browser
            query_string = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            x_gnarly = TikTokSigner.generate_x_gnarly(query_string, self.user_agent)
            params["X-Gnarly"] = x_gnarly

            # 2. Generate X-Bogus from the query string that now includes X-Gnarly
            query_string_with_gnarly = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            timestamp = int(time.time())
            x_bogus = TikTokSigner.generate_x_bogus(
                query_string_with_gnarly, self.user_agent, timestamp
            )
            params["X-Bogus"] = x_bogus

            # Reorder: signatures at end, then build URL manually to preserve
            # '/' in X-Gnarly (curl_cffi params= would encode it as '%2F').
            final_params = {k: v for k, v in params.items() if k not in ("X-Bogus", "X-Gnarly")}
            final_params["X-Bogus"] = x_bogus
            final_params["X-Gnarly"] = x_gnarly
            full_url = (
                url + "?" + urllib.parse.urlencode(final_params, quote_via=urllib.parse.quote)
            )

            if not RateLimiter().acquire_source("tiktok"):
                raise RetryableError("tiktok source rate limit timeout", retry_after_seconds=60)

            try:
                response = self.session.get(full_url, headers=headers, timeout=15)

                if response.status_code == 429:
                    wait = int(response.headers.get("Retry-After", 4)) + random.uniform(0, 1)
                    logger.warning(f"TikTok comments 429 rate limited, waiting {wait:.1f}s")
                    time.sleep(wait)
                    continue

                if response.status_code == 200:
                    if not response.text.strip():
                        logger.warning(
                            f"TikTok returned an empty response for comments (Video: {video_id}). Check signatures or msToken."
                        )
                        break
                    try:
                        data = response.json()
                        batch_comments = data.get("comments", [])
                        if not batch_comments:
                            logger.info("No more comments returned from TikTok API.")
                            break

                        all_comments.extend(batch_comments)

                        if not data.get("has_more", 0):
                            logger.info("TikTok API indicates no more comments available.")
                            break

                        cursor = str(data.get("cursor", len(all_comments)))

                        # Human-like delay between pagination requests
                        if len(all_comments) < count:
                            time.sleep(random.uniform(1.0, 2.5))

                    except Exception as e:
                        logger.warning(f"Failed to parse TikTok comments JSON for {video_id}: {e}")
                        break
                else:
                    logger.warning(
                        f"TikTok comments request failed with status: {response.status_code}"
                    )
                    break
            except Exception as e:
                logger.warning(f"TikTok comments connection error: {e}")
                break

        return all_comments[:count] if count > 0 else all_comments
