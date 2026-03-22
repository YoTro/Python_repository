from __future__ import annotations
import logging
import time
import json
import re
import random
import urllib.parse
from curl_cffi import requests
from typing import Dict, Any, List, Tuple
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
    def __init__(self):
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        self.session = requests.Session(impersonate="chrome")
        
        # Default msToken that bypassed WAF for unauthenticated requests
        self._default_ms_token = "5CXDD9eri9K2V5yFV8FqLWdlGZ60UTQ3f6Io_vtOV6FOVkn19nviaABiUPIj4o8UOgy7KvwMJ1lQy6FiiWx7J_R5wBuD8CIvtSdJM65O_bG0GGBPY6fQKkrwVF7X-2D7KncdPUqKuHv7enl5zegrCig="
        
        self.base_headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "priority": "u=1, i",
            "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agent
        }

    def _seed_ms_token(self) -> str:
        """
        Retrieves a fresh, high-privilege msToken via a minimal request to the recommend API.
        This Token is necessary for accessing 'interaction' data like comments.
        """
        try:
            # Minimal naked request proven to issue a Level 1 Token
            url = "https://www.tiktok.com/api/recommend/item_list/?aid=1988"
            headers = {"User-Agent": self.user_agent}
            self.session.get(url, headers=headers, timeout=10)
            token = self.session.cookies.get_dict().get("msToken", "")
            if token:
                logger.debug(f"Dynamically seeded msToken: {token[:20]}...")
            return token
        except Exception as e:
            logger.warning(f"Failed to dynamically seed msToken: {e}")
            return ""

    def _generate_ms_token(self, randomlength: int = 107) -> str:
        """
        Generate a random string for msToken using valid characters.
        """
        random_str = ''
        base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_='
        length = len(base_str) - 1
        for _ in range(randomlength):
            random_str += base_str[random.randint(0, length)]
        return random_str

    def _get_ttwid_webid(self, req_url: str) -> Tuple[str, str, str]:
        """
        Visits a TikTok page to extract fresh ttwid cookie, webid (odinId), and deviceId (wid).
        """
        for _ in range(3):
            try:
                headers = {
                    "User-Agent": self.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9"
                }

                response = self.session.request("GET", req_url, headers=headers, timeout=5)
                
                # Fetch from session cookies (which accumulate) rather than just response cookies
                cookies_dict = self.session.cookies.get_dict()
                ttwid_str = cookies_dict.get('ttwid', "")
                
                # Extract JSON hydration data
                render_data_text = re.compile(r'\<script id=\"__UNIVERSAL_DATA_FOR_REHYDRATION__\" type\=\"application\/json\"\>(.*?)\<\/script\>').findall(response.text)
                if not render_data_text:
                    render_data_text = re.compile(r'\<script id=\"RENDER_DATA\" type\=\"application\/json\"\>(.*?)\<\/script\>').findall(response.text)

                odin_id = "7619886743638033430" # Fallback
                device_id = "7619886743638033430"
                
                if render_data_text:
                    render_data_text = urllib.parse.unquote(render_data_text[0])
                    try:
                        render_data_json = json.loads(render_data_text, strict=False)
                        
                        # Path 1: New UNIVERSAL_DATA structure
                        app_ctx = render_data_json.get('__DEFAULT_SCOPE__', {}).get('webapp.app-context', {})
                        odin_id = app_ctx.get('odinId', odin_id)
                        device_id = app_ctx.get('wid', device_id)
                        
                        # Path 2: Legacy or App-based structure
                        if str(odin_id) == "7619886743638033430":
                            odin_id = render_data_json.get('app', {}).get('odin', {}).get('user_unique_id', odin_id)
                            
                    except Exception as e:
                        logger.warning(f"JSON parse error for TikTok render data: {e}")
                
                if ttwid_str:
                    return ttwid_str, str(odin_id), str(device_id)
            except Exception as e:
                logger.warning(f"Error fetching ttwid/webid from {req_url}: {e}")
                time.sleep(1)
                
        return "", "7619886743638033430", "7619886743638033430"

    def _request(self, endpoint: str, params: dict, referer: str, ttwid: str) -> Dict[str, Any]:
        """Signs the request with X-Bogus and executes it."""
        url = f"https://www.tiktok.com{endpoint}"
        
        params["WebIdLastTime"] = str(int(time.time()))
        params["app_language"] = "en"
        params["current_region"] = "US"
        params["enter_from"] = "tiktok_web"
        params["fromWeb"] = "1"
        params["is_non_personalized"] = "false"
        
        ms_token = self._generate_ms_token(107)
        params["msToken"] = ms_token
        
        query_string = urllib.parse.urlencode(params)
        timestamp = int(time.time())
        x_bogus = TikTokSigner.generate_x_bogus(query_string, self.user_agent, timestamp)
        params["X-Bogus"] = x_bogus
        
        headers = self.base_headers.copy()
        headers["referer"] = referer
        
        # Use session cookies and manually inject our dynamic msToken
        self.session.cookies.set("msToken", ms_token, domain=".tiktok.com")
        if ttwid:
            self.session.cookies.set("ttwid", ttwid, domain=".tiktok.com")
        
        try:
            response = self.session.get(url, params=params, headers=headers)
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as e:
                    logger.error(f"Failed to parse TikTok JSON response: {e}. Snippet: {response.text[:100]}")
                    return {}
            else:
                logger.error(f"TikTok request failed with status: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"TikTok connection error: {e}")
            return {}

    def get_tag_info(self, tag_name: str) -> Dict[str, Any]:
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
            "aid": "1988",
            "app_name": "tiktok_web",
            "device_platform": "web_pc",
            "device_id": device_id,
            "odinId": odin_id
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

    def get_hashtag_videos(self, challenge_id: str, tag_name: str, count: int = 0) -> List[Dict[str, Any]]:
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
                "aid": "1988",
                "app_language": "en",
                "app_name": "tiktok_web",
                "browser_language": "en-US",
                "browser_name": "Mozilla",
                "browser_online": "true",
                "browser_platform": "MacIntel",
                "browser_version": self.user_agent.replace("Mozilla/", ""),
                "challengeID": challenge_id,
                "channel": "tiktok_web",
                "cookie_enabled": "true",
                "count": str(request_count),
                "cursor": cursor,
                "device_id": device_id,
                "device_platform": "web_pc",
                "focus_state": "true",
                "from_page": "hashtag",
                "history_len": "2",
                "is_fullscreen": "false",
                "is_page_visible": "true",
                "language": "en",
                "odinId": odin_id,
                "os": "mac",
                "priority_region": "",
                "referer": "",
                "region": "US",
                "screen_height": "1440",
                "screen_width": "2560",
                "tz_name": "America/New_York",
                "user_is_login": "false",
                "webcast_language": "en"
            }
            
            logger.info(f"Requesting {request_count} videos at cursor {cursor} (Collected: {len(all_videos)}/{count})")
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
        return all_videos[:count] # Trim to exact requested amount

    def search_videos(self, keyword: str, count: int = 20) -> List[Dict[str, Any]]:
        """
        Search for videos by keyword.
        Dynamically fetches the challenge ID for the keyword as a tag.
        """
        tag_name = keyword.replace(" ", "").replace("#", "")
        tag_info = self.get_tag_info(tag_name)
        
        challenge_id = tag_info.get("id")
        if not challenge_id:
            logger.warning(f"Could not resolve challenge ID for tag [{tag_name}]. Falling back to baseline search.")
            challenge_id = "9789" # Nike fallback for demo
            
        return self.get_hashtag_videos(challenge_id, tag_name, count)

    def get_video_comments(self, video_id: str, count: int = 20, author_id: str = None) -> List[Dict[str, Any]]:
        """
        Fetch comments for a specific video using dynamic X-Bogus and X-Gnarly generation.
        Uses a specific set of parameters and headers proven to bypass unauthenticated anti-bot checks.
        """
        if author_id:
            author_id = author_id if author_id.startswith("@") else f"@{author_id}"
            referer = f"https://www.tiktok.com/{author_id}/video/{video_id}"
        else:
            referer = f"https://www.tiktok.com/video/{video_id}"

        ttwid, odin_id, device_id = self._get_ttwid_webid(referer)

        # Ensure we have valid IDs, fallback to high-trust ones if extraction returned generic defaults
        if str(odin_id) == "7619886743638033430":
            odin_id = "7620022218310960159"
        if str(device_id) == "7619886743638033430":
            device_id = "7620022218281616927"

        # Dynamically seed a high-privilege msToken for this session
        ms_token = self._seed_ms_token()
        if not ms_token:
            # High-trust msToken associated with the fallback IDs above
            ms_token = "_2Rt-OjroXlfNDODlRjBG9mvNjg5SDGmuvV_gGdZ_z_3zWaeMspWSbyUm5Rx3x5NTJESK6MAaB1AmI-MGGnrKcYScouq9OCw9cI7OffmJdp88qR7EXxrvZir6FODu-KIV1bYoA3QwzibI3bmCKltMFc="

        all_comments = []
        cursor = "0"

        url = "https://www.tiktok.com/api/comment/list/"
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': self.user_agent,
            'referer': referer
        }

        logger.info(f"Fetching comments for video: {video_id} (Unauthenticated Bypass Mode)")

        while count == 0 or len(all_comments) < count:
            request_count = 20 if count == 0 else min(count - len(all_comments), 20)            
            params = {
                "WebIdLastTime": str(int(time.time())),
                "aid": "1988",
                "app_language": "en",
                "app_name": "tiktok_web",
                "aweme_id": video_id,
                "browser_language": "en",
                "browser_name": "Mozilla",
                "browser_online": "true",
                "browser_platform": "MacIntel",
                "browser_version": "5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
                "channel": "tiktok_web",
                "cookie_enabled": "true",
                "count": str(request_count),
                "current_region": "US",
                "cursor": cursor,
                "data_collection_enabled": "false",
                "device_id": str(device_id),
                "device_platform": "web_pc",
                "enter_from": "tiktok_web",
                "focus_state": "false",
                "fromWeb": "1",
                "from_page": "video",
                "history_len": "3",
                "is_fullscreen": "false",
                "is_non_personalized": "false",
                "is_page_visible": "true",
                "odinId": str(odin_id),
                "os": "mac",
                "priority_region": "",
                "referer": "",
                "region": "US",
                "screen_height": "1440",
                "screen_width": "2560",
                "tz_name": "America/New_York",
                "user_is_login": "false",
                "webcast_language": "en",
                "msToken": ms_token
            }
            
            # 1. Generate X-Gnarly from the current query string
            query_string = urllib.parse.urlencode(params)
            x_gnarly = TikTokSigner.generate_x_gnarly(query_string, self.user_agent)
            params["X-Gnarly"] = x_gnarly
            
            # 2. Generate X-Bogus from the query string that now includes X-Gnarly
            query_string_with_gnarly = urllib.parse.urlencode(params)
            timestamp = int(time.time())
            x_bogus = TikTokSigner.generate_x_bogus(query_string_with_gnarly, self.user_agent, timestamp)
            params["X-Bogus"] = x_bogus

            # Final URL construction with signatures at the end
            final_params = params.copy()
            xb = final_params.pop("X-Bogus")
            xg = final_params.pop("X-Gnarly")
            final_params["X-Bogus"] = xb
            final_params["X-Gnarly"] = xg
            
            try:
                response = requests.get(url, params=final_params, headers=headers, timeout=15, impersonate="chrome")
                
                if response.status_code == 200:
                    if not response.text.strip():
                        logger.warning(f"TikTok returned an empty response for comments (Video: {video_id}). Check signatures or msToken.")
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
                    logger.warning(f"TikTok comments request failed with status: {response.status_code}")
                    break
            except Exception as e:
                logger.warning(f"TikTok comments connection error: {e}")
                break
                
        return all_comments[:count] if count > 0 else all_comments
