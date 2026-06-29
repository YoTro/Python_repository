from __future__ import annotations

import json
import logging
import re
from typing import Any

from curl_cffi import requests

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
)
_CLIENT_VERSION = "2.20260625.01.00"
_BROWSE_URL = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false"
_NEXT_URL = "https://www.youtube.com/youtubei/v1/next?prettyPrint=false"

# Headers for HTML page fetches (GET requests to youtube.com/hashtag/*)
_PAGE_HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "user-agent": _USER_AGENT,
}

# Headers for internal API calls (POST to youtubei/v1/browse)
_BROWSE_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.youtube.com",
    "user-agent": _USER_AGENT,
    "x-youtube-client-name": "1",
    "x-youtube-client-version": _CLIENT_VERSION,
}


def _parse_count(text: str) -> int:
    """Parse count from strings like '1.93K subscribers', '1,714,990 views', '817 videos'."""
    text = text.replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*([KMBkmb]?)", text)
    if not m:
        return 0
    n, suffix = float(m.group(1)), m.group(2).upper()
    return int(n * {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1))


def _parse_channel_main(data: dict[str, Any], result: dict[str, Any]) -> None:
    """Extract title, handle, subscriber and video counts from main channel page response."""
    header = data.get("header", {})

    # New format: pageHeaderRenderer (2024+)
    phr = header.get("pageHeaderRenderer", {})
    if phr:
        result["title"] = result["title"] or phr.get("pageTitle", "")
        vm = phr.get("content", {}).get("pageHeaderViewModel", {})
        handle_text = vm.get("channelHandleText", {})
        result["handle"] = result["handle"] or handle_text.get("content", "")
        for row in (
            vm.get("metadata", {}).get("contentMetadataViewModel", {}).get("metadataRows", [])
        ):
            for part in row.get("metadataParts", []):
                text = part.get("text", {}).get("content", "")
                if "subscriber" in text.lower() and not result["subscriber_count"]:
                    result["subscriber_count"] = _parse_count(text)
                elif "video" in text.lower() and not result["video_count"]:
                    result["video_count"] = _parse_count(text)

    # Legacy format: c4TabbedHeaderRenderer
    c4 = header.get("c4TabbedHeaderRenderer", {})
    if c4:
        result["title"] = result["title"] or c4.get("title", "")
        handle_runs = c4.get("channelHandleText", {}).get("runs", [])
        result["handle"] = result["handle"] or (
            handle_runs[0].get("text", "") if handle_runs else ""
        )
        if not result["subscriber_count"]:
            result["subscriber_count"] = _parse_count(
                c4.get("subscriberCountText", {}).get("simpleText", "")
            )
        if not result["video_count"]:
            vid_runs = c4.get("videosCountText", {}).get("runs", [])
            result["video_count"] = _parse_count("".join(r.get("text", "") for r in vid_runs))

    # channelMetadataRenderer: title + description
    meta = data.get("metadata", {}).get("channelMetadataRenderer", {})
    result["title"] = result["title"] or meta.get("title", "")
    result["description"] = result["description"] or meta.get("description", "")


def _parse_channel_about(data: dict[str, Any], result: dict[str, Any]) -> None:
    """Extract country, join date, and lifetime view count from the about tab response.

    Handles two YouTube formats:
      - Classic: channelAboutFullMetadataRenderer in tabs (pre-2024)
      - Modern:  aboutChannelViewModel inside engagementPanels (2024+)
    """
    # Format 1: classic channelAboutFullMetadataRenderer (pre-2024 about tab)
    try:
        tabs = data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
        for tab in tabs:
            contents = (
                tab.get("tabRenderer", {})
                .get("content", {})
                .get("sectionListRenderer", {})
                .get("contents", [])
            )
            for section in contents:
                for item in section.get("itemSectionRenderer", {}).get("contents", []):
                    about = item.get("channelAboutFullMetadataRenderer", {})
                    if not about:
                        continue
                    result["country"] = about.get("country", {}).get("simpleText", "")
                    result["total_views"] = _parse_count(
                        about.get("viewCountText", {}).get("simpleText", "")
                    )
                    joined_runs = about.get("joinedDateText", {}).get("runs", [])
                    result["joined_date"] = (
                        "".join(r.get("text", "") for r in joined_runs)
                        .replace("Joined ", "")
                        .strip()
                    )
                    sub_text = about.get("subscriberCountText", {}).get("simpleText", "")
                    if sub_text:
                        result["subscriber_count"] = _parse_count(sub_text)
                    return
    except Exception as e:
        logger.warning(f"[youtube] channelAboutFullMetadataRenderer parse failed: {e}")

    # Format 2: aboutChannelViewModel in engagementPanels (2024+ layout)
    try:
        for panel in data.get("engagementPanels", []):
            slr = (
                panel.get("engagementPanelSectionListRenderer", {})
                .get("content", {})
                .get("sectionListRenderer", {})
            )
            for section in slr.get("contents", []):
                for item in section.get("itemSectionRenderer", {}).get("contents", []):
                    about_vm = (
                        item.get("aboutChannelRenderer", {})
                        .get("metadata", {})
                        .get("aboutChannelViewModel", {})
                    )
                    if not about_vm:
                        continue
                    result["country"] = result["country"] or about_vm.get("country", "")
                    joined = about_vm.get("joinedDateText", {})
                    joined_str = (
                        joined.get("content", "") if isinstance(joined, dict) else str(joined)
                    )
                    result["joined_date"] = (
                        result["joined_date"] or joined_str.replace("Joined ", "").strip()
                    )
                    view_text = about_vm.get("viewCountText", "")
                    if view_text and not result["total_views"]:
                        result["total_views"] = _parse_count(str(view_text))
                    sub_text = about_vm.get("subscriberCountText", "")
                    if sub_text and not result["subscriber_count"]:
                        result["subscriber_count"] = _parse_count(str(sub_text))
                    result["description"] = result["description"] or about_vm.get("description", "")
                    return
    except Exception as e:
        logger.warning(f"[youtube] aboutChannelViewModel parse failed: {e}")

    # Fallback: regex scan of raw JSON (catches any future format changes)
    raw_str = json.dumps(data)
    if not result["country"]:
        m = re.search(r'"country"\s*:\s*"([A-Za-z][^"]{2,40})"', raw_str)
        if m:
            result["country"] = m.group(1)
    if not result["total_views"]:
        m = re.search(r'"viewCountText"\s*:\s*\{"simpleText"\s*:\s*"([^"]+)"', raw_str)
        if not m:
            m = re.search(r'"viewCountText"\s*:\s*"([\d,]+\s*views?)"', raw_str)
        if m:
            result["total_views"] = _parse_count(m.group(1))
    if not result["joined_date"]:
        m = re.search(
            r'"joinedDateText"\s*:\s*\{[^}]*"content"\s*:\s*"([^"]+)"',
            raw_str,
        )
        if not m:
            m = re.search(
                r'"joinedDateText".*?"text"\s*:\s*"Joined\s+".*?"text"\s*:\s*"([^"]+)"',
                raw_str,
                re.DOTALL,
            )
        if m:
            result["joined_date"] = m.group(1).replace("Joined ", "").strip()


def _extract_comments_token(data: dict[str, Any]) -> str:
    """Find the initial comments section continuation token in a video page's ytInitialData.

    Prefers the twoColumnWatchNextResults token (targets "comments-section", returns
    commentRenderer with text/author/likes) over the engagementPanels token (targets
    "engagement-panel-comments-section", returns commentViewModel stubs without text).
    """
    # Location 1: twoColumnWatchNextResults — returns commentRenderer (preferred)
    try:
        contents = (
            data.get("contents", {})
            .get("twoColumnWatchNextResults", {})
            .get("results", {})
            .get("results", {})
            .get("contents", [])
        )
        for block in contents:
            for item in block.get("itemSectionRenderer", {}).get("contents", []):
                token = (
                    item.get("continuationItemRenderer", {})
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token", "")
                )
                if token:
                    return token
    except Exception:
        pass

    # Location 2: engagementPanels — fallback, returns commentViewModel stubs
    for panel in data.get("engagementPanels", []):
        slr = (
            panel.get("engagementPanelSectionListRenderer", {})
            .get("content", {})
            .get("sectionListRenderer", {})
        )
        for section in slr.get("contents", []):
            for item in section.get("itemSectionRenderer", {}).get("contents", []):
                token = (
                    item.get("continuationItemRenderer", {})
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token", "")
                )
                if token:
                    return token

    return ""


class YouTubeClient:
    """
    L1 Data Extractor for YouTube.

    Methods:
      get_hashtag_info(hashtag)       — tag metadata (video_count, channel_count)
      get_hashtag_videos(hashtag)     — raw videoRenderer dicts from the hashtag page
      get_channel_info(channel_id)    — channel metadata via internal browse API
      get_video_comments(video_id)    — top comments via youtubei/v1/next

    Callers should use SocialViralityProcessor.normalize_video(v, "youtube_hashtag")
    to convert raw videoRenderer dicts to the canonical flat schema.
    """

    def __init__(self) -> None:
        self.session = requests.Session(impersonate="chrome")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_page_data(self, url: str) -> dict[str, Any]:
        """Fetch any YouTube HTML page and extract ytInitialData JSON."""
        resp = self.session.get(url, headers=_PAGE_HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Primary: JSONDecoder.raw_decode handles nested braces reliably
        m = re.search(r"var ytInitialData\s*=\s*", html)
        if m:
            try:
                data, _ = json.JSONDecoder().raw_decode(html[m.end() :])
                return data
            except json.JSONDecodeError:
                pass

        # Fallback: bounded regex (less reliable on very large pages)
        m2 = re.search(r"var ytInitialData\s*=\s*(\{.+?\});\s*(?:var |</script>)", html, re.DOTALL)
        if m2:
            try:
                return json.loads(m2.group(1))
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not extract ytInitialData from {url}")

    def _fetch_yt_initial_data(self, hashtag: str) -> dict[str, Any]:
        """Fetch the hashtag page and extract ytInitialData JSON."""
        return self._fetch_page_data(f"https://www.youtube.com/hashtag/{hashtag.lstrip('#')}")

    def _client_context(self) -> dict[str, Any]:
        return {
            "client": {
                "hl": "en",
                "gl": "US",
                "clientName": "WEB",
                "clientVersion": _CLIENT_VERSION,
            }
        }

    def _browse_api(
        self,
        browse_id: str,
        params: str | None = None,
        referer_path: str = "",
    ) -> dict[str, Any]:
        """POST to the YouTube internal browse API (youtubei/v1/browse)."""
        body: dict[str, Any] = {
            "context": self._client_context(),
            "browseId": browse_id,
        }
        if params:
            body["params"] = params

        headers = dict(_BROWSE_HEADERS)
        if referer_path:
            headers["referer"] = f"https://www.youtube.com/{referer_path.lstrip('/')}"

        resp = self.session.post(_BROWSE_URL, json=body, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _next_api(self, continuation: str, referer_path: str = "") -> dict[str, Any]:
        """POST to the YouTube internal next API (youtubei/v1/next) with a continuation token."""
        body: dict[str, Any] = {
            "context": self._client_context(),
            "continuation": continuation,
        }
        headers = dict(_BROWSE_HEADERS)
        if referer_path:
            headers["referer"] = f"https://www.youtube.com/{referer_path.lstrip('/')}"
        resp = self.session.post(_NEXT_URL, json=body, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_hashtag_info(self, hashtag: str) -> dict[str, Any]:
        """
        Returns tag-level metadata.

        Parses the contentMetadataViewModel string, e.g. "380 videos • 275 channels",
        to extract video_count and channel_count.
        """
        data = self._fetch_yt_initial_data(hashtag)
        video_count = 0
        channel_count = 0

        try:
            header = data.get("header", {})
            ht_header = header.get("hashtagHeaderRenderer", {})
            metadata_vm = ht_header.get("metadata", {}).get("contentMetadataViewModel", {})
            for row in metadata_vm.get("metadataRows", []):
                for part in row.get("metadataParts", []):
                    content = part.get("text", {}).get("content", "")
                    vm = re.search(r"([\d,]+)\s+video", content)
                    cm = re.search(r"([\d,]+)\s+channel", content)
                    if vm:
                        video_count = int(vm.group(1).replace(",", ""))
                    if cm:
                        channel_count = int(cm.group(1).replace(",", ""))
        except Exception as e:
            logger.warning(f"[youtube] Metadata parse failed for #{hashtag}: {e}")

        if video_count == 0:
            raw_str = json.dumps(data)
            vm = re.search(r'"content"\s*:\s*"([\d,]+)\s+video', raw_str)
            cm = re.search(r'"content"\s*:\s*"[\d,]+\s+videos\s+\S+\s+([\d,]+)\s+channel', raw_str)
            if vm:
                video_count = int(vm.group(1).replace(",", ""))
            if cm:
                channel_count = int(cm.group(1).replace(",", ""))

        return {
            "hashtag": hashtag.lstrip("#"),
            "video_count": video_count,
            "channel_count": channel_count,
        }

    def get_hashtag_videos(self, hashtag: str, count: int = 50) -> list[dict[str, Any]]:
        """
        Fetch raw videoRenderer dicts from the hashtag page.

        YouTube hashtag pages are server-rendered (not paginated via XHR) —
        the initial page load contains the first batch of ~20–30 videos.
        Returns raw ytInitialData videoRenderer dicts; callers normalise with
        SocialViralityProcessor.normalize_video(v, "youtube_hashtag").
        """
        data = self._fetch_yt_initial_data(hashtag)
        videos: list[dict[str, Any]] = []

        try:
            tabs = (
                data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
            )
            for tab in tabs:
                contents = (
                    tab.get("tabRenderer", {})
                    .get("content", {})
                    .get("richGridRenderer", {})
                    .get("contents", [])
                )
                for item in contents:
                    vr = (
                        item.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
                    )
                    if vr.get("videoId"):
                        videos.append(vr)
        except Exception as e:
            logger.warning(f"[youtube] Grid parse failed for #{hashtag}: {e}")

        if not videos:
            logger.warning(f"[youtube] Falling back to regex videoId extraction for #{hashtag}")
            raw_str = json.dumps(data)
            for m in re.finditer(r'"videoId"\s*:\s*"([^"]{8,})"', raw_str):
                vid: dict[str, Any] = {"videoId": m.group(1)}
                vc = re.search(
                    rf'"videoId"\s*:\s*"{re.escape(m.group(1))}"'
                    r'.{{0,500}}"viewCountText"\s*:\s*\{{"simpleText"\s*:\s*"([^"]+)"',
                    raw_str,
                    re.DOTALL,
                )
                if vc:
                    vid["viewCountText"] = {"simpleText": vc.group(1)}
                if vid not in videos:
                    videos.append(vid)

        return videos[:count]

    def get_channel_info(self, channel_id: str) -> dict[str, Any]:
        """
        Fetch channel metadata.

        Makes two requests:
          1. Browse API (POST) — title, handle, subscriber count, video count.
          2. /about HTML page  — country, join date, lifetime view count.

        The about page is fetched as HTML (same ytInitialData extraction used for
        hashtag pages) because YouTube's browse API no longer serves the about tab
        reliably via params for this data.

        Returns:
          channel_id, title, handle, description,
          subscriber_count (int), video_count (int), total_views (int),
          joined_date (str e.g. "Oct 18, 2022"), country (str e.g. "United States")
        """
        result: dict[str, Any] = {
            "channel_id": channel_id,
            "title": "",
            "handle": "",
            "description": "",
            "subscriber_count": 0,
            "video_count": 0,
            "total_views": 0,
            "joined_date": "",
            "country": "",
        }

        try:
            main_data = self._browse_api(channel_id, referer_path=f"channel/{channel_id}")
            _parse_channel_main(main_data, result)
        except Exception as e:
            logger.warning(f"[youtube] Channel main page failed for {channel_id}: {e}")

        try:
            about_url = f"https://www.youtube.com/channel/{channel_id}/about"
            about_data = self._fetch_page_data(about_url)
            _parse_channel_about(about_data, result)
        except Exception as e:
            logger.warning(f"[youtube] Channel about page failed for {channel_id}: {e}")

        return result

    def get_video_comments(self, video_id: str, count: int = 20) -> list[dict[str, Any]]:
        """
        Fetch top comments for a YouTube video.

        Makes two or more requests:
          1. Video page (GET) — extract initial comments continuation token from ytInitialData.
          2. /youtubei/v1/next (POST) — fetch comment page(s) using continuation token.

        Paginates automatically until ``count`` comments are collected or no more pages exist.

        Returns a list of dicts:
          text (str), author (str), likes (int), reply_count (int), published_time (str)
        """
        try:
            video_data = self._fetch_page_data(f"https://www.youtube.com/watch?v={video_id}")
            token = _extract_comments_token(video_data)
        except Exception as e:
            logger.warning(f"[youtube] Failed to fetch video page for {video_id}: {e}")
            return []

        if not token:
            logger.warning(f"[youtube] No comments token found for {video_id}")
            return []

        comments: list[dict[str, Any]] = []
        referer = f"watch?v={video_id}"

        while len(comments) < count and token:
            try:
                resp = self._next_api(token, referer_path=referer)
            except Exception as e:
                logger.warning(f"[youtube] Comments fetch failed for {video_id}: {e}")
                break

            # YouTube's comment data lives in frameworkUpdates mutations, not in the
            # commentThreadRenderer itself. Build a lookup keyed by commentEntityPayload.key.
            comment_payloads: dict[str, dict[str, Any]] = {}
            for mutation in (
                resp.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", [])
            ):
                cp = mutation.get("payload", {}).get("commentEntityPayload", {})
                if cp and cp.get("key"):
                    comment_payloads[cp["key"]] = cp

            token = ""
            for endpoint in resp.get("onResponseReceivedEndpoints", []):
                # First page: reloadContinuationItemsCommand; subsequent: appendContinuationItemsAction
                items = endpoint.get("reloadContinuationItemsCommand", {}).get(
                    "continuationItems", []
                ) or endpoint.get("appendContinuationItemsAction", {}).get("continuationItems", [])

                for item in items:
                    ctr = item.get("commentThreadRenderer", {})
                    if ctr:
                        comment_key = (
                            ctr.get("commentViewModel", {})
                            .get("commentViewModel", {})
                            .get("commentKey", "")
                        )
                        cp = comment_payloads.get(comment_key)
                        if cp:
                            props = cp.get("properties", {})
                            toolbar = cp.get("toolbar", {})
                            like_a11y = toolbar.get("likeCountA11y", "") or ""
                            reply_str = (toolbar.get("replyCount", "") or "").strip()
                            comments.append(
                                {
                                    "text": props.get("content", {}).get("content", ""),
                                    "author": cp.get("author", {}).get("displayName", ""),
                                    "likes": _parse_count(like_a11y.split()[0]) if like_a11y else 0,
                                    "reply_count": _parse_count(reply_str) if reply_str else 0,
                                    "published_time": props.get("publishedTime", ""),
                                }
                            )

                    next_token = (
                        item.get("continuationItemRenderer", {})
                        .get("continuationEndpoint", {})
                        .get("continuationCommand", {})
                        .get("token", "")
                    )
                    if next_token:
                        token = next_token

        return comments[:count]
