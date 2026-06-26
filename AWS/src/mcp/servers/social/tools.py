import asyncio
import json
import logging
import time

from mcp.types import TextContent, Tool

from src.core.data_cache import data_cache
from src.intelligence.processors.comment_analyzer import CommentAnalyzer
from src.intelligence.processors.hashtag_generator import HashtagGenerator
from src.intelligence.processors.social_virality import SocialViralityProcessor
from src.intelligence.providers.factory import ProviderFactory
from src.mcp.servers.social.tiktok.client import TikTokClient
from src.mcp.servers.social.youtube.client import YouTubeClient
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-social")


async def handle_social_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "tiktok_fetch_data":
        brand = arguments.get("brand", "")
        product_name = arguments.get("product_name", "")
        keyword = arguments.get("keyword") or f"{brand} {product_name}".strip()

        extractor = TikTokClient()

        logger.info(f"[L1] Fetching TikTok raw data for keyword: {keyword}")

        # 1. Fetch tag metadata for global scale
        tag_name = keyword.replace(" ", "").replace("#", "")
        tag_info = await asyncio.to_thread(extractor.get_tag_info, tag_name)

        # 2. Fetch sample videos for engagement analysis
        challenge_id = tag_info.get("id")
        if not challenge_id:
            raise ValueError(
                f"Could not find a valid TikTok hashtag or challenge ID for '{tag_name}'. Fetch aborted."
            )

        # Adaptive sample size: ~10% of total videos, floored at 50, capped at 300.
        # video_count is already in tag_info — no extra API call needed.
        video_count = tag_info.get("video_count", 0)
        fetch_count = max(50, min(video_count // 10, 300))
        videos = await asyncio.to_thread(
            extractor.get_hashtag_videos, challenge_id, tag_name, fetch_count
        )

        # 3. Fetch comments for top videos
        max_comments_per_video = arguments.get("max_comments", 10)
        all_comments = []
        if videos and max_comments_per_video > 0:
            # Filter videos that actually have comments and sort them by comment volume descending
            videos_with_comments = [
                v
                for v in videos
                if (v.get("stats", {}).get("commentCount", 0) or v.get("comments", 0)) > 0
            ]
            sorted_videos = sorted(
                videos_with_comments,
                key=lambda x: x.get("stats", {}).get("commentCount", 0) or x.get("comments", 0),
                reverse=True,
            )

            top_videos = sorted_videos[:3]
            for v in top_videos:
                v_id = v.get("id")
                author_id = v.get("author", {}).get("uniqueId")
                if v_id:
                    comments = await asyncio.to_thread(
                        extractor.get_video_comments,
                        video_id=v_id,
                        count=max_comments_per_video,
                        author_id=author_id,
                    )
                    all_comments.extend(comments)

        # L1 Action: Write to Data Cache
        data_cache.set(
            "tiktok",
            keyword,
            {
                "tag_metadata": tag_info,
                "videos": videos,
                "comments_data": all_comments,
                "brand": brand,
                "product_name": product_name,
            },
        )

        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {
                        "status": "success",
                        "keyword": keyword,
                        "message": f"Successfully fetched and cached {len(videos)} videos and {len(all_comments)} comments. Proceed to use 'tiktok_calculate_virality' to compute the score.",
                    }
                ),
            )
        ]

    elif name == "tiktok_fetch_reference_data":
        brand = arguments.get("brand", "")
        product_name = arguments.get("product_name", "")
        keyword = arguments.get("keyword", "") or f"{brand} {product_name}".strip()
        videos_per_tag = int(arguments.get("videos_per_tag", 20))

        extractor = TikTokClient()

        all_hashtags = await HashtagGenerator(
            provider=ProviderFactory.get_provider()
        ).generate_reference_hashtags(brand, product_name, keyword)

        logger.info(f"[L1] Fetching reference data: hashtags={all_hashtags[:8]}")

        # 3. Fetch videos for each reference hashtag
        reference_videos: list[dict] = []
        hashtags_fetched: list[str] = []

        for tag in all_hashtags[:8]:  # cap at 8 tags to control API load
            try:
                tag_info = await asyncio.to_thread(extractor.get_tag_info, tag)
                cid = tag_info.get("id")
                if not cid:
                    continue
                # Adaptive fetch using video_count already in tag_info — no extra round-trip.
                ref_video_count = tag_info.get("video_count", 0)
                ref_fetch_count = max(50, min(ref_video_count // 10, videos_per_tag))
                vids = await asyncio.to_thread(
                    extractor.get_hashtag_videos, cid, tag, ref_fetch_count
                )
                reference_videos.extend(vids)
                hashtags_fetched.append(tag)
            except Exception as e:
                logger.warning(f"Reference fetch failed for #{tag}: {e}")

        # 4. Cache reference videos under a dedicated key
        ref_cache_key = f"__ref__{keyword}"
        data_cache.set(
            "tiktok",
            ref_cache_key,
            {
                "videos": reference_videos,
                "hashtags": hashtags_fetched,
            },
        )

        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {
                        "status": "success",
                        "keyword": keyword,
                        "reference_hashtags": hashtags_fetched,
                        "total_reference_videos": len(reference_videos),
                        "message": (
                            f"Fetched {len(reference_videos)} reference videos from "
                            f"{len(hashtags_fetched)} hashtags. "
                            f"Run 'tiktok_calculate_virality' to compute the PSI with peer benchmarks."
                        ),
                    }
                ),
            )
        ]

    elif name == "tiktok_fetch_comments":
        keyword = arguments.get("keyword", "")
        window_days = int(arguments.get("window_days", 30))
        total_budget = int(arguments.get("total_budget", 300))
        min_comments_threshold = int(arguments.get("min_comments_threshold", 5))

        if not keyword:
            raise ValueError("keyword is required.")

        cached = data_cache.get("tiktok", keyword)
        if not cached:
            raise ValueError(f"No cached data for '{keyword}'. Run tiktok_fetch_data first.")

        videos = cached.get("videos", [])
        brand = cached.get("brand", "")
        product_name = cached.get("product_name", "")

        # Same cutoff as calculate_promotion_strength — temporal consistency
        cutoff = int(time.time()) - window_days * 24 * 3600
        scored = [v for v in videos if v.get("createTime", 0) >= cutoff]

        if not scored:
            data_cache.set(
                "tiktok",
                f"__comments__{keyword}__w{window_days}",
                {"flat": [], "by_tier": {}, "total": 0, "window_days": window_days},
            )
            return [
                TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "status": "success",
                            "keyword": keyword,
                            "total_comments": 0,
                            "message": "No videos within window_days — comment cache empty.",
                        }
                    ),
                )
            ]

        # Classify into tiers using the same thresholds as SocialViralityProcessor
        tier_buckets: dict[str, list[dict]] = {
            "nano": [],
            "micro": [],
            "mid": [],
            "macro": [],
            "mega": [],
        }
        for v in scored:
            followers = v.get("authorStats", {}).get("followerCount", 0)
            if followers > SocialViralityProcessor.TIER_MACRO_MAX:
                tier_buckets["mega"].append(v)
            elif followers > SocialViralityProcessor.TIER_MID_MAX:
                tier_buckets["macro"].append(v)
            elif followers > SocialViralityProcessor.TIER_MICRO_MAX:
                tier_buckets["mid"].append(v)
            elif followers > SocialViralityProcessor.TIER_NANO_MAX:
                tier_buckets["micro"].append(v)
            else:
                tier_buckets["nano"].append(v)

        total_scored = len(scored)
        extractor = TikTokClient()
        all_comments_flat: list[str] = []
        by_tier: dict[str, list[str]] = {t: [] for t in tier_buckets}

        for tier, vids in tier_buckets.items():
            if not vids:
                continue
            tier_budget = max(1, int((len(vids) / total_scored) * total_budget))

            # Within tier: highest-view videos first
            sorted_vids = sorted(
                vids,
                key=lambda x: x.get("stats", {}).get("playCount", 0) or x.get("views", 0),
                reverse=True,
            )

            remaining = tier_budget
            for v in sorted_vids:
                if remaining <= 0:
                    break
                v_id = v.get("id")
                author_id = v.get("author", {}).get("uniqueId")
                if not v_id:
                    continue
                slot = min(40, remaining)
                try:
                    raw_comments = await asyncio.to_thread(
                        extractor.get_video_comments,
                        video_id=v_id,
                        count=slot,
                        author_id=author_id,
                    )
                except Exception as e:
                    logger.warning(f"Comment fetch failed for video {v_id}: {e}")
                    continue
                texts = [
                    c.get("text", "").strip() for c in raw_comments if c.get("text", "").strip()
                ]
                if len(texts) < min_comments_threshold:
                    continue  # too sparse — skip this video
                by_tier[tier].extend(texts)
                all_comments_flat.extend(texts)
                remaining -= len(texts)

        data_cache.set(
            "tiktok",
            f"__comments__{keyword}__w{window_days}",
            {
                "flat": all_comments_flat,
                "by_tier": by_tier,
                "total": len(all_comments_flat),
                "window_days": window_days,
                "brand": brand,
                "product_name": product_name,
            },
        )

        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {
                        "status": "success",
                        "keyword": keyword,
                        "total_comments": len(all_comments_flat),
                        "by_tier": {t: len(v) for t, v in by_tier.items()},
                        "message": (
                            f"Fetched {len(all_comments_flat)} comments (tier-stratified, "
                            f"window={window_days}d). Run tiktok_calculate_virality to include in PSI."
                        ),
                    }
                ),
            )
        ]

    elif name == "tiktok_calculate_virality":
        keyword = arguments.get("keyword")
        window_days = int(arguments.get("window_days", 30))
        if not keyword:
            raise ValueError("Keyword is required to retrieve cached data.")

        logger.info(f"[L2] Calculating TikTok virality for keyword: {keyword}")

        cached_data = data_cache.get("tiktok", keyword)
        if not cached_data:
            raise ValueError(
                f"No cached data found for keyword '{keyword}'. Please run 'tiktok_fetch_data' first."
            )

        # Load reference videos if tiktok_fetch_reference_data was called
        ref_cache = data_cache.get("tiktok", f"__ref__{keyword}")
        reference_videos = ref_cache.get("videos", []) if ref_cache else []
        if reference_videos:
            logger.info(f"[L2] Using {len(reference_videos)} reference videos for peer benchmarks")

        # Load deep comments if tiktok_fetch_comments was called; run LLM analysis
        comments_cache = data_cache.get("tiktok", f"__comments__{keyword}__w{window_days}")
        llm_comment_analysis: dict | None = None
        if comments_cache and comments_cache.get("flat"):
            flat_comments = comments_cache["flat"]
            _brand = cached_data.get("brand", "")
            _product = cached_data.get("product_name", "")
            logger.info(f"[L2] Running LLM comment analysis on {len(flat_comments)} comments")
            llm_comment_analysis = await CommentAnalyzer(
                provider=ProviderFactory.get_provider()
            ).analyze(flat_comments, _brand, _product)

        processor = SocialViralityProcessor()
        result = processor.calculate_promotion_strength(
            cached_data.get("videos", []),
            brand=cached_data.get("brand", ""),
            product_name=cached_data.get("product_name", ""),
            tag_metadata=cached_data.get("tag_metadata", {}),
            comments_data=cached_data.get("comments_data", []),
            reference_videos=reference_videos or None,
            llm_comment_analysis=llm_comment_analysis,
            window_days=window_days,
        )

        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    elif name == "tiktok_get_video_comments":
        video_id = arguments.get("video_id", "")
        author_id = arguments.get("author_id")
        count = int(arguments.get("count", 20))

        if not video_id:
            raise ValueError("video_id is required.")

        extractor = TikTokClient()
        logger.info(f"[L1] Fetching {count} comments for video {video_id}")
        comments = await asyncio.to_thread(
            extractor.get_video_comments,
            video_id=video_id,
            count=count,
            author_id=author_id,
        )
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"video_id": video_id, "count": len(comments), "comments": comments},
                    ensure_ascii=False,
                ),
            )
        ]

    elif name == "tiktok_get_user_recent_stats":
        author_id = arguments.get("author_id", "")
        days = int(arguments.get("days", 30))

        extractor = TikTokClient()
        logger.info(f"[L1] Fetching last {days}-day video stats for {author_id or '@tiktok'}")
        stats = await asyncio.to_thread(
            extractor.get_user_recent_video_stats,
            author_id=author_id,
            days=days,
        )
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {
                        "author_id": author_id,
                        "days": days,
                        "video_count": len(stats),
                        "videos": stats,
                    },
                    ensure_ascii=False,
                ),
            )
        ]

    elif name == "youtube_get_hashtag_info":
        hashtag = arguments.get("hashtag", "")
        if not hashtag:
            raise ValueError("hashtag is required.")
        client = YouTubeClient()
        logger.info(f"[L1] Fetching YouTube hashtag info for #{hashtag}")
        info = await asyncio.to_thread(client.get_hashtag_info, hashtag)
        return [TextContent(type="text", text=json.dumps(info, ensure_ascii=False))]

    elif name == "youtube_get_hashtag_videos":
        hashtag = arguments.get("hashtag", "")
        count = int(arguments.get("count", 20))
        if not hashtag:
            raise ValueError("hashtag is required.")
        client = YouTubeClient()
        logger.info(f"[L1] Fetching YouTube hashtag videos for #{hashtag}")
        videos = await asyncio.to_thread(client.get_hashtag_videos, hashtag, count)
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"hashtag": hashtag.lstrip("#"), "count": len(videos), "videos": videos},
                    ensure_ascii=False,
                ),
            )
        ]

    elif name == "youtube_get_channel_info":
        channel_id = arguments.get("channel_id", "")
        if not channel_id:
            raise ValueError("channel_id is required.")
        client = YouTubeClient()
        logger.info(f"[L1] Fetching YouTube channel info for {channel_id}")
        info = await asyncio.to_thread(client.get_channel_info, channel_id)
        return [TextContent(type="text", text=json.dumps(info, ensure_ascii=False))]

    elif name == "youtube_get_video_comments":
        video_id = arguments.get("video_id", "")
        count = int(arguments.get("count", 20))
        if not video_id:
            raise ValueError("video_id is required.")
        client = YouTubeClient()
        logger.info(f"[L1] Fetching {count} YouTube comments for video {video_id}")
        comments = await asyncio.to_thread(client.get_video_comments, video_id, count)
        return [
            TextContent(
                type="text",
                text=json.dumps(
                    {"video_id": video_id, "count": len(comments), "comments": comments},
                    ensure_ascii=False,
                ),
            )
        ]

    elif name == "meta_ad_search":
        return [TextContent(type="text", text=json.dumps({"active_ads": 15}))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


social_tools = [
    Tool(
        name="tiktok_fetch_data",
        description=(
            "L1: Scrape TikTok hashtag data for a product keyword and cache it. "
            "Fetches: tag metadata (view_count, video_count), up to 30 trending videos "
            "(id, author, stats: viewCount, likeCount, commentCount, shareCount, play_url), "
            "and comments from the top-3 most-commented videos. "
            "Returns: {status, keyword, message} — data is stored in DataCache under domain='tiktok'. "
            "MUST be called before tiktok_calculate_virality."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "brand": {"type": "string", "description": "Product brand (e.g. 'Anker')"},
                "product_name": {
                    "type": "string",
                    "description": "Product name or model (e.g. 'PowerCore 10000')",
                },
                "keyword": {
                    "type": "string",
                    "description": "Override hashtag. Defaults to '{brand} {product_name}'.",
                },
                "max_comments": {
                    "type": "integer",
                    "default": 10,
                    "description": "Comments to fetch per top video (1–50)",
                },
            },
            "required": ["brand", "product_name"],
        },
    ),
    Tool(
        name="tiktok_fetch_reference_data",
        description=(
            "L1: Fetch competitor/category reference videos for dynamic PSI benchmarking. "
            "Calls the LLM to generate competitor brand hashtags, merges with hardcoded category seeds, "
            "then fetches up to `videos_per_tag` videos per hashtag. "
            "Results are cached and automatically used by tiktok_calculate_virality to set "
            "engagement/share-rate benchmarks from the peer median rather than static defaults. "
            "Call AFTER tiktok_fetch_data, BEFORE tiktok_calculate_virality."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "brand": {"type": "string", "description": "Product brand (e.g. 'Zevo')"},
                "product_name": {
                    "type": "string",
                    "description": "Product name (e.g. 'bug spray')",
                },
                "keyword": {
                    "type": "string",
                    "description": "Target hashtag used in tiktok_fetch_data",
                },
                "videos_per_tag": {
                    "type": "integer",
                    "default": 20,
                    "description": "Videos to fetch per reference hashtag (5–30)",
                },
            },
            "required": ["brand", "product_name", "keyword"],
        },
    ),
    Tool(
        name="tiktok_fetch_comments",
        description=(
            "L1: Fetch comments for the target hashtag's top videos using tier-stratified sampling. "
            "Applies the same window_days filter as tiktok_calculate_virality so comments are "
            "temporally consistent with scored videos. "
            "Budget is allocated proportionally across KOL/KOC tiers (nano/micro/mid/macro/mega) "
            "so the comment pool mirrors the creator distribution. "
            "Videos with fewer than min_comments_threshold returned comments are skipped. "
            "Results are cached and automatically used by tiktok_calculate_virality for LLM deep analysis. "
            "Call AFTER tiktok_fetch_data, BEFORE tiktok_calculate_virality."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "Exact keyword used in tiktok_fetch_data",
                },
                "window_days": {
                    "type": "integer",
                    "default": 30,
                    "description": "Recency window in days — must match tiktok_calculate_virality",
                },
                "total_budget": {
                    "type": "integer",
                    "default": 300,
                    "description": "Total comments to collect across all tiers",
                },
                "min_comments_threshold": {
                    "type": "integer",
                    "default": 5,
                    "description": "Skip a video if fewer than this many comments are returned",
                },
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="tiktok_calculate_virality",
        description=(
            "L2: Calculate TikTok Promotional Strength Index (PSI) from cached data. "
            "MUST call tiktok_fetch_data first. "
            "Optionally call tiktok_fetch_reference_data (peer benchmarks) and "
            "tiktok_fetch_comments (LLM deep comment analysis) before this tool. "
            "Returns: strength_score (0–100), total_tag_videos, total_views_sample, avg_views_per_video, "
            "engagement_rate, amazon_mentions (count), organic_multiplier, "
            "recent_videos_ratio (freshness signal), mega_influencer_ratio, creator_diversity, "
            "kol_koc_matrix, hhi_concentration, promo_tag_ratio, penalties, verdict, "
            "comment_analysis {sentiment, purchase_signals, top_themes, top_objections, confidence, summary}, "
            "benchmarks {source, engagement_rate, share_rate}, "
            "metrics {historical_volume_contribution, recent_volume_contribution, engagement_contribution, "
            "intent_contribution, organic_viral_contribution, recency_contribution, "
            "share_virality_contribution, creator_diversity_contribution}."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {
                    "type": "string",
                    "description": "Exact keyword used in tiktok_fetch_data",
                },
                "window_days": {
                    "type": "integer",
                    "default": 30,
                    "description": "Recency window in days — must match tiktok_fetch_comments if used",
                },
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="tiktok_get_video_comments",
        description=(
            "L1: Fetch comments for a specific TikTok video. "
            "Uses the signed API first (headless); falls back to a real Chrome session via DrissionPage "
            "if the video requires login. "
            "Returns: {video_id, count, comments[]} where each comment contains text, author nickname, "
            "digg_count, reply_comment_count, create_time."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {"type": "string", "description": "TikTok video ID (numeric string)"},
                "author_id": {
                    "type": "string",
                    "description": "Creator handle (e.g. 'ccuj00' or '@ccuj00'). Optional but improves accuracy.",
                },
                "count": {
                    "type": "integer",
                    "default": 20,
                    "description": "Number of comments to fetch (1–200).",
                },
            },
            "required": ["video_id"],
        },
    ),
    Tool(
        name="tiktok_get_user_recent_stats",
        description=(
            "L1: Fetch stats for all videos a TikTok creator posted within the last N days. "
            "Paginates /api/post/item_list/ and stops once videos older than the cutoff are seen. "
            "Falls back to @tiktok if the creator handle cannot be resolved. "
            "Returns: {author_id, days, video_count, videos[]} where each video contains "
            "id, createTime, desc, playCount, likeCount, commentCount, shareCount, collectCount."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "author_id": {
                    "type": "string",
                    "description": "Creator handle (e.g. 'ccuj00' or '@ccuj00').",
                },
                "days": {
                    "type": "integer",
                    "default": 30,
                    "description": "Look-back window in days (e.g. 30, 60, 90).",
                },
            },
            "required": ["author_id"],
        },
    ),
    Tool(
        name="youtube_get_hashtag_info",
        description=(
            "L1: Fetch YouTube hashtag metadata. "
            "Parses the hashtag page header to extract video_count and channel_count. "
            "Use to gauge the scale of a hashtag's presence before fetching videos. "
            "Returns: {hashtag, video_count, channel_count}."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "hashtag": {
                    "type": "string",
                    "description": "Hashtag name without # (e.g. 'bugspray')",
                },
            },
            "required": ["hashtag"],
        },
    ),
    Tool(
        name="youtube_get_hashtag_videos",
        description=(
            "L1: Fetch raw videoRenderer dicts from a YouTube hashtag page. "
            "Returns the first ~20–30 videos from the server-rendered hashtag page (not paginated). "
            "Pass each video through SocialViralityProcessor.normalize_video(v, 'youtube_hashtag') "
            "to convert to the canonical flat schema before computing PSI. "
            "Returns: {hashtag, count, videos[]}."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "hashtag": {
                    "type": "string",
                    "description": "Hashtag name without # (e.g. 'bugspray')",
                },
                "count": {
                    "type": "integer",
                    "default": 20,
                    "description": "Max videos to return (capped by page load, typically ~30).",
                },
            },
            "required": ["hashtag"],
        },
    ),
    Tool(
        name="youtube_get_channel_info",
        description=(
            "L1: Fetch YouTube channel metadata. "
            "Makes two requests: browse API for title/handle/subscriber/video counts, "
            "then /about HTML page for country, join date, and lifetime views. "
            "Returns: {channel_id, title, handle, description, subscriber_count (int), "
            "video_count (int), total_views (int), joined_date (str), country (str)}."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "channel_id": {
                    "type": "string",
                    "description": "YouTube channel ID (e.g. 'UCLMxTjTDircrYzWO0Dsq0YQ') or handle (@name).",
                },
            },
            "required": ["channel_id"],
        },
    ),
    Tool(
        name="youtube_get_video_comments",
        description=(
            "L1: Fetch top comments for a YouTube video via the internal youtubei/v1/next API. "
            "Fetches the video page to extract the initial continuation token, then POSTs to "
            "youtubei/v1/next and reads comment data from frameworkUpdates.entityBatchUpdate.mutations. "
            "Paginates automatically until count is reached or no more pages exist. "
            "Returns: {video_id, count, comments[]} where each comment contains "
            "text, author, likes (int), reply_count (int), published_time (str e.g. '2 years ago')."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "video_id": {
                    "type": "string",
                    "description": "YouTube video ID (11-character string, e.g. '1EGZvk5L5ds')",
                },
                "count": {
                    "type": "integer",
                    "default": 20,
                    "description": "Number of comments to fetch (1–100).",
                },
            },
            "required": ["video_id"],
        },
    ),
    Tool(
        name="meta_ad_search",
        description=(
            "Check Meta Ad Library for active advertising campaigns for a keyword. "
            "Returns: active_ads (count of live ads found). "
            "Note: currently returns stub data — wire to Meta Ad Library API when available."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Brand or product keyword to search"}
            },
            "required": ["keyword"],
        },
    ),
]

_SOCIAL_META = {
    "tiktok_fetch_reference_data": (
        "DATA",
        "JSON with reference_hashtags used, total_reference_videos fetched, and category_inferred.",
    ),
    "tiktok_fetch_data": ("DATA", "Success confirmation indicating data is cached"),
    "tiktok_fetch_comments": (
        "DATA",
        "JSON with total_comments fetched and per-tier breakdown; cached for LLM analysis in tiktok_calculate_virality.",
    ),
    "tiktok_calculate_virality": (
        "COMPUTE",
        "JSON containing 'strength_score' (0-100), 'organic_multiplier', 'recent_videos_ratio', and 'comment_intent_analysis'.",
    ),
    "tiktok_get_video_comments": (
        "DATA",
        "JSON with video_id, count, and comments list (text, author, digg_count, reply_comment_count, create_time).",
    ),
    "tiktok_get_user_recent_stats": (
        "DATA",
        "JSON with author_id, days, video_count, and videos list (id, createTime, desc, playCount, likeCount, commentCount, shareCount, collectCount).",
    ),
    "youtube_get_hashtag_info": (
        "DATA",
        "JSON with hashtag, video_count, and channel_count.",
    ),
    "youtube_get_hashtag_videos": (
        "DATA",
        "JSON with hashtag, count, and raw videoRenderer dicts (normalize with SocialViralityProcessor.normalize_video before PSI).",
    ),
    "youtube_get_channel_info": (
        "DATA",
        "JSON with channel_id, title, handle, description, subscriber_count, video_count, total_views, joined_date, country.",
    ),
    "youtube_get_video_comments": (
        "DATA",
        "JSON with video_id, count, and comments list (text, author, likes, reply_count, published_time).",
    ),
    "meta_ad_search": ("DATA", "count of active advertisements found on Meta platforms"),
}

for tool in social_tools:
    cat, ret = _SOCIAL_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_social_tool, category=cat, returns=ret)
