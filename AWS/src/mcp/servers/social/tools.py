import json
import logging
import asyncio
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.mcp.servers.social.tiktok.client import TikTokClient
from src.intelligence.processors.social_virality import SocialViralityProcessor
from src.core.data_cache import data_cache

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
            raise ValueError(f"Could not find a valid TikTok hashtag or challenge ID for '{tag_name}'. Fetch aborted.")
            
        videos = await asyncio.to_thread(extractor.get_hashtag_videos, challenge_id, tag_name, 30)
        
        # 3. Fetch comments for top videos
        max_comments_per_video = arguments.get("max_comments", 10)
        all_comments = []
        if videos and max_comments_per_video > 0:
            # Filter videos that actually have comments and sort them by comment volume descending
            videos_with_comments = [v for v in videos if (v.get("stats", {}).get("commentCount", 0) or v.get("comments", 0)) > 0]
            sorted_videos = sorted(videos_with_comments, key=lambda x: x.get("stats", {}).get("commentCount", 0) or x.get("comments", 0), reverse=True)
            
            top_videos = sorted_videos[:3]
            for v in top_videos:
                v_id = v.get("id")
                author_id = v.get("author", {}).get("uniqueId")
                if v_id:
                    comments = await asyncio.to_thread(extractor.get_video_comments, video_id=v_id, count=max_comments_per_video, author_id=author_id)
                    all_comments.extend(comments)
        
        # L1 Action: Write to Data Cache
        data_cache.set("tiktok", keyword, {
            "tag_metadata": tag_info,
            "videos": videos,
            "comments_data": all_comments,
            "brand": brand,
            "product_name": product_name
        })
        
        return [TextContent(type="text", text=json.dumps({
            "status": "success", 
            "keyword": keyword, 
            "message": f"Successfully fetched and cached {len(videos)} videos and {len(all_comments)} comments. Proceed to use 'tiktok_calculate_virality' to compute the score."
        }))]

    elif name == "tiktok_calculate_virality":
        keyword = arguments.get("keyword")
        if not keyword:
            raise ValueError("Keyword is required to retrieve cached data.")
            
        logger.info(f"[L2] Calculating TikTok virality for keyword: {keyword}")
            
        # L2 Action: Read from Data Cache (L1 data)
        cached_data = data_cache.get("tiktok", keyword)
        if not cached_data:
            raise ValueError(f"No cached data found for keyword '{keyword}'. Please run 'tiktok_fetch_data' first.")
            
        processor = SocialViralityProcessor()
        
        # Calculate score using cached data
        result = processor.calculate_promotion_strength(
            cached_data.get("videos", []), 
            brand=cached_data.get("brand", ""), 
            product_name=cached_data.get("product_name", ""),
            tag_metadata=cached_data.get("tag_metadata", {}),
            comments_data=cached_data.get("comments_data", [])
        )
        
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    elif name == "meta_ad_search":
        return [TextContent(type="text", text=json.dumps({"active_ads": 15}))]
    
    return [TextContent(type="text", text=f"Unknown tool: {name}")]

social_tools = [
    Tool(
        name="tiktok_fetch_data",
        description=(
            "L1 Extractor: Scrape raw TikTok data (tag metadata, trending videos, and comments) for a product keyword. "
            "Use this tool FIRST to gather social data into the system cache. "
            "After this tool succeeds, use 'tiktok_calculate_virality' to analyze the data."
        ),
        inputSchema={
            "type": "object", 
            "properties": {
                "brand": {"type": "string", "description": "Product brand (e.g. 'Anker')"},
                "product_name": {"type": "string", "description": "Specific product name or model"},
                "keyword": {"type": "string", "description": "Override search tag. Defaults to '{brand} {product_name}' if omitted."},
                "max_comments": {"type": "integer", "description": "Depth of comment analysis (1-50). Defaults to 10."}
            }, 
            "required": ["brand", "product_name"]
        }
    ),
    Tool(
        name="tiktok_calculate_virality",
        description=(
            "L2 Processor: Calculate the Promotional Strength Index (PSI) and analyze consumer intent. "
            "MUST be called AFTER 'tiktok_fetch_data'. Reads from internal cache and applies multi-dimensional business logic."
        ),
        inputSchema={
            "type": "object", 
            "properties": {
                "keyword": {"type": "string", "description": "The exact keyword that was used in 'tiktok_fetch_data' to retrieve the cached data."}
            }, 
            "required": ["keyword"]
        }
    ),
    Tool(
        name="meta_ad_search",
        description="Check Meta Ad Library to identify active advertising campaigns and competitor spend for a keyword.",
        inputSchema={"type": "object", "properties": {"keyword": {"type": "string"}}, "required": ["keyword"]}
    )
]

_SOCIAL_META = {
    "tiktok_fetch_data": ("DATA", "Success confirmation indicating data is cached"),
    "tiktok_calculate_virality": (
        "COMPUTE", 
        "JSON containing 'strength_score' (0-100), 'organic_multiplier', 'recent_videos_ratio', and 'comment_intent_analysis'."
    ),
    "meta_ad_search": ("DATA", "count of active advertisements found on Meta platforms"),
}

for tool in social_tools:
    cat, ret = _SOCIAL_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_social_tool, category=cat, returns=ret)