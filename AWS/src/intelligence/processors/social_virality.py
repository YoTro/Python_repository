from __future__ import annotations
import logging
import time
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class SocialViralityProcessor:
    """
    L2 Intelligence Processor for evaluating social media virality and promotional strength.
    Computes a Promotional Strength Index (PSI) based on raw video data.
    """

    def calculate_promotion_strength(self, videos: List[Dict[str, Any]], brand: str = "", product_name: str = "", tag_metadata: Optional[Dict[str, Any]] = None, comments_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Calculates a Promotional Strength Index (PSI) from 0-100 based on TikTok/Social video data.
        Optional `comments_data` can be provided for LLM-based sentiment and purchase intent analysis.
        """
        if not videos:
            return {
                "strength_score": 0,
                "total_views": 0,
                "engagement_rate": 0.0,
                "verdict": "No Data (Blank Canvas)",
                "amazon_mentions": 0,
                "comment_intent": "Unknown"
            }

        total_views = 0
        total_likes = 0
        total_comments = 0
        total_shares = 0
        amazon_mentions = 0
        
        total_organic_ratio = 0
        recent_video_count = 0
        
        current_time = int(time.time())
        THIRTY_DAYS = 30 * 24 * 3600
        
        # 1. Total Tag Volume (Historical scale)
        global_video_count = tag_metadata.get("video_count", 0) if tag_metadata else 0
        volume_historical_score = min((global_video_count / 1000000) * 100, 100) * 0.15

        # Keywords that indicate conversion intent
        conversion_keywords = ["link in bio", "amazon finds", "amazon must haves", "amazon", product_name.lower(), brand.lower()]

        valid_videos = 0
        for v in videos:
            stats = v.get("stats", {})
            desc = v.get("desc", "").lower()
            author_stats = v.get("authorStats", {})
            
            views = stats.get("playCount", 0) or v.get("views", 0)
            likes = stats.get("diggCount", 0) or v.get("likes", 0)
            comments_count = stats.get("commentCount", 0) or v.get("comments", 0)
            shares = stats.get("shareCount", 0) or v.get("shares", 0)
            
            followers = author_stats.get("followerCount", 1) or 1
            create_time = v.get("createTime", 0)
            
            total_views += views
            total_likes += likes
            total_comments += comments_count
            total_shares += shares
            
            # Organic Virality Ratio: Views / Followers
            organic_ratio = views / followers if followers > 0 else 0
            total_organic_ratio += organic_ratio
            
            # Recency Check
            if create_time > 0 and (current_time - create_time) <= THIRTY_DAYS:
                recent_video_count += 1
                
            if any(kw in desc for kw in conversion_keywords if kw):
                amazon_mentions += 1
                
            valid_videos += 1

        if valid_videos == 0:
            valid_videos = 1

        # 2. Sample Volume (Recent momentum) - Weight: 15%
        volume_recent_score = min((total_views / 5000000) * 100, 100) * 0.15
        
        # 3. Engagement Rate (Likes + Comments + Shares) / Views - Weight: 30%
        total_engagement = total_likes + total_comments + total_shares
        engagement_rate = (total_engagement / total_views) if total_views > 0 else 0
        engagement_score = min((engagement_rate / 0.15) * 100, 100) * 0.30
        
        # 4. Conversion Intent - Weight: 15%
        intent_rate = amazon_mentions / valid_videos
        intent_score = intent_rate * 100 * 0.15
        
        # 5. Organic Virality (Average Views vs Followers ratio) - Weight: 15%
        avg_organic_ratio = total_organic_ratio / valid_videos
        organic_score = min((avg_organic_ratio / 10.0) * 100, 100) * 0.15
        
        # 6. Recency (Trend Lifecycle) - Weight: 10%
        recency_rate = recent_video_count / valid_videos
        recency_score = recency_rate * 100 * 0.10

        # 8. Creator Diversity (Unique authors vs Total Videos)
        # Higher diversity means the trend is broad and not just driven by one account.
        unique_authors = len(set(v.get("author", {}).get("uniqueId") for v in videos if v.get("author")))
        creator_diversity = unique_authors / valid_videos if valid_videos > 0 else 0
        
        final_score = volume_historical_score + volume_recent_score + engagement_score + intent_score + organic_score + recency_score
        
        verdict = "Low (Untapped Market)"
        if final_score > 75:
            if recency_rate < 0.2:
                verdict = "Viral but Fading (Fad)"
            else:
                verdict = "Viral / High Competition (Saturated)"
        elif final_score > 40:
            if avg_organic_ratio > 5.0 and recency_rate > 0.5:
                verdict = "Explosive Growth (Organic Blue Ocean)"
            else:
                verdict = "Medium Virality (Growing Trend)"
                
        # 7. Placeholder for Comment Sentiment/Intent Analysis (LLM Integration Point)
        comment_intent = "Not Analyzed"
        if comments_data:
            # L2 Logic: Using TikTok's native 'is_high_purchase_intent' if available
            buy_intent_signals = sum(1 for c in comments_data if c.get("is_high_purchase_intent", False))
            
            # Fallback to keyword matching if native flag is not set
            buy_intent_keywords = ["where", "buy", "link", "price", "need", "want"]
            keyword_signals = sum(1 for c in comments_data if any(kw in c.get("text", "").lower() for kw in buy_intent_keywords))
            
            total_intent_signals = buy_intent_signals + keyword_signals
            
            # NOTE for future LLM integration:
            # If the raw strings from comments_data are fed into a Prompt Template,
            # an LLM can classify nuanced sentiment (e.g., "I bought this but it broke" vs "Need this now!").
            # For now, we rely on TikTok's internal ML flags and keyword density.
            if total_intent_signals > len(comments_data) * 0.1:
                comment_intent = "High Purchase Intent (Audience wants to buy)"
            else:
                comment_intent = "Low Purchase Intent (Audience is just entertained)"
            
        return {
            "strength_score": round(final_score, 2),
            "total_tag_videos": global_video_count,
            "total_views_sample": total_views,
            "avg_views_per_video": int(total_views / valid_videos) if valid_videos > 0 else 0,
            "engagement_rate": round(engagement_rate, 4),
            "amazon_mentions": amazon_mentions,
            "organic_multiplier": round(avg_organic_ratio, 2),
            "recent_videos_ratio": round(recency_rate, 2),
            "creator_diversity": round(creator_diversity, 2),
            "verdict": verdict,
            "comment_intent_analysis": comment_intent,
            "metrics": {
                "historical_volume_contribution": round(volume_historical_score, 2),
                "recent_volume_contribution": round(volume_recent_score, 2),
                "engagement_contribution": round(engagement_score, 2),
                "intent_contribution": round(intent_score, 2),
                "organic_viral_contribution": round(organic_score, 2),
                "recency_contribution": round(recency_score, 2)
            }
        }
