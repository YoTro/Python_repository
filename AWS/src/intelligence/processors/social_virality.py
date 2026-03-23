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
        mega_influencer_count = 0
        
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
            
            if followers > 1000000:
                mega_influencer_count += 1
            
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
            
        mega_influencer_ratio = mega_influencer_count / valid_videos

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
                
        # 7. Enhanced Comment Sentiment/Intent Analysis
        comment_analysis = {
            "total_analyzed": 0,
            "sentiment": {"positive": 0, "negative": 0, "neutral": 0},
            "intent": {"purchase": 0, "curiosity": 0, "negative": 0},
            "summary": "Not Analyzed"
        }
        
        if comments_data:
            comment_analysis["total_analyzed"] = len(comments_data)
            
            # Keywords for classification
            pos_keywords = ["love", "amazing", "great", "best", "want", "need", "wow", "🔥", "😍"]
            neg_keywords = ["bad", "worst", "expensive", "break", "broke", "scam", "waste", "don't buy"]
            buy_keywords = ["where", "buy", "link", "price", "how much", "available", "store"]
            
            pos_count = 0
            neg_count = 0
            buy_count = 0
            
            for c in comments_data:
                text = c.get("text", "").lower()
                is_high_intent = c.get("is_high_purchase_intent", False)
                
                # Sentiment heuristics
                has_pos = any(kw in text for kw in pos_keywords)
                has_neg = any(kw in text for kw in neg_keywords)
                
                if has_pos and not has_neg:
                    pos_count += 1
                elif has_neg:
                    neg_count += 1
                
                # Intent heuristics
                if is_high_intent or any(kw in text for kw in buy_keywords):
                    buy_count += 1

            comment_analysis["sentiment"]["positive"] = round(pos_count / len(comments_data), 2)
            comment_analysis["sentiment"]["negative"] = round(neg_count / len(comments_data), 2)
            comment_analysis["sentiment"]["neutral"] = round(1 - (comment_analysis["sentiment"]["positive"] + comment_analysis["sentiment"]["negative"]), 2)
            
            comment_analysis["intent"]["purchase"] = round(buy_count / len(comments_data), 2)
            comment_analysis["intent"]["negative"] = comment_analysis["sentiment"]["negative"]
            comment_analysis["intent"]["curiosity"] = round(1 - (comment_analysis["intent"]["purchase"] + comment_analysis["intent"]["negative"]), 2)
            
            if comment_analysis["intent"]["purchase"] > 0.15:
                comment_analysis["summary"] = "High Purchase Intent (Active buyers)"
            elif comment_analysis["sentiment"]["positive"] > 0.4:
                comment_analysis["summary"] = "Strong Positive Sentiment (Brand affinity)"
            else:
                comment_analysis["summary"] = "Mixed/Low Engagement"
            
        return {
            "strength_score": round(final_score, 2),
            "total_tag_videos": global_video_count,
            "total_views_sample": total_views,
            "avg_views_per_video": int(total_views / valid_videos) if valid_videos > 0 else 0,
            "engagement_rate": round(engagement_rate, 4),
            "amazon_mentions": amazon_mentions,
            "organic_multiplier": round(avg_organic_ratio, 2),
            "recent_videos_ratio": round(recency_rate, 2),
            "mega_influencer_ratio": round(mega_influencer_ratio, 2),
            "creator_diversity": round(creator_diversity, 2),
            "verdict": verdict,
            "comment_analysis": comment_analysis,
            "metrics": {
                "historical_volume_contribution": round(volume_historical_score, 2),
                "recent_volume_contribution": round(volume_recent_score, 2),
                "engagement_contribution": round(engagement_score, 2),
                "intent_contribution": round(intent_score, 2),
                "organic_viral_contribution": round(organic_score, 2),
                "recency_contribution": round(recency_score, 2)
            }
        }
