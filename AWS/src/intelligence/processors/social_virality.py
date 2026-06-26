from __future__ import annotations

import logging
import statistics
import time
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class SocialViralityProcessor:
    """
    L2 Intelligence Processor for evaluating social media virality and promotional strength.
    Computes a Promotional Strength Index (PSI) based on raw video data.
    """

    # --- PSI Additive Weights (must sum to 1.0) ---
    W_HISTORICAL_VOLUME = 0.10
    W_RECENT_VOLUME = 0.10
    W_ENGAGEMENT = 0.25
    W_INTENT = 0.15
    W_ORGANIC_VIRALITY = 0.10
    W_RECENCY = 0.10
    W_SHARE_VIRALITY = 0.10
    W_CREATOR_DIVERSITY = 0.10

    # --- Benchmark denominators (value that yields 100 raw points) ---
    BENCHMARK_HISTORICAL_VOLUME = 1_000_000  # tag video count for full historical score
    BENCHMARK_RECENT_VOLUME = 5_000_000  # total sample views for full recent score
    BENCHMARK_ORGANIC_RATIO = 10.0  # views/followers multiplier

    # Fallback benchmarks — used only when no reference_videos are provided.
    BENCHMARK_ENGAGEMENT_RATE = 0.06  # (likes+comments+shares)/views
    BENCHMARK_SHARE_RATE = 0.03  # shares/views

    # --- KOL/KOC Creator Tier Thresholds (by follower count) ---
    TIER_NANO_MAX = 10_000  # Nano:        <10K
    TIER_MICRO_MAX = 100_000  # Micro (KOC): 10K–100K
    TIER_MID_MAX = 500_000  # Mid-tier:    100K–500K
    TIER_MACRO_MAX = 1_000_000  # Macro (KOL): 500K–1M
    # Mega (KOL): >TIER_MACRO_MAX

    # --- Multiplier Penalty Thresholds ---
    # Linear ramp slope applied to every penalty: penalty = min(max_penalty, excess * slope)
    PENALTY_SLOPE = 0.5

    # KOL dominance: (macro+mega)/total above threshold implies paid campaign structure
    KOL_DOMINANCE_THRESHOLD = 0.60
    KOL_MAX_PENALTY = 0.20

    # HHI view concentration: high HHI means a few creators monopolise all views
    HHI_PENALTY_THRESHOLD = 0.25
    HHI_MAX_PENALTY = 0.15

    # Promotional tag saturation: high ratio signals paid seeding, not organic virality
    PROMO_TAG_THRESHOLD = 0.30
    PROMO_TAG_MAX_PENALTY = 0.20

    # Negative comment sentiment: signals product reputation risk
    NEG_SENTIMENT_THRESHOLD = 0.20
    NEG_SENTIMENT_MAX_PENALTY = 0.20

    # --- Promotional Tag Vocabulary ---
    PROMO_TAGS: frozenset[str] = frozenset(
        {
            "#ad",
            "#advertisement",
            "#sponsored",
            "#gifted",
            "#partnership",
            "#collab",
            "#collaboration",
            "#promo",
            "#promotion",
            "#paid",
            "#paidpartnership",
            "#brandambassador",
            "#brandpartner",
            "#spon",
        }
    )

    # --- Verdict Thresholds ---
    PURCHASE_INTENT_THRESHOLD = 0.15  # purchase intent rate → "High Purchase Intent"
    POSITIVE_SENTIMENT_THRESHOLD = 0.40  # positive sentiment rate → "Strong Positive Sentiment"
    FADING_RECENCY_THRESHOLD = 0.20  # recency rate below which viral trend is "Fading"

    # --- Platform-specific conversion intent signals (in addition to brand/product name) ---
    PLATFORM_BUY_SIGNALS: dict[str, list[str]] = {
        "tiktok": ["link in bio", "amazon finds", "amazon must haves", "amazon"],
        "youtube_shorts": ["link in description", "amazon", "shop link", "buy link"],
        "instagram": ["link in bio", "shop link", "amazon", "linktree"],
    }

    @staticmethod
    def normalize_video(raw: dict[str, Any], platform: str = "tiktok") -> dict[str, Any]:
        """Convert a raw platform API video dict to the canonical flat schema.

        Canonical fields guaranteed on output:
          views, likes, comments, shares, followers, uid, desc, createTime (unix int)

        Supported platforms: "tiktok", "youtube_shorts", "instagram".
        Unknown platform → passthrough (caller is responsible for pre-normalization).
        """
        if platform == "tiktok":
            stats = raw.get("stats", {})
            author = raw.get("author", {})
            author_stats = raw.get("authorStats", {})
            return {
                "views": stats.get("playCount", 0) or raw.get("views", 0),
                "likes": stats.get("diggCount", 0) or raw.get("likes", 0),
                "comments": stats.get("commentCount", 0) or raw.get("comments", 0),
                "shares": stats.get("shareCount", 0) or raw.get("shares", 0),
                "followers": author_stats.get("followerCount", 0),
                "uid": author.get("uniqueId", ""),
                "desc": raw.get("desc", ""),
                "createTime": raw.get("createTime", 0),
            }
        if platform == "youtube_shorts":
            stats = raw.get("statistics", {})
            snippet = raw.get("snippet", {})
            published_at = snippet.get("publishedAt", "")
            create_time = 0
            if published_at:
                try:
                    create_time = int(
                        datetime.fromisoformat(published_at.replace("Z", "+00:00")).timestamp()
                    )
                except (ValueError, AttributeError):
                    pass
            tags = snippet.get("tags") or []
            desc = (snippet.get("description", "") + " " + " ".join(f"#{t}" for t in tags)).strip()
            return {
                "views": int(stats.get("viewCount", 0) or 0),
                "likes": int(stats.get("likeCount", 0) or 0),
                "comments": int(stats.get("commentCount", 0) or 0),
                "shares": 0,  # YouTube Data API v3 does not expose share count
                "followers": int(raw.get("channelStatistics", {}).get("subscriberCount", 0) or 0),
                "uid": snippet.get("channelId", "") or raw.get("channelId", ""),
                "desc": desc,
                "createTime": create_time,
            }
        if platform == "instagram":
            timestamp = raw.get("timestamp", "")
            create_time = 0
            if timestamp:
                try:
                    create_time = int(
                        datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
                    )
                except (ValueError, AttributeError):
                    pass
            owner = raw.get("owner", {})
            return {
                "views": raw.get("video_view_count", 0) or raw.get("play_count", 0),
                "likes": raw.get("like_count", 0),
                "comments": raw.get("comments_count", 0),
                "shares": 0,  # Instagram Graph API does not expose share count
                "followers": owner.get("followers_count", 0),
                "uid": owner.get("id", "") or raw.get("ig_id", ""),
                "desc": raw.get("caption", ""),
                "createTime": create_time,
            }
        # Unknown platform — assume already normalized
        return raw

    def _compute_median_benchmarks(
        self, reference_videos: list[dict[str, Any]]
    ) -> tuple[float, float]:
        """
        Derives engagement and share-rate benchmarks from the median of reference videos
        (competitor or category hashtag videos fetched at L1).
        Returns (median_engagement_rate, median_share_rate).
        Falls back to class defaults if reference data is empty or all views are zero.
        """
        eng_rates: list[float] = []
        share_rates: list[float] = []

        for v in reference_videos:
            views = v.get("views", 0)
            if not views:
                continue
            likes = v.get("likes", 0)
            comments = v.get("comments", 0)
            shares = v.get("shares", 0)
            eng_rates.append((likes + comments + shares) / views)
            share_rates.append(shares / views)

        if not eng_rates:
            return self.BENCHMARK_ENGAGEMENT_RATE, self.BENCHMARK_SHARE_RATE

        return statistics.median(eng_rates), statistics.median(share_rates)

    def calculate_promotion_strength(
        self,
        videos: list[dict[str, Any]],
        brand: str = "",
        product_name: str = "",
        tag_metadata: dict[str, Any] | None = None,
        comments_data: list[dict[str, Any]] | None = None,
        reference_videos: list[dict[str, Any]] | None = None,
        llm_comment_analysis: dict[str, Any] | None = None,
        window_days: int = 30,
        platform: str = "tiktok",
    ) -> dict[str, Any]:
        """
        Calculates a Promotional Strength Index (PSI) from 0-100 based on TikTok/Social video data.
        Optional `comments_data` can be provided for sentiment and purchase intent analysis.

        `videos` may be a large adaptive sample (up to 300); only those within `window_days`
        are scored. `createTime` is already present in each video — no extra API call needed.

        `reference_videos` (from tiktok_fetch_reference_data) drives dynamic peer benchmarks;
        the same recency filter is applied so only recent reference content sets the baseline.

        PSI additive weights (sum to 100%):
          Historical Volume 10% | Recent Volume 10% | Engagement 25% | Intent 15%
          Organic Virality 10%  | Recency 10%        | Share Virality 10% | Creator Diversity 10%

        Multiplier penalties (applied sequentially after base score):
          KOL dominance >60%         → up to -20%
          HHI view concentration     → up to -15%
          Promo tag saturation >30%  → up to -20%
          Negative comment sentiment → up to -20%
        """
        if not videos:
            return {
                "strength_score": 0,
                "total_views": 0,
                "engagement_rate": 0.0,
                "verdict": "No Data (Blank Canvas)",
                "amazon_mentions": 0,
                "comment_intent": "Unknown",
            }

        # Normalize raw platform API data to the canonical flat schema.
        videos = [self.normalize_video(v, platform) for v in videos]

        # Filter to the recent window using createTime already present in each video.
        # This uses the full adaptive sample fetched by L1 but scores only current content.
        cutoff = int(time.time()) - window_days * 24 * 3600
        scored_videos = [v for v in videos if v.get("createTime", 0) >= cutoff]

        # Apply the same recency filter to reference videos so the benchmark reflects
        # current peer engagement, not historical noise.
        if reference_videos:
            reference_videos = [self.normalize_video(v, platform) for v in reference_videos]
            recent_ref = [v for v in reference_videos if v.get("createTime", 0) >= cutoff]
            if recent_ref:
                engagement_benchmark, share_benchmark = self._compute_median_benchmarks(recent_ref)
                benchmark_source = f"peer_median (n={len(recent_ref)}, window={window_days}d)"
            else:
                engagement_benchmark = self.BENCHMARK_ENGAGEMENT_RATE
                share_benchmark = self.BENCHMARK_SHARE_RATE
                benchmark_source = "default (no recent ref)"
        else:
            engagement_benchmark = self.BENCHMARK_ENGAGEMENT_RATE
            share_benchmark = self.BENCHMARK_SHARE_RATE
            benchmark_source = "default"

        total_views = 0
        total_likes = 0
        total_comments = 0
        total_shares = 0
        amazon_mentions = 0

        total_organic_ratio = 0
        total_share_ratio = 0
        recent_video_count = 0
        promo_tag_count = 0

        # KOL/KOC tier counters
        tier_counts: dict[str, int] = {"nano": 0, "micro": 0, "mid": 0, "macro": 0, "mega": 0}
        # views per unique creator uid — used for HHI and creator diversity
        creator_views: dict[str, int] = {}

        # 1. Total Tag Volume (Historical scale)
        global_video_count = tag_metadata.get("video_count", 0) if tag_metadata else 0
        volume_historical_score = (
            min((global_video_count / self.BENCHMARK_HISTORICAL_VOLUME) * 100, 100)
            * self.W_HISTORICAL_VOLUME
        )

        buy_signals = self.PLATFORM_BUY_SIGNALS.get(platform, ["amazon"])
        conversion_keywords = [*buy_signals, product_name.lower(), brand.lower()]

        valid_videos = 0
        for v in scored_videos:
            desc = v.get("desc", "").lower()

            views = v.get("views", 0)
            likes = v.get("likes", 0)
            comments_count = v.get("comments", 0)
            shares = v.get("shares", 0)

            followers = v.get("followers", 0)
            uid = v.get("uid", "")

            # KOL/KOC tier classification
            if followers > self.TIER_MACRO_MAX:
                tier_counts["mega"] += 1
            elif followers > self.TIER_MID_MAX:
                tier_counts["macro"] += 1
            elif followers > self.TIER_MICRO_MAX:
                tier_counts["mid"] += 1
            elif followers > self.TIER_NANO_MAX:
                tier_counts["micro"] += 1
            else:
                tier_counts["nano"] += 1

            # Accumulate views per creator for HHI and diversity
            if uid:
                creator_views[uid] = creator_views.get(uid, 0) + views

            total_views += views
            total_likes += likes
            total_comments += comments_count
            total_shares += shares

            # Organic Virality Ratio: Views / Followers
            total_organic_ratio += views / followers if followers > 0 else 0

            # Share Virality Ratio: Shares / Views
            total_share_ratio += shares / views if views > 0 else 0

            # All scored_videos are already within window_days — count every one as recent.
            recent_video_count += 1

            if any(kw in desc for kw in conversion_keywords if kw):
                amazon_mentions += 1

            # Promotional tag detection
            if any(tag in desc for tag in self.PROMO_TAGS):
                promo_tag_count += 1

            valid_videos += 1

        if valid_videos == 0:
            valid_videos = 1

        # --- KOL/KOC Matrix ---
        kol_koc_matrix = {
            tier: round(count / valid_videos, 2) for tier, count in tier_counts.items()
        }
        koc_ratio = (tier_counts["nano"] + tier_counts["micro"]) / valid_videos
        kol_ratio = (tier_counts["macro"] + tier_counts["mega"]) / valid_videos
        kol_koc_matrix["koc_ratio"] = round(koc_ratio, 2)
        kol_koc_matrix["kol_ratio"] = round(kol_ratio, 2)

        mega_influencer_ratio = tier_counts["mega"] / valid_videos

        # --- HHI Creator Concentration Index ---
        # Σ(creator_views / total_views)²; 0 = perfectly distributed, 1 = monopoly
        hhi = (
            sum((cv / total_views) ** 2 for cv in creator_views.values())
            if total_views > 0 and creator_views
            else 1.0
        )

        # --- Promotional Tag Ratio ---
        promo_tag_ratio = promo_tag_count / valid_videos

        # --- Comment Analysis ---
        # Priority: LLM deep analysis (tiktok_fetch_comments) > keyword fallback > video desc signals
        comment_analysis: dict[str, Any]
        neg_rate: float = 0.0
        if llm_comment_analysis:
            comment_analysis = llm_comment_analysis
            _signals = llm_comment_analysis.get("purchase_signals", {})
            _total = (
                llm_comment_analysis.get("confidence", {}).get("total_comments_analyzed", 1) or 1
            )
            _buy = _signals.get("explicit_buy_intent", 0) + _signals.get("product_inquiry", 0)
            intent_rate = _buy / _total
            neg_rate = llm_comment_analysis.get("sentiment", {}).get("negative", 0.0)
        elif comments_data:
            _total = len(comments_data)
            _pos_kw = ["love", "amazing", "great", "best", "want", "need", "wow", "🔥", "😍"]
            _neg_kw = ["bad", "worst", "expensive", "break", "broke", "scam", "waste", "don't buy"]
            _buy_kw = ["where", "buy", "link", "price", "how much", "available", "store"]
            _pos = _neg = _buy = 0
            for c in comments_data:
                _text = c.get("text", "").lower()
                _has_pos = any(kw in _text for kw in _pos_kw)
                _has_neg = any(kw in _text for kw in _neg_kw)
                if _has_pos and not _has_neg:
                    _pos += 1
                elif _has_neg:
                    _neg += 1
                if c.get("is_high_purchase_intent", False) or any(kw in _text for kw in _buy_kw):
                    _buy += 1
            _pos_rate = _pos / _total
            neg_rate = _neg / _total
            intent_rate = _buy / _total
            comment_analysis = {
                "total_analyzed": _total,
                "sentiment": {
                    "positive": round(_pos_rate, 2),
                    "negative": round(neg_rate, 2),
                    "neutral": round(max(0.0, 1 - _pos_rate - neg_rate), 2),
                },
                "intent": {
                    "purchase": round(intent_rate, 2),
                    "curiosity": round(max(0.0, 1 - intent_rate - neg_rate), 2),
                    "negative": round(neg_rate, 2),
                },
                "summary": (
                    "High Purchase Intent (Active buyers)"
                    if intent_rate > self.PURCHASE_INTENT_THRESHOLD
                    else "Strong Positive Sentiment (Brand affinity)"
                    if _pos_rate > self.POSITIVE_SENTIMENT_THRESHOLD
                    else "Mixed/Low Engagement"
                ),
            }
        else:
            intent_rate = amazon_mentions / valid_videos
            comment_analysis = {
                "summary": "Not Analyzed",
                "confidence": {"sample_quality": "none"},
            }

        # 2. Sample Volume (Recent momentum)
        volume_recent_score = (
            min((total_views / self.BENCHMARK_RECENT_VOLUME) * 100, 100) * self.W_RECENT_VOLUME
        )

        # 3. Engagement Rate (Likes + Comments + Shares) / Views
        total_engagement = total_likes + total_comments + total_shares
        engagement_rate = total_engagement / total_views if total_views > 0 else 0
        engagement_score = (
            min((engagement_rate / engagement_benchmark) * 100, 100) * self.W_ENGAGEMENT
        )

        # 4. Conversion Intent — rate set by comment analysis block above
        intent_score = min(intent_rate * 100, 100) * self.W_INTENT

        # 5. Organic Virality (Average Views / Followers ratio)
        avg_organic_ratio = total_organic_ratio / valid_videos
        organic_score = (
            min((avg_organic_ratio / self.BENCHMARK_ORGANIC_RATIO) * 100, 100)
            * self.W_ORGANIC_VIRALITY
        )

        # 6. Recency (Trend Lifecycle)
        recency_rate = recent_video_count / valid_videos
        recency_score = recency_rate * 100 * self.W_RECENCY

        # 7. Share Virality (Spreadability)
        avg_share_ratio = total_share_ratio / valid_videos
        share_virality_score = (
            min((avg_share_ratio / share_benchmark) * 100, 100) * self.W_SHARE_VIRALITY
        )

        # 8. Creator Diversity (Unique authors vs Total Videos)
        unique_authors = len(creator_views)
        creator_diversity = unique_authors / valid_videos if valid_videos > 0 else 0
        creator_diversity_score = creator_diversity * 100 * self.W_CREATOR_DIVERSITY

        base_score = (
            volume_historical_score
            + volume_recent_score
            + engagement_score
            + intent_score
            + organic_score
            + recency_score
            + share_virality_score
            + creator_diversity_score
        )

        # --- Multiplier Penalties ---

        # KOL Dominance: macro+mega above threshold implies paid campaign, not organic spread
        kol_penalty = 1.0
        if kol_ratio > self.KOL_DOMINANCE_THRESHOLD:
            kol_penalty = 1.0 - min(
                (kol_ratio - self.KOL_DOMINANCE_THRESHOLD) * self.PENALTY_SLOPE,
                self.KOL_MAX_PENALTY,
            )

        # HHI Concentration: few creators monopolising views reduces organic credibility
        hhi_penalty = 1.0
        if hhi > self.HHI_PENALTY_THRESHOLD:
            hhi_penalty = 1.0 - min(
                (hhi - self.HHI_PENALTY_THRESHOLD) * self.PENALTY_SLOPE,
                self.HHI_MAX_PENALTY,
            )

        # Promo Tag Saturation: high ratio signals paid seeding, not organic virality
        promo_tag_penalty = 1.0
        if promo_tag_ratio > self.PROMO_TAG_THRESHOLD:
            promo_tag_penalty = 1.0 - min(
                (promo_tag_ratio - self.PROMO_TAG_THRESHOLD) * self.PENALTY_SLOPE,
                self.PROMO_TAG_MAX_PENALTY,
            )

        final_score = base_score * kol_penalty * hhi_penalty * promo_tag_penalty

        # Negative Sentiment Penalty — neg_rate set by comment analysis block above
        if neg_rate > self.NEG_SENTIMENT_THRESHOLD:
            sentiment_penalty = 1.0 - min(
                (neg_rate - self.NEG_SENTIMENT_THRESHOLD) * self.PENALTY_SLOPE,
                self.NEG_SENTIMENT_MAX_PENALTY,
            )
            final_score *= sentiment_penalty

        verdict = "Low (Untapped Market)"
        if final_score > 75:
            if recency_rate < self.FADING_RECENCY_THRESHOLD:
                verdict = "Viral but Fading (Fad)"
            else:
                verdict = "Viral / High Competition (Saturated)"
        elif final_score > 40:
            if avg_organic_ratio > 5.0 and recency_rate > 0.5:
                if promo_tag_ratio > self.PROMO_TAG_THRESHOLD:
                    verdict = "Explosive Growth (Paid Seeding Campaign)"
                else:
                    verdict = "Explosive Growth (Organic Blue Ocean)"
            else:
                verdict = "Medium Virality (Growing Trend)"

        return {
            "strength_score": round(final_score, 2),
            "total_tag_videos": global_video_count,
            "sample": {
                "fetched": len(videos),
                "scored": len(scored_videos),
                "window_days": window_days,
            },
            "total_views_sample": total_views,
            "avg_views_per_video": int(total_views / valid_videos) if valid_videos > 0 else 0,
            "engagement_rate": round(engagement_rate, 4),
            "amazon_mentions": amazon_mentions,
            "organic_multiplier": round(avg_organic_ratio, 2),
            "share_virality_ratio": round(avg_share_ratio, 4),
            "recent_videos_ratio": round(recency_rate, 2),
            "mega_influencer_ratio": round(mega_influencer_ratio, 2),
            "creator_diversity": round(creator_diversity, 2),
            "kol_koc_matrix": kol_koc_matrix,
            "hhi_concentration": round(hhi, 4),
            "promo_tag_ratio": round(promo_tag_ratio, 2),
            "verdict": verdict,
            "penalties": {
                "kol_dominance": round(kol_penalty, 4),
                "hhi_concentration": round(hhi_penalty, 4),
                "promo_tag": round(promo_tag_penalty, 4),
            },
            "comment_analysis": comment_analysis,
            "benchmarks": {
                "source": benchmark_source,
                "engagement_rate": round(engagement_benchmark, 4),
                "share_rate": round(share_benchmark, 4),
            },
            "metrics": {
                "historical_volume_contribution": round(volume_historical_score, 2),
                "recent_volume_contribution": round(volume_recent_score, 2),
                "engagement_contribution": round(engagement_score, 2),
                "intent_contribution": round(intent_score, 2),
                "organic_viral_contribution": round(organic_score, 2),
                "recency_contribution": round(recency_score, 2),
                "share_virality_contribution": round(share_virality_score, 2),
                "creator_diversity_contribution": round(creator_diversity_score, 2),
            },
        }
