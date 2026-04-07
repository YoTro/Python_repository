from __future__ import annotations
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
import re
from src.core.models.review import Review, ReviewSummary
from src.intelligence.providers.base import BaseLLMProvider
from src.intelligence.prompts.manager import prompt_manager

logger = logging.getLogger(__name__)

class ReviewSummarizer:
    """
    Advanced Processor to analyze product reviews with weighting, 
    sampling, and quantitative metrics (Velocity, Distribution, Barrier).
    """
    
    def __init__(self, provider: BaseLLMProvider):
        self.provider = provider

    def _parse_amazon_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parses various Amazon review date formats (US centric)."""
        if not date_str: return None
        # Extract 'Month Day, Year' part
        match = re.search(r'([A-Za-z]+ \d{1,2}, \d{4})', date_str)
        if match:
            try:
                return datetime.strptime(match.group(1), "%B %d, %Y")
            except:
                pass
        return None

    def _calculate_metrics(self, reviews: List[Review], benchmark: int = 500) -> Dict[str, Any]:
        """Calculates quantitative metrics from raw reviews."""
        if not reviews:
            return {"velocity": 0.0, "distribution": {}, "barrier_months": None}

        # 1. Rating Distribution
        distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        for r in reviews:
            if r.rating and 1 <= r.rating <= 5:
                distribution[r.rating] += 1

        # 2. Review Velocity (based on time range of sample)
        dates = [self._parse_amazon_date(r.date) for r in reviews]
        valid_dates = [d for d in dates if d]
        
        velocity = 0.0
        barrier_months = None
        
        if len(valid_dates) >= 2:
            time_span = max(valid_dates) - min(valid_dates)
            days = max(1, time_span.days)
            # Monthly velocity = (count / days) * 30
            velocity = (len(valid_dates) / days) * 30.44
            
            if velocity > 0:
                # Time to benchmark
                current_total = len(reviews) # In a real scenario, this would be the actual total
                needed = max(0, benchmark - current_total)
                barrier_months = round(needed / velocity, 1)

        return {
            "velocity": round(velocity, 2),
            "distribution": distribution,
            "barrier_months": barrier_months
        }

    async def summarize(self, reviews: List[Review], competitive_benchmark: int = 500) -> ReviewSummary:
        if not reviews:
            raise ValueError("No reviews provided for summarization.")

        # 1. Calculate Quantitative Metrics
        stats = self._calculate_metrics(reviews, benchmark=competitive_benchmark)

        # 2. Sort & Prioritize for LLM (Verified Purchase + Helpful Votes)
        sorted_reviews = sorted(
            reviews, 
            key=lambda x: (x.is_verified, x.helpful_votes or 0), 
            reverse=True
        )

        # 3. Balanced Sampling for text analysis
        positive = [r for r in sorted_reviews if r.rating and r.rating >= 4][:15]
        negative = [r for r in sorted_reviews if r.rating and r.rating <= 2][:15]
        neutral = [r for r in sorted_reviews if r.rating == 3][:5]
        
        selected_reviews = positive + negative + neutral

        # 4. Build optimized review data string
        review_data = f"--- QUANTITATIVE METRICS ---\n"
        review_data += f"Monthly Review Velocity: {stats['velocity']} reviews/month\n"
        review_data += f"Rating Distribution: {stats['distribution']}\n"
        review_data += f"Estimated Time to Reach {competitive_benchmark} Reviews: {stats['barrier_months']} months\n\n"
        
        review_data += "--- RAW REVIEW SAMPLES ---\n"
        for i, r in enumerate(selected_reviews):
            status = "Verified" if r.is_verified else "Non-Verified"
            votes = f"{r.helpful_votes} helpful" if r.helpful_votes else "0 votes"
            review_data += (
                f"Review {i+1} [{r.rating} stars | {status} | {votes}]\n"
                f"Title: {r.title}\n"
                f"Content: {r.content}\n\n"
            )

        # 5. LOAD PROMPT FROM MANAGER
        system_msg, user_prompt = prompt_manager.render(
            name="review_analysis",
            variables={"review_data": review_data}
        )

        logger.info(f"Summarizing {len(selected_reviews)} reviews with quantitative context...")
        
        # 6. Synthesis
        summary: ReviewSummary = await self.provider.generate_structured(
            prompt=user_prompt,
            schema=ReviewSummary,
            system_message=system_msg
        )
        
        # 7. Inject calculated stats into the final Pydantic model
        summary.review_velocity = stats['velocity']
        summary.rating_distribution = stats['distribution']
        summary.competitive_barrier_months = stats['barrier_months']
        
        return summary
