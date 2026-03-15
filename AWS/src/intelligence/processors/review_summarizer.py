from __future__ import annotations
import logging
from typing import List, Optional
from src.core.models.review import Review, ReviewSummary
from src.intelligence.providers.base import BaseLLMProvider
from src.intelligence.prompts.manager import prompt_manager

logger = logging.getLogger(__name__)

class ReviewSummarizer:
    """
    Advanced Processor to analyze product reviews with weighting, 
    sampling, and decoupled prompts.
    """
    
    def __init__(self, provider: BaseLLMProvider):
        self.provider = provider

    async def summarize(self, reviews: List[Review]) -> ReviewSummary:
        if not reviews:
            raise ValueError("No reviews provided for summarization.")

        # 1. Sort & Prioritize: Verified Purchase + Helpful Votes
        sorted_reviews = sorted(
            reviews, 
            key=lambda x: (x.is_verified, x.helpful_votes or 0), 
            reverse=True
        )

        # 2. Balanced Sampling
        positive = [r for r in sorted_reviews if r.rating and r.rating >= 4][:15]
        negative = [r for r in sorted_reviews if r.rating and r.rating <= 2][:15]
        neutral = [r for r in sorted_reviews if r.rating == 3][:5]
        
        selected_reviews = positive + negative + neutral

        # 3. Build optimized review data string
        review_data = ""
        for i, r in enumerate(selected_reviews):
            status = "Verified" if r.is_verified else "Non-Verified"
            votes = f"{r.helpful_votes} helpful" if r.helpful_votes else "0 votes"
            review_data += (
                f"--- Review {i+1} [{r.rating} stars | {status} | {votes}] ---\n"
                f"Title: {r.title}\n"
                f"Content: {r.content}\n\n"
            )

        # 4. LOAD PROMPT FROM MANAGER (Decoupled!)
        # This replaces the hardcoded f-string
        system_msg, user_prompt = prompt_manager.render(
            name="review_analysis",
            variables={"review_data": review_data}
        )

        logger.info(f"Summarizing {len(selected_reviews)} reviews via decoupled prompt 'review_analysis'...")
        
        # 5. Synthesis
        return await self.provider.generate_structured(
            prompt=user_prompt,
            schema=ReviewSummary,
            system_message=system_msg
        )
