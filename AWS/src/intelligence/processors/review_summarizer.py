from __future__ import annotations
import logging
import statistics
import re
from typing import List, Optional, Dict, Any
from datetime import datetime
from collections import Counter
from src.core.models.review import Review, ReviewSummary
from src.intelligence.providers.base import BaseLLMProvider
from src.intelligence.prompts.manager import prompt_manager

logger = logging.getLogger(__name__)

class ReviewSummarizer:
    """
    Advanced Processor to analyze product reviews with weighting, 
    sampling, and quantitative metrics (Velocity, Distribution, Barrier, and Manipulation Risk).
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
            # Monthly velocity = (count / days) * 30.44
            velocity = (len(valid_dates) / days) * 30.44
            
            if velocity > 0:
                # Time to benchmark
                current_total = len(reviews)
                needed = max(0, benchmark - current_total)
                barrier_months = round(needed / velocity, 1)

        return {
            "velocity": round(velocity, 2),
            "distribution": distribution,
            "barrier_months": barrier_months
        }

    def _analyze_manipulation_risk(self, reviews: List[Review], est_monthly_sales: int = 0) -> Dict[str, Any]:
        """
        Stage 2: Comprehensive Review Manipulation Detection.
        Algorithms: Rating Cliff Index (RCI), Semantic Overlap, and Review-to-Sales Ratio (RSR).
        """
        if not reviews or len(reviews) < 5:
            return {"score": 0, "verdict": "INSUFFICIENT_DATA", "metrics": {}}

        # 1. Rating Cliff Index (RCI) - High 5-star vs low 4/3-star
        dist = Counter([r.rating for r in reviews])
        # RCI = 5-star / (4-star + 3-star + 1)
        rci = dist[5] / (dist[4] + dist[3] + 1)
        
        # 2. Semantic Overlap (Fingerprinting)
        # We check for duplicate or near-duplicate review templates
        contents = [r.content.lower().strip() for r in reviews if r.content and len(r.content) > 20]
        overlap_ratio = 0.0
        if len(contents) > 1:
            # Simple prefix fingerprinting (first 40 chars) to detect templates
            fingerprints = [c[:40] for c in contents]
            overlap_ratio = (len(fingerprints) - len(set(fingerprints))) / len(fingerprints)

        # 3. Review-to-Sales Ratio (RSR)
        # Monthly velocity / Monthly sales. Natural is 1-3%. >10% is highly suspicious.
        stats = self._calculate_metrics(reviews)
        velocity = stats.get("velocity", 0)
        rsr = (velocity / est_monthly_sales) if est_monthly_sales > 0 else 0.02 # Default to 2% if unknown

        # 4. Final Scoring Logic
        # Weighting: RCI (30%), Overlap (40%), RSR (30%)
        rci_score = min(100, (rci / 10.0) * 100) # RCI of 10+ is max risk
        overlap_score = min(100, (overlap_ratio / 0.2) * 100) # 20% template overlap is max risk
        rsr_score = min(100, (rsr / 0.12) * 100) # 12% RSR is max risk
        
        total_risk_score = (rci_score * 0.3) + (overlap_score * 0.4) + (rsr_score * 0.3)
        
        verdict = "SAFE"
        if total_risk_score > 70: verdict = "CRITICAL"
        elif total_risk_score > 40: verdict = "SUSPICIOUS"

        return {
            "score": round(total_risk_score, 2),
            "verdict": verdict,
            "metrics": {
                "rating_cliff_index": round(rci, 2),
                "template_overlap_pct": f"{overlap_ratio:.1%}",
                "review_to_sales_pct": f"{rsr:.1%}" if est_monthly_sales > 0 else "N/A"
            }
        }

    async def summarize(self, reviews: List[Review], competitive_benchmark: int = 500, est_monthly_sales: int = 0) -> ReviewSummary:
        if not reviews:
            raise ValueError("No reviews provided for summarization.")

        # 1. Calculate Quantitative Metrics & Manipulation Risk
        stats = self._calculate_metrics(reviews, benchmark=competitive_benchmark)
        risk = self._analyze_manipulation_risk(reviews, est_monthly_sales=est_monthly_sales)

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
        review_data += f"Estimated Time to Reach {competitive_benchmark} Reviews: {stats['barrier_months']} months\n"
        review_data += f"MANIPULATION RISK SCORE: {risk['score']}/100 ({risk['verdict']})\n"
        review_data += f"Integrity Details: {risk['metrics']}\n\n"
        
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

        logger.info(f"Summarizing {len(selected_reviews)} reviews with quantitative & integrity context...")
        
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
        summary.manipulation_risk = risk
        
        return summary
