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

    def _deduplicate_reviews(self, reviews: List[Review]) -> List[Review]:
        """Step 1: Remove near-duplicate reviews using prefix fingerprinting."""
        seen_fingerprints = set()
        unique_reviews = []
        for r in reviews:
            if not r.content or len(r.content) < 10:
                unique_reviews.append(r)
                continue
            # Use first 50 chars as fingerprint to catch templates
            fp = r.content.lower().strip()[:50]
            if fp not in seen_fingerprints:
                seen_fingerprints.add(fp)
                unique_reviews.append(r)
        return unique_reviews

    async def summarize(self, reviews: List[Review], competitive_benchmark: int = 500, est_monthly_sales: int = 0) -> ReviewSummary:
        if not reviews:
            raise ValueError("No reviews provided for summarization.")

        # 1. Calculate base metrics and risk before deduplication (reflects reality)
        stats = self._calculate_metrics(reviews, benchmark=competitive_benchmark)
        risk = self._analyze_manipulation_risk(reviews, est_monthly_sales=est_monthly_sales)

        # 2. Step 1: Deduplicate for LLM analysis (reflects unique info)
        unique_reviews = self._deduplicate_reviews(reviews)

        # 3. Step 2: Adaptive Quota Sampling (Total Budget: 30)
        # Sort by quality: Verified + Helpful
        sorted_reviews = sorted(
            unique_reviews, 
            key=lambda x: (x.is_verified, x.helpful_votes or 0), 
            reverse=True
        )

        pos_pool = [r for r in sorted_reviews if r.rating and r.rating >= 4]
        neg_pool = [r for r in sorted_reviews if r.rating and r.rating <= 2]
        neu_pool = [r for r in sorted_reviews if r.rating == 3]

        # --- Budget Management (Fixed 30) ---
        total_budget = 30
        
        # A. Allocate Neutral first (Limited slice of the budget)
        neu_count = min(3, len(neu_pool))
        remaining_budget = total_budget - neu_count
        
        # B. Allocate Negative with Floor (Signal guarantee)
        neg_floor = 8 
        actual_neg_ratio = len(neg_pool) / len(unique_reviews) if unique_reviews else 0
        neg_count = max(neg_floor, int(remaining_budget * actual_neg_ratio))
        neg_count = min(neg_count, len(neg_pool)) # Cap by actual availability
        
        # C. Allocate Positive (The "Fallback" bucket)
        # If neg_pool is small, positive reviews will fill the remaining space to maximize info density.
        pos_count = remaining_budget - neg_count
        pos_count = min(pos_count, len(pos_pool))
        
        selected_reviews = neg_pool[:neg_count] + pos_pool[:pos_count] + neu_pool[:neu_count]

        # 4. Step 3: Build review data with TRUNCATION (200 chars)
        review_data = f"--- QUANTITATIVE METRICS ---\n"
        review_data += f"Monthly Review Velocity: {stats['velocity']} reviews/month\n"
        review_data += f"Rating Distribution: {stats['distribution']}\n"
        review_data += f"MANIPULATION RISK: {risk['score']}/100 ({risk['verdict']})\n\n"
        
        review_data += "--- CURATED REVIEW SAMPLES (TRUNCATED) ---\n"
        for i, r in enumerate(selected_reviews):
            status = "Verified" if r.is_verified else "Non-Verified"
            helpful = f"{r.helpful_votes} helpful" if r.helpful_votes else "0 votes"
            # Truncate to 200 chars to save 60%+ tokens
            content_preview = (r.content[:200] + "...") if len(r.content or "") > 200 else (r.content or "")
            review_data += (
                f"R{i+1} [{r.rating}*|{status}|{helpful}]: {r.title} - {content_preview}\n"
            )

        # 5. LOAD PROMPT FROM MANAGER
        system_msg, user_prompt = prompt_manager.render(
            name="review_analysis",
            variables={"review_data": review_data}
        )

        logger.info(f"Summarizing {len(selected_reviews)} unique/truncated reviews...")
        
        # 6. Synthesis
        summary: ReviewSummary = await self.provider.generate_structured(
            prompt=user_prompt,
            schema=ReviewSummary,
            system_message=system_msg
        )
        
        # 7. Final response enrichment
        summary.review_velocity = stats['velocity']
        summary.rating_distribution = stats['distribution']
        summary.competitive_barrier_months = stats['barrier_months']
        summary.manipulation_risk = risk
        
        return summary
