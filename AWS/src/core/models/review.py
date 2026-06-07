from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, List

class Review(BaseModel):
    """
    Standard Product Review model. Used by LLMs to perform sentiment analysis and pros/cons extraction.
    """
    asin: str = Field(..., description="ASIN associated with this review")
    author: Optional[str] = Field(None, description="Name of the reviewer")
    rating: Optional[int] = Field(None, description="Numerical rating (1-5 stars)")
    title: Optional[str] = Field(None, description="Title of the review")
    content: Optional[str] = Field(None, description="The body text of the review")
    date: Optional[str] = Field(None, description="Review date string (e.g., 'October 12, 2023')")
    is_verified: Optional[bool] = Field(None, description="True if the review is from a verified purchase")
    helpful_votes: Optional[int] = Field(0, description="Total number of 'Helpful' votes received")
    image_urls: List[str] = Field(default_factory=list, description="Links to images attached to the review")

class ReviewSummary(BaseModel):
    """
    Structured summary of reviews processed by an LLM processor.
    Owns all review-derived analytics; Product retains only page-scraped review_count and rating.
    """
    pros: List[str] = Field(..., description="List of positive product traits mentioned by customers")
    cons: List[str] = Field(..., description="List of negative product traits or complaints")
    sentiment_score: Optional[float] = Field(None, description="Overall sentiment score from -1.0 (negative) to 1.0 (positive)")
    top_complaints: List[str] = Field(..., description="Common pain points or recurring defects")
    buyer_persona: str = Field(..., description="Description of the typical customer based on review tone and content")

    # Quantitative metrics enriched post-LLM
    review_velocity: float = Field(0.0, description="Estimated reviews added per month based on the provided sample")
    rating_breakdown: dict = Field(default_factory=dict, description="Distribution of star ratings {1..5: percentage}")
    competitive_barrier_months: Optional[float] = Field(None, description="Estimated months required to reach the competitive benchmark (e.g. 500 reviews) at current velocity")

    # Review quality signals (migrated from Product)
    vp_review_ratio: Optional[float] = Field(None, description="Ratio of Verified Purchase reviews (0.0 - 1.0)")
    recent_rating_avg: Optional[float] = Field(None, description="Average rating in the last 30 days")
    media_review_ratio: Optional[float] = Field(None, description="Ratio of reviews with photos/videos (0.0 - 1.0)")

    # Manipulation Risk
    manipulation_risk: Optional[dict] = Field(None, description="Integrity analysis result including RCI, similarity scores, and verdict")
