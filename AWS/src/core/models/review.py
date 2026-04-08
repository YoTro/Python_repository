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
    """
    pros: List[str] = Field(..., description="List of positive product traits mentioned by customers")
    cons: List[str] = Field(..., description="List of negative product traits or complaints")
    sentiment_score: float = Field(..., description="Overall sentiment score from -1.0 (negative) to 1.0 (positive)")
    top_complaints: List[str] = Field(..., description="Common pain points or recurring defects")
    buyer_persona: str = Field(..., description="Description of the typical customer based on review tone and content")
    
    # NEW: Quantitative Metrics
    review_velocity: float = Field(0.0, description="Estimated reviews added per month based on the provided sample")
    rating_distribution: dict[int, int] = Field(default_factory=dict, description="Distribution of star ratings (1-5)")
    competitive_barrier_months: Optional[float] = Field(None, description="Estimated months required to reach the competitive benchmark (e.g. 500 reviews) at current velocity")
    
    # Stage 2: Manipulation Risk
    manipulation_risk: Optional[dict] = Field(None, description="Integrity analysis result including RCI, similarity scores, and verdict")
