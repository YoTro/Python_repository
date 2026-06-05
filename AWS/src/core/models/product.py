from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional

class Product(BaseModel):
    """
    Standard Amazon Product model for all internal analysis and LLM orchestration.
    Each field includes descriptions to guide LLMs during tool selection and reasoning.
    """
    asin: str = Field(..., description="Amazon Standard Identification Number (Unique ID)")
    title: Optional[str] = Field(None, description="The full product title")
    features: List[str] = Field(default_factory=list, description="A list of bullet points highlighting key features")
    description: Optional[str] = Field(None, description="Long-form product description")
    price: Optional[float] = Field(None, description="Current listing price in local currency")
    sales_rank: Optional[int] = Field(None, description="Best Sellers Rank (BSR) in its primary category")
    review_count: Optional[int] = Field(None, description="Total number of reviews received")
    rating: Optional[float] = Field(None, description="Average customer rating (out of 5.0)")
    main_image_url: Optional[str] = Field(None, description="URL of the primary product image")
    category_name: Optional[str] = Field(None, description="Primary category name")
    category_node_id: Optional[str] = Field(None, description="Amazon Browse Node ID (for BSR calibration)")
    brand: Optional[str] = Field(None, description="Brand or manufacturer name")
    past_month_sales: Optional[int] = Field(None, description="Estimated unit sales in the last 30 days")
    stock_level: Optional[int] = Field(None, description="Current available stock in cart (if accessible)")
    is_fba: Optional[bool] = Field(None, description="True if the product is Fulfilled by Amazon")
    has_a_plus_content: Optional[bool] = Field(None, description="True if the product has A+ (Enhanced Brand Content)")
    rating_breakdown: Optional[dict[int, float]] = Field(None, description="Percentage of 1-5 star ratings")
    vp_review_ratio: Optional[float] = Field(None, description="Ratio of Verified Purchase reviews (0.0 - 1.0)")
    recent_rating_avg: Optional[float] = Field(None, description="Average rating in the last 30 days")
    media_review_ratio: Optional[float] = Field(None, description="Ratio of reviews with photos/videos (0.0 - 1.0)")
    sentiment_score: Optional[float] = Field(None, description="Quantified sentiment score (-1.0 to 1.0)")
    images: List[str] = Field(default_factory=list, description="List of image URLs")
    videos: List[str] = Field(default_factory=list, description="List of video URLs")
    aplus_images: List[str] = Field(default_factory=list, description="A+ content image URLs extracted from the product page")

    class Config:
        from_attributes = True
