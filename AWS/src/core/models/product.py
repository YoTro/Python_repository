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
    past_month_sales: Optional[int] = Field(None, description="Estimated unit sales in the last 30 days")
    stock_level: Optional[int] = Field(None, description="Current available stock in cart (if accessible)")
    is_fba: Optional[bool] = Field(None, description="True if the product is Fulfilled by Amazon")

    class Config:
        from_attributes = True
