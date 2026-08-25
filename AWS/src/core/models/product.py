from __future__ import annotations
from pydantic import BaseModel, ConfigDict, Field, computed_field
from typing import List, Optional


class Product(BaseModel):
    """
    Standard Amazon Product model for all internal analysis and LLM orchestration.
    Each field includes descriptions to guide LLMs during tool selection and reasoning.
    """

    model_config = ConfigDict(from_attributes=True)

    asin: str = Field(..., description="Amazon Standard Identification Number (Unique ID)")
    title: Optional[str] = Field(
        None,
        description=(
            "The primary product title (span#productTitle). Amazon now caps this at "
            "~75 chars including spaces; overflow moves to title_highlights."
        ),
    )
    title_highlights: Optional[str] = Field(
        None,
        description=(
            "Title differentiators / 'item highlights' shown beneath the title "
            "(div.dp-title-differentiators) — up to ~125 additional chars of product "
            "detail. Empty on older listings that still keep everything in the title."
        ),
    )
    features: List[str] = Field(
        default_factory=list, description="A list of bullet points highlighting key features"
    )
    description: Optional[str] = Field(None, description="Long-form product description")
    price: Optional[float] = Field(None, description="Current listing price in local currency")
    sales_rank: Optional[int] = Field(
        None, description="Best Sellers Rank (BSR) in its primary category"
    )
    review_count: Optional[int] = Field(None, description="Total number of reviews received")
    rating: Optional[float] = Field(None, description="Average customer rating (out of 5.0)")
    main_image_url: Optional[str] = Field(None, description="URL of the primary product image")
    category_name: Optional[str] = Field(None, description="Primary category name")
    category_node_id: Optional[str] = Field(
        None, description="Amazon Browse Node ID (for BSR calibration)"
    )
    brand: Optional[str] = Field(None, description="Brand or manufacturer name")
    past_month_sales: Optional[int] = Field(
        None, description="Estimated unit sales in the last 30 days"
    )
    stock_level: Optional[int] = Field(
        None, description="Current available stock in cart (if accessible)"
    )
    is_fba: Optional[bool] = Field(None, description="True if the product is Fulfilled by Amazon")
    sold_by: Optional[str] = Field(
        None,
        description="Seller name (e.g. 'Amazon.com' for first-party, or 3rd-party seller name)",
    )
    has_a_plus_content: Optional[bool] = Field(
        None, description="True if the product has A+ (Enhanced Brand Content)"
    )
    images: List[str] = Field(default_factory=list, description="List of image URLs")
    images_metadata: dict = Field(
        default_factory=dict,
        description="Map of image URL to {width, height} scraped from data-a-dynamic-image",
    )
    videos: List[str] = Field(default_factory=list, description="List of video URLs")
    aplus_images: List[str] = Field(
        default_factory=list, description="A+ content image URLs extracted from the product page"
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def full_title(self) -> Optional[str]:
        """Primary title plus item-highlight differentiators, as the buyer effectively reads it.

        Joined with ' | ' to match the canonical format Amazon's own profitability API
        returns for `title` (e.g. "Main Title | highlight details"), so full titles are
        consistent across scraped pages and API sources. Use this (rather than `title`
        alone) for keyword extraction and listing-quality analysis, since Amazon may
        relocate up to ~125 chars of the title into the differentiators block. Falls back
        to `title` when no highlights are present.
        """
        parts = [p.strip() for p in (self.title, self.title_highlights) if p and p.strip()]
        return " | ".join(parts) if parts else self.title
