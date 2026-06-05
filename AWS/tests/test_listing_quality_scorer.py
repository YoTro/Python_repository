from src.core.models.product import Product
from src.intelligence.processors.listing_quality_scorer import ListingQualityScorer


def test_listing_quality_scorer_robustness():
    scorer = ListingQualityScorer()

    # Create a product missing critical info
    product = Product(
        asin="B012345678",
        title="Yoga Mat",
        brand="SuperBrand",
        features=["Blue color", "Soft texture"],
        is_fba=False,
    )

    result = scorer.score(product)

    # Verify it penalizes missing brand in title
    assert result["overall_quality_score"] < 100
    assert any("Title Pillar Missing: Brand" in issue for issue in result["improvement_plan"])

    # Verify it penalizes feature richness (e.g., count or length)
    assert any(
        "bullet points" in issue.lower() or "too short" in issue.lower()
        for issue in result["improvement_plan"]
    )


def test_excellent_product():
    scorer = ListingQualityScorer()

    # Create a robust product that meets new criteria
    product = Product(
        asin="B087654321",
        title="SuperBrand Premium Yoga Mat for Home Workout, Non-Slip Eco-Friendly Rubber, 72x24 inch x 6mm Thick",
        brand="SuperBrand",
        features=[
            "Easy to use yoga mat with non-slip surface",
            "High density material for joint protection",
            "Perfect solution for yoga and pilates",
            "Durable rubber material with premium texture",
            "Lifetime warranty and satisfaction guarantee",
        ],
        is_fba=True,
        has_a_plus_content=True,
        images=["img1.jpg"] * 7,
        main_image_url="img1.jpg",
        videos=["vid1.mp4"],
        rating=4.5,
        review_count=50,
    )

    img_meta = {
        "img1.jpg": {
            "is_pure_white_bg": True,
            "has_text_or_watermark": False,
            "width": 2000,
            "height": 2000,
        }
    }

    result = scorer.score(product, image_metadata=img_meta)

    # Should be high score
    assert result["overall_quality_score"] >= 80
    assert result["status"] in ["Good", "Excellent"]
