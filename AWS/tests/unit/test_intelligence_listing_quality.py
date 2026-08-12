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


def test_brand_keyword_suppression():
    """Third-party brand names must not appear in the improvement plan."""
    scorer = ListingQualityScorer()

    product = Product(
        asin="B001",
        title="AcmeCo Rodent Repellent Pouches for Car Engine",
        brand="AcmeCo",
        features=[
            "Natural peppermint oil repels mice and rodents effectively",
            "Safe for use in cars, trucks, and RVs without chemicals",
            "Easy to place under hood or in cabin for lasting protection",
            "Long-lasting formula stays effective for up to 30 days per pouch",
            "Satisfaction guarantee — replacement or full refund on any order",
        ],
        rating=4.4,
        review_count=80,
        is_fba=True,
        has_a_plus_content=True,
        images=["i.jpg"] * 7,
        videos=["v.mp4"],
    )
    competitors = [
        Product(asin="B002", title="Vamoose Rodent Repelling Pouches Natural Deterrent"),
        Product(asin="B003", title="MouseOut Car Rodent Repellent Sachets Peppermint"),
        Product(asin="B004", title="RodentShield Peppermint Mouse Repellent Pouches"),
    ]
    # keyword_config simulates Xiyouzhaoci returning branded + generic terms
    keyword_config = {
        "core": [
            "rodent repellent",  # generic — should be flagged if missing
            "vamoose",  # pure brand — must be suppressed
            "vamoose rodent repellent",  # brand phrase — must be suppressed
        ],
        "modifiers": ["peppermint car repellent", "vamoose rodent-repelling pouches"],
        "scenes": [],
    }

    result = scorer.score(product, keyword_config=keyword_config, competitors=competitors)
    plan = result["improvement_plan"]

    # Brand-only terms must not appear in any improvement plan item
    for item in plan:
        assert "vamoose" not in item.lower(), f"Brand 'vamoose' leaked into plan: {item!r}"

    # Generic terms that are absent from the listing should still be flagged
    # ("rodent repellent" is in title, so no flag expected here; verify no crash)
    assert isinstance(plan, list)


def test_build_token_freq_map():
    scorer = ListingQualityScorer()
    product = Product(
        asin="B001",
        title="AcmeCo Rodent Repellent Pouches",
        features=["Peppermint oil repels mice"],
    )
    competitors = [
        Product(asin="B002", title="MouseOut Rodent Repellent Sachets"),
        Product(asin="B003", title="RodentShield Peppermint Repellent Pouches"),
    ]
    freq = scorer._build_token_freq_map(product, competitors)

    # "repellent" appears in main product + both competitors → freq 3
    assert freq["repellent"] >= 3
    # "vamoose" appears in none of the sources → freq 0
    assert freq.get("vamoose", 0) == 0
    # brand-unique tokens score low
    assert freq.get("mouseout", 0) == 1


def test_is_generic_keyword():
    scorer = ListingQualityScorer()
    from collections import Counter

    freq = Counter({"rodent": 3, "repellent": 3, "peppermint": 2, "vamoose": 1, "pouches": 3})

    assert scorer._is_generic_keyword("rodent repellent", freq, min_sources=2) is True
    assert scorer._is_generic_keyword("peppermint pouches", freq, min_sources=2) is True
    assert scorer._is_generic_keyword("vamoose", freq, min_sources=2) is False
    assert scorer._is_generic_keyword("vamoose rodent repellent", freq, min_sources=2) is False
    # stop-word-only / empty → generic by default
    assert scorer._is_generic_keyword("for the", freq, min_sources=2) is True
