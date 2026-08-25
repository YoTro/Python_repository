from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import src.workflows.definitions  # noqa: F401 — registers all workflows
from src.core.models.product import Product
from src.core.models.review import Review, ReviewSummary
from src.workflows.engine import WorkflowContext
from src.workflows.registry import WorkflowRegistry

_MOCK_REVIEW_SUMMARY = ReviewSummary(
    pros=["Durable", "Non-slip"],
    cons=["Thin"],
    sentiment_score=0.72,
    top_complaints=["Slips on hardwood"],
    buyer_persona="Yoga enthusiasts aged 25-40",
    review_velocity=45.3,
    rating_breakdown={1: 2, 2: 3, 3: 10, 4: 30, 5: 55},
    competitive_barrier_months=8.2,
    manipulation_risk={"score": 12.0, "verdict": "SAFE", "metrics": {}},
)

_MOCK_REVIEWS = [
    Review(asin="B001", rating=5, content="Great mat!", is_verified=True),
    Review(asin="B001", rating=3, content="A bit thin.", is_verified=True),
]


@pytest.fixture
def mock_product_data():
    return Product(
        asin="B001",
        title="Super Yoga Mat",
        brand="SuperBrand",
        features=["Soft", "Non-slip", "72x24 inch", "Rubber material", "1 year warranty"],
        is_fba=True,
        has_a_plus_content=True,
        rating=4.5,
        review_count=100,
    )


@pytest.fixture
def mock_competitor_data():
    return [
        Product(asin="B002", title="Competitor Mat 1", rating=4.2, review_count=50, brand="Comp1"),
        Product(asin="B003", title="Competitor Mat 2", rating=4.8, review_count=500, brand="Comp2"),
    ]


@pytest.mark.asyncio
async def test_listing_diagnosis_workflow(mock_product_data, mock_competitor_data):
    with (
        patch(
            "src.mcp.servers.amazon.extractors.product_details.ProductDetailsExtractor.get_product_details",
            return_value=mock_product_data,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.product_details.ProductDetailsExtractor.enrich_product",
            side_effect=lambda p: p,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.search.SearchExtractor.search",
            return_value=mock_competitor_data,
        ),
        patch(
            "src.mcp.servers.amazon.extractors.images.ImageExtractor.get_product_images",
            return_value={"Images": ["http://img.com"]},
        ),
        patch(
            "src.mcp.servers.amazon.extractors.videos.VideoExtractor.has_videos",
            return_value={"HasVideos": True, "VideoCount": 1},
        ),
        patch(
            "src.mcp.servers.amazon.extractors.comments.CommentsExtractor.get_all_comments",
            new_callable=AsyncMock,
            return_value=_MOCK_REVIEWS,
        ),
        patch(
            "src.intelligence.processors.review_summarizer.ReviewSummarizer.summarize",
            new_callable=AsyncMock,
            return_value=_MOCK_REVIEW_SUMMARY,
        ),
    ):
        mock_cloud = MagicMock()
        mock_router = MagicMock()
        mock_router.cloud = mock_cloud
        mock_router.route_and_execute = AsyncMock(return_value="LLM Diagnosis Result")

        ctx = WorkflowContext(job_id="test_diag", router=mock_router)
        params = {"asin": "B001"}

        workflow = WorkflowRegistry.build("listing_diagnosis")
        result = await workflow.execute(job_id="test_diag", params=params, ctx=ctx)

        assert result.completed is True
        assert len(result.final_items) == 1

        final_report = result.final_items[0]["final_report"]
        assert final_report["asin"] == "B001"
        assert "overall_summary" in final_report
        assert "module_performance" in final_report
        assert "comparative_analysis" in final_report
        assert final_report["qualitative_diagnosis"] == "LLM Diagnosis Result"
        assert final_report["overall_summary"]["competitor_avg_score"] > 0
        assert len(final_report["comparative_analysis"]["competitors"]) == 2

        # Review intelligence fields
        ri = final_report["review_intelligence"]
        assert ri["sentiment_score"] == pytest.approx(0.72)
        assert ri["review_velocity"] == pytest.approx(45.3)
        assert ri["competitive_barrier_months"] == pytest.approx(8.2)
        assert ri["manipulation_risk"]["verdict"] == "SAFE"
        assert "Durable" in ri["pros"]
        assert ri["buyer_persona"] == "Yoga enthusiasts aged 25-40"


# ===========================================================================
# Content-validation branch (own-store, gated behind enable_validate_content)
# ===========================================================================

import asyncio

from src.workflows.definitions import listing_diagnosis as ld

_ERP_MODULE = "src.mcp.servers.erp.registry.get_erp_client"
_SP_MODULE = "src.mcp.servers.amazon.sp_api.client.SPAPIClient"


def _erp_record(**over):
    """A realistic Lingxing product record whose facts span every deterministic check."""
    rec = {
        "model": "XR-200",
        "brand_name": "Acme",
        "cg_product_material": "Silicone",
        "cg_package_length": "10",
        "cg_package_width": "5",
        "cg_package_height": "3",
        "cg_package_spec_unit": "cm",
        "cg_product_gross_weight": "100",
        "cg_product_gross_weight_unit": "g",
        "custom_fields": {"1": {"name": "功能特点", "val_text": "防水设计"}},
    }
    rec.update(over)
    return rec


def _sp_listing(asin="B001", attributes=None, issues=None, suppressed=False):
    attrs = (
        attributes
        if attributes is not None
        else {
            "item_name": [{"value": "Acme Widget Model: XR-200 Silicone 100 g"}],
            "bullet_point": [{"value": "Size 10 x 5 x 3 cm"}],
            "product_description": [{"value": "Waterproof design, silicone build"}],
            "brand": [{"value": "Acme"}],
        }
    )
    return {
        "sku": "AMZ-1",
        "summaries": [{"asin": asin, "itemName": "Acme Widget"}],
        "attributes": attrs,
        "issues": issues or [],
        "suppressed": suppressed,
        "enforcement_actions": [],
    }


class _FakeERP:
    """ERP client whose search_product returns records from a value→record mapping."""

    def __init__(self, mapping=None, record=None, raises=None):
        # `mapping` keys are search values; `record` is a single-record shortcut.
        self.mapping = mapping or ({} if record is None else {"__any__": record})
        self.raises = raises
        self.calls = []

    def search_product(self, value, search_field="sku", fetch_all=True):
        self.calls.append((value, search_field))
        if self.raises:
            raise self.raises
        rec = self.mapping.get(value, self.mapping.get("__any__"))
        return [rec] if rec else []


def _sp_class(listing_for=None, single=None, capture=None, raises=None):
    """Build a fake SPAPIClient class. `listing_for` maps seller SKU→listing dict."""

    class _FakeSP:
        def __init__(self, store_id=None):
            self.store_id = store_id

        async def get_listings_item(self, sku, **kw):
            if capture is not None:
                capture.append((sku, self.store_id))
            if raises:
                raise raises
            if listing_for is not None:
                return listing_for.get(sku)
            return single

    return _FakeSP


def _router(route_return=None, route_side_effect=None, has_cloud=True):
    router = MagicMock()
    router.cloud = MagicMock() if has_cloud else None
    router.route_and_execute = AsyncMock(return_value=route_return, side_effect=route_side_effect)
    return router


def _run_validate(items, cfg, *, erp_client, sp_cls, router):
    ctx = WorkflowContext(job_id="t", config=cfg, router=router)
    with (
        patch(_ERP_MODULE, return_value=erp_client),
        patch(_SP_MODULE, sp_cls),
    ):
        return asyncio.run(ld._validate_content(items, ctx))


_CFG_ON = {"enable_validate_content": True, "sku": "L-1", "msku": "AMZ-1", "store_id": "US"}


# --- 1. SP-API success with complete content --------------------------------
def test_sp_api_success_complete_content():
    router = _router(route_return={"findings": [], "covered_labels": ["功能特点"]})
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=_sp_listing(asin="B001")),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert cv["status"] == "ok"
    assert cv["content_source"] == "sp_api"
    assert cv["llm_status"] == "ok"
    assert cv["partial"] is False
    assert cv["coverage_score"] == 100  # 5 deterministic + 1 LLM-covered CJK fact
    assert cv["deterministic_finding_count"] == 0
    assert cv["missing"] == [] and cv["conflicts"] == []


# --- 2. SP-API empty/partial response falls back to the scraped PDP ----------
def test_sp_api_empty_falls_back_to_pdp():
    router = _router(route_return={"findings": [], "covered_labels": ["功能特点"]})
    item = {
        "asin": "B001",
        "product_data": {
            "title": "Acme Widget Model: XR-200",
            "features": ["100 g", "10 x 5 x 3 cm", "Silicone", "防水设计"],
            "description": "",
        },
    }
    # SP returns a listing object but with no usable content (empty attributes and
    # summaries without an itemName) — a thin/partial response.
    thin = {
        "sku": "AMZ-1",
        "summaries": [{"asin": "B001"}],
        "attributes": {},
        "issues": [],
        "suppressed": False,
        "enforcement_actions": [],
    }
    items = _run_validate(
        [item],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=thin),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert cv["status"] == "ok"
    assert cv["content_source"] == "scraped_pdp"  # thin SP did not suppress the PDP


# --- 3. SP-API ASIN mismatch → discard SP data, fall back to PDP ------------
def test_sp_api_asin_mismatch_discarded():
    router = _router(route_return={"findings": [], "covered_labels": ["功能特点"]})
    item = {
        "asin": "B001",
        "product_data": {"title": "Acme Widget", "features": ["Silicone"], "description": ""},
    }
    # SP returns a *different* ASIN than the target — the SKU mapping is wrong.
    wrong = _sp_listing(asin="B999", issues=[{"code": "X", "message": "m", "severity": "ERROR"}])
    items = _run_validate(
        [item],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=wrong),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert cv["content_source"] == "scraped_pdp"
    assert cv["suppressed"] is None  # SP listing was discarded
    assert cv["listing_issues"] == []  # its issues must not leak in


# --- 4. ERP lookup failure → status no_erp ----------------------------------
def test_erp_lookup_failure():
    router = _router(route_return={"findings": [], "covered_labels": []})
    # (a) search_product raises
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(raises=RuntimeError("ERP down")),
        sp_cls=_sp_class(single=None),
        router=router,
    )
    assert items[0]["content_validation"]["status"] == "no_erp"
    # (b) search_product returns nothing
    items2 = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(mapping={}),
        sp_cls=_sp_class(single=None),
        router=router,
    )
    cv = items2[0]["content_validation"]
    assert cv["status"] == "no_erp"
    assert "no ERP master" in cv["reason"]


# --- 5. LLM timeout or malformed output → deterministic-only partial --------
def test_llm_timeout_is_partial_not_zero():
    router = _router(route_side_effect=TimeoutError())
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=_sp_listing(asin="B001")),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert cv["llm_status"] == "llm_failed"
    assert cv["partial"] is True
    # 5 of 6 facts covered deterministically; failure is a lower bound, never 0/None.
    assert cv["coverage_score"] == 83


def test_llm_malformed_output_is_partial():
    bad = MagicMock()
    bad.text = "{ this is not valid json "
    router = _router(route_return=bad)
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=_sp_listing(asin="B001")),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert cv["llm_status"] == "llm_failed"
    assert cv["partial"] is True


# --- 6. Multiple items with different SKU mappings ---------------------------
def test_multiple_items_distinct_sku_mappings():
    erp = _FakeERP(mapping={"L1": _erp_record(model="M1"), "L2": _erp_record(model="M2")})
    capture = []
    sp_cls = _sp_class(
        listing_for={"AMZ-1": _sp_listing(asin="B001"), "AMZ-2": _sp_listing(asin="B002")},
        capture=capture,
    )
    router = _router(route_return={"findings": [], "covered_labels": ["功能特点"]})
    items = [
        {"asin": "B001", "sku": "L1", "msku": "AMZ-1"},
        {"asin": "B002", "sku": "L2", "msku": "AMZ-2"},
    ]
    out = _run_validate(
        items, {"enable_validate_content": True}, erp_client=erp, sp_cls=sp_cls, router=router
    )
    assert {v for v, _ in erp.calls} == {"L1", "L2"}
    assert {sku for sku, _ in capture} == {"AMZ-1", "AMZ-2"}
    assert all(o["content_validation"]["status"] == "ok" for o in out)


# --- 7. Local SKU vs Amazon SKU (ERP by local, SP by msku) ------------------
def test_local_sku_vs_amazon_sku():
    erp = _FakeERP(mapping={"LOCAL-1": _erp_record()})
    capture = []
    sp_cls = _sp_class(single=_sp_listing(asin="B001"), capture=capture)
    router = _router(route_return={"findings": [], "covered_labels": ["功能特点"]})
    cfg = {"enable_validate_content": True, "sku": "LOCAL-1", "msku": "AMZ-9", "store_id": "US"}
    _run_validate([{"asin": "B001"}], cfg, erp_client=erp, sp_cls=sp_cls, router=router)
    assert erp.calls == [("LOCAL-1", "sku")]  # ERP searched by the local SKU
    assert capture == [("AMZ-9", "US")]  # SP-API queried by the Amazon seller SKU


def test_resolve_seller_sku_never_reuses_local():
    # explicit msku wins
    assert ld._resolve_seller_sku("AMZ-1", "LOCAL", "sku", None) == "AMZ-1"
    # searching on the msku dimension: value IS the seller SKU
    assert ld._resolve_seller_sku(None, "AMZ-2", "msku", None) == "AMZ-2"
    # local SKU / other fields never become a seller SKU (safe: None → PDP fallback)
    assert ld._resolve_seller_sku(None, "LOCAL", "sku", None) is None
    assert ld._resolve_seller_sku(None, "Widget", "product_name", None) is None
    # last-resort read from the matched ERP record
    assert ld._resolve_seller_sku(None, "LOCAL", "sku", {"msku": "AMZ-3"}) == "AMZ-3"


# --- 8. Duplicate and unknown covered labels ---------------------------------
def test_duplicate_and_unknown_covered_labels():
    # LLM returns duplicates + labels that are unknown or already deterministically
    # handled — none may inflate coverage beyond the real fact count.
    router = _router(
        route_return={
            "findings": [],
            "covered_labels": ["功能特点", "功能特点", "Nonexistent", "Model", "Brand"],
        }
    )
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=_sp_listing(asin="B001")),
        router=router,
    )
    cv = items[0]["content_validation"]
    assert (
        cv["coverage_score"] == 100
    )  # dedup'd 功能特点 credited once; junk ignored, capped at 100

    # And prove unknown-only labels grant no credit (leaves the CJK fact uncovered).
    router2 = _router(route_return={"findings": [], "covered_labels": ["JunkA", "JunkB"]})
    items2 = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(record=_erp_record()),
        sp_cls=_sp_class(single=_sp_listing(asin="B001")),
        router=router2,
    )
    assert items2[0]["content_validation"]["coverage_score"] == 83  # only the 5 deterministic


# --- 9. Unit-equivalent values (100 g == 0.1 kg) ----------------------------
def test_unit_equivalent_values_no_conflict():
    facts = [{"label": "Gross Weight", "value": "100 g"}]
    content = {"title": "", "bullets": [], "description": "Net weight 0.1 kg", "keywords": ""}
    findings, covered, handled = ld._deterministic_content_checks(facts, content)
    assert "Gross Weight" in covered
    assert not any(f["kind"] == "conflict" for f in findings)


# --- 10. Conflicting dimensions ---------------------------------------------
def test_conflicting_dimensions():
    facts = [{"label": "Package Dimensions", "value": "10 × 5 × 3 cm"}]
    content = {
        "title": "",
        "bullets": ["Item size 10 x 5 x 4 cm"],
        "description": "",
        "keywords": "",
    }
    findings, covered, _ = ld._deterministic_content_checks(facts, content)
    assert "Package Dimensions" not in covered
    assert findings and findings[0]["kind"] == "conflict"


# --- 11. Markdown-special characters are escaped in tables -------------------
def test_markdown_special_characters_escaped():
    cv = {
        "status": "ok",
        "llm_status": "ok",
        "partial": False,
        "content_source": "sp_api",
        "erp_fact_count": 1,
        "coverage_score": 50,
        "deterministic_finding_count": 1,
        "missing": [
            {
                "erp_label": "Spec|Pipe",
                "erp_value": "A|B\nC",
                "detail": "use `#1`",
                "confidence": 90,
            }
        ],
        "conflicts": [],
        "unverifiable": [],
        "listing_issues": [],
        "suppressed": None,
        "enforcement_actions": [],
    }
    md = ld._render_content_validation_md(cv)
    assert "\\|" in md  # pipes escaped
    # No row is broken by a raw newline: every table line has a consistent pipe count.
    row = next(ln for ln in md.splitlines() if "A\\|B" in ln)
    assert "\n" not in row and "C" in row  # newline collapsed into the same cell


# --- 12. Validation disabled vs validation unavailable ----------------------
def test_disabled_vs_unavailable_are_distinct():
    # Disabled: flag off → items untouched, no content_validation set.
    ctx = WorkflowContext(job_id="t", config={"asin": "B001"}, router=_router())
    out = asyncio.run(ld._validate_content([{"asin": "B001"}], ctx))
    assert "content_validation" not in out[0]

    # Unavailable: enabled but ERP master missing → explicit no_erp state.
    items = _run_validate(
        [{"asin": "B001"}],
        dict(_CFG_ON),
        erp_client=_FakeERP(mapping={}),
        sp_cls=_sp_class(single=None),
        router=_router(),
    )
    unavailable = items[0]["content_validation"]
    assert unavailable["status"] == "no_erp"

    # The three states must read differently in the final prompt.
    disabled_txt = ld._format_content_validation(None)
    unavailable_txt = ld._format_content_validation(unavailable)
    ok_txt = ld._format_content_validation(
        {
            "status": "ok",
            "llm_status": "ok",
            "partial": False,
            "content_source": "sp_api",
            "erp_fact_count": 3,
            "coverage_score": 100,
            "deterministic_finding_count": 0,
            "missing": [],
            "conflicts": [],
            "unverifiable": [],
            "listing_issues": [],
        }
    )
    assert len({disabled_txt, unavailable_txt, ok_txt}) == 3
    assert "was not enabled" in disabled_txt
    assert "could not run" in unavailable_txt


if __name__ == "__main__":
    pytest.main([__file__])
