from __future__ import annotations

import asyncio
import functools
import json
import logging
from datetime import datetime
from pathlib import Path

from src.core.models.product import Product
from src.intelligence.processors.listing_quality_scorer import ListingQualityScorer
from src.intelligence.processors.review_summarizer import ReviewSummarizer
from src.intelligence.prompts.manager import prompt_manager
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.images import ImageExtractor
from src.mcp.servers.amazon.extractors.product_details import ProductDetailsExtractor
from src.mcp.servers.amazon.extractors.search import SearchExtractor
from src.mcp.servers.amazon.extractors.videos import VideoExtractor
from src.workflows.engine import Workflow, WorkflowContext
from src.workflows.registry import WorkflowRegistry
from src.workflows.steps.base import ComputeTarget
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep

logger = logging.getLogger(__name__)

# ── Lazy Singletons ────────────────────────────────────────────────────────
# Instantiated on first use, not at import time.


@functools.lru_cache(maxsize=1)
def _details_extractor() -> ProductDetailsExtractor:
    return ProductDetailsExtractor()


@functools.lru_cache(maxsize=1)
def _image_extractor() -> ImageExtractor:
    return ImageExtractor()


@functools.lru_cache(maxsize=1)
def _video_extractor() -> VideoExtractor:
    return VideoExtractor()


@functools.lru_cache(maxsize=1)
def _search_extractor() -> SearchExtractor:
    return SearchExtractor()


@functools.lru_cache(maxsize=1)
def _scorer() -> ListingQualityScorer:
    return ListingQualityScorer()


@functools.lru_cache(maxsize=1)
def _comments_extractor() -> CommentsExtractor:
    return CommentsExtractor()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STOP_WORDS = frozenset(
    {
        "for",
        "with",
        "and",
        "or",
        "the",
        "a",
        "an",
        "to",
        "in",
        "of",
        "by",
        "on",
        "at",
        "from",
        "into",
        "your",
        "our",
        "new",
        "best",
    }
)

_COMPETITOR_FIELDS = (
    "asin",
    "title",
    "features",
    "rating",
    "review_count",
    "price",
    "has_a_plus_content",
    "is_fba",
    "past_month_sales",
)


def _extract_search_keywords(title: str, n: int = 6) -> str:
    """Return n content words from title, skipping stop words and likely brand prefix."""
    words = title.split()
    # Drop leading word if it looks like a standalone brand token (short, no lowercase mix)
    if len(words) > 4 and words[0][0].isupper() and len(words[0]) < 15:
        words = words[1:]
    content = [w for w in words if w.lower() not in _STOP_WORDS]
    return " ".join(content[:n])


def _slim_competitor(c: dict) -> dict:
    return {k: c.get(k) for k in _COMPETITOR_FIELDS}


def _format_review_summary(rs: dict | None) -> str:
    if not rs:
        return "No review data available."
    risk = rs.get("manipulation_risk") or {}
    barrier = rs.get("competitive_barrier_months")
    barrier_str = (
        f"~{barrier} months to reach 500-review benchmark" if barrier is not None else "N/A"
    )
    pros = "\n".join(f"  + {p}" for p in rs.get("pros", []))
    complaints = "\n".join(f"  - {c}" for c in rs.get("top_complaints", []))
    return (
        f"Sentiment Score: {rs.get('sentiment_score', 0):.2f}  "
        f"(−1.0 = very negative, +1.0 = very positive)\n"
        f"Review Velocity: {rs.get('review_velocity', 0):.1f} reviews/month\n"
        f"Competitive Barrier: {barrier_str}\n"
        f"Manipulation Risk: {risk.get('score', 0):.0f}/100 "
        f"— verdict: {risk.get('verdict', 'N/A')}\n"
        f"\nPros (customer-reported):\n{pros or '  (none identified)'}\n"
        f"\nTop Complaints:\n{complaints or '  (none identified)'}\n"
        f"\nBuyer Persona: {rs.get('buyer_persona', 'N/A')}"
    )


# ---------------------------------------------------------------------------
# Extractor functions
# ---------------------------------------------------------------------------


async def _fetch_product_details(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch deep-dive details for the ASIN including images and videos."""
    asin = item.get("asin")
    if not asin:
        logger.warning("No ASIN provided in item, skipping fetch.")
        return {}

    logger.info(f"Fetching deep-dive details for target ASIN: {asin}")

    results = await asyncio.gather(
        _details_extractor().get_product_details(asin),
        _image_extractor().get_product_images(asin),
        _video_extractor().has_videos(asin),
        return_exceptions=True,
    )
    product, images_data, videos_data = results

    if isinstance(product, Exception):
        logger.error(f"Failed to fetch product details for {asin}: {product}")
        return {}

    images_data = images_data if not isinstance(images_data, Exception) else {}
    videos_data = videos_data if not isinstance(videos_data, Exception) else {}

    product.images = (images_data or {}).get("Images", [])
    logger.info(f"Found {len(product.images)} images for {asin}")

    videos_info = videos_data or {}
    if videos_info.get("HasVideos"):
        count = videos_info.get("VideoCount", 0)
        logger.info(f"Found {count} videos for {asin}")
        product.videos = ["has_video_placeholder"] * count
    else:
        logger.info(f"No videos found for {asin}")

    return {"product_data": product.model_dump()}


async def _search_competitors(item: dict, ctx: WorkflowContext) -> dict:
    """Search for competitors using keywords from the main product."""
    product_data = item.get("product_data", {})
    title = product_data.get("title", "")
    if not title:
        logger.warning("No title available for competitor discovery.")
        return {"competitor_list": []}

    keywords = _extract_search_keywords(title)
    logger.info(f"Searching for competitors using keywords: '{keywords}'")

    results = await _search_extractor().search(keywords, page=1)

    main_asin = item.get("asin")
    competitors = [r.model_dump() for r in results if r.asin != main_asin][:5]

    logger.info(f"Discovered {len(competitors)} competitors for ASIN: {main_asin}")
    return {"competitor_list": competitors, "search_keywords": keywords}


async def _enrich_competitors(item: dict, ctx: WorkflowContext) -> dict:
    """Enrich detail data for all discovered competitors."""
    competitors = item.get("competitor_list", [])
    if not competitors:
        logger.info("No competitors found to enrich.")
        return {}

    logger.info(f"Enriching data for {len(competitors)} competitors...")

    async def _enrich_one(c: dict):
        asin = c.get("asin")
        p = Product(**c)

        enriched_p, images_data, videos_data = await asyncio.gather(
            _details_extractor().enrich_product(p),
            _image_extractor().get_product_images(asin),
            _video_extractor().has_videos(asin),
            return_exceptions=True,
        )

        if isinstance(enriched_p, Exception):
            logger.warning(f"Failed to enrich competitor {asin}: {enriched_p}")
            return p.model_dump()

        images_data = images_data if not isinstance(images_data, Exception) else {}
        videos_data = videos_data if not isinstance(videos_data, Exception) else {}

        enriched_p.images = (images_data or {}).get("Images", [])
        videos_info = videos_data or {}
        if videos_info.get("HasVideos"):
            enriched_p.videos = ["has_video_placeholder"] * videos_info.get("VideoCount", 0)

        return enriched_p.model_dump()

    tasks = [asyncio.create_task(_enrich_one(c)) for c in competitors]
    done, pending = await asyncio.wait(tasks, timeout=30.0)

    if pending:
        logger.warning(
            f"Competitor enrichment timed out after 30s; {len(pending)}/{len(competitors)} "
            "tasks still pending and will be cancelled."
        )
        for t in pending:
            t.cancel()

    results = []
    for t in done:
        try:
            results.append(t.result())
        except Exception as e:
            logger.error(f"Task result error: {e}")
            # _enrich_one handles most errors, but we catch top-level task failure just in case.
            results.append(e)

    enriched_competitors = [r for r in results if not isinstance(r, Exception)]
    logger.info(
        f"Competitor enrichment complete: {len(enriched_competitors)}/{len(competitors)} succeeded."
    )
    return {"competitor_data": enriched_competitors}


async def _fetch_and_summarize_reviews(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch up to 3 pages of reviews and produce a ReviewSummary via the cloud provider."""
    asin = item.get("asin")
    if not asin:
        return {"review_summary": None}

    provider = getattr(ctx.router, "cloud", None) if ctx.router else None
    if not provider:
        logger.warning("No cloud provider on ctx.router; skipping review summarization.")
        return {"review_summary": None}

    logger.info(f"Fetching reviews for {asin} (max 3 pages)...")
    try:
        reviews = await _comments_extractor().get_all_comments(asin, max_pages=3)
    except Exception as e:
        logger.warning(f"Review fetch failed for {asin}: {e}")
        return {"review_summary": None}

    if not reviews:
        logger.info(f"No reviews found for {asin}.")
        return {"review_summary": None}

    logger.info(f"Summarizing {len(reviews)} reviews for {asin}...")
    try:
        summary = await ReviewSummarizer(provider=provider).summarize(reviews)
        return {"review_summary": summary.model_dump()}
    except Exception as e:
        logger.warning(f"Review summarization failed for {asin}: {e}")
        return {"review_summary": None}


# ---------------------------------------------------------------------------
# Processing functions
# ---------------------------------------------------------------------------


def _run_scoring(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Run ListingQualityScorer on main product and competitors."""
    scorer = _scorer()
    for item in items:
        if "product_data" in item:
            p = Product(**item["product_data"])
            res = scorer.score(p)
            item["main_score"] = res
            logger.info(
                f"Target ASIN {p.asin} quality score: {res.get('overall_quality_score')} ({res.get('status')})"
            )

        if "competitor_data" in item:
            comp_data = item["competitor_data"]
            scores = [scorer.score(Product(**c)) for c in comp_data]
            item["competitor_scores"] = scores
            avg = (
                sum(s.get("overall_quality_score", 0) for s in scores) / len(scores)
                if scores
                else 0
            )
            logger.info(f"Average competitor score: {avg:.1f} (based on {len(scores)} products)")

    return items


def _prepare_llm_prompt(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Render the listing_diagnosis prompt spec for each item."""
    logger.info("Preparing LLM prompts for qualitative analysis...")
    for item in items:
        p_data = item.get("product_data", {})
        c_data = item.get("competitor_data", [])

        variables = {
            "asin": item.get("asin"),
            "title": p_data.get("title", "N/A"),
            "description": p_data.get("description", ""),
            "features": "\n".join([f"- {f}" for f in p_data.get("features", [])]),
            "competitor_data": json.dumps(
                [_slim_competitor(c) for c in c_data],
                indent=2,
                ensure_ascii=False,
            ),
            "review_summary": _format_review_summary(item.get("review_summary")),
        }

        rendered = prompt_manager.render_spec("listing_diagnosis", variables)
        item["llm_prompt"] = rendered.user
        item["llm_system"] = rendered.system

    return items


_REPORTS_DIR = Path("data/reports")


def _render_markdown(report: dict) -> str:
    """Render a report dict as a Markdown document."""
    asin = report["asin"]
    ts = report["generated_at"]
    ov = report["overall_summary"]
    mods = report["module_performance"]
    ri = report["review_intelligence"]
    risk = ri.get("manipulation_risk") or {}
    comps = report["comparative_analysis"]["competitors"]
    plan = report["improvement_plan"]
    diagnosis = report["qualitative_diagnosis"] or "_No LLM analysis available._"

    # Module performance table
    mod_rows = "\n".join(
        f"| {name.replace('_', ' ').title()} | {score} |" for name, score in mods.items()
    )

    # Competitor table
    comp_rows = (
        "\n".join(
            f"| {c.get('asin', '—')} | {c.get('score', '—')} | {c.get('status', '—')} |"
            for c in comps
        )
        or "| — | — | — |"
    )

    # Review intelligence block
    barrier = ri.get("competitive_barrier_months")
    barrier_str = f"~{barrier} months to 500-review benchmark" if barrier is not None else "N/A"
    pros_md = "\n".join(f"- {p}" for p in ri.get("pros", [])) or "_None identified._"
    complaints_md = (
        "\n".join(f"- {c}" for c in ri.get("top_complaints", [])) or "_None identified._"
    )

    # Improvement plan
    plan_md = "\n".join(f"{i + 1}. {issue}" for i, issue in enumerate(plan)) or "_None._"

    return f"""# Listing Quality Diagnosis: {asin}

> **Generated:** {ts}

---

## Overall Summary

| Metric | Value |
|--------|-------|
| Quality Score | **{ov.get("score", "—")} / 100** ({ov.get("status", "—")}) |
| Competitor Avg Score | {ov.get("competitor_avg_score", "—")} / 100 |

---

## Module Performance

| Module | Score |
|--------|-------|
{mod_rows}

---

## Review Intelligence

| Metric | Value |
|--------|-------|
| Sentiment Score | {ri.get("sentiment_score", "—")} |
| Review Velocity | {ri.get("review_velocity", "—")} reviews/month |
| Competitive Barrier | {barrier_str} |
| Manipulation Risk | {risk.get("score", "—")}/100 — **{risk.get("verdict", "N/A")}** |

**Buyer Persona:** {ri.get("buyer_persona") or "_N/A_"}

### Customer Pros
{pros_md}

### Top Complaints
{complaints_md}

---

## Comparative Analysis

| Competitor ASIN | Score | Status |
|-----------------|-------|--------|
{comp_rows}

---

## Qualitative Diagnosis

{diagnosis}

---

## Improvement Plan

{plan_md}
"""


def _generate_report(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Consolidate all data into a final report and write it as a local Markdown file."""
    logger.info("Generating final listing diagnosis reports...")
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    for item in items:
        main_asin = item.get("asin")
        main_score = item.get("main_score", {})
        comp_scores = item.get("competitor_scores", [])
        llm_analysis = item.get("llm_diagnosis", "")
        rs = item.get("review_summary") or {}

        report = {
            "asin": main_asin,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "overall_summary": {
                "score": main_score.get("overall_quality_score"),
                "status": main_score.get("status"),
                "competitor_avg_score": round(
                    sum(s.get("overall_quality_score", 0) for s in comp_scores) / len(comp_scores)
                    if comp_scores
                    else 0,
                    1,
                ),
            },
            "module_performance": main_score.get("module_scores", {}),
            "review_intelligence": {
                "sentiment_score": rs.get("sentiment_score"),
                "review_velocity": rs.get("review_velocity"),
                "competitive_barrier_months": rs.get("competitive_barrier_months"),
                "manipulation_risk": rs.get("manipulation_risk"),
                "pros": rs.get("pros", []),
                "top_complaints": rs.get("top_complaints", []),
                "buyer_persona": rs.get("buyer_persona"),
            },
            "comparative_analysis": {
                "competitors": [
                    {
                        "asin": s.get("asin"),
                        "score": s.get("overall_quality_score"),
                        "status": s.get("status"),
                    }
                    for s in comp_scores
                ]
            },
            "qualitative_diagnosis": llm_analysis,
            "improvement_plan": main_score.get("improvement_plan", []),
        }

        # Write Markdown report to disk
        date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        md_path = _REPORTS_DIR / f"listing_diagnosis_{main_asin}_{date_tag}.md"
        md_text = _render_markdown(report)
        try:
            md_path.write_text(md_text, encoding="utf-8")
            item["report_file_path"] = str(md_path)
            logger.info(f"Report written to {md_path}")
        except OSError as e:
            logger.error(f"Failed to write report file for {main_asin}: {e}")

        filename = md_path.name
        item["response"] = (
            md_text[:400].rstrip() + f"\n\n…（完整报告已保存为 `{filename}`，正在发送为附件）"
        )
        item["final_report"] = report

    return items


# ---------------------------------------------------------------------------
# Workflow Definition
# ---------------------------------------------------------------------------


@WorkflowRegistry.register("listing_diagnosis")
def build_listing_diagnosis(config: dict) -> Workflow:
    return Workflow(
        name="listing_diagnosis",
        steps=[
            EnrichStep(
                name="fetch_main_product",
                extractor_fn=_fetch_product_details,
                fields=["product_data"],
            ),
            EnrichStep(
                name="discover_competitors",
                extractor_fn=_search_competitors,
                fields=["competitor_list", "search_keywords"],
            ),
            EnrichStep(
                name="enrich_competitors",
                extractor_fn=_enrich_competitors,
                fields=["competitor_data"],
            ),
            EnrichStep(
                name="fetch_and_summarize_reviews",
                extractor_fn=_fetch_and_summarize_reviews,
                fields=["review_summary"],
            ),
            ProcessStep(
                name="deterministic_scoring",
                fn=_run_scoring,
            ),
            ProcessStep(
                name="prepare_llm_prompt",
                fn=_prepare_llm_prompt,
            ),
            ProcessStep(
                name="llm_diagnosis",
                compute_target=ComputeTarget.CLOUD_LLM,
                prompt_template="{llm_prompt}",
                system_prompt_field="llm_system",
                output_field="llm_diagnosis",
            ),
            ProcessStep(
                name="generate_report",
                fn=_generate_report,
            ),
        ],
    )
