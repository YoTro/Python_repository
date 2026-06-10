from __future__ import annotations

import asyncio
import functools
import json
import logging
import re
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, Field

from src.core.models.product import Product
from src.core.models.review import ReviewSummary
from src.intelligence.processors.listing_quality_scorer import (
    LISTING_STOP_WORDS,
    ListingQualityScorer,
)
from src.intelligence.processors.review_summarizer import ReviewSummarizer
from src.intelligence.prompts.manager import PromptSpec, prompt_manager
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.product_details import ProductDetailsExtractor
from src.mcp.servers.amazon.extractors.search import SearchExtractor
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

# Matches dimension/quantity tokens that start with a digit: "100g", "2-pack", "3pcs", "10x12"
_DIMENSION_RE = re.compile(r"^\d[\w./\-]*$")

_STOP_WORDS = LISTING_STOP_WORDS

_COMPETITOR_FIELDS = (
    "asin",
    "title",
    "features",
    "rating",
    "review_count",
    "price",
    "has_a_plus_content",
    "past_month_sales",
)


def _extract_search_keywords(title: str, n: int = 6) -> str:
    """Return n content words from title, skipping stop words, dimension/quantity terms, and likely brand prefix."""
    if not title:
        return ""
    words = title.split()
    if len(words) > 4 and words[0][0].isupper() and len(words[0]) < 15:
        words = words[1:]
    content = []
    for w in words:
        w = re.sub(r"^[^\w]+|[^\w]+$", "", w)  # strip leading/trailing punctuation
        if not w or w.lower() in _STOP_WORDS or _DIMENSION_RE.match(w):
            continue
        content.append(w)
    return " ".join(content[:n])


def _slim_competitor(c: dict) -> dict:
    slim = {k: c.get(k) for k in _COMPETITOR_FIELDS}
    slim["image_count"] = len(c.get("images") or [])
    slim["video_count"] = len(c.get("videos") or [])
    return slim


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

    try:
        product = await _details_extractor().get_product_details(asin)
    except Exception as e:
        logger.error(f"Failed to fetch product details for {asin}: {e}")
        return {}

    logger.info(
        f"Fetched {len(product.images)} image(s) "
        f"({len(product.images_metadata)} with metadata) "
        f"and {len(product.videos)} video(s) for {asin}"
    )
    return {"product_data": product.model_dump()}


async def _search_competitors(item: dict, ctx: WorkflowContext) -> dict:
    """Search for competitors using keywords from the main product."""
    product_data = item.get("product_data", {})
    title = product_data.get("title", "")
    if not title:
        logger.warning("No title available for competitor discovery.")
        return {"competitor_list": []}

    keywords = _extract_search_keywords(title)
    if not keywords:
        logger.warning(f"Could not extract search keywords from title: {title!r}")
        return {"competitor_list": []}

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
        try:
            enriched_p = await _details_extractor().enrich_product(p)
        except Exception as e:
            logger.warning(f"Failed to enrich competitor {asin}: {e}")
            return p.model_dump()
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
        # Drain cancellations so CancelledError propagates into each coroutine and
        # any held resources (connections, file handles) are released before we continue.
        await asyncio.gather(*pending, return_exceptions=True)

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


def _slim_review(r) -> dict:
    return {
        "rating": r.rating,
        "title": r.title or "",
        "content": (r.content[:300] + "…") if len(r.content or "") > 300 else (r.content or ""),
        "is_verified": r.is_verified,
        "helpful_votes": r.helpful_votes or 0,
    }


async def _fetch_and_summarize_reviews(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch reviews for summarization and negative reviews for the report in parallel."""
    asin = item.get("asin")
    if not asin:
        return {"review_summary": None, "low_star_reviews": []}

    provider = getattr(ctx.router, "cloud", None) if ctx.router else None
    if not provider:
        logger.warning("No cloud provider on ctx.router; skipping review summarization.")
        return {"review_summary": None, "low_star_reviews": []}

    extractor = _comments_extractor()
    logger.info(f"Fetching all reviews and negative reviews for {asin} in parallel...")
    try:
        reviews, neg_reviews = await asyncio.gather(
            extractor.get_all_comments(asin, max_pages=3),
            extractor.get_negative_reviews(asin, max_pages=2),
            return_exceptions=True,
        )
    except Exception as e:
        logger.warning(f"Review fetch failed for {asin}: {e}")
        return {"review_summary": None, "low_star_reviews": []}

    if isinstance(reviews, Exception):
        logger.warning(f"All-reviews fetch failed for {asin}: {reviews}")
        reviews = []
    if isinstance(neg_reviews, Exception):
        logger.warning(f"Negative-reviews fetch failed for {asin}: {neg_reviews}")
        neg_reviews = []

    low_star_reviews = sorted(neg_reviews, key=lambda r: r.helpful_votes or 0, reverse=True)[:5]
    low_star_reviews = [_slim_review(r) for r in low_star_reviews]

    if not reviews:
        logger.info(f"No reviews found for {asin}.")
        return {"review_summary": None, "low_star_reviews": low_star_reviews}

    logger.info(f"Summarizing {len(reviews)} reviews for {asin}...")
    try:
        summary = await ReviewSummarizer(provider=provider).summarize(reviews)
        return {"review_summary": summary.model_dump(), "low_star_reviews": low_star_reviews}
    except Exception as e:
        logger.warning(f"Review summarization failed for {asin}: {e}")
        return {"review_summary": None, "low_star_reviews": low_star_reviews}


# ---------------------------------------------------------------------------
# Processing functions
# ---------------------------------------------------------------------------


def _compute_competitive_delta(main_score: dict, competitor_scores: list[dict]) -> dict:
    """Per-module and overall comparison of target scores against competitor averages."""
    if not competitor_scores:
        return {}

    main_modules: dict[str, float] = main_score.get("module_scores", {})
    all_modules = set(main_modules) | {
        k for s in competitor_scores for k in s.get("module_scores", {})
    }

    module_deltas: dict[str, dict] = {}
    for mod in sorted(all_modules):
        target_val = float(main_modules.get(mod) or 0)
        comp_avg = sum(
            float(s.get("module_scores", {}).get(mod) or 0) for s in competitor_scores
        ) / len(competitor_scores)
        module_deltas[mod] = {
            "target": round(target_val, 1),
            "competitor_avg": round(comp_avg, 1),
            "delta": round(target_val - comp_avg, 1),
        }

    main_overall = float(main_score.get("overall_quality_score") or 0)
    comp_overall_avg = sum(
        float(s.get("overall_quality_score") or 0) for s in competitor_scores
    ) / len(competitor_scores)

    weaker_modules = sorted(
        (mod for mod, v in module_deltas.items() if v["delta"] < 0),
        key=lambda mod: module_deltas[mod]["delta"],
    )

    return {
        "overall_delta": round(main_overall - comp_overall_avg, 1),
        "competitor_avg_score": round(comp_overall_avg, 1),
        "module_deltas": module_deltas,
        "weaker_modules": weaker_modules,
    }


def _parse_keyword_config(asin: str, raw: dict) -> dict[str, list[str]]:
    """
    Tier Xiyouzhaoci ASIN research keywords by the ASIN's click share.

    Tiers (rank-based, not threshold, so counts are predictable):
      core      — top  5: primary buyer-intent terms
      modifiers — next 10: attribute / use-case terms
      scenes    — next 15: long-tail / scenario terms
    """
    entries = (raw.get("data") or {}).get("list") or raw.get("list") or []

    scored: list[tuple[float, str]] = []
    for entry in entries:
        term = (entry.get("searchTerm") or "").strip()
        if not term:
            continue
        top_asins = (entry.get("topAsins") or {}).get("list") or []
        # Prefer this ASIN's own click share; fall back to overall traffic ratio.
        click_share = next(
            (float(a.get("clickShare") or 0) for a in top_asins if a.get("asin") == asin),
            float((entry.get("trafficRatio") or {}).get("total") or 0),
        )
        scored.append((click_share, term))

    scored.sort(reverse=True)
    terms = [t for _, t in scored]
    return {
        "core": terms[:5],
        "modifiers": terms[5:15],
        "scenes": terms[15:30],
    }


async def _fetch_keywords(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch ASIN traffic keywords from Xiyouzhaoci and tier them for the scorer."""
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

    asin = item.get("asin")
    if not asin:
        return {}

    end = _date.today() - timedelta(days=1)
    start = end - timedelta(days=29)

    try:
        client = XiyouZhaociAPI()
        raw = await asyncio.to_thread(client.get_asin_keywords, "US", asin, str(start), str(end))
        kw_config = _parse_keyword_config(asin, raw)
    except Exception as e:
        logger.warning(f"Xiyouzhaoci keyword fetch/parse failed for {asin}: {e}")
        return {}

    total = sum(len(v) for v in kw_config.values())
    if total == 0:
        logger.warning(f"No keywords returned from Xiyouzhaoci for {asin}")
        return {}

    logger.info(
        f"Keywords fetched for {asin}: "
        f"core={len(kw_config['core'])}, "
        f"modifiers={len(kw_config['modifiers'])}, "
        f"scenes={len(kw_config['scenes'])}"
    )
    return {"keyword_config": kw_config}


_VISUAL_SCORING_PROMPT = """\
You are evaluating Amazon product listing images for quality and conversion effectiveness.

The first {n_target} image(s) are from the TARGET listing (ASIN: {asin}).
The remaining {n_comp} image(s) are the primary images of competing listings (one per competitor).

Score the TARGET listing on four dimensions (0–100, 100 = best).
Use the competitive images as context — "good" is relative to what competitors show.

- image_quality: Professional staging, appropriate lighting, sharp focus, clean backgrounds.
  100 = all images are commercially professional. Deduct for blurry, dark, or amateur shots.
- content_diversity: Variety of shot types covering the buyer journey — hero, lifestyle/in-use,
  infographic with specs, size or dimension reference, variant or detail shots.
  100 = 5+ distinct shot types. Deduct for hero-only or repetitive angles.
- lifestyle_representation: Product shown in realistic use scenarios that communicate its value.
  100 = 2+ clear in-use images showing a concrete benefit. 0 = white background only.
- purchase_confidence: Would a first-time buyer feel confident enough to purchase based solely
  on these images, relative to what competitors offer?

Also rate your own confidence in these scores (0–100):
100 = images are large and clear; lower if images are small, unclear, or edge cases.

Return JSON with keys: image_quality, content_diversity, lifestyle_representation,
purchase_confidence, confidence, rationale (one sentence summarising the main finding).\
"""


async def _score_visual_with_vision(item: dict, ctx: WorkflowContext) -> dict:
    """
    Pass target images + competitor main images to a vision LLM and score
    visual quality across four genuinely vision-dependent dimensions.
    Uses Gemini (primary) or Claude (fallback) — whichever the router provides.
    Caps at 5 target images and 1 image per competitor to control token cost.
    """
    p_data = item.get("product_data", {})
    target_images = (p_data.get("images") or [])[:5]
    if not target_images:
        logger.info(f"No images for visual scoring of {item.get('asin')}")
        return {}

    comp_images = [c["images"][0] for c in (item.get("competitor_data") or []) if c.get("images")][
        :5
    ]

    provider = getattr(ctx.router, "cloud", None) if ctx.router else None
    if not provider:
        logger.warning("No cloud provider on ctx.router; skipping visual semantic scoring.")
        return {}

    prompt = _VISUAL_SCORING_PROMPT.format(
        n_target=len(target_images),
        asin=item.get("asin", ""),
        n_comp=len(comp_images),
    )
    all_images = target_images + comp_images

    try:
        result: _VisualSemanticDimensions = await provider.generate_vision_structured(
            image_urls=all_images,
            prompt=prompt,
            schema=_VisualSemanticDimensions,
            max_tokens=2048,  # headroom for reasoning models that consume tokens before output
        )
        vis = result if isinstance(result, dict) else result.model_dump()
        if vis.get("confidence", 100) < 40:
            logger.warning(
                f"Visual scoring low confidence ({vis['confidence']}) for {item.get('asin')} "
                "— scores may be unreliable due to small or unclear images."
            )
        logger.info(
            f"Visual semantic scores for {item.get('asin')}: "
            f"quality={vis.get('image_quality')} diversity={vis.get('content_diversity')} "
            f"lifestyle={vis.get('lifestyle_representation')} "
            f"confidence_purchase={vis.get('purchase_confidence')} "
            f"model_confidence={vis.get('confidence')}"
        )
        return {"visual_semantic_score": vis}
    except NotImplementedError:
        logger.warning(
            f"{provider.__class__.__name__} does not support vision; skipping visual scoring."
        )
        return {}
    except Exception as e:
        logger.warning(f"Visual semantic scoring failed for {item.get('asin')}: {e}")
        return {}


def _stub_review_summary(product_dict: dict) -> ReviewSummary:
    """
    Fallback ReviewSummary when real review data is unavailable.
    Synthesises rating_breakdown from product.rating via the same linear proxy
    used elsewhere; LLM-computed fields remain None.
    """
    rating = product_dict.get("rating") or 0.0
    low_star_ratio = max(0.0, (4.0 - rating) / 3.0) if rating else 0.0
    high_star_ratio = 1.0 - low_star_ratio
    return ReviewSummary(
        pros=[],
        cons=[],
        top_complaints=[],
        buyer_persona="",
        rating_breakdown={
            1: round(low_star_ratio * 0.4 * 100),
            2: round(low_star_ratio * 0.3 * 100),
            3: round(low_star_ratio * 0.3 * 100),
            4: round(high_star_ratio * 0.35 * 100),
            5: round(high_star_ratio * 0.65 * 100),
        },
    )


async def _enrich_competitor_reviews(item: dict, ctx: WorkflowContext) -> dict:
    """Fetch and summarize reviews for all competitors in parallel.
    Uses max_pages=2 (vs 3 for the target) to limit extra I/O per competitor.
    """
    competitors = item.get("competitor_data", [])
    if not competitors:
        return {}

    provider = getattr(ctx.router, "cloud", None) if ctx.router else None
    if not provider:
        logger.warning("No cloud provider; skipping competitor review summarization.")
        return {}

    async def _summarize_one(asin: str) -> tuple[str, dict | None]:
        try:
            reviews = await _comments_extractor().get_all_comments(asin, max_pages=2)
            if not reviews:
                return asin, None
            summary = await ReviewSummarizer(provider=provider).summarize(reviews)
            return asin, summary.model_dump()
        except Exception as e:
            logger.warning(f"Competitor review summarization failed for {asin}: {e}")
            return asin, None

    asins = [c["asin"] for c in competitors if c.get("asin")]
    logger.info(f"Summarizing reviews for {len(asins)} competitor(s)...")

    try:
        raw_results = await asyncio.wait_for(
            asyncio.gather(*[_summarize_one(a) for a in asins], return_exceptions=True),
            timeout=90.0,
        )
    except TimeoutError:
        logger.warning("Competitor review summarization timed out after 90s.")
        raw_results = []

    summaries: dict[str, dict] = {}
    for result in raw_results:
        if isinstance(result, tuple):
            asin, summary = result
            if summary:
                summaries[asin] = summary

    logger.info(f"Competitor review summaries: {len(summaries)}/{len(asins)} succeeded.")
    return {"competitor_review_summaries": summaries}


def _run_scoring(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Score main product and competitors, then compute per-module competitive delta."""
    scorer = _scorer()
    for item in items:
        keyword_config: dict | None = item.get("keyword_config")

        comp_data: list[dict] = item.get("competitor_data") or []
        comp_products = [Product(**c) for c in comp_data if isinstance(c, dict)]

        if "product_data" in item:
            p = Product(**item["product_data"])
            rs_raw = item.get("review_summary")
            review_summary: ReviewSummary | None = (
                ReviewSummary(**rs_raw)
                if isinstance(rs_raw, dict) and rs_raw
                else rs_raw
                if isinstance(rs_raw, ReviewSummary)
                else None
            )
            image_metadata: dict | None = p.images_metadata or None
            res = scorer.score(
                p,
                keyword_config=keyword_config,
                review_summary=review_summary,
                image_metadata=image_metadata,
                competitors=comp_products,
            )
            item["main_score"] = res
            logger.info(
                f"Target ASIN {p.asin} quality score: {res.get('overall_quality_score')} "
                f"({res.get('status')}) "
                f"[keywords: {'xiyouzhaoci' if keyword_config else 'none'}]"
            )

        if comp_data:
            comp_summaries: dict[str, dict] = item.get("competitor_review_summaries", {})
            scores = [
                scorer.score(
                    Product(**c),
                    keyword_config=keyword_config,
                    review_summary=(
                        ReviewSummary(**comp_summaries[c["asin"]])
                        if c.get("asin") in comp_summaries
                        else _stub_review_summary(c)
                    ),
                )
                for c in comp_data
            ]
            item["competitor_scores"] = scores
            avg = (
                sum(s.get("overall_quality_score", 0) for s in scores) / len(scores)
                if scores
                else 0
            )
            logger.info(f"Average competitor score: {avg:.1f} (based on {len(scores)} products)")

        if item.get("main_score") and item.get("competitor_scores"):
            cd = _compute_competitive_delta(item["main_score"], item["competitor_scores"])
            item["competitive_delta"] = cd
            delta = cd.get("overall_delta", 0)
            logger.info(
                f"Competitive delta: {delta:+.1f} pts "
                f"({'above' if delta >= 0 else 'below'} competitor average)"
            )

    return items


# ---------------------------------------------------------------------------
# Layer 2 — Semantic scoring schema & prompt spec (inline, no external files)
# ---------------------------------------------------------------------------


class _TitleSemanticDimensions(BaseModel):
    product_type_clarity: int = Field(default=50, ge=0, le=100)
    usp_presence: int = Field(default=50, ge=0, le=100)
    buyer_comprehension: int = Field(default=50, ge=0, le=100)
    claim_risk_penalty: int = Field(default=100, ge=0, le=100)
    rationale: str = Field(default="")


class _BulletSemanticDimensions(BaseModel):
    specificity: int = Field(default=50, ge=0, le=100)
    purchase_decision_coverage: int = Field(default=50, ge=0, le=100)
    buyer_language_alignment: int = Field(default=50, ge=0, le=100)
    claim_risk_penalty: int = Field(default=100, ge=0, le=100)
    rationale: str = Field(default="")


class _VisualSemanticDimensions(BaseModel):
    image_quality: int = Field(default=50, ge=0, le=100)
    content_diversity: int = Field(default=50, ge=0, le=100)
    lifestyle_representation: int = Field(default=50, ge=0, le=100)
    purchase_confidence: int = Field(default=50, ge=0, le=100)
    confidence: int = Field(default=50, ge=0, le=100)
    rationale: str = Field(default="")


class SemanticScoringOutput(BaseModel):
    title: _TitleSemanticDimensions = Field(default_factory=_TitleSemanticDimensions)
    bullets: _BulletSemanticDimensions = Field(default_factory=_BulletSemanticDimensions)


_SEMANTIC_SCORING_TEMPLATE = """\
Semantically evaluate the Amazon listing below across eight quality dimensions.
Use the full 0–100 range — do not cluster scores around 50.

ASIN: {asin}

## Title
{title}

## Bullet Points
{features}

---

Score each dimension 0–100 (100 = best).

### title
- product_type_clarity: Does the title immediately and unambiguously communicate what the
  product IS to a first-time buyer who has never seen this item?
- usp_presence: Is there at least one concrete, differentiated unique selling proposition —
  not just generic adjectives like "premium" or "high quality"?
- buyer_comprehension: Can a target buyer understand the complete product offering (what it
  is, what it does, who it is for) at a glance, without any prior context?
- claim_risk_penalty: Are all claims realistic and unlikely to violate Amazon's restricted or
  misleading-claims policy? Score 100 for fully clean copy; deduct for superlatives without
  proof, absolute claims ("best", "number 1"), restricted terms, or medical/legal language.
- rationale: One concise sentence justifying the four title scores.

### bullets
- specificity: Are the selling points concrete, measurable, or evidence-backed — or do they
  rely on vague filler language ("great quality", "perfect for everyone")?
- purchase_decision_coverage: Do the bullets collectively give a buyer everything they need
  to make a purchase decision: core function, safety, dimensions/fit, compatibility, use cases?
- buyer_language_alignment: Does the copy speak from the buyer's perspective — prioritising
  benefits over features, using their vocabulary, addressing their real concerns?
- claim_risk_penalty: Same scale as title.claim_risk_penalty. Flag exaggerated efficacy
  claims, unverified certifications, or any language likely to trigger Amazon review.
- rationale: One concise sentence justifying the four bullet scores.\
"""

prompt_manager.register_spec(
    PromptSpec(
        id="listing_semantic_scoring",
        version="0.3.0",
        scope="per_item",
        token_budget=6000,
        role_id="product_manager",
        required_vars=["asin", "title", "features"],
        template=_SEMANTIC_SCORING_TEMPLATE,
    )
)


def _prepare_semantic_prompt(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Render the semantic scoring prompt for each item."""
    for item in items:
        p_data = item.get("product_data", {})
        variables = {
            "asin": item.get("asin", ""),
            "title": p_data.get("title", "N/A"),
            "features": "\n".join(f"{i + 1}. {f}" for i, f in enumerate(p_data.get("features", [])))
            or "(no bullet points)",
        }
        rendered = prompt_manager.render_spec("listing_semantic_scoring", variables)
        item["semantic_prompt"] = rendered.user
        item["semantic_system"] = rendered.system
    return items


# Weights when both deterministic (Layer 1) and semantic (Layer 2) scores are present.
# Deterministic modules collectively contribute 70%; semantic modules 30% (3 × 0.100).
_BLENDED_WEIGHTS: dict[str, float] = {
    "title": 0.175,
    "bullet_points": 0.140,
    "media": 0.140,
    "social_proof": 0.140,
    "aplus": 0.105,
    "title_semantics": 0.100,
    "bullet_semantics": 0.100,
    "visual_semantics": 0.100,
}

# Deterministic-only module keys and their total weight (= 0.70).
# Used to rescale the target's deterministic score to 0-100 for apples-to-apples
# competitive comparison — competitors are never given semantic scores.
_DET_KEYS: frozenset[str] = frozenset(k for k in _BLENDED_WEIGHTS if not k.endswith("_semantics"))
_DET_WEIGHT_TOTAL: float = sum(_BLENDED_WEIGHTS[k] for k in _DET_KEYS)  # 0.70


def _merge_semantic_scores(items: list[dict], ctx: WorkflowContext) -> list[dict]:
    """Blend LLM semantic dimension scores into main_score and recompute overall quality."""
    for item in items:
        raw = item.get("semantic_score")
        vis_raw = item.get("visual_semantic_score")

        # Skip only when both sources are absent — a vision-only success must still be merged.
        if not raw and not vis_raw:
            continue

        def _avg(*keys: str, src: dict) -> float:
            vals = [src[k] for k in keys if isinstance(src.get(k), int | float)]
            return round(sum(vals) / len(vals), 1) if vals else 50.0

        # ── Text semantics (title + bullets) ──────────────────────────────────
        t: dict = {}
        b: dict = {}
        title_sem: float | None = None
        bullet_sem: float | None = None

        if raw:
            if isinstance(raw, dict):
                t = raw.get("title", {})
                b = raw.get("bullets", {})
            elif isinstance(raw, SemanticScoringOutput):
                t = raw.title.model_dump()
                b = raw.bullets.model_dump()
            else:
                logger.warning(
                    f"Unrecognised semantic_score type for {item.get('asin')}: {type(raw)}"
                )

            if t:
                title_sem = _avg(
                    "product_type_clarity",
                    "usp_presence",
                    "buyer_comprehension",
                    "claim_risk_penalty",
                    src=t,
                )
            if b:
                bullet_sem = _avg(
                    "specificity",
                    "purchase_decision_coverage",
                    "buyer_language_alignment",
                    "claim_risk_penalty",
                    src=b,
                )

        # ── Visual semantics (vision LLM step, independent) ───────────────────
        v: dict = {}
        visual_sem: float | None = None

        if vis_raw:
            v = vis_raw if isinstance(vis_raw, dict) else vis_raw.model_dump()
            if v:
                visual_sem = _avg(
                    "image_quality",
                    "content_diversity",
                    "lifestyle_representation",
                    "purchase_confidence",
                    src=v,
                )

        item["semantic_details"] = {
            **({"title": t} if t else {}),
            **({"bullets": b} if b else {}),
            **({"visual": v} if v else {}),
        }

        main_score = item.get("main_score")
        if not main_score:
            logger.warning(
                f"Skipping score blend for {item.get('asin')}: deterministic main_score missing."
            )
            continue

        module_scores = main_score.get("module_scores", {})
        if title_sem is not None:
            module_scores["title_semantics"] = title_sem
        if bullet_sem is not None:
            module_scores["bullet_semantics"] = bullet_sem
        if visual_sem is not None:
            module_scores["visual_semantics"] = visual_sem
        main_score["module_scores"] = module_scores

        blended = round(
            max(
                0.0,
                min(100.0, sum(module_scores.get(k, 0) * w for k, w in _BLENDED_WEIGHTS.items())),
            ),
            1,
        )
        main_score["overall_quality_score"] = blended
        main_score["status"] = (
            "Excellent"
            if blended >= 90
            else "Good"
            if blended >= 75
            else "Poor"
            if blended >= 50
            else "Critical"
        )
        # Deterministic-only rescaled score: divide each det weight by 0.70 so the
        # five modules still sum to 100.  This matches the scale competitors are
        # scored on (they have no semantic modules) and is the only valid basis for
        # the competitive delta.
        det_score = round(
            max(
                0.0,
                min(
                    100.0,
                    sum(
                        module_scores.get(k, 0) * (_BLENDED_WEIGHTS[k] / _DET_WEIGHT_TOTAL)
                        for k in _DET_KEYS
                    ),
                ),
            ),
            1,
        )
        main_score["det_quality_score"] = det_score
        item["main_score"] = main_score

        # Refresh overall_delta using det_score so the comparison is apples-to-apples.
        # module_deltas for the five deterministic modules remain valid unchanged.
        cd = item.get("competitive_delta")
        if cd:
            comp_avg = float(cd.get("competitor_avg_score") or 0)
            cd["overall_delta"] = round(det_score - comp_avg, 1)
            item["competitive_delta"] = cd

        logger.info(
            f"Semantic scores merged for {item.get('asin')}: "
            f"title_sem={title_sem}, bullet_sem={bullet_sem}, blended_overall={blended}"
            + (f", overall_delta={cd['overall_delta']:+.1f}" if cd else "")
        )

    return items


def _format_competitive_gap(cd: dict | None) -> str:
    if not cd:
        return "No competitive comparison data available."
    sign = "+" if (cd.get("overall_delta") or 0) >= 0 else ""
    lines = [
        f"Overall Score Delta: {sign}{cd['overall_delta']:.1f} pts "
        f"(target vs. competitor avg of {cd['competitor_avg_score']})",
        "",
        "Per-Module Breakdown (Target → Competitor Avg → Delta):",
    ]
    for mod, v in cd.get("module_deltas", {}).items():
        indicator = "▲" if v["delta"] > 0 else ("▼" if v["delta"] < 0 else "=")
        lines.append(
            f"  {mod.replace('_', ' ').title()}: "
            f"{v['target']} → avg {v['competitor_avg']} "
            f"({indicator} {v['delta']:+.1f})"
        )
    weak = cd.get("weaker_modules", [])
    if weak:
        lines.append(
            "\nModules lagging behind competition: " + ", ".join(m.replace("_", " ") for m in weak)
        )
    return "\n".join(lines)


def _format_semantic_details(sd: dict | None) -> str:
    """Render semantic_details into a structured text block for the final LLM prompt."""
    if not sd:
        return "Semantic scoring not available (step was skipped or returned no data)."

    def _avg(*keys: str, src: dict) -> str:
        vals = [src[k] for k in keys if isinstance(src.get(k), int | float)]
        return f"{sum(vals) / len(vals):.1f}" if vals else "—"

    t = sd.get("title") or {}
    b = sd.get("bullets") or {}
    v = sd.get("visual") or {}
    lines: list[str] = []

    if t:
        avg = _avg(
            "product_type_clarity",
            "usp_presence",
            "buyer_comprehension",
            "claim_risk_penalty",
            src=t,
        )
        lines += [
            f"### Title Semantics  (avg {avg}/100)",
            f"- Product Type Clarity: {t.get('product_type_clarity', '—')} "
            f"| USP Presence: {t.get('usp_presence', '—')} "
            f"| Buyer Comprehension: {t.get('buyer_comprehension', '—')} "
            f"| Claim Risk (100=clean): {t.get('claim_risk_penalty', '—')}",
            f"- Rationale: {t.get('rationale') or '—'}",
            "",
        ]

    if b:
        avg = _avg(
            "specificity",
            "purchase_decision_coverage",
            "buyer_language_alignment",
            "claim_risk_penalty",
            src=b,
        )
        lines += [
            f"### Bullet Semantics  (avg {avg}/100)",
            f"- Specificity: {b.get('specificity', '—')} "
            f"| Purchase Decision Coverage: {b.get('purchase_decision_coverage', '—')} "
            f"| Buyer Language Alignment: {b.get('buyer_language_alignment', '—')} "
            f"| Claim Risk (100=clean): {b.get('claim_risk_penalty', '—')}",
            f"- Rationale: {b.get('rationale') or '—'}",
            "",
        ]

    if v:
        avg = _avg(
            "image_quality",
            "content_diversity",
            "lifestyle_representation",
            "purchase_confidence",
            src=v,
        )
        lines += [
            f"### Visual Semantics  (avg {avg}/100, model confidence: {v.get('confidence', '—')})",
            f"- Image Quality: {v.get('image_quality', '—')} "
            f"| Content Diversity: {v.get('content_diversity', '—')} "
            f"| Lifestyle Representation: {v.get('lifestyle_representation', '—')} "
            f"| Purchase Confidence: {v.get('purchase_confidence', '—')}",
            f"- Rationale: {v.get('rationale') or '—'}",
        ]

    return "\n".join(lines) if lines else "Semantic scoring not available."


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
            "target_image_count": len(p_data.get("images") or []),
            "target_video_count": len(p_data.get("videos") or []),
            "competitor_data": json.dumps(
                [_slim_competitor(c) for c in c_data],
                indent=2,
                ensure_ascii=False,
            ),
            "review_summary": _format_review_summary(item.get("review_summary")),
            "competitive_gap": _format_competitive_gap(item.get("competitive_delta")),
            "semantic_scoring": _format_semantic_details(item.get("semantic_details")),
        }

        rendered = prompt_manager.render_spec("listing_diagnosis", variables)
        item["llm_prompt"] = rendered.user
        item["llm_system"] = rendered.system

    return items


_REPORTS_DIR = Path("data/reports")
_UNSAFE_CHARS_RE = re.compile(r"[^a-zA-Z0-9_\-]")


def _safe_stem(text: str) -> str:
    """Replace any character not safe in a filename with '_'."""
    return _UNSAFE_CHARS_RE.sub("_", text or "unknown")


def _render_markdown(report: dict) -> str:
    """Render a report dict as a Markdown document."""
    asin = report["asin"]
    ts = report["generated_at"]
    ov = report["overall_summary"]
    mods = report["module_performance"]
    ri = report["review_intelligence"]
    risk = ri.get("manipulation_risk") or {}
    sd = report.get("semantic_details") or {}
    t_sem = sd.get("title") or {}
    b_sem = sd.get("bullets") or {}
    v_sem = sd.get("visual") or {}
    ca = report["comparative_analysis"]
    comps = ca["competitors"]
    overall_delta = ca.get("overall_delta")
    module_deltas = ca.get("module_deltas", {})
    plan = report["improvement_plan"]
    diagnosis = report["qualitative_diagnosis"] or "_No LLM analysis available._"

    # Split module scores by basis so readers are not misled about comparability.
    det_mod_rows = "\n".join(
        f"| {name.replace('_', ' ').title()} | {score} |"
        for name, score in mods.items()
        if not name.endswith("_semantics")
    )
    sem_mod_rows = "\n".join(
        f"| {name.replace('_', ' ').title()} | {score} |"
        for name, score in mods.items()
        if name.endswith("_semantics")
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
    low_star_reviews = ri.get("low_star_reviews") or []
    if low_star_reviews:
        low_star_md = "\n".join(
            f"| {'⭐' * r['rating']} ({r['rating']}/5) "
            f"| {'✓ Verified' if r['is_verified'] else 'Unverified'} "
            f"| {r['helpful_votes']} helpful "
            f"| **{r['title']}** — {r['content']} |"
            for r in low_star_reviews
        )
        low_star_md = (
            "| Rating | Status | Votes | Review |\n|--------|--------|-------|--------|\n"
            + low_star_md
        )
    else:
        low_star_md = "_No 1–3 star reviews found._"

    # Semantic detail tables (only rendered when LLM semantic step ran)
    def _sem_row(label: str, val) -> str:
        return f"| {label} | {val if val is not None else '—'} |"

    title_sem_md = (
        "\n".join(
            [
                _sem_row("Product Type Clarity", t_sem.get("product_type_clarity")),
                _sem_row("USP Presence", t_sem.get("usp_presence")),
                _sem_row("Buyer Comprehension", t_sem.get("buyer_comprehension")),
                _sem_row("Claim Risk (100=clean)", t_sem.get("claim_risk_penalty")),
            ]
        )
        if t_sem
        else "| — | — |"
    )
    title_sem_rationale = t_sem.get("rationale", "_Not available._")
    bullet_sem_md = (
        "\n".join(
            [
                _sem_row("Specificity", b_sem.get("specificity")),
                _sem_row("Purchase Decision Coverage", b_sem.get("purchase_decision_coverage")),
                _sem_row("Buyer Language Alignment", b_sem.get("buyer_language_alignment")),
                _sem_row("Claim Risk (100=clean)", b_sem.get("claim_risk_penalty")),
            ]
        )
        if b_sem
        else "| — | — |"
    )
    bullet_sem_rationale = b_sem.get("rationale", "_Not available._")
    visual_sem_md = (
        "\n".join(
            [
                _sem_row("Image Quality", v_sem.get("image_quality")),
                _sem_row("Content Diversity", v_sem.get("content_diversity")),
                _sem_row("Lifestyle Representation", v_sem.get("lifestyle_representation")),
                _sem_row("Purchase Confidence", v_sem.get("purchase_confidence")),
                _sem_row("Model Confidence", v_sem.get("confidence")),
            ]
        )
        if v_sem
        else "| — | — |"
    )
    visual_sem_rationale = v_sem.get("rationale", "_Vision scoring not run or no images._")

    # Module gap table
    delta_sign = f"{overall_delta:+.1f}" if overall_delta is not None else "N/A"
    gap_rows = (
        "\n".join(
            f"| {mod.replace('_', ' ').title()} | {v['target']} | {v['competitor_avg']} | {v['delta']:+.1f} |"
            for mod, v in module_deltas.items()
        )
        or "| — | — | — | — |"
    )

    # Improvement plan
    plan_md = "\n".join(f"{i + 1}. {issue}" for i, issue in enumerate(plan)) or "_None._"

    return f"""# Listing Quality Diagnosis: {asin}

> **Generated:** {ts}

---

## Overall Summary

| Metric | Value |
|--------|-------|
| Quality Score (blended) | **{ov.get("score", "—")} / 100** ({ov.get("status", "—")}) |
| Quality Score (det. only) | {ov.get("det_score", "—")} / 100 — used for competitive delta |
| Competitor Avg Score | {ov.get("competitor_avg_score", "—")} / 100 |

---

## Module Performance

### Deterministic Modules
_Scored by rule-based heuristics. Used as the basis for competitive comparison._

| Module | Score |
|--------|-------|
{det_mod_rows}

### Semantic Modules
_Scored by LLM analysis. Contribute to the blended quality score but are **not** included
in the competitive gap below — competitors are not LLM-scored._

| Module | Score |
|--------|-------|
{sem_mod_rows if sem_mod_rows else "| — | — |"}

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

#### Representative 1–3 Star Reviews
{low_star_md}

---

## Semantic Quality Analysis

### Title

| Dimension | Score |
|-----------|-------|
{title_sem_md}

_{title_sem_rationale}_

### Bullet Points

| Dimension | Score |
|-----------|-------|
{bullet_sem_md}

_{bullet_sem_rationale}_

### Visual Content (Vision LLM)

| Dimension | Score |
|-----------|-------|
{visual_sem_md}

_{visual_sem_rationale}_

---

## Comparative Analysis

### Competitor Scores

| Competitor ASIN | Score | Status |
|-----------------|-------|--------|
{comp_rows}

### Deterministic Module Gap (Target vs. Competitor Avg)
_Semantic modules excluded — this gap reflects only the five deterministic modules
on which competitors are also scored, ensuring an apples-to-apples comparison._

**Overall delta (det. score): {delta_sign} pts**

| Module | Target | Comp Avg | Delta |
|--------|--------|----------|-------|
{gap_rows}

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
        raw_diagnosis = item.get("llm_diagnosis") or ""
        if hasattr(raw_diagnosis, "text"):  # LLMResponse object
            llm_analysis = raw_diagnosis.text or ""
        elif isinstance(raw_diagnosis, dict):  # model_dump() of LLMResponse
            llm_analysis = raw_diagnosis.get("text") or ""
        else:
            llm_analysis = raw_diagnosis
        rs = item.get("review_summary") or {}
        cd = item.get("competitive_delta") or {}
        sd = item.get("semantic_details") or {}

        report = {
            "asin": main_asin,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "overall_summary": {
                "score": main_score.get("overall_quality_score"),
                "det_score": main_score.get("det_quality_score"),
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
                "low_star_reviews": item.get("low_star_reviews", []),
            },
            "comparative_analysis": {
                "competitors": [
                    {
                        "asin": s.get("asin"),
                        "score": s.get("overall_quality_score"),
                        "status": s.get("status"),
                    }
                    for s in comp_scores
                ],
                "overall_delta": cd.get("overall_delta"),
                "module_deltas": cd.get("module_deltas", {}),
                "weaker_modules": cd.get("weaker_modules", []),
            },
            "semantic_details": sd,
            "qualitative_diagnosis": llm_analysis,
            "improvement_plan": main_score.get("improvement_plan", []),
        }

        # Write Markdown report to disk
        date_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        md_path = _REPORTS_DIR / f"listing_diagnosis_{_safe_stem(main_asin)}_{date_tag}.md"
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
                fields=["review_summary", "low_star_reviews"],
            ),
            EnrichStep(
                name="enrich_competitor_reviews",
                extractor_fn=_enrich_competitor_reviews,
                fields=["competitor_review_summaries"],
            ),
            EnrichStep(
                name="fetch_keywords",
                extractor_fn=_fetch_keywords,
                fields=["keyword_config"],
            ),
            EnrichStep(
                name="score_visual_semantics",
                extractor_fn=_score_visual_with_vision,
                fields=["visual_semantic_score"],
            ),
            ProcessStep(
                name="deterministic_scoring",
                fn=_run_scoring,
            ),
            ProcessStep(
                name="prepare_semantic_prompt",
                fn=_prepare_semantic_prompt,
            ),
            ProcessStep(
                name="semantic_scoring",
                compute_target=ComputeTarget.CLOUD_LLM,
                prompt_template="{semantic_prompt}",
                system_prompt_field="semantic_system",
                output_schema=SemanticScoringOutput,
                output_field="semantic_score",
            ),
            ProcessStep(
                name="merge_semantic_scores",
                fn=_merge_semantic_scores,
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
