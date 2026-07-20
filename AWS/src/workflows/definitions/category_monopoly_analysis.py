from __future__ import annotations

"""
Category Monopoly Analysis Workflow

Performs a deep-dive analysis of an Amazon category to determine monopoly levels
and competition intensity across 7 dimensions.
"""

import asyncio
import calendar
import hashlib as _hl
import json
import logging
import os
import re
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from src.core.data_cache import data_cache as _data_cache
from src.core.utils.decorators import exponential_backoff
from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
from src.intelligence.processors.sales_estimator import SalesEstimator
from src.intelligence.processors.social_virality import SocialViralityProcessor
from src.intelligence.prompts.manager import prompt_manager
from src.intelligence.router import TaskCategory
from src.mcp.servers.amazon.ads.client import AmazonAdsClient
from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor
from src.mcp.servers.amazon.extractors.feedback import SellerFeedbackExtractor
from src.mcp.servers.amazon.extractors.fulfillment import FulfillmentExtractor
from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
from src.mcp.servers.amazon.extractors.profitability_search import ProfitabilitySearchExtractor
from src.mcp.servers.amazon.extractors.review_count import ReviewRatioExtractor
from src.mcp.servers.amazon.extractors.search import SearchExtractor
from src.mcp.servers.market.deals.client import DealHistoryClient
from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
from src.mcp.servers.social.tiktok.client import TikTokClient
from src.mcp.servers.social.youtube.client import YouTubeClient
from src.workflows.engine import Workflow
from src.workflows.registry import WorkflowRegistry
from src.workflows.steps.base import ComputeTarget
from src.workflows.steps.enrich import EnrichStep
from src.workflows.steps.process import ProcessStep

logger = logging.getLogger(__name__)

# ── L2 cache helpers ─────────────────────────────────────────────────────────
_L2_DOMAIN = "cat_monopoly"

_TTL_BSR = 3_600  # 1  h — BSR scrape
_TTL_SALES = 86_400  # 24 h — past-month sales
_TTL_SELLER = 21_600  # 6  h — seller/fulfillment info
_TTL_SIGNALS = 3_600  # 1  h — ABA + SERP + CPC market signals
_TTL_TIMESERIES = 86_400  # 24 h — 12-month historical trends + keyword weekly
_TTL_SS_BSR = 86_400  # 24 h — Sellersprite monthly snapshots
_TTL_KEYWORDS = 21_600  # 6  h — LLM keyword extraction from BSR titles
_TTL_EXTERNAL = 43_200  # 12 h — TikTok PSI + deal intensity
_TTL_TRAFFIC = 21_600  # 6  h — Xiyouzhaoci batch ad-traffic ratios
_TTL_CVR = 7 * 86_400  # 7 d  — Amazon Ads category CVR benchmark
_TTL_CRITICAL_REVIEWS = 86_400  # 24 h — recent critical reviews per ASIN


def _l2_key(ctx, *parts) -> str:
    tid = getattr(ctx, "tenant_id", None) or "default"
    sid = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    return ":".join(str(p) for p in (tid, sid) + parts)


def _l2_get(ctx, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)


# ---------------------------------------------------------------------------
# Extractor Wrappers
# ---------------------------------------------------------------------------


async def _fetch_bsr_list(_items: list[dict], ctx: Any) -> list[dict]:
    """Seed step: fetches up to 100 BSR products (2 pages × 50) and returns them as the new item list.

    Actual count may be less than 100: lazy-loaded items (ranks 31-50 per page)
    are fetched via a separate ACP API and silently skipped on failure.
    Replaces whatever _items were passed in — downstream steps operate on the
    BSR product list, not the workflow seed items.
    """
    url = ctx.config.get("url")
    if not url:
        raise ValueError("No URL provided in workflow config for category_monopoly_analysis.")

    url_hash = _hl.md5(url.encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_BSR, "bsr_list", url_hash)
    if cached is not None:
        logger.info(f"[cat_monopoly] BSR list L2 cache hit for url_hash={url_hash}")
        return cached

    extractor = BestSellersExtractor()
    products = await extractor.get_bestsellers(url, max_pages=2)
    if not products:
        raise ValueError(f"BSR extractor returned no products for URL: {url}")
    _l2_set(ctx, products, "bsr_list", url_hash)
    logger.info(f"[cat_monopoly] Fetched {len(products)} BSR products, cached url_hash={url_hash}")
    return products


async def _enrich_sales(items: list[dict], ctx: Any) -> list[dict]:
    """Fetch past month sales for all items in one batch (20 ASINs per request).
    Cache per-ASIN; only fetches ASINs that are not already in L2.
    """
    all_asins = [(item.get("ASIN") or item.get("asin") or "").strip().upper() for item in items]

    # Resolve from cache where available
    sales_map: dict[str, int] = {}
    missing: list[str] = []
    for asin in all_asins:
        if not asin:
            continue
        hit = _l2_get(ctx, _TTL_SALES, "sales", asin)
        if hit is not None:
            sales_map[asin] = hit
        else:
            missing.append(asin)

    # Batch-fetch only the uncached ASINs
    if missing:
        extractor = PastMonthSalesExtractor()
        fetched = await extractor.get_batch_past_month_sales(missing)
        for asin, val in fetched.items():
            sales_map[asin] = val or 0
            _l2_set(ctx, val or 0, "sales", asin)

    for item, asin in zip(items, all_asins, strict=False):
        item["sales"] = sales_map.get(asin) or 0
    return items


async def _enrich_seller_info(item: dict, ctx: Any) -> dict:
    """Fetch fulfillment, seller feedback, and written-vs-global review counts."""
    asin = item.get("ASIN") or item.get("asin")
    if not asin:
        return {
            "seller_type": "Unknown",
            "seller_id": None,
            "feedback_count": 0,
            "global_ratings": None,
            "written_reviews": None,
            "review_ratio": None,
        }

    cached = _l2_get(ctx, _TTL_SELLER, "seller_info", asin)
    if cached is not None:
        return cached

    f_extractor, s_extractor, rc_extractor = (
        FulfillmentExtractor(),
        SellerFeedbackExtractor(),
        ReviewRatioExtractor(),
    )
    f_res, rc_res = await asyncio.gather(
        f_extractor.get_fulfillment_info(asin),
        rc_extractor.get_review_count(asin),
    )

    seller_id = f_res.get("SellerId")
    fulfilled_by = f_res.get("FulfilledBy")

    # SellerSprite fallback when Amazon page scraping fails (FulfilledBy is None)
    if fulfilled_by is None:
        ss_snapshots = ctx.cache.get("sellersprite_snapshots") or {}
        if ss_snapshots:
            _latest = ss_snapshots.get(max(ss_snapshots), [])
            _ss_item = next((p for p in _latest if p.get("asin") == asin), None)
            if _ss_item:
                _fba = _ss_item.get("fba")
                _ss_seller_name = _ss_item.get("seller_name") or ""
                if _fba is True:
                    fulfilled_by = "Amazon"
                elif _fba is False:
                    fulfilled_by = _ss_seller_name or "3P"
                if not seller_id:
                    seller_id = _ss_item.get("seller_id")
                logger.info(
                    f"[enrich_seller_info] {asin}: SellerSprite fallback → "
                    f"seller_type={fulfilled_by!r}, seller_id={seller_id!r}"
                )

    feedback_count = 0
    if seller_id:
        s_res = await s_extractor.get_seller_feedback_count(seller_id)
        feedback_count = s_res.get("FeedbackCount", 0)

    result = {
        "seller_type": fulfilled_by or "Unknown",
        "seller_id": seller_id,
        "feedback_count": feedback_count,
        "global_ratings": rc_res.get("GlobalRatings"),
        "written_reviews": rc_res.get("WrittenReviews"),
        "review_ratio": rc_res.get("Ratio"),
    }
    _l2_set(ctx, result, "seller_info", asin)
    return result


def _ngram_candidates(titles: list, min_doc_freq: int = 3, top_n: int = 15) -> list:
    """
    Return unigrams and bigrams ranked by document frequency (# titles containing them).

    Used to anchor LLM keyword extraction to terms actually present in the data,
    preventing hallucination and ensuring stability across runs.
    """
    _STOP = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "for",
        "of",
        "in",
        "with",
        "to",
        "from",
        "by",
        "is",
        "are",
        "was",
        "be",
        "as",
        "on",
        "at",
        "up",
        "it",
        "its",
        "this",
        "that",
        "all",
        "new",
        "set",
        "pack",
        "pcs",
        "piece",
        "pieces",
        "count",
        "ct",
        "oz",
        "lb",
        "ft",
        "inch",
    }
    doc_counts: Counter = Counter()
    for title in titles:
        tokens = [
            t for t in re.findall(r"[a-z0-9]+", title.lower()) if t not in _STOP and len(t) > 1
        ]
        seen_in_doc: set = set()
        for n in (1, 2):
            for i in range(len(tokens) - n + 1):
                gram = " ".join(tokens[i : i + n])
                if gram not in seen_in_doc:
                    doc_counts[gram] += 1
                    seen_in_doc.add(gram)
    return [term for term, cnt in doc_counts.most_common(top_n) if cnt >= min_doc_freq]


async def _fetch_core_keywords(items: list[dict], ctx: Any) -> list[dict]:
    """
    Step 1 of 2 for market context.
    Derives the top 3 core search keywords that define the niche.
    Must complete before _fetch_market_signals (ABA, SERP, CPC all key on these).
    Writes: ctx.cache["core_keywords"], ctx.cache["main_keyword"]

    Primary path — ASIN-keyword intersection (Xiyouzhaoci get_asin_keywords):
      1. Call get_asin_keywords concurrently for the top 10 BSR ASINs.
      2. Build a term→{asins} map.  Keep only terms present in ≥ MIN_ASIN_OVERLAP
         ASINs — these are cross-product niche keywords, not sub-niche specific ones.
      3. Sort surviving terms by (overlap_count × median_weekly_volume), take top 10
         as candidate anchors, then ask LLM to pick the best 3.

    Why intersection matters:
      A "killer" BSR top-10 mixes insecticides, Neem Oil, flea/tick, wasp spray, and
      deer repellents.  Terms like "wasp spray" only appear in a subset of ASINs;
      true category-level terms (e.g. "bug killer", "pest control") span all of them.
      Title-level n-grams cannot detect this because sub-niche product names overlap
      by accident; cross-ASIN traffic data reflects actual buyer search behaviour.

    Fallback — n-gram title frequency:
      Used when the Xiyouzhaoci call fails or returns < MIN_CANDIDATES keywords.

    Post-validation: LLM output is checked against the candidate set; fewer than
    2 survivors → use top intersection candidates directly (no LLM).
    """
    _MIN_ASIN_OVERLAP = 3  # keyword must drive traffic to at least this many ASINs
    _MIN_CANDIDATES = 3  # minimum intersection hits before falling back to n-grams

    if not items:
        return []

    top_asins = [
        item.get("ASIN") or item.get("asin")
        for item in items[:10]
        if item.get("ASIN") or item.get("asin")
    ]
    top_titles = [item.get("Title", "") for item in items[:20] if item.get("Title")]
    cache_hash = _hl.md5(("|".join(top_asins) + "|".join(top_titles[:5])).encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_KEYWORDS, "core_keywords", cache_hash)
    if cached is not None:
        ctx.cache["core_keywords"] = cached["core_keywords"]
        ctx.cache["main_keyword"] = cached["main_keyword"]
        logger.info(f"[cat_monopoly] Core keywords L2 cache hit hash={cache_hash}")
        return items

    # ── n-gram fallback (always precomputed) ─────────────────────────────
    freq_candidates = _ngram_candidates(top_titles, min_doc_freq=3)
    stat_keywords = freq_candidates[:3] if freq_candidates else ["unknown niche"]

    # ── Primary: cross-ASIN keyword intersection ──────────────────────────
    intersection_candidates: list = []
    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        api = XiyouZhaociAPI(tenant_id=tenant_id)

        today = datetime.utcnow().date()
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")

        # Fetch keywords for each ASIN concurrently
        async def _kw_for_asin(asin: str) -> tuple[str, list]:
            try:
                res = await asyncio.to_thread(
                    api.get_asin_keywords, country, asin, start_date, end_date, 1, 50
                )
                return asin, res.get("list") or []
            except Exception as e:
                logger.warning(f"[fetch_core_keywords] get_asin_keywords({asin}): {e}")
                return asin, []

        results = await asyncio.gather(*[_kw_for_asin(a) for a in top_asins])

        # Build term → set of ASINs that have this keyword
        term_asins: dict = defaultdict(set)
        term_vol: dict = defaultdict(list)
        for asin, kw_list in results:
            for kw in kw_list:
                term = (kw.get("searchTerm") or "").strip().lower()
                vol = (kw.get("searchTermReport") or {}).get("weeklySearchVolume") or 0
                if term and 1 <= len(term.split()) <= 5:
                    term_asins[term].add(asin)
                    term_vol[term].append(vol)

        # Keep only terms appearing across ≥ MIN_ASIN_OVERLAP distinct ASINs
        qualified = [
            (term, len(asins), statistics.median(term_vol[term]))
            for term, asins in term_asins.items()
            if len(asins) >= _MIN_ASIN_OVERLAP
        ]
        # Sort: more ASINs first, then higher volume as tiebreaker
        qualified.sort(key=lambda x: (x[1], x[2]), reverse=True)
        intersection_candidates = [term for term, _, _ in qualified[:12]]
        logger.info(
            f"[fetch_core_keywords] intersection: {len(qualified)} terms ≥ {_MIN_ASIN_OVERLAP} ASINs; "
            f"top={intersection_candidates[:5]}"
        )
    except Exception as e:
        logger.warning(
            f"[fetch_core_keywords] ASIN intersection failed: {e}; using n-gram fallback"
        )

    # Prefer intersection candidates; fall back to n-grams if too thin
    candidates = (
        intersection_candidates
        if len(intersection_candidates) >= _MIN_CANDIDATES
        else freq_candidates
    )
    if not candidates:
        candidates = stat_keywords

    # ── LLM refinement (pick best 3 from the data-grounded candidates) ───
    _REFUSAL_PREFIXES = (
        "sorry",
        "i ",
        "i'",
        "based on",
        "the keyword",
        "here are",
        "unfortunately",
        "as an",
        "i cannot",
        "i can't",
    )
    core_keywords = candidates[:3]
    try:
        if ctx.router and candidates:
            source_label = (
                "cross-ASIN traffic data" if intersection_candidates else "BSR title frequency"
            )
            candidate_str = ", ".join(candidates[:12])
            prompt = (
                "You are classifying an Amazon BSR niche. "
                f"The following search terms are grounded in {source_label} across the top BSR products: "
                f"[{candidate_str}]. "
                "From these candidates ONLY, pick the TOP 3 that best represent the core buyer "
                "search intent for this niche — terms a buyer would type when category-shopping, "
                "before deciding on a specific type, brand, or variant.\n"
                "A term is SUB-NICHE SPECIFIC (exclude it) if it:\n"
                "  • names only one product subtype or mechanism "
                "(e.g. 'glue board' or 'snap trap' when the category has both)\n"
                "  • targets a specific demographic, species, or location "
                "(e.g. 'for dogs', 'outdoor', 'kitchen')\n"
                "  • is an attribute modifier rather than a product name "
                "(e.g. 'reusable', 'electric', 'organic', 'heavy duty', 'scented')\n"
                "  • is a brand name, proper noun, model number, or quantity descriptor "
                "(e.g. 'Victor', 'v11', '2 pack', '12 count')\n"
                "A term is CATEGORY-LEVEL (prefer it) if a buyer using it would consider "
                "ALL or most of the top BSR products relevant results.\n"
                "Return a comma-separated list of exactly 3 terms — no explanation, no numbering."
            )
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            raw_text = res.text.strip().replace('"', "").replace("'", "").lower()
            raw_text = re.sub(r"(?m)^\s*\d+[\.\)]\s*", "", raw_text)
            raw_text = re.sub(r"(?m)^\s*[-•*]\s*", "", raw_text)
            raw_text = re.sub(r"\n+", ",", raw_text)
            raw_text = raw_text.replace(";", ",")
            parsed = [k.strip() for k in raw_text.split(",") if k.strip()]
            # Reject model-number / quantity-code patterns: terms starting with
            # 0–3 letters then a digit (v11, xr2, 4pack) or a bare digit (12 count).
            _brand_model_re = re.compile(r"^[a-z]{0,3}\d")
            llm_valid = [
                k
                for k in parsed
                if 1 <= len(k.split()) <= 5
                and not any(k.startswith(p) for p in _REFUSAL_PREFIXES)
                and k in candidates  # must be from the data-grounded set
                and not _brand_model_re.match(k)
            ]
            if len(llm_valid) >= 2:
                core_keywords = llm_valid[:3]
            else:
                logger.warning(
                    f"[fetch_core_keywords] LLM output not in candidates "
                    f"(got {llm_valid!r}); using top candidates {candidates[:3]}"
                )
    except Exception as e:
        logger.warning(f"[fetch_core_keywords] LLM refinement failed: {e}; using top candidates")

    ctx.cache["core_keywords"] = core_keywords
    ctx.cache["main_keyword"] = core_keywords[0]
    _l2_set(
        ctx,
        {"core_keywords": core_keywords, "main_keyword": core_keywords[0]},
        "core_keywords",
        cache_hash,
    )
    logger.info(
        f"[fetch_core_keywords] final={core_keywords} "
        f"(source={'intersection' if intersection_candidates else 'ngram'}, "
        f"candidates={candidates[:5]})"
    )
    return items


async def _filter_category_coherence(items: list[dict], ctx: Any) -> list[dict]:
    """
    Remove off-category products from the BSR list before any metrics are computed.

    Phase 1 — Jaccard + DBSCAN (reference-free, no core_keywords needed):
      Tokenize each title, build a pairwise Jaccard distance matrix, run DBSCAN to
      identify the dominant product cluster.  Items outside that cluster (noise points
      and minority clusters) are removed.

      Escalates to Phase 2 when:
        - No cluster is found (all items are DBSCAN noise)
        - A second cluster reaches ≥ 40% of the dominant cluster's size
          (ambiguous category mix that lexical distance cannot resolve)

    Phase 2 — LLM taxonomy (escalation only):
      Ask the router to identify the dominant product type from the top-30 titles,
      build a refined keyword set from that cluster, then re-score all items.

    Results stored in ctx.cache["contamination_stats"] for report inclusion.
    Must run BEFORE fetch_market_signals.
    """
    _MIN_RETAINED = 10
    _DBSCAN_EPS = 0.75  # Jaccard distance ceiling for two titles to be neighbours
    _DBSCAN_MIN_SAMPLES = 3  # minimum neighbourhood density to form a cluster core
    _AMBIGUOUS_RATIO = 0.40  # second cluster ≥ 40% of dominant → ambiguous, escalate
    # Stop-word list aligned with _ngram_candidates — prevents generic tokens
    # (set, pack, kit, oz…) from creating false cross-category similarity
    _STOP = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "for",
        "of",
        "in",
        "with",
        "to",
        "from",
        "by",
        "is",
        "are",
        "was",
        "be",
        "as",
        "on",
        "at",
        "up",
        "it",
        "its",
        "this",
        "that",
        "all",
        "new",
        "set",
        "pack",
        "pcs",
        "piece",
        "pieces",
        "count",
        "ct",
        "oz",
        "lb",
        "ft",
        "inch",
    }

    if not items:
        return items

    def _stem(t: str) -> str:
        # Naive plural normalisation: "traps"→"trap", "gnats"→"gnat".
        # Only strip trailing-s when len > 4 to avoid mangling short words.
        return t[:-1] if t.endswith("s") and len(t) > 4 else t

    def _tokenize(title: str) -> frozenset:
        return frozenset(
            _stem(t)
            for t in re.findall(r"[a-z0-9]+", title.lower())
            if t not in _STOP and len(t) > 1 and not t.isdigit()
        )

    def _jaccard_dist(a: frozenset, b: frozenset) -> float:
        union = len(a | b)
        return 1.0 - len(a & b) / union if union else 0.0

    token_sets = [_tokenize(item.get("Title", "")) for item in items]
    n = len(token_sets)
    _phase2_reason = ""

    # ── Phase 1: Jaccard distance matrix + DBSCAN ────────────────────────────
    try:
        import numpy as np
        from sklearn.cluster import DBSCAN as _DBSCAN

        dist = np.zeros((n, n), dtype=np.float32)
        for i in range(n):
            for j in range(i + 1, n):
                d = _jaccard_dist(token_sets[i], token_sets[j])
                dist[i, j] = dist[j, i] = d

        labels = _DBSCAN(
            eps=_DBSCAN_EPS, min_samples=_DBSCAN_MIN_SAMPLES, metric="precomputed"
        ).fit_predict(dist)

        cluster_sizes = Counter(lbl for lbl in labels if lbl >= 0)
        noise_count = int(np.sum(labels < 0))

        if not cluster_sizes:
            _phase2_reason = "no_clusters (all DBSCAN noise)"
            raise ValueError(_phase2_reason)

        sorted_clusters = cluster_sizes.most_common()
        dominant_label, dominant_size = sorted_clusters[0]
        second_size = sorted_clusters[1][1] if len(sorted_clusters) > 1 else 0

        if second_size >= _AMBIGUOUS_RATIO * dominant_size:
            _phase2_reason = f"ambiguous (dominant={dominant_size}, second={second_size})"
            raise ValueError(_phase2_reason)

        # Partition into dominant-cluster indices vs the rest
        dom_idx = [i for i, lbl in enumerate(labels) if lbl == dominant_label]
        non_idx = [i for i, lbl in enumerate(labels) if lbl != dominant_label]

        # ── Phase 1b: exclusion-token sweep ──────────────────────────────────
        # Tokens that are ≥20% more frequent in the non-dominant set than in
        # the dominant cluster are "off-category signals".  Expel any dominant-
        # cluster item whose token set contains one — this catches products that
        # leaked in through brand-name or generic-word transitive bridges
        # (e.g. "mouse" appears in 0% of gnat traps but 30% of noise items).
        _EXCL_CONTRAST = 0.20  # token must be ≥20% more frequent in non-dominant than dominant
        _EXCL_MIN_COUNT = (
            2  # token must appear in ≥2 non-dominant items (blocks 1-item brand noise)
        )
        n_non = len(non_idx)
        if n_non >= 3:
            dom_freq: Counter = Counter(tok for i in dom_idx for tok in token_sets[i])
            non_freq: Counter = Counter(tok for i in non_idx for tok in token_sets[i])
            excl_toks = frozenset(
                tok
                for tok, cnt in non_freq.items()
                if cnt >= _EXCL_MIN_COUNT
                and cnt / n_non - dom_freq.get(tok, 0) / dominant_size >= _EXCL_CONTRAST
            )
            if excl_toks:
                dom_clean = [i for i in dom_idx if not (token_sets[i] & excl_toks)]
                expelled = [i for i in dom_idx if token_sets[i] & excl_toks]
                if len(dom_clean) >= _MIN_RETAINED:
                    logger.info(
                        f"[category_coherence] Phase 1b: expelled {len(expelled)} items "
                        f"via exclusion tokens {sorted(excl_toks)[:6]}"
                    )
                    dom_idx = dom_clean
                    dominant_size = len(dom_idx)
                    non_idx = non_idx + expelled

        # ── Phase 1c: configurable + data-driven off-category token filter ──────
        # Two signal sources are unioned to form `active_contra`:
        #
        #  (A) Domain hints — injectable via ctx.cache["contra_token_hints"].
        #      Defaults to a pest-control set; callers in other verticals can
        #      supply their own (e.g. {"drill", "saw"} for a "screwdriver" BSR).
        #
        #  (B) Data-driven derivation from the non-dominant cluster: tokens
        #      that appear more often in off-category items than in the dominant
        #      cluster and are absent from core_keywords.  Works for any domain
        #      without needing explicit hints.
        #
        # A token is only contra-active when it is NOT present in core_keywords,
        # which prevents misfires (e.g. "ant" stays inactive for "ant trap" BSRs
        # even though it is in the default hint set).
        core_kw_text = " ".join(ctx.cache.get("core_keywords") or []).lower()
        core_kw_toks = frozenset(
            _stem(t) for t in re.findall(r"[a-z]+", core_kw_text) if len(t) > 2 and t not in _STOP
        )
        _DEFAULT_HINTS: frozenset = frozenset(
            {
                "mouse",
                "mice",
                "rat",
                "roach",
                "cockroach",
                "ant",
                "wasp",
                "spider",
                "snake",
                "bedbug",
            }
        )
        _hint_src = ctx.cache.get("contra_token_hints")
        hint_contra = (frozenset(_hint_src) if _hint_src else _DEFAULT_HINTS) - core_kw_toks

        # Data-driven: tokens in non-dom items with ≥20 pp frequency advantage
        # over the dominant cluster and not in core_kw_toks.
        derived_contra: frozenset = frozenset()
        if non_idx and core_kw_toks:
            _n_non = len(non_idx)
            _non_freq: Counter = Counter(tok for i in non_idx for tok in token_sets[i])
            _dom_freq2: Counter = Counter(tok for i in dom_idx for tok in token_sets[i])
            _n_dom = len(dom_idx)
            _c_floor = max(1, _n_non // 3)
            derived_contra = frozenset(
                tok
                for tok, cnt in _non_freq.items()
                if cnt >= _c_floor
                and tok not in core_kw_toks
                and cnt / _n_non - _dom_freq2.get(tok, 0) / max(_n_dom, 1) >= 0.20
            )

        active_contra = hint_contra | derived_contra
        _phase1c_fired = False
        if active_contra and core_kw_toks:
            dom_1c = [i for i in dom_idx if not (token_sets[i] & active_contra)]
            expelled_1c = [i for i in dom_idx if token_sets[i] & active_contra]
            if len(dom_1c) >= max(5, _MIN_RETAINED // 2) and expelled_1c:
                triggered = sorted(
                    active_contra & frozenset().union(*(token_sets[i] for i in expelled_1c))
                )
                logger.info(
                    f"[category_coherence] Phase 1c: expelled {len(expelled_1c)} items "
                    f"via contra-tokens {triggered[:5]}"
                )
                dom_idx = dom_1c
                dominant_size = len(dom_idx)
                non_idx = non_idx + expelled_1c
                _phase1c_fired = True
        # ─────────────────────────────────────────────────────────────────────

        # Clean: DBSCAN found a single cluster with no noise AND neither
        # Phase 1b nor 1c expelled anything — nothing to filter.
        if noise_count == 0 and len(cluster_sizes) == 1 and len(non_idx) == 0:
            ctx.cache["contamination_stats"] = {
                "status": "clean",
                "method": "dbscan_jaccard",
                "n_removed": 0,
                "n_retained": n,
            }
            return items

        kept = [items[i] for i in dom_idx]
        removed = [items[i] for i in non_idx]

        # Phase 1c is semantically grounded (pest type ≠ category keyword), so
        # its results warrant a lower floor than the statistical DBSCAN minimum.
        effective_min = max(5, _MIN_RETAINED // 2) if _phase1c_fired else _MIN_RETAINED
        if len(kept) < effective_min:
            ctx.cache["contamination_stats"] = {
                "status": "warning",
                "method": "dbscan_jaccard",
                "outlier_rate": round(len(removed) / n, 3),
                "n_removed": 0,
                "n_retained": n,
                "note": f"dominant cluster too small ({len(kept)}) — filtering skipped",
                "sample_outliers": [it.get("Title", "")[:60] for it in removed[:5]],
            }
            return items

        sample = [it.get("Title", "")[:60] for it in removed[:5]]
        ctx.cache["contamination_stats"] = {
            "status": "filtered",
            "method": "dbscan_jaccard",
            "outlier_rate": round(len(removed) / n, 3),
            "dominant_cluster_size": dominant_size,
            "noise_count": noise_count,
            "n_removed": len(removed),
            "n_retained": len(kept),
            "sample_removed": sample,
        }
        logger.info(
            f"[category_coherence] DBSCAN: dominant={dominant_size}/{n}, "
            f"removed={len(removed)} ({len(removed) / n:.0%}), noise={noise_count}; "
            f"sample: {sample[:2]}"
        )
        return kept

    except ImportError:
        _phase2_reason = "sklearn/numpy unavailable"
        logger.warning(f"[category_coherence] Phase 1 skipped: {_phase2_reason}")
    except ValueError as exc:
        logger.info(f"[category_coherence] Phase 1 → Phase 2: {exc}")

    # ── Phase 2: LLM taxonomy (escalation only) ───────────────────────────────
    if ctx.router:
        try:
            classify_titles = [
                item.get("Title", f"(no title #{i + 1})") for i, item in enumerate(items[:30])
            ]
            numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(classify_titles))
            prompt = (
                "You are a product taxonomy assistant. The list shows Amazon BSR product "
                "titles from a category page that contains mixed product types.\n\n"
                f"{numbered}\n\n"
                "Identify the single DOMINANT product type (the majority group).\n"
                "Return exactly two lines — no other text:\n"
                "Line 1: dominant product type name (3–7 words, e.g. 'mouse snap trap')\n"
                "Line 2: comma-separated 1-based index numbers of titles in that type"
            )
            res = await ctx.router.route_and_execute(prompt, category=TaskCategory.SIMPLE_CLEANING)
            lines = [ln.strip() for ln in res.text.strip().splitlines() if ln.strip()]
            if len(lines) >= 2:
                dominant_type = lines[0].strip("\"'").lower()
                keep_idx = {
                    int(x) - 1
                    for x in re.findall(r"\d+", lines[1])
                    if x.isdigit() and 0 <= int(x) - 1 < len(classify_titles)
                }
                if len(keep_idx) >= _MIN_RETAINED:
                    dom_titles = [items[i].get("Title", "") for i in sorted(keep_idx)]
                    refined_ngrams = set(_ngram_candidates(dom_titles, min_doc_freq=2, top_n=15))
                    refined_tokens = {
                        tok for ng in refined_ngrams for tok in ng.split() if len(tok) > 2
                    }
                    refined_tokens |= {tok for tok in dominant_type.split() if len(tok) > 2}

                    def _refined_score(title: str) -> int:
                        t = title.lower()
                        if dominant_type and dominant_type in t:
                            return 3
                        for ng in refined_ngrams:
                            if len(ng.split()) >= 2 and ng in t:
                                return 2
                        t_toks = set(re.findall(r"[a-z0-9]+", t))
                        overlap = refined_tokens & t_toks
                        return len(overlap) if len(overlap) >= 2 else 0

                    refined_scored = [(it, _refined_score(it.get("Title", ""))) for it in items]
                    retained = [it for it, s in refined_scored if s > 0]
                    if len(retained) < _MIN_RETAINED:
                        retained = items
                    n_removed = len(items) - len(retained)
                    sample = [it.get("Title", "")[:60] for it, s in refined_scored if s == 0][:5]
                    ctx.cache["contamination_stats"] = {
                        "status": "filtered",
                        "method": "llm_taxonomy",
                        "dominant_type": dominant_type,
                        "phase2_reason": _phase2_reason,
                        "outlier_rate": round(n_removed / len(items), 3),
                        "n_removed": n_removed,
                        "n_retained": len(retained),
                        "sample_removed": sample,
                    }
                    logger.info(
                        f"[category_coherence] LLM taxonomy: dominant='{dominant_type}', "
                        f"retained {len(retained)}/{len(items)}, removed {n_removed}"
                    )
                    return retained
        except Exception as e:
            logger.warning(f"[category_coherence] Phase 2 LLM failed: {e}")

    # Fallback: both phases failed or were unsafe → warn only
    sample = [it.get("Title", "")[:60] for it in items[:5]]
    ctx.cache["contamination_stats"] = {
        "status": "warning",
        "n_removed": 0,
        "n_retained": n,
        "sample_outliers": sample,
        "note": f"filtering skipped ({_phase2_reason or 'both phases failed'})",
    }
    logger.warning(
        f"[category_coherence] Filtering skipped ({_phase2_reason or 'both phases failed'})"
    )
    return items


async def _fetch_market_signals(items: list[dict], ctx: Any) -> list[dict]:
    """
    Step 2 of 2 for market context. Depends on ctx.cache["core_keywords"].
    Runs ABA keyword data, SERP ad ratio, and CPC bid recommendations concurrently.
    Writes: ctx.cache["keyword_data"], ctx.cache["ad_ratio"],
            ctx.cache["detailed_bid_analysis"]
    """
    core_keywords = ctx.cache.get("core_keywords", ["unknown niche"])
    main_keyword = core_keywords[0]
    kw_hash = _hl.md5(",".join(sorted(core_keywords)).encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_SIGNALS, "market_signals", kw_hash)
    if cached is not None:
        ctx.cache["keyword_data"] = cached.get("keyword_data", {})
        ctx.cache["ad_ratio"] = cached.get("ad_ratio", 0.3)
        ctx.cache["detailed_bid_analysis"] = cached.get("detailed_bid_analysis", {})
        logger.info(f"[cat_monopoly] Market signals L2 cache hit kw_hash={kw_hash}")
        return items

    async def _fetch_aba() -> None:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        try:
            aba_res = await asyncio.to_thread(
                XiyouZhaociAPI(tenant_id=tenant_id).get_aba_top_asins, country, [main_keyword]
            )
            ctx.cache["keyword_data"] = (
                aba_res["searchTerms"][0] if aba_res and aba_res.get("searchTerms") else {}
            )
        except Exception as e:
            logger.error(f"[fetch_market_signals] ABA fetch failed: {e}")
            ctx.cache.setdefault("keyword_data", {})

    async def _fetch_ad_ratio() -> None:
        try:
            search_results = await SearchExtractor().search(main_keyword, page=1)
            sponsored = sum(1 for r in search_results if getattr(r, "is_sponsored", False))
            ctx.cache["ad_ratio"] = sponsored / (len(search_results) or 1)
        except Exception as e:
            logger.error(f"[fetch_market_signals] SERP ad ratio fetch failed: {e}")
            ctx.cache.setdefault("ad_ratio", 0.3)

    async def _fetch_cpc_bids() -> None:
        try:
            ads_client = AmazonAdsClient(store_id=ctx.config.get("store_id"))
            kws = [
                {"keyword": kw, "matchType": m} for kw in core_keywords for m in ("EXACT", "PHRASE")
            ]
            # Do NOT pass competitor BSR ASINs — the bid API requires an ASIN owned
            # by the advertiser. The client will auto-discover one via _get_owned_asin_fallback().
            # Two separate calls — the API only accepts one strategy string per request.
            legacy_res, auto_res = await asyncio.gather(
                ads_client.get_keyword_bid_recommendations(
                    keywords=kws, strategy="LEGACY_FOR_SALES"
                ),
                ads_client.get_keyword_bid_recommendations(keywords=kws, strategy="AUTO_FOR_SALES"),
                return_exceptions=True,
            )
            ctx.cache["detailed_bid_analysis"] = {
                "LEGACY_FOR_SALES": (
                    legacy_res.get("bidRecommendations", [])
                    if not isinstance(legacy_res, Exception)
                    else []
                ),
                "AUTO_FOR_SALES": (
                    auto_res.get("bidRecommendations", [])
                    if not isinstance(auto_res, Exception)
                    else []
                ),
            }
            if isinstance(legacy_res, Exception):
                logger.error(f"[fetch_market_signals] LEGACY bid fetch failed: {legacy_res}")
            if isinstance(auto_res, Exception):
                logger.error(f"[fetch_market_signals] AUTO bid fetch failed: {auto_res}")
        except Exception as e:
            logger.error(f"[fetch_market_signals] CPC bid fetch failed: {e}")
            ctx.cache.setdefault("detailed_bid_analysis", {})

    await asyncio.gather(_fetch_aba(), _fetch_ad_ratio(), _fetch_cpc_bids())
    _l2_set(
        ctx,
        {
            "keyword_data": ctx.cache.get("keyword_data", {}),
            "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
            "detailed_bid_analysis": ctx.cache.get("detailed_bid_analysis", {}),
        },
        "market_signals",
        kw_hash,
    )
    return items


_SOCIAL_PLATFORMS = ["tiktok", "youtube"]
_SOCIAL_MAX_RETRIES = 2


async def _enrich_external_intensity(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetches multi-platform social signals and deal promotion intensity for the category.

    Platforms searched: TikTok, YouTube Shorts (concurrently per hashtag).
    Hashtags searched:
      1. Category keyword hashtag  (e.g. #gnattrapsforhouseindoor)
      2. Top-3 brand hashtags by BSR frequency (e.g. #ZEVO, #Catchmaster)
      3. New-entrant brand hashtags (products listed in last 12 months, up to 5)
    Brand data requires fetch_sellersprite_bsr to have run first (previous step).
    """
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword:
        return items

    # ── brand sets from Sellersprite snapshot ────────────────────────────────
    _MAX_TOP_BRANDS = 3
    _MAX_NEW_ENTRANT_BRANDS = 5
    ss_snapshots = ctx.cache.get("sellersprite_snapshots") or {}
    _top_brand_set: set[str] = set()
    _new_entrant_brand_set: set[str] = set()

    if ss_snapshots:
        base_ym = ctx.cache.get("sellersprite_base_ym", "")
        _latest_snap = ss_snapshots.get(max(ss_snapshots), [])

        # Top brands by product count in BSR
        _brand_counts: Counter = Counter(p["brand"] for p in _latest_snap if p.get("brand"))
        _top_brand_set = {b for b, _ in _brand_counts.most_common(_MAX_TOP_BRANDS) if b}

        # New-entrant brands: products listed within the last 12 months
        # Anchor cutoff to base_ym (T-2 months) to match _run_monopoly_analysis logic
        if base_ym and len(base_ym) == 6:
            _by, _bm = int(base_ym[:4]), int(base_ym[4:])
            _total = _by * 12 + (_bm - 1) - 12
            _cy, _cm = _total // 12, _total % 12 + 1
            _cutoff_ms = calendar.timegm((_cy, _cm, 1, 0, 0, 0)) * 1000
        else:
            _cutoff_ms = (time.time() - 365 * 86400) * 1000

        _new_entrant_products = [
            p
            for p in _latest_snap
            if p.get("available_date_ms")
            and p["available_date_ms"] >= _cutoff_ms
            and p.get("brand")
        ]
        # Preserve rank order (lower rank = higher BSR position = more relevant)
        _seen: set[str] = set()
        for _p in sorted(_new_entrant_products, key=lambda x: x.get("rank") or 999):
            if _p["brand"] not in _seen and len(_seen) < _MAX_NEW_ENTRANT_BRANDS:
                _new_entrant_brand_set.add(_p["brand"])
                _seen.add(_p["brand"])

    # All brands to search: top brands ∪ new entrants, deduped, top brands first
    _all_brand_list = list(
        dict.fromkeys(sorted(_top_brand_set) + sorted(_new_entrant_brand_set - _top_brand_set))
    )

    kw_hash = _hl.md5(
        (main_keyword + "|" + ",".join(sorted(_all_brand_list))).encode()
    ).hexdigest()[:12]

    # ── helpers (defined before cache check so partial-retry can reuse them) ──

    @exponential_backoff(
        max_retries=_SOCIAL_MAX_RETRIES, base_delay=1.0, retry_on_exceptions=(Exception,)
    )
    async def _platform_psi(tag: str, platform: str) -> dict:
        """Fetch PSI for one hashtag on one platform. Non-retryable soft failures
        (no tag, unsupported platform) return a result dict; retryable network/API
        errors are raised so the decorator can back off and retry."""
        clean = re.sub(r"[^a-zA-Z0-9]", "", tag)
        result_base = {
            "platform": platform,
            "hashtag": f"#{clean}",
            "psi": 0,
            "video_count": 0,
            "n_kol": 0,
            "n_koc": 0,
            "unique_creators": 0,
        }
        if platform == "tiktok":
            client = TikTokClient()
            tag_info = await asyncio.to_thread(client.get_tag_info, clean)
            if not tag_info.get("id"):
                return {**result_base, "verdict": "No Tag Found"}
            videos = await asyncio.to_thread(
                client.get_hashtag_videos, tag_info["id"], clean, count=20
            )
            # TikTok's challenge page aggregates partial/superset/fuzzy tags server-side,
            # so raw results include videos for #BAIM, #BAIMNOCMSJF, #BGAIMSNOCM when
            # searching #BAIMNOCM.  Keep only videos whose desc contains the exact tag
            # as a standalone token (not preceded or followed by another alphanumeric char).
            _exact = re.compile(
                r"(?<![a-zA-Z0-9])#" + re.escape(clean) + r"(?![a-zA-Z0-9])",
                re.IGNORECASE,
            )
            videos = [v for v in videos if _exact.search(v.get("desc", ""))]
            if not videos:
                return {**result_base, "verdict": "No Exact Tag Match"}
            analysis = SocialViralityProcessor().calculate_promotion_strength(
                videos, tag_metadata=tag_info, platform="tiktok"
            )
        elif platform == "youtube":
            client = YouTubeClient()
            tag_info = await asyncio.to_thread(client.get_hashtag_info, clean)
            if not tag_info.get("video_count"):
                return {**result_base, "verdict": "No Tag Found"}
            videos = await asyncio.to_thread(client.get_hashtag_videos, clean, count=20)
            # get_hashtag_videos returns raw ytInitialData videoRenderer dicts;
            # "youtube_hashtag" is the correct normalizer (not "youtube_shorts" which
            # expects YouTube Data API v3 statistics/snippet fields).
            analysis = SocialViralityProcessor().calculate_promotion_strength(
                videos, tag_metadata=tag_info, platform="youtube_hashtag"
            )
        else:
            return {**result_base, "verdict": "Unsupported"}
        _kol_koc = analysis.get("kol_koc_matrix", {})
        return {
            "platform": platform,
            "hashtag": f"#{clean}",
            "psi": analysis.get("strength_score", 0),
            "verdict": analysis.get("verdict", "Unknown"),
            "video_count": len(videos),
            "n_kol": _kol_koc.get("n_kol_unique", 0),
            "n_koc": _kol_koc.get("n_koc_unique", 0),
            "unique_creators": _kol_koc.get("unique_creators_total", 0),
        }

    async def _tag_all_platforms(tag: str) -> dict:
        """Search one hashtag across all platforms concurrently.
        Each platform call retries internally via @exponential_backoff; only if
        retries are exhausted does gather capture the exception here."""
        clean = re.sub(r"[^a-zA-Z0-9]", "", tag)
        raw = await asyncio.gather(
            *[_platform_psi(tag, p) for p in _SOCIAL_PLATFORMS],
            return_exceptions=True,
        )
        platform_results = []
        for p, result in zip(_SOCIAL_PLATFORMS, raw, strict=False):
            if isinstance(result, Exception):
                logger.warning(f"[external_intensity] {p} #{clean} exhausted retries: {result}")
                platform_results.append(
                    {
                        "platform": p,
                        "hashtag": f"#{clean}",
                        "psi": 0,
                        "verdict": "Error",
                        "video_count": 0,
                        "n_kol": 0,
                        "n_koc": 0,
                        "unique_creators": 0,
                    }
                )
            else:
                platform_results.append(result)
        best = max(platform_results, key=lambda x: x["psi"])
        return {
            "psi": best["psi"],
            "verdict": best["verdict"],
            "video_count": sum(r["video_count"] for r in platform_results),
            "n_kol": sum(r.get("n_kol", 0) for r in platform_results),
            "n_koc": sum(r.get("n_koc", 0) for r in platform_results),
            "unique_creators": sum(r.get("unique_creators", 0) for r in platform_results),
            "platforms": platform_results,
        }

    def _collect_failed(results: list[dict]) -> list[tuple]:
        """Return (result_idx, platform_idx) pairs where verdict == 'Error'."""
        return [
            (ri, pi)
            for ri, r in enumerate(results)
            for pi, p in enumerate(r.get("platforms", []))
            if p.get("verdict") == "Error"
        ]

    def _recompute_aggregate(result: dict) -> None:
        """Recompute top-level aggregates from platforms list in-place."""
        platforms = result["platforms"]
        best = max(platforms, key=lambda x: x["psi"])
        result.update(
            {
                "psi": best["psi"],
                "verdict": best["verdict"],
                "video_count": sum(p["video_count"] for p in platforms),
                "n_kol": sum(p.get("n_kol", 0) for p in platforms),
                "n_koc": sum(p.get("n_koc", 0) for p in platforms),
                "unique_creators": sum(p.get("unique_creators", 0) for p in platforms),
            }
        )

    # ── L2 cache check ────────────────────────────────────────────────────────
    cached = _l2_get(ctx, _TTL_EXTERNAL, "external_intensity", kw_hash)
    if cached is not None:
        # Reconstruct tag_results shape from cache to reuse retry logic uniformly
        _c_kw = {
            "psi": cached.get("category_social_psi", 0),
            "verdict": cached.get("category_social_verdict", "Unknown"),
            "video_count": sum(
                p.get("video_count", 0) for p in cached.get("category_social_platforms", [])
            ),
            "platforms": [dict(p) for p in cached.get("category_social_platforms", [])],
        }
        _c_brands_raw = cached.get("brand_social_data", [])
        _c_brand_results = [
            {
                "psi": e["psi"],
                "verdict": e["verdict"],
                "video_count": e["video_count"],
                "platforms": [dict(p) for p in e.get("platforms", [])],
            }
            for e in _c_brands_raw
        ]
        _c_all = [_c_kw] + _c_brand_results
        _c_tags = [main_keyword.replace(" ", "")] + [e["brand"] for e in _c_brands_raw]

        if not _collect_failed(_c_all):
            ctx.cache.update(cached)
            logger.info(f"[cat_monopoly] External intensity L2 cache hit kw_hash={kw_hash}")
            return items

        _failed = _collect_failed(_c_all)
        logger.info(
            f"[cat_monopoly] Cache hit but {len(_failed)} platform error(s) found, "
            f"re-fetching kw_hash={kw_hash}"
        )
        # Call _platform_psi directly — @exponential_backoff handles retries per call.
        retry_raw = await asyncio.gather(
            *[
                _platform_psi(_c_tags[ri], _c_all[ri]["platforms"][pi]["platform"])
                for ri, pi in _failed
            ],
            return_exceptions=True,
        )
        for (ri, pi), result in zip(_failed, retry_raw, strict=False):
            if isinstance(result, Exception):
                logger.warning(
                    f"[external_intensity] {_c_all[ri]['platforms'][pi]['platform']} "
                    f"#{_c_tags[ri]} still failing after retries: {result}"
                )
            else:
                _c_all[ri]["platforms"][pi] = result
            _recompute_aggregate(_c_all[ri])

        _upd_brand_data = [
            {
                **{
                    k: v
                    for k, v in e.items()
                    if k not in ("psi", "verdict", "video_count", "platforms")
                },
                **tr,
            }
            for e, tr in zip(_c_brands_raw, _c_all[1:], strict=False)
        ]
        _upd_brand_data.sort(key=lambda x: x["psi"], reverse=True)
        _upd_cached = {
            **cached,
            "category_social_psi": _c_all[0]["psi"],
            "category_social_verdict": _c_all[0]["verdict"],
            "category_social_platforms": _c_all[0]["platforms"],
            "brand_social_data": _upd_brand_data,
        }
        ctx.cache.update(_upd_cached)
        _still_errors = len(_collect_failed(_c_all))
        if _still_errors == 0:
            _l2_set(ctx, _upd_cached, "external_intensity", kw_hash)
            logger.info(
                f"[external_intensity] Partial re-fetch succeeded, cache refreshed kw_hash={kw_hash}"
            )
        else:
            logger.warning(
                f"[external_intensity] {_still_errors} platform(s) still failing after retries, "
                f"skipping cache write kw_hash={kw_hash}"
            )
        return items

    # ── fresh fetch ───────────────────────────────────────────────────────────
    kw_tag = main_keyword.replace(" ", "")
    all_tags = [kw_tag] + _all_brand_list
    tag_results = list(await asyncio.gather(*[_tag_all_platforms(t) for t in all_tags]))
    # _tag_all_platforms already exhausted per-platform retries via @exponential_backoff;
    # any remaining "Error" entries represent genuinely unrecoverable failures.

    kw_result = tag_results[0]
    brand_results = [
        {
            "brand": brand,
            "is_top_brand": brand in _top_brand_set,
            "is_new_entrant": brand in _new_entrant_brand_set,
            **res,
        }
        for brand, res in zip(_all_brand_list, tag_results[1:], strict=False)
    ]
    brand_results.sort(key=lambda x: x["psi"], reverse=True)

    ctx.cache.update(
        {
            "category_social_psi": kw_result["psi"],
            "category_social_verdict": kw_result["verdict"],
            "category_social_platforms": kw_result["platforms"],
            "brand_social_data": brand_results,
        }
    )
    _platform_summary = ", ".join(
        r["platform"] + "=" + str(r["psi"]) for r in kw_result["platforms"]
    )
    _brand_summary = " | ".join(
        "#"
        + re.sub(r"[^a-zA-Z0-9]", "", r["brand"])
        + "("
        + ("top" if r["is_top_brand"] else "")
        + ("/" if r["is_top_brand"] and r["is_new_entrant"] else "")
        + ("new" if r["is_new_entrant"] else "")
        + ")"
        + " PSI="
        + str(r["psi"])
        for r in brand_results
    )
    logger.info(
        f"[external_intensity] #{kw_tag} PSI={kw_result['psi']} ({_platform_summary})"
        + (f" | {_brand_summary}" if _brand_summary else "")
    )

    # ── deal intensity ────────────────────────────────────────────────────────
    async def fetch_deal_count(item):
        return len(
            await DealHistoryClient().get_deal_history(
                asin=item.get("ASIN", ""), keyword=item.get("Title", ""), max_pages=1
            )
        )

    try:
        deal_counts = await asyncio.gather(*(fetch_deal_count(item) for item in items[:10]))
        total_deals = sum(deal_counts)
        ctx.cache["category_deal_intensity"] = (
            9 if total_deals > 5 else 6 if total_deals > 2 else 3 if total_deals > 0 else 0
        )
    except Exception as e:
        logger.error(f"[external_intensity] Deal intensity: {e}")

    _ext = {
        "category_social_psi": ctx.cache.get("category_social_psi", 0),
        "category_social_verdict": ctx.cache.get("category_social_verdict", "Unknown"),
        "category_social_platforms": ctx.cache.get("category_social_platforms", []),
        "category_deal_intensity": ctx.cache.get("category_deal_intensity", 0),
        "brand_social_data": ctx.cache.get("brand_social_data", []),
    }
    _remaining_errors = len(_collect_failed(tag_results))
    if _remaining_errors == 0:
        _l2_set(ctx, _ext, "external_intensity", kw_hash)
    else:
        logger.warning(
            f"[external_intensity] {_remaining_errors} platform(s) still failing after retries, "
            f"skipping cache write kw_hash={kw_hash}"
        )
    logger.info(
        f"External intensity: Social PSI={_ext['category_social_psi']}, "
        f"Deal Intensity={_ext['category_deal_intensity']}, "
        f"Brands searched: {len(_ext['brand_social_data'])}"
    )
    return items


async def _fetch_historical_trends(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 12-month daily BSR, rating, and price time-series for Top 20 ASINs.

    Stored fields per day: date, bsr, stars, ratings, price.
    Consumers:
      - CategoryMonopolyAnalyzer._analyze_market_churn()  (bsr)
      - CategoryMonopolyAnalyzer._analyze_seasonality()   (bsr)
      - _run_monopoly_analysis price-trend block           (price: first-30d vs last-30d median)
    Runs concurrently; failures are soft-skipped so the workflow is never blocked.
    """
    top_asins = [
        (item.get("ASIN") or item.get("asin"))
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    ]
    if not top_asins:
        return items

    _tz = ZoneInfo(
        ctx.config.get("timezone", "America/Los_Angeles")
        if hasattr(ctx, "config")
        else "America/Los_Angeles"
    )
    _now = datetime.now(tz=_tz)
    end_date = (_now - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (_now - timedelta(days=365)).strftime("%Y-%m-%d")
    country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
    api = XiyouZhaociAPI(tenant_id=tenant_id)
    historical_data: dict[str, list[dict[str, Any]]] = {}

    def _parse_daily_records(res: dict, asin: str) -> list:
        """
        Extract daily records from the Xiyouzhaoci get_asin_daily_trends response.

        Actual API shape (confirmed via test_xiyou_date_limits.py):
          res["entities"][i]["trends"][j]["localDate" | "date", "bsr_rank", "rating",
                                          "review_count", "price"]

        Legacy / alternative shapes also handled:
          res["data"]["entities"][i]["dailyData"]
          res["data"][asin]["dailyData"]
          res["data"]   (flat list)

        Returns a normalised list of {"date", "bsr", "stars", "ratings", "price"}.
        """

        def _normalise(daily_list: list) -> list:
            out = []
            for d in daily_list:
                # Date field: actual API uses "localDate"; legacy used "date"
                raw_date = d.get("localDate") or d.get("date") or ""
                date_str = str(raw_date)[:10]
                if not date_str or date_str == "N":
                    continue
                out.append(
                    {
                        "date": date_str,
                        # Actual API: "bsr_rank"; legacy: "bsr" / "bestSellerRank"
                        "bsr": d.get("bsr_rank") or d.get("bsr") or d.get("bestSellerRank"),
                        # Actual API: "rating"; legacy: "stars" / "avgStarRating"
                        "stars": d.get("rating") or d.get("stars") or d.get("avgStarRating"),
                        # Actual API: "review_count"; legacy: "ratings" / "reviewCount"
                        "ratings": d.get("review_count")
                        or d.get("ratings")
                        or d.get("reviewCount"),
                        "price": d.get("price"),
                    }
                )
            return out

        def _entity_trends(entity: dict) -> list:
            # Actual API uses "trends"; legacy used "dailyData"
            return entity.get("trends") or entity.get("dailyData") or []

        # ── Actual shape: {"entities": [...]} at top level (no "data" wrapper) ──
        top_entities = res.get("entities")
        if isinstance(top_entities, list):
            for entity in top_entities:
                if entity.get("asin") == asin:
                    return _normalise(_entity_trends(entity))

        data = res.get("data") or {}

        # ── Legacy shape A: {"data": {"entities": [...]}} ──
        if isinstance(data, dict) and "entities" in data:
            for entity in data["entities"] or []:
                if entity.get("asin") == asin:
                    return _normalise(_entity_trends(entity))

        # ── Legacy shape B: {"data": {"B0xxx": {"dailyData": [...]}}} ──
        if isinstance(data, dict) and asin in data:
            asin_data = data[asin]
            daily = (
                _entity_trends(asin_data)
                if isinstance(asin_data, dict)
                else (asin_data if isinstance(asin_data, list) else [])
            )
            return _normalise(daily)

        # ── Legacy shape C: {"data": [...]} flat list ──
        if isinstance(data, list):
            return _normalise(data)

        logger.debug(
            f"[historical_trends] Unrecognised response shape for {asin}: "
            f"top-level keys={list(res.keys())}"
        )
        return []

    async def _fetch_one(asin: str) -> None:
        try:
            res = await asyncio.to_thread(
                api.get_asin_daily_trends, country, asin, start_date, end_date
            )
            records = _parse_daily_records(res, asin)
            if records:
                historical_data[asin] = records
            else:
                logger.debug(
                    f"[historical_trends] No records parsed for {asin}; top-level keys: {list(res.keys())}"
                )
        except Exception as e:
            logger.warning(f"Historical trend fetch skipped for {asin}: {e}")

    await asyncio.gather(*[_fetch_one(asin) for asin in top_asins])
    ctx.cache["historical_data"] = historical_data
    logger.info(
        f"Historical trends fetched: {len(historical_data)}/{len(top_asins)} ASINs ({start_date} → {end_date})"
    )
    return items


async def _enrich_batch_traffic_scores(items: list[dict], ctx: Any) -> list[dict]:
    """Fetches batch traffic scores for Top 20 ASINs to calculate average ad dependency."""
    if not items or not ctx.mcp:
        return items

    top_asins = sorted(
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    )
    if not top_asins:
        return items

    asins_hash = _hl.md5(",".join(top_asins).encode()).hexdigest()[:12]
    cached = _l2_get(ctx, _TTL_TRAFFIC, "traffic_scores", asins_hash)
    if cached is not None:
        ctx.cache["actual_bsr_ad_ratio"] = cached["actual_bsr_ad_ratio"]
        logger.info(f"[cat_monopoly] Traffic scores L2 cache hit asins_hash={asins_hash}")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        resp = await ctx.mcp.call_tool_json(
            "xiyou_get_traffic_scores", {"asins": top_asins, "country": country}
        )
        if isinstance(resp, list) and len(resp) > 0:
            data = json.loads(resp[0].get("text", "{}"))
            if data.get("success") and data.get("data"):
                ratios = [d.get("advertisingTrafficScoreRatio", 0.0) for d in data["data"]]
                if ratios:
                    avg_ratio = statistics.mean(ratios)
                    ctx.cache["actual_bsr_ad_ratio"] = avg_ratio
                    _l2_set(ctx, {"actual_bsr_ad_ratio": avg_ratio}, "traffic_scores", asins_hash)
                    logger.info(f"Calculated average BSR ad dependency: {avg_ratio:.2%}")
    except Exception as e:
        logger.error(f"Failed to fetch batch traffic scores: {e}")
    return items


async def _fetch_keyword_weekly_trends(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 3-year weekly search-volume trends for the primary keyword via
    XiyouZhaociAPI.get_search_term_trends().

    Stored in ctx.cache["keyword_weekly_trends"] for the monopoly analyzer.
    When present, the analyzer uses keyword-based seasonality (more direct
    demand signal) instead of BSR-proxy seasonality.

    Soft-fails: login errors or missing auth do not block the workflow.
    """
    main_keyword = ctx.cache.get("main_keyword")
    if not main_keyword:
        logger.warning("[keyword_weekly_trends] No main_keyword in cache; skipping")
        return items

    try:
        country = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
        tenant_id = ctx.config.get("tenant_id", "default") if hasattr(ctx, "config") else "default"
        api = XiyouZhaociAPI(tenant_id=tenant_id)
        res = await asyncio.to_thread(
            api.get_search_term_trends,
            country=country,
            search_term=main_keyword,
        )
        terms = (res or {}).get("searchTerms") or []
        if terms and terms[0].get("trends", {}).get("weekSearch"):
            ctx.cache["keyword_weekly_trends"] = res
            week_count = len(terms[0]["trends"]["weekSearch"])
            logger.info(
                f"[keyword_weekly_trends] Fetched {week_count} weekly data points "
                f"for '{main_keyword}'"
            )
        else:
            logger.warning(f"[keyword_weekly_trends] Empty trends data for '{main_keyword}'")
    except Exception as e:
        logger.warning(f"[keyword_weekly_trends] Failed, seasonality will use BSR proxy: {e}")

    return items


async def _fetch_time_series_data(items: list[dict], ctx: Any) -> list[dict]:
    """
    Parallel wrapper: runs _fetch_historical_trends and _fetch_keyword_weekly_trends
    concurrently. Both are independent XiyouZhaoci reads with no mutual dependency.
    Replaces the two sequential steps to save ~5-10s of serial wait time.
    """
    top_asins = sorted(
        (item.get("ASIN") or item.get("asin") or "").strip().upper()
        for item in items[:20]
        if (item.get("ASIN") or item.get("asin"))
    )
    main_keyword = ctx.cache.get("main_keyword", "")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    ts_hash = _hl.md5(
        (",".join(top_asins) + main_keyword + start_date + end_date).encode()
    ).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_TIMESERIES, "time_series", ts_hash)
    if cached is not None:
        ctx.cache["historical_data"] = cached.get("historical_data", {})
        ctx.cache["keyword_weekly_trends"] = cached.get("keyword_weekly_trends")
        logger.info(f"[cat_monopoly] Time series L2 cache hit ts_hash={ts_hash}")
        return items

    await asyncio.gather(
        _fetch_historical_trends(items, ctx),
        _fetch_keyword_weekly_trends(items, ctx),
    )
    _l2_set(
        ctx,
        {
            "historical_data": ctx.cache.get("historical_data", {}),
            "keyword_weekly_trends": ctx.cache.get("keyword_weekly_trends"),
        },
        "time_series",
        ts_hash,
    )
    return items


async def _fetch_sellersprite_bsr(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch 4 monthly BSR snapshots from Sellersprite to calculate
    (true list churn rate): T, T-3, T-6, T-12 months.

    Each snapshot stores only the core fields needed for set comparison:
        {"asin", "rank", "brand"}
    Stored in ctx.cache["sellersprite_snapshots"] as Dict[YYYYMM, List[dict]].

    Churn rate = fraction of ASINs in T that were NOT present N months ago.
    Soft-fails: missing auth or API errors do not block the workflow.
    """
    url = ctx.config.get("url", "")
    m = re.search(r"/(?:gp/bestsellers|zgbs)/[^/]+/(\d+)", url)
    if not m:
        logger.warning("[sellersprite_bsr] Could not extract node ID from URL; skipping")
        return items

    node_id = m.group(1)
    store_id = ctx.config.get("store_id", "US")
    market_id = {"US": 1, "DE": 6, "JP": 8, "UK": 3, "FR": 4, "IT": 5, "ES": 7, "CA": 2}.get(
        store_id, 1
    )

    def month_offset(base_ym: str, delta: int) -> str:
        """Return YYYYMM string `delta` months before base_ym."""
        y, mo = int(base_ym[:4]), int(base_ym[4:])
        total = y * 12 + (mo - 1) - delta
        return f"{total // 12:04d}{total % 12 + 1:02d}"

    # Base = 2 months prior (ensures Sellersprite data is fully published)
    now = datetime.now()
    base_y = now.year if now.month > 2 else now.year - 1
    base_mo = now.month - 2 if now.month > 2 else now.month + 10
    base_ym = f"{base_y:04d}{base_mo:02d}"

    # 4 snapshot keys: T, T-3, T-6, T-12
    snapshot_yms = [
        base_ym,
        month_offset(base_ym, 3),
        month_offset(base_ym, 6),
        month_offset(base_ym, 12),
    ]

    try:
        ss_cache_key = f"{node_id}:{base_ym}"
        cached = _l2_get(ctx, _TTL_SS_BSR, "ss_bsr", ss_cache_key)
        if cached is not None:
            ctx.cache["sellersprite_snapshots"] = cached.get("snapshots", {})
            ctx.cache["sellersprite_base_ym"] = cached.get("base_ym", base_ym)
            if cached.get("ss_median_cvr"):
                ctx.cache["ss_median_cvr"] = cached["ss_median_cvr"]
            if cached.get("ss_node_id_path"):
                ctx.cache["ss_node_id_path"] = cached["ss_node_id_path"]
                ctx.cache["ss_market_id"] = cached.get("ss_market_id", market_id)
            logger.info(
                f"[cat_monopoly] Sellersprite BSR L2 cache hit node={node_id} base_ym={base_ym}"
            )
            return items

        tenant_id = ctx.config.get("tenant_id", "default")
        api = SellerspriteAPI(tenant_id=tenant_id)
        if not api.auth_token:
            logger.warning("[sellersprite_bsr] No auth token; skipping")
            return items

        # Resolve the full nodeIdPath by searching each snapshot table.
        # Primary: bare Amazon numeric node_id (works when it appears in SellerSprite's
        #   nodeIdPath, i.e. the SellerSprite internal ID matches Amazon's).
        # Fallback: label-based search derived from the URL slug, tried from shortest
        #   leaf label to progressively wider prefixes.  Needed when Amazon's short
        #   node ID (e.g. 3741941 for Air Fryers) differs from SellerSprite's internal
        #   IDs (e.g. 17659096011), so the numeric query returns no match.
        def _label_candidates_from_url(bsr_url: str) -> list[str]:
            m2 = re.search(r"/Best-Sellers[-_](.+?)/zgbs/", bsr_url, re.IGNORECASE)
            if not m2:
                return []
            words = m2.group(1).replace("-", " ").replace("_", " ").split()
            return [" ".join(words[-n:]) for n in range(2, min(5, len(words) + 1))]

        node_id_path = None
        _resolve_queries = [node_id] + _label_candidates_from_url(url)
        for query in _resolve_queries:
            for ym in snapshot_yms:
                table = f"bsr_sales_monthly_{ym}"
                nodes = await asyncio.to_thread(
                    api.resolve_node_path, market_id=market_id, table=table, query=query
                )
                if nodes:
                    node_id_path = nodes[0].get("id")
                if node_id_path:
                    logger.info(
                        f"[sellersprite_bsr] Resolved query={query!r} → {node_id_path} (table={table})"
                    )
                    break
            if node_id_path:
                break

        if not node_id_path:
            logger.warning(
                f"[sellersprite_bsr] Could not resolve nodeIdPath for node_id={node_id} "
                f"or URL labels in any snapshot"
            )
            return items

        # Persist so _fetch_category_cvr can use SellerSprite market research as CVR fallback.
        ctx.cache["ss_node_id_path"] = node_id_path
        ctx.cache["ss_market_id"] = market_id

        async def fetch_snapshot(ym: str) -> tuple:
            table = f"bsr_sales_monthly_{ym}"
            try:
                result = await asyncio.to_thread(
                    api.get_competing_lookup,
                    market=store_id,
                    month_name=table,
                    node_id_paths=[node_id_path],
                    size=100,
                )
                raw_items = result.get("items") or []
                # Log actual field names on the first item so mismatches surface immediately.
                if raw_items and logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"[fetch_snapshot] item keys (ym={ym}): {sorted(raw_items[0].keys())}"
                    )
                slim = [
                    {
                        "asin": p.get("asin") or p.get("parent") or "",
                        # API field is "bsrRank"; "rank"/"rankingPosition" are legacy fallbacks
                        "rank": p.get("bsrRank")
                        or p.get("rank")
                        or p.get("rankingPosition")
                        or (i + 1),
                        # "brand0" is the brand name at snapshot time; "brand" is the current value
                        "brand": (p.get("brand") or p.get("brand0") or p.get("brandName") or ""),
                        # availableDate is ms-since-epoch; kept as-is for downstream math
                        "available_date_ms": p.get("availableDate"),
                        # per-product click-to-purchase rate; used as category CVR proxy
                        "conversion_rate": p.get("conversionRate"),
                        # seller fields — used as FulfillmentExtractor fallback
                        "fba": p.get("fba"),
                        "seller_id": p.get("sellerId"),
                        "seller_name": p.get("sellerName"),
                    }
                    for i, p in enumerate(raw_items)
                    if p.get("asin") or p.get("parent")
                ]
                missing_brand = sum(1 for p in slim if not p["brand"])
                if missing_brand:
                    logger.warning(
                        f"[fetch_snapshot] ym={ym}: {missing_brand}/{len(slim)} items have no brand"
                    )
                return ym, slim
            except Exception as e:
                logger.warning(f"[sellersprite_bsr] Snapshot {ym} failed: {e}")
                return ym, []

        results = await asyncio.gather(*[fetch_snapshot(ym) for ym in snapshot_yms])
        snapshots = {ym: products for ym, products in results if products}
        ctx.cache["sellersprite_snapshots"] = snapshots
        ctx.cache["sellersprite_base_ym"] = base_ym

        # Compute category-level CVR from the latest snapshot so _fetch_category_cvr
        # can use it directly without an extra get_market_research API call.
        _latest_for_cvr = snapshots.get(max(snapshots)) if snapshots else []
        _cvr_vals = [
            float(p["conversion_rate"])
            for p in _latest_for_cvr
            if p.get("conversion_rate") is not None and float(p["conversion_rate"]) > 0
        ]
        if _cvr_vals:
            ctx.cache["ss_median_cvr"] = statistics.median(_cvr_vals)
            logger.info(
                f"[sellersprite_bsr] ss_median_cvr={ctx.cache['ss_median_cvr']:.2%} "
                f"(n={len(_cvr_vals)})"
            )

        _l2_set(
            ctx,
            {
                "snapshots": snapshots,
                "base_ym": base_ym,
                "ss_node_id_path": node_id_path,
                "ss_market_id": market_id,
                "ss_median_cvr": ctx.cache.get("ss_median_cvr"),
            },
            "ss_bsr",
            ss_cache_key,
        )
        logger.info(
            "[sellersprite_bsr] Snapshots: "
            + ", ".join(f"{ym}({len(p)})" for ym, p in sorted(snapshots.items()))
        )
    except Exception as e:
        logger.warning(f"[sellersprite_bsr] Failed: {e}")

    return items


# ---------------------------------------------------------------------------
# Compliance Risk Detection
# ---------------------------------------------------------------------------

# Each entry covers one regulatory domain.
# "triggers" are lowercase substrings matched against titles + keywords.
# Ordered from most to least severe so the first match sets the floor level.
_COMPLIANCE_DB: list = [
    {
        "id": "pesticide_fifra",
        "label": "EPA-Registered Pesticide (FIFRA)",
        "risk_level": "CRITICAL",
        "triggers": [
            "insecticide",
            "pesticide",
            "pest control",
            "bug killer",
            "insect killer",
            "mosquito killer",
            "roach killer",
            "ant killer",
            "flea killer",
            "pyrethrin",
            "pyrethroid",
            "permethrin",
            "bifenthrin",
            "cypermethrin",
            "imidacloprid",
            "spinosad",
            "fipronil",
            "deltamethrin",
            "malathion",
            "glyphosate",
            "weed killer",
            "herbicide",
            "fungicide",
            "rodenticide",
            "rat killer",
            "mouse killer",
            "rat poison",
            "kills insects",
            "kills bugs",
            "kills mosquito",
        ],
        "regulations": [
            "FIFRA (Federal Insecticide, Fungicide, and Rodenticide Act) — EPA registration required",
            "Amazon Pesticide Policy — EPA Reg. No. must appear in listing; unregistered claims blocked",
            "State Registration — CA, NY, and others maintain separate approved-product registries",
            "Prop 65 (CA) — specific active ingredients trigger cancer/reproductive-harm disclosure",
        ],
        "fba_note": (
            "Concentrated pesticides and aerosol formulations are typically Amazon Hazmat/Dangerous Goods. "
            "Requires Safety Data Sheet (SDS), UN number, and FBA hazmat approval. "
            "Some products restricted to seller-fulfilled only."
        ),
        "new_entrant_burden": (
            "Obtain EPA registration (18–36 months, $50k–$500k+) OR use an already-registered "
            "formulation under a supplemental label. Budget for state-by-state registration fees. "
            "Mandatory: hire a regulatory consultant before listing."
        ),
    },
    {
        "id": "flea_tick_pet",
        "label": "Flea / Tick / Pet Parasite Control",
        "risk_level": "HIGH",
        "triggers": [
            "flea",
            "tick",
            "flea and tick",
            "tick repellent",
            "flea repellent",
            "flea collar",
            "tick collar",
            "heartworm",
            "parasite control",
            "flea treatment",
            "flea spray for pets",
        ],
        "regulations": [
            "EPA registration required when chemical active ingredient is used (FIFRA §3)",
            "FDA oversight if product makes drug claims (e.g. 'treats' infestation)",
            "Amazon Pet Insecticide Policy — requires EPA Reg. No. in back-end keywords",
            "Topical spot-on treatments: additional FIFRA data requirements (pet safety studies)",
        ],
        "fba_note": (
            "Aerosol flea sprays and concentrated dips may be classified as hazmat. "
            "Flea collars with DDVP or tetrachlorvinphos face additional state bans (CA, NY)."
        ),
        "new_entrant_burden": (
            "EPA registration or tolerance exemption required. "
            "If selling under 'natural/essential oil' claims, verify each active is on EPA's 25(b) exempt list. "
            "Dual EPA + FDA exposure if any drug claims are made."
        ),
    },
    {
        "id": "repellent_deet",
        "label": "Human / Animal Repellent",
        "risk_level": "HIGH",
        "triggers": [
            "mosquito repellent",
            "insect repellent",
            "deet",
            "picaridin",
            "ir3535",
            "repel mosquito",
            "bug repellent",
            "bug spray",
            "deer repellent",
            "rabbit repellent",
            "animal repellent",
            "rodent repellent",
            "snake repellent",
        ],
        "regulations": [
            "DEET / Picaridin / IR3535 skin-applied repellents: EPA registration (FIFRA §3)",
            "Amazon DEET Policy — concentration limits enforced at listing level",
            "Deer/animal repellents with chemical actives: EPA registration required; "
            "'natural' repellents may qualify for FIFRA §25(b) exemption if active ingredients are listed",
        ],
        "fba_note": (
            "Aerosol repellents and DEET concentrations >40% are common FBA hazmat triggers. "
            "Verify UN number and FBA category approval before shipping to fulfilment centres."
        ),
        "new_entrant_burden": (
            "DEET-based products require EPA registration. "
            "§25(b) (natural/exempt) path requires all active ingredients to appear on the EPA exempt list — "
            "do NOT assume 'natural' or 'essential oil' means unregulated."
        ),
    },
    {
        "id": "aerosol_flammable",
        "label": "Aerosol / Flammable / Pressurised Container",
        "risk_level": "MEDIUM",
        "triggers": [
            "aerosol",
            "spray can",
            "pressurized",
            "pressurised",
            "fogger",
            "total release fogger",
            "fumigator",
            "propellant",
            "flammable",
            "combustible",
        ],
        "regulations": [
            "DOT Hazardous Materials Regulations (49 CFR) — aerosols classified as UN 1950",
            "CPSC 16 CFR §1500 — flammable aerosols consumer product safety standard",
            "IATA DGR — air-freight restrictions; affects Amazon's inbound freight lanes",
        ],
        "fba_note": (
            "Aerosols and flammable liquids require FBA Dangerous Goods approval. "
            "Must submit SDS and pass Amazon's hazmat review (typical 1–4 weeks). "
            "Storage quantity limits apply per fulfilment centre."
        ),
        "new_entrant_burden": (
            "Prepare SDS per GHS/OSHA HazCom 2012. "
            "Submit for Amazon Dangerous Goods review before first inbound shipment. "
            "Plan for longer lead time and possible repackaging to meet DOT specification packaging."
        ),
    },
    {
        "id": "disinfectant_sanitizer",
        "label": "Disinfectant / Sanitizer / Antimicrobial",
        "risk_level": "HIGH",
        "triggers": [
            "disinfectant",
            "sanitizer",
            "sanitiser",
            "antimicrobial",
            "kills 99",
            "kills bacteria",
            "kills virus",
            "kills germs",
            "hospital grade",
            "kills covid",
            "virucidal",
            "bactericidal",
            "bleach",
            "hydrogen peroxide",
            "quaternary ammonium",
        ],
        "regulations": [
            "EPA Pesticide Registration (FIFRA) — all antimicrobial claims require Reg. No.",
            "FDA OTC Drug Monograph — hand sanitisers regulated as drugs (21 CFR 333)",
            "Amazon Disinfectant/Sanitizer Policy — EPA Reg. No. mandatory in listing",
            "FTC Green Guides — 'kills 99.9%' efficacy claims require substantiated test data",
        ],
        "fba_note": (
            "Bleach-based and alcohol-based disinfectants (>70% alcohol) are FBA hazmat. "
            "Corrosive products require UN-specification packaging."
        ),
        "new_entrant_burden": (
            "EPA registration for each claimed pathogen/surface combination. "
            "FDA NDA/ANDA if drug claims. "
            "Third-party efficacy testing (AOAC or EN standards) required for '99.9% kill' claims."
        ),
    },
    {
        "id": "pool_spa_chemical",
        "label": "Pool / Spa / Water Treatment Chemical",
        "risk_level": "MEDIUM",
        "triggers": [
            "pool chemical",
            "pool shock",
            "pool algaecide",
            "algaecide",
            "pool chlorine",
            "chlorine tablet",
            "spa chemical",
            "hot tub chemical",
            "water treatment",
            "pond treatment",
            "clarifier",
            "water clarifier",
        ],
        "regulations": [
            "EPA registration (FIFRA) for algaecides and biocidal water treatments",
            "DOT UN1791 (sodium hypochlorite), UN2468 (trichloroisocyanuric acid) — hazmat",
            "CPSC — oxidising pool chemicals have consumer safety labelling requirements",
        ],
        "fba_note": (
            "Oxidising chemicals (chlorine, peroxide-based) are Amazon Hazmat. "
            "Incompatible with many other FBA product types; separate storage required."
        ),
        "new_entrant_burden": (
            "EPA registration for any algaecide/biocide claim. "
            "DOT hazmat packaging and labelling. "
            "Amazon DG approval required before first shipment."
        ),
    },
]

# Risk level ordering for aggregation
_RISK_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0}


def _detect_compliance_risks(scan_texts: list) -> dict:
    """
    Scan product titles and keywords for regulated-substance signals.

    Returns a dict with:
      detected   — list of matched compliance DB entries (id, label, risk_level, ...)
      overall_risk — highest risk_level across all matches ("NONE" if no hits)
      fba_hazmat  — True when any HIGH+ match is found
      triggered_by — sample of trigger terms actually found in the text
    """
    combined = " ".join(t.lower() for t in scan_texts if t)
    detected = []
    all_triggers: list = []

    def _trigger_hit(trigger: str) -> bool:
        # Use word-boundary regex so "tick" does not match inside "nonstick",
        # "ant" does not match "antifog", etc.
        return bool(re.search(r"\b" + re.escape(trigger) + r"\b", combined))

    for entry in _COMPLIANCE_DB:
        hit_triggers = [t for t in entry["triggers"] if _trigger_hit(t)]
        if not hit_triggers:
            continue
        detected.append(
            {
                "id": entry["id"],
                "label": entry["label"],
                "risk_level": entry["risk_level"],
                "regulations": entry["regulations"],
                "fba_note": entry["fba_note"],
                "burden": entry["new_entrant_burden"],
                "triggered_by": hit_triggers[:5],
            }
        )
        all_triggers.extend(hit_triggers[:3])

    if not detected:
        return {"detected": [], "overall_risk": "NONE", "fba_hazmat": False, "triggered_by": []}

    best = max(detected, key=lambda d: _RISK_ORDER.get(d["risk_level"], 0))
    fba_hazmat = any(_RISK_ORDER.get(d["risk_level"], 0) >= _RISK_ORDER["HIGH"] for d in detected)
    return {
        "detected": detected,
        "overall_risk": best["risk_level"],
        "fba_hazmat": fba_hazmat,
        "triggered_by": list(dict.fromkeys(all_triggers))[:10],  # dedup, preserve order
    }


def _ads_category_matches_niche(browse_category: str | None, core_keywords: list[str]) -> bool:
    """
    Return True when the Amazon Ads benchmark category is relevant to the analyzed niche.

    Strategy: tokenise both sides, strip common stop-words, and check for any token
    overlap.  A hit means the store's ad history is from the same broad category as
    the BSR page being analyzed so the benchmark CVR is valid.

    When there is no overlap (e.g. browse_category="Patio, Lawn & Garden" while the
    keywords are ["bluetooth speaker", "wireless earbuds"]), the benchmark CVR belongs
    to a different category and must be discarded.
    """
    if not browse_category or not core_keywords:
        return False
    _STOP = {"the", "a", "an", "for", "in", "of", "and", "&", "or", "to", "with", "by"}
    cat_tokens = {t for t in re.split(r"[\s,&/]+", browse_category.lower()) if t and t not in _STOP}
    kw_tokens = {t for kw in core_keywords for t in kw.lower().split() if t not in _STOP}
    return bool(cat_tokens & kw_tokens)


async def _fetch_category_cvr(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch category-median click-to-purchase CVR for steady-state ACOS estimation.

    Primary:   Amazon Ads crossProgramBenchmarks → newToBrandPurchaseRateP50,
               accepted only when browse_category overlaps the analyzed niche keywords
               (guard against the store's ad account being in a different category).
    Fallback:  SellerSprite get_market_research → search_to_buy_ratio_pm / 1000
               (requires fetch_sellersprite_bsr to have run first so ss_node_id_path
               is already in ctx.cache).
    Default:   0.10  (conservative Amazon SP category average)

    Stores:
      ctx.cache["category_cvr"]         float
      ctx.cache["category_cvr_source"]  str
    """
    store_id = ctx.config.get("store_id", "US") if hasattr(ctx, "config") else "US"
    # Include the SellerSprite node_id_path in the cache key so different categories
    # served by the same store_id get independent CVR values.
    ss_node = ctx.cache.get("ss_node_id_path") or ""
    cvr_hash = _hl.md5(f"{store_id}:{ss_node}".encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_CVR, "category_cvr", cvr_hash)
    if cached is not None:
        ctx.cache["category_cvr"] = cached["cvr"]
        ctx.cache["category_cvr_source"] = cached["source"]
        ctx.cache["category_cpc_p50"] = cached.get("cpc_p50")
        logger.info(
            f"[cat_monopoly] Category CVR L2 cache hit store={store_id} node={ss_node[:20]}"
        )
        return items

    core_keywords: list[str] = ctx.cache.get("core_keywords") or []
    cvr: float | None = None
    cpc_p50: float | None = None
    source = "default_0.10"

    # ── Primary: Amazon Ads benchmark ────────────────────────────────────────
    try:
        result = await AmazonAdsClient(store_id=store_id).get_category_cvr_benchmark(
            days=30, time_unit="MONTHLY"
        )
        browse_category = result.get("browse_category")
        median_cvr = result.get("category_median_cvr")
        cpc_p50 = result.get("cpc_p50")

        if median_cvr and 0 < median_cvr < 1:
            if _ads_category_matches_niche(browse_category, core_keywords):
                cvr = median_cvr
                source = (
                    f"amazon_ads_benchmark newToBrandPurchaseRateP50"
                    f" (category={browse_category!r}"
                    f", peers={result.get('peer_set_size', 'N/A')})"
                )
                logger.info(f"[fetch_category_cvr] Amazon Ads CVR={cvr:.2%} — {source}")
            else:
                logger.warning(
                    f"[fetch_category_cvr] Amazon Ads browse_category={browse_category!r} "
                    f"does not match niche keywords {core_keywords}; discarding benchmark"
                )
        if cpc_p50:
            logger.info(f"[fetch_category_cvr] Amazon Ads cpcP50=${cpc_p50:.2f}")
    except Exception as e:
        logger.warning(f"[fetch_category_cvr] Amazon Ads benchmark failed: {e}")

    # ── Fallback: SellerSprite median conversionRate from BSR snapshot ──────────
    # conversionRate is already fetched per-product during _fetch_sellersprite_bsr
    # (stored as ss_median_cvr); no extra API call needed.
    if cvr is None:
        ss_cvr = ctx.cache.get("ss_median_cvr")
        if ss_cvr and 0 < ss_cvr < 1:
            cvr = ss_cvr
            source = f"sellersprite_bsr_snapshot median conversionRate ({cvr:.2%})"
            logger.info(f"[fetch_category_cvr] SellerSprite CVR={cvr:.2%} — {source}")

    if cvr is None:
        cvr = 0.10
        source = "default_0.10 (Amazon Ads category mismatch; SellerSprite unavailable)"

    ctx.cache["category_cvr"] = cvr
    ctx.cache["category_cvr_source"] = source
    ctx.cache["category_cpc_p50"] = cpc_p50
    _l2_set(ctx, {"cvr": cvr, "source": source, "cpc_p50": cpc_p50}, "category_cvr", cvr_hash)
    return items


# ---------------------------------------------------------------------------
# Non-fixable review pre-filter
# ---------------------------------------------------------------------------

# Class A — Logistics / fulfillment: shipping damage, wrong item, carrier delays.
# Entirely outside the manufacturer's control; give no actionable entry signal.
_NON_FIXABLE_LOGISTICS_RE = re.compile(
    r"\b("
    r"shipping|delivery|delivered|in[- ]transit|arrived damaged"
    r"|package[d]?|box was (crushed|damaged|dented)|packaging (damaged|broken)"
    r"|carrier|postal service|fedex|u\.?p\.?s\.?|usps"
    r"|never arrived|lost in (mail|transit)"
    r"|wrong item|wrong product|not what i ordered|sent the wrong|received (wrong|incorrect)"
    r"|return (policy|process)|refund (request|issue)"
    r"|amazon (fulfillment|warehouse)|prime delivery"
    r")\b",
    re.IGNORECASE,
)

# Class B — "Works as designed": reviewer dislikes the product category, not the
# execution. A new entrant cannot fix an inherent category trade-off.
_NON_FIXABLE_WAD_RE = re.compile(
    r"("
    r"works (exactly )?as (described|expected|advertised|intended|designed)"
    r"|does (exactly )?what it('s| is) (supposed|meant) to"
    r"|product (itself )?is (fine|good|great|okay|ok)\b"
    r"|no (product |design )?(defect|flaw|issue|problem)"
    r")",
    re.IGNORECASE,
)

# Override: clear product-defect signal that overrides a single incidental logistics mention
# (e.g. "fast shipping but the product broke" is a product complaint, not a logistics one).
_PRODUCT_DEFECT_SIGNAL_RE = re.compile(
    r"\b("
    r"broke|broken|cracked|snapped|shattered|defective|defect|malfunction"
    r"|stopped (working|after)|doesn'?t work|does not work|dead on arrival"
    r"|poor (quality|build|material|construction)|cheap (plastic|material|quality|build)"
    r"|flimsy|fell apart|peeled|rusted|leaked|leaking|disintegrat"
    r"|missing (part|piece|component|accessory|screw|hardware)"
    r"|wrong size|incorrect size|doesn'?t fit|too small|too large|too short|too long"
    r")\b",
    re.IGNORECASE,
)


def _is_fixable_review(title: str | None, content: str | None) -> bool:
    """
    Return True when the review describes a fixable product deficiency.

    Rejection rules (applied in order):
    1. "Works as designed" signal — unconditional reject.
    2. 2+ logistics hits — logistics-dominant, reject.
    3. 1 logistics hit in a short review (< 40 words) WITHOUT a product-defect
       signal — logistics-only short rant, reject.
       Exception: if a product-defect signal is also present (e.g. "fast
       shipping but the product broke"), the logistics mention is incidental
       and the review is kept.
    """
    text = f"{title or ''} {content or ''}"
    if _NON_FIXABLE_WAD_RE.search(text):
        return False
    logistics_hits = _NON_FIXABLE_LOGISTICS_RE.findall(text)
    if len(logistics_hits) >= 2:
        return False
    if logistics_hits and len(text.split()) < 40:
        if not _PRODUCT_DEFECT_SIGNAL_RE.search(text):
            return False
    return True


async def _fetch_critical_reviews_top_brands(items: list[dict], ctx: Any) -> list[dict]:
    """
    Fetch recent critical (1–3 star) reviews using stratified sampling across
    three market segments so no single segment dominates the signal:

      Stratum A — Leaders     (BSR rank  1–10):  up to 2 brands
      Stratum B — Mid-tier    (BSR rank 11–30):  up to 2 brands  (excl. A)
      Stratum C — New entrants (listed ≤12 mo):  up to 1 brand   (excl. A+B)

    Cross-stratum complaint themes are the strongest category-wide entry
    signals; stratum-specific complaints surface segment weaknesses only.
    Reviews older than 90 days are discarded — incumbents may have already
    patched those issues.

    Stores: ctx.cache["critical_reviews_data"]
      {asin: {"brand": str, "rank": int, "stratum": str,
              "recent_critical_reviews": [...]}}
    """
    _RECENCY_DAYS = 90
    _MAX_PAGES = 2
    _CONCURRENCY = 3
    _STRATA_ALLOC = {"leaders": 2, "mid_tier": 2, "new_entrant": 1}

    # Sellersprite snapshot → authoritative brand byline + listing date
    ss_snapshots = ctx.cache.get("sellersprite_snapshots", {})
    _brand_lookup: dict = {}
    _asin_available_ms: dict = {}
    if ss_snapshots:
        _latest = ss_snapshots.get(max(ss_snapshots), [])
        for p in _latest:
            if p.get("asin"):
                if p.get("brand"):
                    _brand_lookup[p["asin"]] = p["brand"]
                if p.get("available_date_ms"):
                    _asin_available_ms[p["asin"]] = p["available_date_ms"]

    # "New entrant" cutoff anchored to Sellersprite base snapshot (same as elsewhere)
    base_ym = ctx.cache.get("sellersprite_base_ym", "")
    if base_ym and len(base_ym) == 6:
        _by, _bm = int(base_ym[:4]), int(base_ym[4:])
        _total = _by * 12 + (_bm - 1) - 12
        _cy, _cm = _total // 12, _total % 12 + 1
        new_entrant_cutoff_ms = calendar.timegm((_cy, _cm, 1, 0, 0, 0)) * 1000
    else:
        new_entrant_cutoff_ms = (time.time() - 365 * 86400) * 1000

    def _parse_rank(raw) -> int:
        m = re.search(r"\d+", str(raw or "9999").replace(",", ""))
        return int(m.group()) if m else 9999

    # Build candidate list with rank, brand, and new-entrant flag
    candidates: list[dict] = []
    for item in items:
        asin = (item.get("ASIN") or item.get("asin") or "").strip().upper()
        if not asin:
            continue
        brand = _brand_lookup.get(asin) or item.get("Brand") or item.get("brand") or "Unknown"
        if brand == "Unknown":
            continue
        rank = _parse_rank(item.get("Rank"))
        avail_ms = _asin_available_ms.get(asin)
        is_new = avail_ms is not None and avail_ms >= new_entrant_cutoff_ms
        candidates.append({"asin": asin, "brand": brand, "rank": rank, "is_new": is_new})

    def _pick_stratum(pool: list[dict], n: int, exclude_brands: set) -> list[dict]:
        """Return up to n entries — one per brand — sorted by rank, excluding known brands."""
        brand_best: dict = {}
        for e in pool:
            b = e["brand"]
            if b in exclude_brands:
                continue
            if b not in brand_best or e["rank"] < brand_best[b]["rank"]:
                brand_best[b] = e
        return sorted(brand_best.values(), key=lambda x: x["rank"])[:n]

    leaders_pool = [e for e in candidates if e["rank"] <= 10]
    mid_tier_pool = [e for e in candidates if 11 <= e["rank"] <= 30]
    new_entrant_pool = [e for e in candidates if e["is_new"]]

    leaders = _pick_stratum(leaders_pool, _STRATA_ALLOC["leaders"], set())
    used = {e["brand"] for e in leaders}
    mid_tier = _pick_stratum(mid_tier_pool, _STRATA_ALLOC["mid_tier"], used)
    used |= {e["brand"] for e in mid_tier}
    new_entrants = _pick_stratum(new_entrant_pool, _STRATA_ALLOC["new_entrant"], used)

    for e in leaders:
        e["stratum"] = "leaders"
    for e in mid_tier:
        e["stratum"] = "mid_tier"
    for e in new_entrants:
        e["stratum"] = "new_entrant"

    top_entries = leaders + mid_tier + new_entrants
    if not top_entries:
        logger.warning("[critical_reviews] No stratified candidates found; skipping")
        return items

    logger.info(
        f"[critical_reviews] Stratified selection: "
        f"leaders={len(leaders)}, mid_tier={len(mid_tier)}, new_entrant={len(new_entrants)}"
    )

    asin_key = ",".join(sorted(e["asin"] for e in top_entries))
    reviews_hash = _hl.md5(asin_key.encode()).hexdigest()[:12]

    cached = _l2_get(ctx, _TTL_CRITICAL_REVIEWS, "critical_reviews", reviews_hash)
    if cached is not None:
        ctx.cache["critical_reviews_data"] = cached
        logger.info(f"[cat_monopoly] Critical reviews L2 cache hit hash={reviews_hash}")
        return items

    # Parse "Reviewed in the United States on March 15, 2025" or bare "March 15, 2025"
    _DATE_RE = re.compile(r"(\w+ \d+, \d{4})")
    _cutoff = datetime.now() - timedelta(days=_RECENCY_DAYS)

    def _is_recent(date_str: str | None) -> bool:
        if not date_str:
            return False
        m = _DATE_RE.search(date_str)
        if not m:
            return False
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y") >= _cutoff
        except ValueError:
            return False

    sem = asyncio.Semaphore(_CONCURRENCY)

    async def _fetch_one(entry: dict) -> tuple[str, list]:
        asin = entry["asin"]
        async with sem:
            try:
                extractor = CommentsExtractor()
                reviews = await extractor.get_negative_reviews(asin, max_pages=_MAX_PAGES)
                recent = [r for r in reviews if _is_recent(r.date)]
                fixable = [r for r in recent if _is_fixable_review(r.title, r.content)]
                n_dropped = len(recent) - len(fixable)
                logger.info(
                    f"[critical_reviews] [{entry['stratum']}] ASIN={asin} "
                    f"brand={entry['brand']}: {len(fixable)} fixable "
                    f"(dropped {n_dropped} non-fixable, {len(reviews) - len(recent)} stale) "
                    f"of {len(reviews)} total"
                )
                return asin, fixable
            except Exception as e:
                logger.warning(f"[critical_reviews] Failed for ASIN={asin}: {e}")
                return asin, []

    results = await asyncio.gather(*[_fetch_one(e) for e in top_entries])

    critical_data: dict = {}
    for entry, (asin, reviews) in zip(top_entries, results, strict=False):
        critical_data[asin] = {
            "brand": entry["brand"],
            "rank": entry["rank"],
            "stratum": entry["stratum"],
            "recent_critical_reviews": [
                {
                    "rating": r.rating,
                    "title": r.title,
                    "content": (r.content or "")[:500],
                    "date": r.date,
                    "is_verified": r.is_verified,
                    "helpful_votes": r.helpful_votes,
                }
                for r in reviews
            ],
        }

    ctx.cache["critical_reviews_data"] = critical_data
    _l2_set(ctx, critical_data, "critical_reviews", reviews_hash)
    total = sum(len(v["recent_critical_reviews"]) for v in critical_data.values())
    logger.info(
        f"[critical_reviews] {total} recent critical reviews collected "
        f"({_RECENCY_DAYS}d filter): "
        f"leaders={len(leaders)}, mid_tier={len(mid_tier)}, new_entrant={len(new_entrants)}"
    )
    return items


async def _run_monopoly_analysis(items: list[dict], ctx: Any) -> list[dict]:
    """Calculates scores and generates flattened niche benchmarks."""

    def _parse_float(raw, default: float = 0.0) -> float:
        """Extract the first decimal number from a US-locale price/rating string.

        Handles: currency symbols ($), thousand-separator commas, ranges ($9–$15),
        suffixes/spaces.  Commas are always thousand separators in this context
        (US locale), so they are stripped before matching — avoids misreading
        "$1,299.99" as 1.299 when comma is naively replaced by a dot.
        """
        s = str(raw or "").replace(",", "")  # strip thousand separators
        m = re.search(r"\d+(?:\.\d+)?", s)
        if not m:
            return default
        try:
            return float(m.group())
        except ValueError:
            return default

    def _parse_int(raw, default: int = 0) -> int:
        """Extract the first integer from a messy string (handles commas, suffixes, parens)."""
        m = re.search(r"\d+", str(raw or "").replace(",", ""))
        if not m:
            return default
        try:
            return int(m.group())
        except ValueError:
            return default

    analyzer = CategoryMonopolyAnalyzer()
    external_data = {
        "social_psi": ctx.cache.get("category_social_psi"),
        "deal_intensity": ctx.cache.get("category_deal_intensity"),
    }

    contamination_stats = ctx.cache.get("contamination_stats", {})
    _cs_status = contamination_stats.get("status", "not_run")
    _cs_n_removed = contamination_stats.get("n_removed", 0)

    # Build ASIN→brand lookup from the most recent Sellersprite snapshot.
    # Amazon's BSR card HTML carries no brand byline, so this is the authoritative source.
    ss_snapshots = ctx.cache.get("sellersprite_snapshots", {})
    _brand_lookup: dict = {}
    if ss_snapshots:
        _latest = ss_snapshots.get(max(ss_snapshots), [])
        _brand_lookup = {
            p["asin"]: p.get("brand") for p in _latest if p.get("asin") and p.get("brand")
        }

    analysis_input = [
        {
            "rank": _parse_int(item.get("Rank"), default=999),
            "price": _parse_float(item.get("Price")),
            "sales": item.get("sales", 0),
            "brand": _brand_lookup.get(item.get("ASIN") or item.get("asin"))
            or item.get("Brand")
            or item.get("brand")
            or None,
            "seller_type": item.get("seller_type", "Unknown"),
            "seller_id": item.get("seller_id"),
            "feedback_count": item.get("feedback_count", 0),
            "review_count": _parse_int(item.get("Reviews")),
            "rating": _parse_float(item.get("Stars")),
            # Written reviews vs global ratings (from ReviewCountExtractor)
            "global_ratings": item.get("global_ratings"),
            "written_reviews": item.get("written_reviews"),
            "review_ratio": item.get("review_ratio"),
        }
        for item in items
    ]

    # Combined Ad Data with Multi-Keyword CPC
    detailed_bids = ctx.cache.get("detailed_bid_analysis", {})
    ad_data = {
        "ad_ratio": ctx.cache.get("ad_ratio", 0.3),
        "actual_bsr_ad_ratio": ctx.cache.get("actual_bsr_ad_ratio"),
        "detailed_bids": detailed_bids,
    }

    result = analyzer.analyze(
        analysis_input,
        keyword_data=ctx.cache.get("keyword_data"),
        ad_data=ad_data,
        external_data=external_data,
        historical_data=ctx.cache.get("historical_data"),
        bsr_snapshots=ctx.cache.get("sellersprite_snapshots"),
        keyword_weekly_trends=ctx.cache.get("keyword_weekly_trends"),
    )

    # Pass raw bid recommendations directly — no pre-formatting.
    # The LLM receives the full API payload for both strategies and renders it.
    bid_raw = {
        "LEGACY_FOR_SALES": detailed_bids.get("LEGACY_FOR_SALES", []),
        "AUTO_FOR_SALES": detailed_bids.get("AUTO_FOR_SALES", []),
    }
    # Count total keyword-level entries for data-quality tracking
    bid_entry_count = sum(
        len(rec.get("bidRecommendationsForTargetingExpressions", []))
        for strategy_recs in bid_raw.values()
        for rec in strategy_recs
    )

    prices = [p["price"] for p in analysis_input if p["price"] > 0]
    median_price = statistics.median(prices) if prices else 25.0
    total_monthly_units = result.get("niche_benchmarks", {}).get("total_estimated_monthly_units", 0)
    niche_monthly_gmv = int(total_monthly_units * median_price)

    # ── Steady-state ad burden ────────────────────────────────────────────────
    # Estimated ACOS = median_cpc / (median_price × category_cvr)
    # Ad profit drag = actual_bsr_ad_ratio × estimated_acos
    # Both inputs (CPC and CVR) come from the Amazon Ads ecosystem so the
    # estimate is internally consistent regardless of category.
    _category_cvr = ctx.cache.get("category_cvr") or 0.10

    _cpc_values: list[float] = []
    for _strategy_recs in bid_raw.values():
        for _rec in _strategy_recs:
            for _expr in _rec.get("bidRecommendationsForTargetingExpressions", []):
                _rb = _expr.get("recommendedBid", {})
                _s = float(_rb.get("startBid") or _rb.get("bid") or 0)
                _e = float(_rb.get("endBid") or _s)
                if _s > 0:
                    _cpc_values.append((_s + _e) / 2)

    # Fall back to category benchmark cpcP50 when bid-recommendations API is unavailable
    # (e.g. no owned listing in this category — the bid API requires an advertiser ASIN).
    _median_cpc: float | None = (
        statistics.median(_cpc_values) if _cpc_values else ctx.cache.get("category_cpc_p50")
    )
    _actual_ad_ratio: float | None = ctx.cache.get("actual_bsr_ad_ratio")

    # Breakeven ACOS: 1 - COGS% - referral% - FBA%.
    # Primary: call Amazon's public fee calculator API for the BSR product whose
    # price is closest to the category median — this gives the real 2026 fee for
    # the actual product weight/size class in this category.
    # Fallback: select the appropriate weight tier from the local 2026 fee schedule
    # using a price→weight heuristic on comparable-price BSR products.
    _REFERRAL_FEE_PCT = 0.15
    _COGS_PCT = 0.30
    _fba_fee_pct = 0.18  # fallback: Large Standard 12-16 oz at $25 ≈ $4.76/25

    # Pick the BSR product with price closest to median_price as the representative ASIN.
    _rep_item = (
        min(
            (item for item in analysis_input if item.get("price", 0) > 0),
            key=lambda x: abs(x["price"] - median_price),
        )
        if any(item.get("price", 0) > 0 for item in analysis_input)
        else None
    )
    _rep_asin: str | None = (
        ((_rep_item.get("asin") or _rep_item.get("ASIN") or "").strip().upper() or None)
        if _rep_item
        else None
    )

    # --- Primary: live API ---
    _api_fee_resolved = False
    if _rep_asin and median_price > 0:
        try:
            _fee_data = await ProfitabilitySearchExtractor().get_fees(_rep_asin, median_price)
            # Response shape: {"programResults": {"Core#0": {"fees": [...], "totalFee": N}}}
            _prog = (_fee_data.get("programResults") or {}).get("Core#0") or {}
            _total: float | None = _prog.get("totalFee")
            if _total is None:
                # Sum individual fee components that represent fulfillment charges.
                _total = (
                    sum(
                        float(f.get("amount") or f.get("feeAmount", {}).get("amount") or 0)
                        for f in (_prog.get("fees") or [])
                        if "fulfillment" in (f.get("type") or f.get("feeType") or "").lower()
                    )
                    or None
                )
            if _total and _total > 0:
                _fba_fee_pct = _total / median_price
                _api_fee_resolved = True
                logger.info(
                    f"[monopoly] FBA fee from API: ${_total:.2f} ({_fba_fee_pct:.1%}) "
                    f"for ASIN {_rep_asin} @ ${median_price:.2f}"
                )
        except Exception:
            pass  # fall through to local JSON lookup

    # --- Fallback: local 2026 fee schedule with price→weight heuristic ---
    if not _api_fee_resolved:
        try:
            _fba_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../../mcp/servers/finance/fba_fee.json",
            )
            with open(_fba_path) as _fba_f:
                _fba_data = json.load(_fba_f)
            _fuel_mult = 1 + _fba_data["meta"]["fuel_surcharge"]["rate_pct"] / 100
            # Narrow to products priced within ±35% of the category median to avoid
            # outliers skewing the representative weight tier.
            _comp_prices = [p for p in prices if median_price * 0.65 <= p <= median_price * 1.35]
            _ref_price = statistics.median(_comp_prices) if _comp_prices else median_price
            # Price → typical weight tier for standard US consumer goods (2026 schedule).
            # Lighter/cheaper items map to Small Standard; heavier/pricier to Large Standard.
            _PRICE_TO_TIER: list[tuple[float, str, str]] = [
                (10, "Small Standard", "8+ to 10"),
                (18, "Small Standard", "14+ to 16"),
                (30, "Large Standard", "8+ to 12"),
                (50, "Large Standard", "12+ to 16"),
                (75, "Large Standard", "1+ to 1.25 lb"),
                (float("inf"), "Large Standard", "1.75+ to 2 lb"),
            ]
            _rep_tier = None
            for _max_p, _size, _wt_fragment in _PRICE_TO_TIER:
                if _ref_price < _max_p:
                    _rep_tier = next(
                        (
                            t
                            for t in _fba_data["fba_fulfillment_fees"]["standard_non_apparel"][
                                "tiers"
                            ]
                            if t.get("size_tier") == _size
                            and _wt_fragment in (t.get("weight_range") or "")
                            and isinstance(
                                t.get("price_brackets", {}).get("10_to_50"), (int, float)
                            )
                        ),
                        None,
                    )
                    break
            if _rep_tier and median_price > 0:
                _bracket = (
                    "under_10"
                    if median_price < 10
                    else ("over_50" if median_price > 50 else "10_to_50")
                )
                _fba_fee_usd = float(_rep_tier["price_brackets"][_bracket]) * _fuel_mult
                _fba_fee_pct = _fba_fee_usd / median_price
        except Exception:
            pass  # use fallback 0.18

    _breakeven_acos = max(0.0, 1.0 - _COGS_PCT - _REFERRAL_FEE_PCT - _fba_fee_pct)

    if _median_cpc and median_price > 0 and _category_cvr > 0:
        _estimated_acos = _median_cpc / (median_price * _category_cvr)
        _ad_profit_drag = (_actual_ad_ratio or 0.5) * _estimated_acos
        _ad_burden_verdict = (
            "Critical"
            if _estimated_acos >= _breakeven_acos * 1.5
            else "High"
            if _estimated_acos >= _breakeven_acos
            else "Moderate"
            if _estimated_acos >= _breakeven_acos * 0.7
            else "Low"
        )
    else:
        _estimated_acos = _ad_profit_drag = None
        _ad_burden_verdict = "unknown"

    estimator = SalesEstimator()
    node_id = ctx.config.get("category_node_id")
    baseline = estimator.category_params.get(str(node_id), {}).get("market_logic", {})

    # ── review_disparity: top-10 avg / bottom-tail avg ────────────────────
    # Require ≥5 products in both buckets; otherwise the ratio is meaningless
    # (tail empty → tail_avg collapses to 1 → disparity = raw top-10 average, not a ratio).
    _MIN_BUCKET = 5
    top10_reviews = [p["review_count"] for p in analysis_input[:10]]
    tail_reviews = [p["review_count"] for p in analysis_input[50:]]
    if len(top10_reviews) >= _MIN_BUCKET and len(tail_reviews) >= _MIN_BUCKET:
        top10_avg = statistics.mean(top10_reviews)
        tail_avg = statistics.mean(tail_reviews)
        review_disparity_val = round(top10_avg / max(tail_avg, 1), 1)
    else:
        review_disparity_val = None

    # Total scraped BSR products. Use 1 as the denominator fallback for coverage
    # percentages, matching the existing data-quality reporting semantics.
    n_total = len(analysis_input) or 1

    # ── price distribution ────────────────────────────────────────────────────
    valid_prices = sorted(p["price"] for p in analysis_input if p["price"] > 0)
    if valid_prices:

        def _pct_val(lst, pct):
            k = (len(lst) - 1) * pct / 100
            lo, hi = int(k), min(int(k) + 1, len(lst) - 1)
            return lst[lo] + (lst[hi] - lst[lo]) * (k - lo)

        def _tier_stats(prices: list) -> dict:
            if not prices:
                return {}
            return {
                "n": len(prices),
                "pct": f"{len(prices) / len(valid_prices):.0%}",
                "min": f"${prices[0]:.2f}",
                "median": f"${statistics.median(prices):.2f}",
                "max": f"${prices[-1]:.2f}",
            }

        price_p10 = _pct_val(valid_prices, 10)
        price_p25 = _pct_val(valid_prices, 25)
        price_p75 = _pct_val(valid_prices, 75)
        price_p90 = _pct_val(valid_prices, 90)
        price_mean = statistics.mean(valid_prices)
        price_min = valid_prices[0]
        price_max = valid_prices[-1]

        # Dynamic buckets: ~5-7 equal-width bands spanning min→max, rounded to $5
        _step = max(5, round((price_max - price_min) / 5 / 5) * 5) or 5
        _lo = int(price_min // _step) * _step
        buckets: list[dict] = []
        b = _lo
        while b < price_max + _step:
            lo_b, hi_b = b, b + _step
            cnt = sum(1 for p in valid_prices if lo_b <= p < hi_b)
            pct = cnt / len(valid_prices) * 100
            buckets.append(
                {
                    "range": f"${lo_b:.0f}–${hi_b:.0f}",
                    "lo": lo_b,
                    "hi": hi_b,
                    "count": cnt,
                    "pct": f"{pct:.0f}%",
                }
            )
            b += _step

        # ── Bimodal / tier detection ──────────────────────────────────────────
        # A "valley" bucket separates two populated clusters when:
        #   - its count ≤ 5% of total AND both neighbouring regions have ≥ 10% each.
        # Scan interior buckets only (skip first and last).
        _VALLEY_THRESH = 0.05  # bucket share ≤ 5% = sparse
        _CLUSTER_THRESH = 0.10  # region share ≥ 10% = populated
        n_total_prices = len(valid_prices)

        tiers: list[dict] = []
        is_bimodal = False
        valley_range: str = ""

        for vi in range(1, len(buckets) - 1):
            vb = buckets[vi]
            if vb["count"] / n_total_prices > _VALLEY_THRESH:
                continue
            left_cnt = sum(bk["count"] for bk in buckets[:vi])
            right_cnt = sum(bk["count"] for bk in buckets[vi + 1 :])
            if (
                left_cnt / n_total_prices >= _CLUSTER_THRESH
                and right_cnt / n_total_prices >= _CLUSTER_THRESH
            ):
                # Found a valley — split here
                is_bimodal = True
                valley_range = vb["range"]
                left_prices = [p for p in valid_prices if p < vb["lo"]]
                right_prices = [p for p in valid_prices if p >= vb["hi"]]
                tiers = [
                    {
                        "label": "Budget tier",
                        **_tier_stats(left_prices),
                        "range": f"${left_prices[0]:.0f}–${left_prices[-1]:.0f}",
                    },
                    {
                        "label": "Premium tier",
                        **_tier_stats(right_prices),
                        "range": f"${right_prices[0]:.0f}–${right_prices[-1]:.0f}",
                    },
                ]
                break  # first valley is sufficient; deeper splits are edge cases

        price_dist = {
            "n": n_total_prices,
            "total_bsr": n_total,
            "min": f"${price_min:.2f}",
            "p10": f"${price_p10:.2f}",
            "p25": f"${price_p25:.2f}",
            "median": f"${median_price:.2f}",
            "mean": f"${price_mean:.2f}",
            "p75": f"${price_p75:.2f}",
            "p90": f"${price_p90:.2f}",
            "max": f"${price_max:.2f}",
            "buckets": [{k: v for k, v in bk.items() if k not in ("lo", "hi")} for bk in buckets],
            "is_bimodal": is_bimodal,
            "valley_range": valley_range,
            "tiers": tiers,
        }
    else:
        price_dist = {"n": 0, "total_bsr": n_total, "buckets": [], "is_bimodal": False, "tiers": []}

    # ── price trend: compare first-30d median vs last-30d median per ASIN ─
    historical_data = ctx.cache.get("historical_data") or {}
    price_deltas = []
    for _asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r["price"]) for r in records if r.get("price") and r["price"] > 0],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        early = statistics.median([p for _, p in pts[:30]])
        late = statistics.median([p for _, p in pts[-30:]])
        if early > 0:
            price_deltas.append((late - early) / early)
    if price_deltas:
        avg_price_change = statistics.mean(price_deltas)
        price_trend_direction = (
            "deflating"
            if avg_price_change < -0.05
            else "inflating"
            if avg_price_change > 0.05
            else "stable"
        )
        price_trend_str = f"{price_trend_direction} ({avg_price_change:+.1%} avg over 12 months, n={len(price_deltas)})"
    else:
        price_trend_str = "unknown (insufficient price history)"

    # ── New entrant ratio: % of T-snapshot ASINs listed within last 12 months ─
    # availableDate from Sellersprite is ms-since-epoch (e.g. 1686873600000).
    # "new" = product went live within 12 months before the snapshot month (base_ym).
    base_ym = ctx.cache.get("sellersprite_base_ym", "")
    snapshots = ctx.cache.get("sellersprite_snapshots") or {}
    t_snapshot = snapshots.get(base_ym) or (snapshots.get(max(snapshots)) if snapshots else [])
    if t_snapshot:
        # Cutoff = first day of (base_ym − 12 months), converted to ms-since-epoch.
        # Must anchor to base_ym, NOT to now — base_ym is T-2 months, so using now
        # would shift the window 2 months forward and under-count new entrants.
        if base_ym and len(base_ym) == 6:
            _by, _bm = int(base_ym[:4]), int(base_ym[4:])
            _total = _by * 12 + (_bm - 1) - 12
            _cy, _cm = _total // 12, _total % 12 + 1
            cutoff_ms = calendar.timegm((_cy, _cm, 1, 0, 0, 0)) * 1000
        else:
            cutoff_ms = (time.time() - 365 * 86400) * 1000  # fallback when base_ym missing
        dated = [p for p in t_snapshot if p.get("available_date_ms")]
        new_entrants = [p for p in dated if p["available_date_ms"] >= cutoff_ms]
        new_entrant_ratio_val = len(new_entrants) / len(t_snapshot) if t_snapshot else 0.0
        new_entrant_str = f"{new_entrant_ratio_val:.0%} ({len(new_entrants)}/{len(t_snapshot)} ASINs listed in last 12 months)"
    else:
        new_entrant_ratio_val = 0.0
        new_entrant_str = "unknown (no T-snapshot)"

    # ── Integrity Alert: category-level review manipulation risk ─────────
    # Signal 1 — RSR (Review-to-Sales Ratio): adapted from ReviewSummarizer.
    #   Monthly new reviews / monthly sales. Natural rate 1–3%; >10% suspicious.
    # Signal 2 — Rating jump: stars increase > 0.3 within any 30-day window
    #   (positive review bombing indicator).
    # Signal 3 — Written/Global ratio: written_reviews / global_ratings.
    #   Natural ≈ 0.10 (1:10). Paid reviewers always leave text → ratio spikes.
    #   e.g. 298 written / 470 global = 0.63 → highly suspicious.
    # Seasonal categories raise threshold to 0.40 to suppress false positives.
    sales_map = {
        str(item.get("ASIN") or item.get("asin") or "").upper(): item.get("sales", 0)
        for item in items
    }
    # ── Named thresholds (documented here for auditability) ─────────────────
    # Signal 1 — Review-to-Sales Ratio (RSR): monthly review growth / monthly sales.
    #   Natural ≈ 1–5%; >10% suggests coordinated review injection.
    _RSR_THRESHOLD = 0.10  # monthly-review-growth / monthly-sales; >10% = suspicious
    # Signal 2 — Rating jump: sustained +0.3★ rise in 30 days is implausible organically.
    _JUMP_STARS = 0.3  # minimum stars rise over a 30-day window
    _JUMP_WINDOW = 30  # days
    # Signal 3 — Written/global ratio: natural ≈ 0.10; paid reviewers always leave text.
    _RATIO_THRESHOLD = 0.50  # written_reviews / global_ratings; >50% = suspicious
    # Combined trigger thresholds (fraction of eligible ASINs flagged):
    _THRESH_HIGH_BASE = 0.30  # >30% flagged → HIGH (non-seasonal)
    _THRESH_HIGH_SEAS = 0.40  # >40% flagged → HIGH (seasonal — fewer natural spikes expected)
    _THRESH_MEDIUM = 0.15  # >15% flagged → MEDIUM

    flagged_rsr = 0
    flagged_jump = 0
    integrity_total = 0
    total_bsr = len(items)

    for asin, records in historical_data.items():
        pts = sorted(
            [(r["date"], r.get("stars"), r.get("ratings") or 0) for r in records if r.get("date")],
            key=lambda x: x[0],
        )
        if len(pts) < 60:
            continue
        integrity_total += 1

        # Signal 1: RSR — data source: Xiyouzhaoci daily_trends ratings field
        # + SalesEstimator monthly sales estimate
        monthly_sales = sales_map.get(asin.upper(), 0)
        if monthly_sales > 0:
            review_delta = pts[-1][2] - pts[0][2]
            months_spanned = max(len(pts) / 30, 1)
            rsr = (review_delta / months_spanned) / monthly_sales
            if rsr > _RSR_THRESHOLD:
                flagged_rsr += 1

        # Signal 2: rating jump — data source: Xiyouzhaoci daily_trends stars field
        stars_series = [s for _, s, _ in pts if s]
        for i in range(_JUMP_WINDOW, len(stars_series)):
            if stars_series[i] - stars_series[i - _JUMP_WINDOW] > _JUMP_STARS:
                flagged_jump += 1
                break

    # Signal 3: written/global ratio — data source: ReviewCountExtractor (BSR page scrape)
    ratio_eligible = [p for p in analysis_input if p.get("review_ratio") is not None]
    flagged_ratio = [p for p in ratio_eligible if p["review_ratio"] > _RATIO_THRESHOLD]
    flagged_ratio_count = len(flagged_ratio)
    ratio_flagged_pct = len(flagged_ratio) / len(ratio_eligible) if ratio_eligible else 0.0

    seasonality_pattern_for_threshold = result.get("seasonality", {}).get("pattern", "")
    is_seasonal = "seasonal" in seasonality_pattern_for_threshold
    integrity_threshold = _THRESH_HIGH_SEAS if is_seasonal else _THRESH_HIGH_BASE

    if integrity_total > 0 or ratio_eligible:
        ts_ratio = max(flagged_rsr, flagged_jump) / integrity_total if integrity_total > 0 else 0.0
        # Either time-series signals OR ratio signal can elevate risk independently
        combined_ratio = max(ts_ratio, ratio_flagged_pct)
        integrity_risk = (
            "HIGH"
            if combined_ratio >= integrity_threshold
            else "MEDIUM"
            if combined_ratio >= _THRESH_MEDIUM
            else "LOW"
        )
        seasonal_note = (
            f" [seasonal category — HIGH threshold raised to {_THRESH_HIGH_SEAS:.0%} to suppress false positives]"
            if is_seasonal
            else ""
        )
        integrity_str = (
            f"{integrity_risk}{seasonal_note} — "
            f"Signal 1 RSR (Xiyouzhaoci trends + sales estimate, threshold >{_RSR_THRESHOLD:.0%}): "
            f"{flagged_rsr}/{integrity_total} ASINs flagged; "
            f"Signal 2 rating-jump (Xiyouzhaoci trends, >{_JUMP_STARS}★ in {_JUMP_WINDOW}d): "
            f"{flagged_jump}/{integrity_total} ASINs flagged; "
            f"time-series coverage: {integrity_total}/{total_bsr} BSR products had ≥60 days history; "
            f"Signal 3 written/global ratio (BSR page scrape, threshold >{_RATIO_THRESHOLD:.0%}): "
            f"{flagged_ratio_count}/{len(ratio_eligible)} products flagged "
            f"({ratio_flagged_pct:.0%} of {len(ratio_eligible)} with ratio data); "
            f"combined trigger threshold: >{integrity_threshold:.0%}"
        )
    else:
        integrity_risk = "unknown"
        integrity_str = (
            "unknown — no ASINs had ≥60 days of Xiyouzhaoci price/rating history "
            "and ReviewCountExtractor returned no written/global ratio data; "
            "integrity signals cannot be computed"
        )

    # ── Compliance risk detection ────────────────────────────────────────────
    # Scan BSR titles + core keywords for regulated-substance signals.
    # Pure heuristic — no LLM, no extra API call.
    _scan_texts = (
        [ctx.cache.get("main_keyword", "")]
        + list(ctx.cache.get("core_keywords", []))
        + [item.get("Title", "") for item in items[:20]]
    )
    compliance_risks = _detect_compliance_risks(_scan_texts)

    # ── Opportunity signals ──────────────────────────────────────────────────
    # Concrete, number-backed anchors for the LLM's opportunity section.
    # All derived from already-computed data — no extra API calls.
    opportunity_signals: dict = {}
    try:
        # 1. Review floor to crack BSR top-20
        top20_reviews = [
            p["review_count"] for p in analysis_input[:20] if (p.get("review_count") or 0) > 0
        ]
        if top20_reviews:
            opportunity_signals["review_floor_top20"] = int(min(top20_reviews))
            opportunity_signals["review_median_top20"] = int(statistics.median(top20_reviews))

        # 2. Largest price gap: find the [$step]-wide band with fewest products
        if valid_prices and len(valid_prices) >= 5:
            _step = max(5, round((valid_prices[-1] - valid_prices[0]) / 5 / 5) * 5) or 5
            _lo = int(valid_prices[0] // _step) * _step
            _gap_band, _gap_cnt = None, len(valid_prices)
            b = _lo
            while b < valid_prices[-1]:
                cnt = sum(1 for p in valid_prices if b <= p < b + _step)
                if cnt < _gap_cnt:
                    _gap_cnt = cnt
                    _gap_band = f"${b:.0f}–${b + _step:.0f}"
                b += _step
            if _gap_band and _gap_cnt <= max(2, len(valid_prices) // 10):
                opportunity_signals["price_gap_band"] = _gap_band
                opportunity_signals["price_gap_count"] = _gap_cnt

        # 3. Sub-niche fragmentation: cluster top-50 BSR titles via LLM
        if ctx.router and items[:50]:
            try:
                _sub_titles = [
                    (raw_item.get("Title") or f"(no title #{i + 1})")[:80]
                    for i, raw_item in enumerate(items[:50])
                ]
                _numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(_sub_titles))
                _sub_prompt = (
                    f"You are a product analyst for the '{ctx.cache.get('main_keyword', 'unknown')}' "
                    f"Amazon category. Group the following {len(_sub_titles)} BSR product titles "
                    "into 3–7 sub-niches based on distinct product type or use case.\n\n"
                    f"{_numbered}\n\n"
                    "Return ONLY a JSON object: keys are short snake_case sub-niche names "
                    "(e.g. 'snap_trap', 'glue_board'), values are arrays of 1-based title indices. "
                    "Every title must appear in exactly one sub-niche. No explanation, no markdown."
                )
                _sub_res = await ctx.router.route_and_execute(
                    _sub_prompt, category=TaskCategory.SIMPLE_CLEANING
                )
                _raw = _sub_res.text.strip()
                _m = re.search(r"\{[\s\S]*\}", _raw)
                if _m:
                    _sub_map: dict = json.loads(_m.group())
                    sub_counts = {
                        k: len(v) for k, v in _sub_map.items() if isinstance(v, list) and v
                    }
                    if sub_counts:
                        opportunity_signals["sub_niche_counts"] = sub_counts
                        smallest = min(sub_counts, key=sub_counts.get)
                        if sub_counts[smallest] >= 2:
                            opportunity_signals["least_crowded_sub_niche"] = {
                                "name": smallest.replace("_", " "),
                                "count": sub_counts[smallest],
                            }
            except Exception as _sub_err:
                logger.warning(f"[sub_niche_fragmentation] LLM clustering failed: {_sub_err}")

        # 4. Rank positions with the lowest review counts (easiest to displace)
        ranked_by_reviews = sorted(
            [
                {"rank": p["rank"], "reviews": p["review_count"]}
                for p in analysis_input
                if p.get("rank") and 10 <= p["rank"] <= 50 and (p.get("review_count") or 0) > 0
            ],
            key=lambda x: x["reviews"],
        )
        if ranked_by_reviews:
            weakest = ranked_by_reviews[0]
            opportunity_signals["weakest_rank_slot"] = {
                "rank": weakest["rank"],
                "reviews": weakest["reviews"],
                "implication": (
                    f"BSR #{weakest['rank']} held with only {weakest['reviews']:,} reviews — "
                    f"lowest in the rank-10–50 band, suggesting a beachhead entry point"
                ),
            }

        # 5. Compliance-accessible angle (when CRITICAL regulatory risk detected,
        #    flag the §25(b) natural/exempt path as the lower-barrier entry route)
        if compliance_risks.get("overall_risk") == "CRITICAL":
            natural_ids = {"neem_natural", "repellent"}
            detected_ids = {d["id"] for d in compliance_risks.get("detected", [])}
            if not natural_ids & detected_ids:
                opportunity_signals["compliance_accessible_angle"] = (
                    "FIFRA §25(b) exempt path: products using only EPA-listed minimum-risk "
                    "active ingredients (e.g. citric acid, peppermint oil, garlic) require NO "
                    "EPA registration, dramatically reducing time-to-market and regulatory cost. "
                    "The natural/organic sub-segment is underrepresented in this BSR top-50."
                )

        # 6. New entrant proof points from Sellersprite snapshot
        ss_new_asins = []
        if t_snapshot and "cutoff_ms" in dir():
            ss_new_asins = [
                p["asin"]
                for p in t_snapshot
                if p.get("available_date_ms") and p["available_date_ms"] >= cutoff_ms  # type: ignore[name-defined]
            ]
        if ss_new_asins:
            # Cross-reference with raw BSR items to get titles
            asin_to_title = {
                (it.get("ASIN") or it.get("asin")): it.get("Title", "") for it in items
            }
            examples = [asin_to_title[a][:60] for a in ss_new_asins[:3] if asin_to_title.get(a)]
            if examples:
                opportunity_signals["recent_breakthrough_examples"] = examples

    except Exception as _opp_err:
        logger.warning(f"[opportunity_signals] computation failed: {_opp_err}")

    # Extract churn / seasonality / BSR churn signals for prompt template
    churn = result.get("market_churn", {})
    seasonality = result.get("seasonality", {})
    bsr_churn = result.get("bsr_churn", {})
    peak_months_str = ", ".join(str(m) for m in seasonality.get("peak_months", [])) or "N/A"
    platform_warning = (
        " ⚠️ Peak overlaps platform events (Prime Day/Black Friday)"
        if seasonality.get("platform_event_in_peak")
        else ""
    )

    # ── Data quality coverage (passed to LLM so it can caveat low-coverage claims) ──
    dq_sales = sum(1 for p in analysis_input if (p.get("sales") or 0) > 0)
    dq_seller = sum(1 for p in analysis_input if p.get("seller_type") not in (None, "", "Unknown"))
    dq_rating = sum(1 for p in analysis_input if (p.get("rating") or 0) > 0)
    dq_reviews = sum(1 for p in analysis_input if (p.get("review_count") or 0) > 0)
    dq_brand = sum(1 for p in analysis_input if p.get("brand"))
    dq_hist = integrity_total  # ASINs with ≥60-day Xiyouzhaoci history
    dq_ratio = len(ratio_eligible)  # ASINs with written/global ratio from scrape
    dq_snapshots = len(bsr_churn.get("snapshots_available", []))
    dq_cpc = bid_entry_count  # number of keyword CPC entries available

    def _pct(n: int) -> str:
        return f"{n}/{n_total} ({n / n_total:.0%})"

    data_quality_str = (
        f"BSR products scraped: {n_total} | "
        f"sales estimate coverage: {_pct(dq_sales)} | "
        f"seller-type coverage: {_pct(dq_seller)} | "
        f"star-rating coverage: {_pct(dq_rating)} | "
        f"review-count coverage: {_pct(dq_reviews)} | "
        f"brand coverage: {_pct(dq_brand)} | "
        f"price coverage: {_pct(len(valid_prices))} | "
        f"Xiyouzhaoci ≥60-day history: {dq_hist}/{n_total} ({dq_hist / n_total:.0%}) | "
        f"written/global ratio data: {dq_ratio}/{n_total} ({dq_ratio / n_total:.0%}) | "
        f"Sellersprite BSR snapshots available: {dq_snapshots} months | "
        f"CPC keyword entries: {dq_cpc} | "
        f"contamination check: {_cs_n_removed} products removed ({_cs_status})"
    )

    # ── Contamination warning (full human-readable string for the report) ────
    _cs = contamination_stats
    if _cs_status == "clean":
        contamination_warning = "none — all BSR products matched core category keywords"
    elif _cs_status == "filtered":
        _cs_dom = _cs.get("dominant_type", "")
        _cs_dom_str = f"; dominant type: '{_cs_dom}'" if _cs_dom else ""
        _cs_sample = _cs.get("sample_removed", [])
        _cs_sample_str = f"; examples removed: {_cs_sample[:2]}" if _cs_sample else ""
        contamination_warning = (
            f"{_cs_n_removed} off-category products removed "
            f"({_cs.get('outlier_rate', 0):.0%}) via {_cs.get('method', 'unknown')}"
            f"{_cs_dom_str}; "
            f"{_cs.get('n_retained', n_total)} products retained for analysis"
            f"{_cs_sample_str}"
        )
    elif _cs_status == "warning":
        contamination_warning = (
            f"⚠️ HIGH CONTAMINATION ({_cs.get('outlier_rate', 0):.0%}) — "
            f"filtering skipped; metrics may be skewed across mixed product types. "
            f"Sample outliers: {_cs.get('sample_outliers', [])[:2]}"
        )
    elif _cs_status == "skipped":
        contamination_warning = f"check skipped ({_cs.get('reason', 'unknown')})"
    else:
        contamination_warning = "not run"

    # ── Startup capital breakdown ─────────────────────────────────────────────
    # Conservative rule-of-thumb for a new FBA product launch.
    # All constants are auditable here; change them to tune the recommendation.
    #
    # Basis: ONE first batch of _CAP_UNITS units sold at median price.
    # Inventory, PPC, and fees all use the same _CAP_UNITS denominator so the
    # totals are directly comparable.  _CAP_SELL_MONTHS is a transparency label
    # only (how long it realistically takes to sell one batch); it must NOT be
    # used as a multiplier on PPC or fees — that would imply selling 3× the
    # batch, making the three line items inconsistent.
    _CAP_UNITS = 1000  # first-batch order quantity (units)
    _CAP_COGS = 0.30  # COGS as fraction of retail price (China-manufactured)
    _CAP_ACOS = 0.30  # target ACOS during the ranking phase
    _CAP_FEES = _REFERRAL_FEE_PCT + _fba_fee_pct  # referral 15% + FBA from 2026 fee schedule
    _CAP_SELL_MONTHS = 3  # expected months to sell through the first batch (display only)
    _CAP_OVERHEAD = 2000  # fixed launch costs: photography, A+, listing, freight ($)
    _CAP_BUFFER = 0.20  # working capital buffer on subtotal

    # If bimodal, compute capital against each tier's median separately
    # so the operator can see how the budget changes by tier choice.
    _cap_price = median_price  # overall median — may be in the gap if bimodal
    _cap_inv = int(_CAP_UNITS * _cap_price * _CAP_COGS)
    _cap_ppc = int(_CAP_UNITS * _cap_price * _CAP_ACOS)
    _cap_fees = int(_CAP_UNITS * _cap_price * _CAP_FEES)
    _cap_sub = _cap_inv + _cap_ppc + _cap_fees + _CAP_OVERHEAD
    _cap_total = int(_cap_sub * (1 + _CAP_BUFFER))

    # Per-tier capital estimates when bimodal
    _tier_capitals: list[dict] = []
    if price_dist.get("is_bimodal"):
        for tier in price_dist.get("tiers", []):
            t_median_str = tier.get("median", "")
            try:
                t_med = float(t_median_str.lstrip("$"))
            except ValueError:
                continue
            t_inv = int(_CAP_UNITS * t_med * _CAP_COGS)
            t_ppc = int(_CAP_UNITS * t_med * _CAP_ACOS)
            t_fees = int(_CAP_UNITS * t_med * _CAP_FEES)
            t_sub = t_inv + t_ppc + t_fees + _CAP_OVERHEAD
            _tier_capitals.append(
                {
                    "tier": tier["label"],
                    "median": t_median_str,
                    "inventory": f"${t_inv:,}",
                    "ppc": f"${t_ppc:,}",
                    "fees": f"${t_fees:,}",
                    "overhead": f"${_CAP_OVERHEAD:,}",
                    "total": f"${int(t_sub * (1 + _CAP_BUFFER)):,}",
                }
            )

    # ── Brand & Seller concentration ──────────────────────────────────────────
    _brand_pos: Counter = Counter()
    _brand_sales_cnt: Counter = Counter()
    _seller_pos: Counter = Counter()
    _seller_sales_cnt: Counter = Counter()
    _seller_brands: dict = {}

    for _ai in analysis_input:
        _b = _ai.get("brand") or "Unknown"
        _s = _ai.get("sales") or 0
        _sid = _ai.get("seller_id")
        _brand_pos[_b] += 1
        _brand_sales_cnt[_b] += _s
        if _sid:
            _seller_pos[_sid] += 1
            _seller_sales_cnt[_sid] += _s
            _seller_brands.setdefault(_sid, set()).add(_b)

    _N_items = len(analysis_input)
    _total_brand_sales = sum(_brand_sales_cnt.values())

    def _conc_cr(counter: Counter, top_n: int) -> float:
        _tot = sum(counter.values()) or 1
        return round(sum(v for _, v in counter.most_common(top_n)) / _tot, 4)

    def _conc_hhi(counter: Counter) -> float:
        _tot = sum(counter.values()) or 1
        return round(sum((v / _tot) ** 2 for v in counter.values()), 4)

    _conc_brand = {
        "cr3_position": _conc_cr(_brand_pos, 3),
        "cr5_position": _conc_cr(_brand_pos, 5),
        "hhi_position": _conc_hhi(_brand_pos),
        "cr3_sales": _conc_cr(_brand_sales_cnt, 3) if _total_brand_sales else None,
        "cr5_sales": _conc_cr(_brand_sales_cnt, 5) if _total_brand_sales else None,
        "hhi_sales": _conc_hhi(_brand_sales_cnt) if _total_brand_sales else None,
        "top_brands_by_position": [
            {"brand": _b, "positions": _c, "position_share": round(_c / _N_items, 4)}
            for _b, _c in _brand_pos.most_common(10)
        ],
        "top_brands_by_sales": [
            {
                "brand": _b,
                "monthly_units": _c,
                "sales_share": round(_c / max(_total_brand_sales, 1), 4),
            }
            for _b, _c in _brand_sales_cnt.most_common(10)
            if _c > 0
        ],
    }
    _multi_brand_sellers = sorted(
        [
            {
                "seller_id": _sid,
                "brands": sorted(_brands),
                "positions": _seller_pos[_sid],
                "monthly_units": _seller_sales_cnt[_sid],
                "position_share": round(_seller_pos[_sid] / _N_items, 4),
            }
            for _sid, _brands in _seller_brands.items()
            if len(_brands) > 1
        ],
        key=lambda x: x["positions"],
        reverse=True,
    )
    _conc_seller = {
        "n_sellers_identified": len(_seller_pos),
        "cr3_position": _conc_cr(_seller_pos, 3) if _seller_pos else None,
        "hhi_position": _conc_hhi(_seller_pos) if _seller_pos else None,
        "n_multi_brand_sellers": len(_multi_brand_sellers),
        "multi_brand_sellers": _multi_brand_sellers[:5],
    }
    concentration_data = {
        "n_products": _N_items,
        "n_brands": len(_brand_pos),
        "brand": _conc_brand,
        "seller": _conc_seller,
    }

    # ── Top ASIN evidence table (top 10 by BSR rank) ──────────────────────────
    # analysis_input[i] corresponds to items[i] (same index).
    # Sort by rank using the original index so both arrays stay aligned.
    _TOP_N = 10
    sorted_indices = sorted(
        range(len(items)),
        key=lambda i: _parse_int(items[i].get("Rank"), default=9999),
    )
    top_asin_rows = []
    for i in sorted_indices[:_TOP_N]:
        raw = items[i]
        enriched = analysis_input[i]
        title_raw = raw.get("Title") or ""
        title = title_raw[:40].rstrip() + ("…" if len(title_raw) > 40 else "")
        top_asin_rows.append(
            {
                "rank": enriched["rank"],
                "asin": (raw.get("ASIN") or raw.get("asin") or "N/A"),
                "brand": (enriched.get("brand") or "Unknown")[:20],
                "title": title,
                "price": f"${enriched['price']:.2f}" if enriched["price"] else "N/A",
                "rating": f"{enriched['rating']:.1f}★" if enriched["rating"] else "N/A",
                "reviews": f"{enriched['review_count']:,}" if enriched["review_count"] else "N/A",
                "units_mo": f"{enriched['sales']:,}" if enriched["sales"] else "N/A",
                "seller_type": enriched.get("seller_type") or "Unknown",
            }
        )

    return [
        {
            "analysis_result": json.dumps(result, ensure_ascii=False),
            "monopoly_score": round(result.get("overall_score", 0), 2),
            "monopoly_status": result.get("status", "N/A"),
            "main_keyword": ctx.cache.get("main_keyword"),
            "core_keywords": ", ".join(ctx.cache.get("core_keywords", [])),
            "niche_median_price": f"${median_price:.2f}",
            "niche_monthly_units": f"{total_monthly_units:,} units",
            "niche_monthly_gmv": f"${niche_monthly_gmv:,}",
            "bid_insight": json.dumps(bid_raw, ensure_ascii=False)
            if bid_entry_count
            else "N/A (no CPC data fetched)",
            "data_quality": data_quality_str,
            "review_disparity": (
                f"{review_disparity_val}x"
                if review_disparity_val is not None
                else "N/A (fewer than 51 BSR products — top/tail buckets too small)"
            ),
            "price_trend": price_trend_str,
            "new_entrant_ratio": new_entrant_str,
            "integrity_alert": integrity_str,
            "integrity_hist_cov": (
                f"{integrity_total}/{n_total} ({integrity_total / n_total:.0%})"
            ),
            "integrity_ratio_cov": (
                f"{len(ratio_eligible)}/{n_total} ({len(ratio_eligible) / n_total:.0%})"
            ),
            "recommended_capital": f"${_cap_total:,}",
            "capital_inventory": f"${_cap_inv:,}",
            "capital_ppc": f"${_cap_ppc:,}",
            "capital_fees": f"${_cap_fees:,}",
            "capital_overhead": f"${_CAP_OVERHEAD:,}",
            "capital_units": str(_CAP_UNITS),
            "capital_cogs_pct": f"{_CAP_COGS:.0%}",
            "capital_acos_pct": f"{_CAP_ACOS:.0%}",
            "capital_fees_pct": f"{_CAP_FEES:.0%}",
            "capital_sell_months": str(_CAP_SELL_MONTHS),
            "industry_typical_cr3": f"{baseline.get('typical_cr3', 0.4) * 100}%",
            "data_confidence_r2": estimator.category_params.get(str(node_id), {}).get(
                "r_squared", 0.95
            ),
            "social_psi": ctx.cache.get("category_social_psi", "N/A"),
            "social_verdict": ctx.cache.get("category_social_verdict", "N/A"),
            "deal_intensity": ctx.cache.get("category_deal_intensity", "N/A"),
            "brand_social_data": json.dumps(
                ctx.cache.get("brand_social_data", []), ensure_ascii=False
            ),
            # Rating-based churn (xiyouzhaoci daily trends)
            "churn_pattern": churn.get("pattern", "unknown"),
            "churn_score": churn.get("churn_score", "N/A"),
            "new_product_ratio": f"{churn.get('new_product_ratio', 0):.0%}",
            "collapse_rate": f"{churn.get('collapse_rate', 0):.0%}",
            # BSR listing metabolism (set-comparison across monthly snapshots)
            "bsr_churn_label": bsr_churn.get("label", "unknown"),
            "bsr_churn_3m": f"{bsr_churn.get('churn_3m') or 0:.0%}",
            "bsr_churn_6m": f"{bsr_churn.get('churn_6m') or 0:.0%}",
            "bsr_churn_12m": f"{bsr_churn.get('churn_12m') or 0:.0%}",
            "bsr_snapshots": ", ".join(bsr_churn.get("snapshots_available", [])) or "N/A",
            # Seasonality
            "seasonality_pattern": seasonality.get("pattern", "unknown"),
            "seasonality_score": seasonality.get("seasonality_score", "N/A"),
            "seasonality_source": seasonality.get("source", "bsr_daily_trends"),
            "seasonality_n_points": seasonality.get("n_data_points", 0),
            "peak_months": peak_months_str + platform_warning,
            # Top ASIN evidence table (JSON → rendered as markdown table by LLM)
            "top_asin_table": json.dumps(top_asin_rows, ensure_ascii=False),
            # Price distribution (JSON → rendered as table by LLM)
            "price_distribution": json.dumps(price_dist, ensure_ascii=False),
            # Per-tier capital when bimodal (empty list if unimodal)
            "tier_capitals": json.dumps(_tier_capitals, ensure_ascii=False),
            # Compliance / regulatory / hazmat risk (heuristic scan of titles + keywords)
            "compliance_risks": json.dumps(compliance_risks, ensure_ascii=False),
            # Concrete opportunity anchors for the actionable section
            "opportunity_signals": json.dumps(opportunity_signals, ensure_ascii=False),
            # Category contamination: off-category products detected and removed
            "contamination_warning": contamination_warning,
            "contamination_stats": json.dumps(contamination_stats, ensure_ascii=False),
            # Steady-state ad burden (no owned product required — derived from market data)
            "category_cvr": f"{_category_cvr:.1%}",
            "category_cvr_source": ctx.cache.get("category_cvr_source", "N/A"),
            "median_cpc": f"${_median_cpc:.2f}" if _median_cpc else "N/A",
            "estimated_steady_state_acos": f"{_estimated_acos:.0%}" if _estimated_acos else "N/A",
            "ad_profit_drag": f"{_ad_profit_drag:.0%}" if _ad_profit_drag else "N/A",
            "fba_fee_pct": f"{_fba_fee_pct:.1%}",
            "breakeven_acos": f"{_breakeven_acos:.0%}",
            "ad_burden_verdict": _ad_burden_verdict,
            # Brand & seller concentration (position share, sales share, CR3/CR5, HHI)
            "concentration_data": json.dumps(concentration_data, ensure_ascii=False),
            # Recent critical reviews for top brands (≤90 days — fixable deficiency signals)
            "critical_reviews_data": json.dumps(
                ctx.cache.get("critical_reviews_data", {}), ensure_ascii=False
            ),
        }
    ]


def _trim_repetition(text: str, min_run: int = 4) -> str:
    """
    Remove trailing repetitive content caused by LLM degeneration near max_output_tokens.

    When a model hits its output limit inside a markdown table it starts repeating
    the last line (e.g. the separator row "| :--- | :--- |") dozens of times.
    This function detects any run of ≥ min_run consecutive identical non-empty lines
    and trims everything from the start of that run onward, keeping exactly one copy.

    Returns the original text unchanged if no repetition is detected.
    """
    lines = text.splitlines()
    n = len(lines)
    if n < min_run * 2:
        return text

    i = n - 1
    while i >= min_run - 1:
        line = lines[i].strip()
        if not line:
            i -= 1
            continue
        # Walk backward to find the start of a run of this exact stripped line
        run_start = i
        while run_start > 0 and lines[run_start - 1].strip() == line:
            run_start -= 1
        run_len = i - run_start + 1
        if run_len >= min_run:
            trimmed = "\n".join(lines[: run_start + 1])
            logger.warning(
                f"[prepare_report_artifact] Trimmed {run_len - 1} repeated lines "
                f"(pattern: {line[:60]!r})"
            )
            return trimmed.rstrip() + "\n\n*（报告在此截断：模型输出已达上限，重复内容已删除）*\n"
        # No long run ending at i — move up past this line
        i = run_start - 1

    return text


async def _prepare_report_artifact(items: list[dict], ctx: Any) -> list[dict]:
    """Saves the report to a local Markdown file, stripping trailing repetition."""
    if not items or "deliver_report" not in items[0]:
        return items
    report_data = items[0]["deliver_report"]
    report_text = (
        report_data.text
        if hasattr(report_data, "text")
        else report_data.get("text")
        if isinstance(report_data, dict)
        else str(report_data)
    )
    if not report_text or report_text == "None":
        return items

    # Strip LLM degeneration artifacts before persisting
    report_text = _trim_repetition(report_text)

    raw_kw = str(ctx.cache.get("main_keyword", "niche"))
    keyword = re.sub(r"[^\w]", "_", raw_kw, flags=re.ASCII)[:40].strip("_") or "niche"
    _tz = ZoneInfo(ctx.config.get("timezone", "America/Los_Angeles"))
    filename = f"Monopoly_Analysis_{keyword}_{datetime.now(tz=_tz).strftime('%Y%m%d_%H%M')}.md"
    report_dir = os.path.abspath("data/reports")
    os.makedirs(report_dir, exist_ok=True)
    file_path = os.path.join(report_dir, filename)
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        items[0]["report_file_path"] = file_path
        logger.info(f"Artifact prepared at: {file_path} ({len(report_text)} chars)")
    except Exception as e:
        logger.error(f"Failed to write report file: {e}")
    return items


@WorkflowRegistry.register("category_monopoly_analysis")
def build_category_monopoly_analysis(config: dict) -> Workflow:
    monopoly_spec = prompt_manager.get_spec("monopoly_report")
    ctx_vars = {
        name: f"{{{name}}}" for name in (monopoly_spec.required_vars if monopoly_spec else [])
    }

    return Workflow(
        name="category_monopoly_analysis",
        steps=[
            ProcessStep(name="fetch_bsr_top_100", fn=_fetch_bsr_list),
            ProcessStep(name="fetch_core_keywords", fn=_fetch_core_keywords),
            ProcessStep(name="filter_category_coherence", fn=_filter_category_coherence),
            ProcessStep(name="enrich_sales_data", fn=_enrich_sales),
            EnrichStep(
                name="enrich_seller_background",
                extractor_fn=_enrich_seller_info,
                parallel=True,
                concurrency=5,
            ),
            ProcessStep(name="fetch_market_signals", fn=_fetch_market_signals),
            ProcessStep(name="fetch_sellersprite_bsr", fn=_fetch_sellersprite_bsr),
            ProcessStep(name="fetch_category_cvr", fn=_fetch_category_cvr),
            ProcessStep(name="enrich_external_intensity", fn=_enrich_external_intensity),
            ProcessStep(name="enrich_batch_traffic_scores", fn=_enrich_batch_traffic_scores),
            ProcessStep(name="fetch_time_series_data", fn=_fetch_time_series_data),
            ProcessStep(
                name="fetch_critical_reviews_top_brands",
                fn=_fetch_critical_reviews_top_brands,
            ),
            ProcessStep(name="calculate_monopoly_score", fn=_run_monopoly_analysis),
            ProcessStep(
                name="deliver_report",
                batch_threshold=1,
                prompt_template=prompt_manager.render_spec("monopoly_report", ctx_vars),
                compute_target=ComputeTarget.CLOUD_LLM,
            ),
            ProcessStep(name="prepare_report_artifact", fn=_prepare_report_artifact),
        ],
    )
