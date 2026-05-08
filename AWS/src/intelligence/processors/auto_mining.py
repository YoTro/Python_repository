from __future__ import annotations
"""
Auto campaign search term mining.

Pipeline
--------
Stage 1 — Empirical Bayes prior fitting
    Fit Beta(α, β) from observed per-term CVRs via Method of Moments.
    Captures account-level CVR distribution without hardcoding category thresholds.
    Prior is data-driven: a product with 8% account CVR gets a different baseline
    than one with 1%, so statistical thresholds auto-scale.

Stage 2 — Negative keyword candidates (dual-gate, OR logic)
    Gate A (statistical):  EB-expected orders >= threshold AND actual orders == 0.
        Threshold 3.0 → P0  (P(X=0|λ=3) ≈ 0.05, equivalent to p<0.05)
        Threshold 1.5 → P1  (P(X=0|λ=1.5) ≈ 0.22, directionally clear)
    Gate B (absolute spend): spend >= breakeven_spend × multiplier AND orders == 0.
        breakeven_spend = avg_price × target_acos (max acceptable spend for one order)
        1.5× breakeven → P0;  0.75× breakeven → P1
    Either gate triggers; priority is the stronger of the two.
    Gate C (converting but wasteful): effective_CPO > 3× breakeven → P1.

Stage 3 — Harvest-to-manual candidates
    Conditions:
      - orders >= 2  (minimum statistical signal)
      - actual ACOS <= target_acos  (genuinely efficient)
      - Wilson lower-bound CVR > 0  (at 95% CI; avoids promoting lucky single events)
    Suggested bid = target_acos × avg_price × Wilson_CVR_lower_bound  (conservative).
    Deduplication: skipped if term already exists in manual keyword set.

Entry point
-----------
    build_auto_mining_actions(
        search_term_records, auto_pt_cids, existing_manual_kws,
        avg_price, target_acos, days
    ) → Dict
"""

import logging
import math
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ── Tuneable constants (no category-specific values) ─────────────────────────
_PRIOR_MIN_CLICKS      = 5     # minimum clicks per term for EB prior fitting
_PRIOR_MIN_TERMS       = 10    # minimum distinct terms for reliable MOM estimate
_NEG_P0_EXPECTED       = 3.0   # expected_orders threshold for P0 (p≈0.05)
_NEG_P1_EXPECTED       = 1.5   # expected_orders threshold for P1 (p≈0.22)
_NEG_SPEND_P0_MULT     = 1.5   # spend ≥ breakeven × this → P0
_NEG_SPEND_P1_MULT     = 0.75  # spend ≥ breakeven × this → P1
_NEG_CPO_MULT          = 3.0   # effective_CPO > breakeven × this → Gate C P1
_HARVEST_MIN_ORDERS    = 2     # minimum orders to harvest a term
_HARVEST_CI_ALPHA      = 0.05  # one-sided Wilson CI level for CVR lower bound


# ── Stage 1: Empirical Bayes prior ────────────────────────────────────────────

def _fit_beta_prior(term_totals: List[Dict]) -> Tuple[float, float]:
    """
    Fit Beta(α, β) from observed CVRs via Method of Moments.

    Only terms with >= _PRIOR_MIN_CLICKS clicks are used to avoid fitting
    on noise.  If fewer than _PRIOR_MIN_TERMS valid terms exist, falls back
    to a weak prior anchored at pooled CVR (5-click equivalent strength).

    Returns (alpha, beta) with both values ≥ 0.1 for numerical stability.
    """
    valid = [t for t in term_totals if t.get("clicks", 0) >= _PRIOR_MIN_CLICKS]

    # Pooled CVR as base reference
    total_c  = sum(t["clicks"] for t in valid)
    total_o  = sum(t["orders"] for t in valid)
    mu_pool  = total_o / total_c if total_c > 0 else 0.02

    if len(valid) < _PRIOR_MIN_TERMS:
        k = 5.0
        return max(mu_pool * k, 0.1), max((1.0 - mu_pool) * k, 0.1)

    # MOM: only terms with at least 1 conversion carry variance information
    obs_cvrs = [t["orders"] / t["clicks"] for t in valid if t["orders"] > 0]
    if len(obs_cvrs) < 3:
        k = 5.0
        return max(mu_pool * k, 0.1), max((1.0 - mu_pool) * k, 0.1)

    mu  = sum(obs_cvrs) / len(obs_cvrs)
    var = sum((x - mu) ** 2 for x in obs_cvrs) / len(obs_cvrs)

    # k = μ(1−μ)/σ² − 1; clamp variance away from 0 and the theoretical max
    var_max = mu * (1.0 - mu) * 0.99
    var     = min(max(var, 1e-6), var_max)
    k       = max(mu * (1.0 - mu) / var - 1.0, 1.0)

    alpha = max(mu * k, 0.1)
    beta  = max((1.0 - mu) * k, 0.1)
    logger.debug(
        f"[auto_mining] EB prior: α={alpha:.3f} β={beta:.3f} "
        f"(μ={mu:.4f}, n_obs={len(obs_cvrs)}, n_valid={len(valid)})"
    )
    return alpha, beta


# ── Stage 2: Negative detection ───────────────────────────────────────────────

def _negative_priority(
    clicks: int,
    orders: int,
    spend: float,
    alpha: float,
    beta: float,
    breakeven_spend: float,
) -> Optional[str]:
    """
    Returns 'P0', 'P1', or None.
    OR-logic: the higher (lower-indexed) priority from Gate A, B, C wins.
    """
    if clicks <= 0:
        return None

    prior_cvr = alpha / (alpha + beta)
    expected  = clicks * prior_cvr

    if orders == 0:
        # Gate A — statistical
        if expected >= _NEG_P0_EXPECTED:
            gate_a = "P0"
        elif expected >= _NEG_P1_EXPECTED:
            gate_a = "P1"
        else:
            gate_a = None

        # Gate B — absolute spend
        gate_b = None
        if breakeven_spend > 0:
            if spend >= breakeven_spend * _NEG_SPEND_P0_MULT:
                gate_b = "P0"
            elif spend >= breakeven_spend * _NEG_SPEND_P1_MULT:
                gate_b = "P1"

        prios = [p for p in (gate_a, gate_b) if p is not None]
        if not prios:
            return None
        return "P0" if "P0" in prios else "P1"

    # Gate C — converting but CPO > 3× breakeven
    if breakeven_spend > 0 and orders > 0:
        if (spend / orders) > breakeven_spend * _NEG_CPO_MULT:
            return "P1"

    return None


# ── Stage 3: Harvest detection ────────────────────────────────────────────────

def _harvest_signal(
    clicks: int,
    orders: int,
    spend: float,
    avg_price: float,
    target_acos: float,
) -> Optional[Tuple[str, float]]:
    """
    Returns (priority, suggested_bid) or None.

    suggested_bid = target_acos × avg_price × Wilson_CVR_lower_bound
    This is the break-even bid at the lower-bound CVR — conservative by design.
    """
    if orders < _HARVEST_MIN_ORDERS or clicks <= 0 or avg_price <= 0:
        return None

    actual_acos = spend / (orders * avg_price)
    if actual_acos > target_acos:
        return None

    # Wilson lower-bound CVR (exact Beta percentile if scipy available)
    try:
        from scipy.stats import beta as _scipy_beta
        cvr_lower = float(
            _scipy_beta.ppf(_HARVEST_CI_ALPHA, orders + 1, clicks - orders + 1)
        )
    except Exception:
        # Normal-approx Wilson fallback
        p = orders / clicks
        z = 1.645
        n = clicks
        denom = 1.0 + z ** 2 / n
        centre = p + z ** 2 / (2 * n)
        margin = z * math.sqrt(p * (1 - p) / n + z ** 2 / (4 * n ** 2))
        cvr_lower = (centre - margin) / denom

    if cvr_lower <= 0:
        return None

    suggested_bid = round(target_acos * avg_price * cvr_lower, 2)
    priority = "P0" if orders >= 5 else "P1"
    return priority, suggested_bid


# ── Aggregation ───────────────────────────────────────────────────────────────

def _aggregate_search_terms(
    records: List[Dict],
    cids: Set[str],
) -> List[Dict]:
    """
    Aggregate raw (possibly multi-day) spSearchTerm records for auto/PT campaigns.

    For auto campaigns `keyword_text` in the raw report IS the customer search
    query (there is no underlying keyword).  Records with empty keyword_text
    (e.g., ASIN-based product targeting) are grouped under the empty string
    and later filtered.

    Returns list of aggregated per-term dicts.
    """
    agg: Dict[Tuple[str, str], Dict] = defaultdict(
        lambda: {"clicks": 0, "orders": 0, "spend": 0.0, "sales": 0.0,
                 "campaign_id": "", "query": ""}
    )
    for r in records:
        cid = str(r.get("campaign_id", "") or "")
        if cid not in cids:
            continue
        # keyword_text holds the search query for auto campaigns
        query = (r.get("keyword_text") or "").strip()
        if not query:
            continue
        key = (cid, query.lower())
        a = agg[key]
        a["campaign_id"]  = cid
        a["query"]        = query
        a["clicks"]      += int(r.get("clicks", 0) or 0)
        a["orders"]      += int(r.get("orders", 0) or 0)
        a["spend"]       += float(r.get("spend", 0) or 0)
        a["sales"]       += float(r.get("sales", 0) or 0)
    return list(agg.values())


# ── Entry point ───────────────────────────────────────────────────────────────

def build_auto_mining_actions(
    search_term_records: List[Dict],
    auto_pt_cids: Set[str],
    existing_manual_kws: Set[str],
    avg_price: float,
    target_acos: float,
    days: int,
) -> Dict[str, Any]:
    """
    Main entry point for auto campaign search term mining.

    Parameters
    ----------
    search_term_records : raw spSearchTerm records (all campaigns, all dates)
    auto_pt_cids        : campaign IDs outside LP scope (auto / PT campaigns)
    existing_manual_kws : lowercased keyword_text set already in manual campaigns
                          (used for harvest deduplication)
    avg_price           : average selling price
    target_acos         : fractional target ACOS (e.g. 0.35)
    days                : lookback window length

    Returns
    -------
    {
      "negatives": [...],   negative keyword candidates
      "harvest":   [...],   search terms to promote to manual
      "beta_prior": {...},  fitted prior diagnostics
      "summary":   {...},   aggregate stats
    }
    """
    if not auto_pt_cids:
        return {"negatives": [], "harvest": [], "beta_prior": {},
                "summary": {"skipped": True, "reason": "no auto/PT campaigns identified"}}

    term_totals = _aggregate_search_terms(search_term_records, auto_pt_cids)

    if not term_totals:
        return {"negatives": [], "harvest": [], "beta_prior": {},
                "summary": {"skipped": True,
                            "reason": "no search term data found for auto/PT campaigns"}}

    # Stage 1: fit EB prior from this ASIN's own data
    alpha, beta = _fit_beta_prior(term_totals)
    prior_cvr_pct = round(alpha / (alpha + beta) * 100, 2)

    # Breakeven spend: price a seller is willing to pay per order at target ACOS
    breakeven_spend = avg_price * target_acos if avg_price > 0 and target_acos > 0 else 0.0

    negatives: List[Dict] = []
    harvest:   List[Dict] = []

    for t in term_totals:
        query   = t["query"]
        cid     = t["campaign_id"]
        clicks  = t["clicks"]
        orders  = t["orders"]
        spend   = t["spend"]
        daily_spend = round(spend / days, 2) if days > 0 else round(spend, 2)

        # Stage 2: negative detection
        neg_p = _negative_priority(clicks, orders, spend, alpha, beta, breakeven_spend)
        if neg_p:
            expected = round(clicks * (alpha / (alpha + beta)), 2)
            # Single-word queries → exact negative; multi-word → phrase negative
            suggested_match = "EXACT" if len(query.split()) == 1 else "PHRASE"
            negatives.append({
                "action":             "add_negative_keyword",
                "priority":           neg_p,
                "campaign_id":        cid,
                "keyword_text":       query,
                "suggested_match":    suggested_match,
                "clicks":             clicks,
                "orders":             orders,
                "spend_total":        round(spend, 2),
                "daily_spend":        daily_spend,
                "expected_orders_eb": expected,
                "breakeven_spend":    round(breakeven_spend, 2),
                "rationale": (
                    f"${spend:.2f} spent ({clicks} clicks, {orders} orders); "
                    f"EB expected {expected:.1f} orders at account CVR "
                    f"{prior_cvr_pct:.1f}%; "
                    f"breakeven_spend=${breakeven_spend:.2f}"
                ),
            })
            # A negative candidate is not evaluated for harvest
            continue

        # Stage 3: harvest detection (skip if already a manual keyword)
        if query.lower() not in existing_manual_kws:
            result = _harvest_signal(clicks, orders, spend, avg_price, target_acos)
            if result:
                harv_p, suggested_bid = result
                actual_acos_pct = round(spend / (orders * avg_price) * 100, 1) \
                    if orders > 0 and avg_price > 0 else None
                harvest.append({
                    "action":          "harvest_to_manual",
                    "priority":        harv_p,
                    "campaign_id":     cid,
                    "keyword_text":    query,
                    "suggested_match": "EXACT",
                    "suggested_bid":   suggested_bid,
                    "clicks":          clicks,
                    "orders":          orders,
                    "spend_total":     round(spend, 2),
                    "daily_spend":     daily_spend,
                    "acos_pct":        actual_acos_pct,
                    "rationale": (
                        f"{orders} orders @ ACOS {actual_acos_pct}% "
                        f"(target {round(target_acos * 100):.0f}%); "
                        f"suggested bid ${suggested_bid} "
                        f"(target_acos × avg_price × Wilson CVR lower bound)"
                    ),
                })

    # Sort: priority first, then by spend (negatives) or orders (harvest)
    negatives.sort(key=lambda x: (x["priority"], -x["spend_total"]))
    harvest.sort(key=lambda x: (x["priority"], -x["orders"]))

    total_wasted = sum(n["spend_total"] for n in negatives if n["orders"] == 0)

    return {
        "negatives": negatives[:30],
        "harvest":   harvest[:20],
        "beta_prior": {
            "alpha":           round(alpha, 4),
            "beta":            round(beta, 4),
            "implied_cvr_pct": prior_cvr_pct,
            "n_terms_fitted":  len([t for t in term_totals
                                    if t.get("clicks", 0) >= _PRIOR_MIN_CLICKS]),
        },
        "summary": {
            "auto_pt_cids_count":  len(auto_pt_cids),
            "terms_analyzed":      len(term_totals),
            "negative_count":      len(negatives),
            "harvest_count":       len(harvest),
            "total_wasted_spend":  round(total_wasted, 2),
            "daily_wasted_spend":  round(total_wasted / days, 2) if days > 0 else 0.0,
            "breakeven_spend":     round(breakeven_spend, 2),
        },
    }
