from __future__ import annotations

import datetime
import logging
import math
import statistics
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)


class CategoryMonopolyAnalyzer:
    """
    Processor to calculate monopoly and competition scores for an Amazon category
    based on Top 100 BSR data, seller details, and keyword traffic.
    """

    def __init__(self, custom_weights: dict[str, float] | None = None):
        from src.workflows.config import merge_config  # deferred — avoids circular import

        config = merge_config("category_monopoly_analysis")
        self.weights = custom_weights or config.get("weights", {})
        self.thresholds = config.get("thresholds", {})

    def analyze(
        self,
        products: list[dict[str, Any]],
        keyword_data: dict[str, Any] | None = None,
        keyword_data_all: list[dict[str, Any]] | None = None,
        ad_data: dict[str, Any] | None = None,
        external_data: dict[str, Any] | None = None,
        historical_data: dict[str, list[dict[str, Any]]] | None = None,
        bsr_snapshots: dict[str, list[dict[str, Any]]] | None = None,
        keyword_weekly_trends: dict[str, Any] | None = None,
        acos_data: dict[str, Any] | None = None,
        new_entrant_ratio: float | None = None,
        brand_search_data: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """
        Main analysis entry point.

        :param historical_data: ASIN → list of daily records from XiyouZhaociAPI.get_asin_daily_trends().
                                Each record: {"date": "YYYY-MM-DD", "price": float,
                                              "stars": float, "ratings": int, "bsr": int}
        :param bsr_snapshots: Dict[YYYYMM, List[{"asin", "rank", "brand"}]] — 4 monthly BSR
                              snapshots (T, T-3, T-6, T-12) from sellersprite_competing_lookup.
                              Used to calculate true list churn rate via ASIN set comparison.
        :param keyword_weekly_trends: Raw response from XiyouZhaociAPI.get_search_term_trends().
                                      When provided, keyword-based seasonality replaces the
                                      BSR-proxy method (more direct demand signal).
        :param acos_data: {"estimated_acos": float, "breakeven_acos": float} — pre-computed
                          from cached CVR/CPC. Drives the ad_burden dimension.
        :param new_entrant_ratio: Fraction of current Top-100 ASINs listed in the past 12 months.
                                  Pre-computed from sellersprite snapshots before analyze() is called.
                                  Drives the bsr_metabolism dimension alongside bsr_churn_result.
        :param brand_search_data: Simplified brand keyword-leadership list pre-computed from
                                  keyword_data_all + analysis_input brand lookup. Each entry:
                                  {"brand", "keywords_led", "n_keywords", "keyword_lead_share"}.
                                  None = keyword data was not fetched (neutral score).
                                  [] = fetched but no brand leads any keyword (fragmented).
        """
        if not products:
            return {"error": "No product data provided for analysis."}

        sorted_products = sorted(products, key=lambda x: x.get("rank", 999))

        sales_scores = self._analyze_sales_distribution(sorted_products)
        brand_score = self._analyze_brand_concentration(sorted_products)
        seller_score = self._analyze_seller_background(sorted_products)
        review_score = self._analyze_review_barrier(sorted_products)
        price_score = self._analyze_price_convergence(sorted_products)
        keyword_score = self._analyze_keyword_monopoly(keyword_data, keyword_data_all)
        ad_score = self._analyze_ad_competition(ad_data)
        social_score, deal_score = self._analyze_external_intensity(external_data)
        churn_result = self._analyze_market_churn(sorted_products, historical_data)
        if keyword_weekly_trends:
            seasonality_result = self._analyze_seasonality_from_keyword_trends(
                keyword_weekly_trends
            )
        else:
            seasonality_result = self._analyze_seasonality(historical_data)
        bsr_churn_result = self._analyze_bsr_churn(bsr_snapshots or {})

        review_integrity_score = self._analyze_review_integrity(sorted_products)
        seller_conc_score = self._analyze_seller_concentration(sorted_products)
        ad_burden_score = self._analyze_ad_burden(acos_data)
        demand_score = self._analyze_demand_trajectory(seasonality_result)
        metabolism_score = self._analyze_bsr_metabolism(bsr_churn_result, new_entrant_ratio)
        brand_search_score = self._analyze_brand_search_monopoly(brand_search_data)

        metrics = {
            "sales_curve_top3": sales_scores["top3_concentration"],
            "sales_survival_space": sales_scores["survival_space"],
            "brand_concentration": brand_score,
            "seller_background": seller_score,
            "review_curve": review_score,
            "keyword_traffic": keyword_score,
            "price_compression": price_score,
            "ad_traffic_ratio": ad_score,
            "social_promotion_intensity": social_score,
            "deal_promotion_intensity": deal_score,
            "review_integrity": review_integrity_score,
            "seller_concentration": seller_conc_score,
            "ad_burden": ad_burden_score,
            "demand_trajectory": demand_score,
            "bsr_metabolism": metabolism_score,
            "brand_search_monopoly": brand_search_score,
        }

        total_score, details = self._calculate_weighted_score(metrics)
        status = self._interpret_score(
            total_score, churn_result, seasonality_result, bsr_churn_result
        )

        return {
            "overall_score": round(total_score, 2),
            "status": status,
            "dimension_details": details,
            "summary_metrics": {
                "cr3": sales_scores.get("cr3"),
            },
            "niche_benchmarks": {
                "median_price": statistics.median(_prices)
                if (_prices := [p.get("price", 0) for p in products if p.get("price", 0) > 0])
                else 0,
                "avg_reviews_top10": int(
                    statistics.mean([p.get("review_count", 0) for p in sorted_products[:10]])
                )
                if len(sorted_products) >= 10
                else 0,
                "avg_reviews_bottom50": int(
                    statistics.mean([p.get("review_count", 0) for p in sorted_products[50:]])
                )
                if len(sorted_products) > 50
                else 0,
                "total_estimated_monthly_units": int(sum(p.get("sales", 0) for p in products)),
            },
            "market_churn": churn_result,
            "seasonality": seasonality_result,
            "bsr_churn": bsr_churn_result,
        }

    def _calculate_weighted_score(self, metrics: dict[str, float]) -> tuple[float, dict[str, Any]]:
        total_score, details = 0.0, {}
        for key, value in metrics.items():
            weight = self.weights.get(key, 0.0)
            contribution = value * weight
            total_score += contribution
            details[key] = {
                "raw_score": round(value, 2),
                "weight": weight,
                "weighted_contribution": round(contribution, 2),
            }
        return total_score, details

    def _interpret_score(
        self,
        score: float,
        churn_result: dict[str, Any] | None = None,
        seasonality_result: dict[str, Any] | None = None,
        bsr_churn_result: dict[str, Any] | None = None,
    ) -> str:
        # BSR churn (listing metabolism) overrides when strongly signalled
        if bsr_churn_result and bsr_churn_result.get("snapshots_available"):
            bsr_label = bsr_churn_result.get("label", "unknown")
            if bsr_label == "fomo_spike_die":
                base = "Spike-and-Die Market (High BSR Churn)"
            elif bsr_label == "high_churn":
                base = "High-Churn Market (Unstable Rankings)"
            elif bsr_label == "blue_ocean":
                base = "Blue Ocean (Low Churn + Open Entry)"
            elif bsr_label == "mature_stable":
                base = "Mature Stable Market (Incumbent Dominance)"
            else:
                base = None  # fall through to concentration-based label

            if base:
                # Still layer on review-manipulation signals if present
                if churn_result:
                    pattern = churn_result.get("pattern", "normal")
                    if pattern == "rating_attack":
                        base += " + Rating Attack"
                    elif pattern == "predatory_competition":
                        base += " + Predatory Competition"
                if seasonality_result and seasonality_result.get("is_seasonal"):
                    base += f" + Seasonal ({seasonality_result.get('pattern', '')})"
                return base

        # Fallback: daily-trend churn patterns override concentration score
        if churn_result:
            pattern = churn_result.get("pattern", "normal")
            if pattern == "predatory_competition":
                return "Predatory Market (High Churn + Rating Attack)"
            if pattern == "lemon_market":
                return "Lemon Market (Quality Death Spiral)"
            if pattern == "rating_attack":
                return "Rating Attack Market (Review Manipulation)"

        if score >= self.thresholds.get("high_monopoly_score", 70):
            base = "High Monopoly (Red Ocean)"
        elif score >= self.thresholds.get("high_competition_score", 55):
            base = "Medium-High Competition (Hard Entry)"
        elif score >= self.thresholds.get("opportunity_score", 35):
            base = "Medium Competition (Addressable)"
        else:
            base = "Low Competition (Blue Ocean)"

        if seasonality_result and seasonality_result.get("is_seasonal"):
            return f"{base} + Seasonal ({seasonality_result.get('pattern', '')})"
        return base

    def _analyze_sales_distribution(self, products: list[dict[str, Any]]) -> dict[str, float]:
        total_sales = sum(p.get("sales") or 0 for p in products) or 1
        top3_sales = sum(p.get("sales") or 0 for p in products[:3])
        survival_sales = sum(p.get("sales") or 0 for p in products[19:50])
        cr3 = top3_sales / total_sales
        cr3_limit = self.thresholds.get("cr3_monopoly_limit", 0.60)
        conc_score = min(100, (cr3 / cr3_limit) * 100)
        survival_ratio = survival_sales / total_sales
        survival_score = max(0, 100 - (survival_ratio / 0.20) * 100)
        return {
            "top3_concentration": conc_score,
            "survival_space": survival_score,
            "cr3": round(cr3, 4),
        }

    def _analyze_brand_concentration(self, products: list[dict[str, Any]]) -> float:
        brands = [p.get("brand") for p in products if p.get("brand")]
        if not brands:
            return 50
        counts = {b: brands.count(b) for b in set(brands)}
        brand_ratio = len(counts) / len(brands)
        return max(0, 100 - (brand_ratio * 150))

    def _analyze_seller_background(self, products: list[dict[str, Any]]) -> float:
        amazon_count = sum(
            1 for p in products if p.get("seller_type") in ["Amazon", "AMZ", "Retail"]
        )
        mega_seller_feedback = self.thresholds.get("mega_seller_feedback", 10000)
        large_seller_count = sum(
            1 for p in products if (p.get("feedback_count") or 0) > mega_seller_feedback
        )
        amz_ratio = amazon_count / len(products)
        large_ratio = large_seller_count / len(products)
        return min(100, (amz_ratio * 300) + (large_ratio * 100))

    def _analyze_review_barrier(self, products: list[dict[str, Any]]) -> float:
        if len(products) < 20:
            return 50
        top_10 = products[:10]
        bottom_50 = products[49:] if len(products) > 50 else products[len(products) // 2 :]
        avg_reviews_top = statistics.mean([p.get("review_count", 0) for p in top_10]) or 1
        avg_reviews_bottom = statistics.mean([p.get("review_count", 0) for p in bottom_50]) or 1
        review_disparity = avg_reviews_top / avg_reviews_bottom
        disparity_threshold = self.thresholds.get("review_disparity_threshold", 5.0)
        review_score = min(100, (review_disparity / disparity_threshold) * 100)
        avg_rating_top = statistics.mean([p.get("rating", 0) for p in top_10]) or 4.0
        rating_cap = self.thresholds.get("rating_hard_barrier", 4.5)
        rating_score = (
            max(0, (avg_rating_top - 4.0) / (rating_cap - 4.0) * 100) if avg_rating_top > 4.0 else 0
        )
        return (review_score * 0.7) + (rating_score * 0.3)

    def _analyze_price_convergence(self, products: list[dict[str, Any]]) -> float:
        prices = [p.get("price") for p in products if p.get("price")]
        if len(prices) < 5:
            return 50
        avg_price, std_dev = statistics.mean(prices), statistics.stdev(prices)
        cv = std_dev / avg_price
        cv_threshold = self.thresholds.get("price_cv_compression", 0.15)
        if cv < cv_threshold:
            return 100
        return max(0, 100 - (cv / 0.6 * 100))

    def _analyze_keyword_monopoly(
        self,
        keyword_data: dict[str, Any] | None,
        keyword_data_all: list[dict[str, Any]] | None = None,
    ) -> float:
        all_terms = keyword_data_all or ([keyword_data] if keyword_data else [])
        if not all_terms:
            return 50
        scores = [
            min(
                100,
                (
                    sum(
                        t.get("clickShare", 0)
                        for t in (term.get("topAsins") or term.get("top_asins") or [])[:3]
                    )
                    / 0.50
                )
                * 100,
            )
            for term in all_terms
            if term and (term.get("topAsins") or term.get("top_asins"))
        ]
        return statistics.mean(scores) if scores else 50

    def _analyze_ad_competition(self, ad_data: dict[str, Any] | None) -> float:
        if not ad_data:
            return 50

        # 1. BSR Winners Ad Dependency (Actual Sales Driver)
        # Xiyouzhaoci traffic-weighted ad dependency ratio for BSR top-20 ASINs.
        # If winners rely heavily on ads, organic moat is weak or CAC is high for all.
        bsr_ad_ratio = ad_data.get("actual_bsr_ad_ratio") or 0.5
        dependency_score = min(100, (bsr_ad_ratio / 0.50) * 100)  # 50% dependency is critical

        # 2. Detailed Bid Analysis (Capital barrier)
        detailed_bids = ad_data.get("detailed_bids", {})
        if detailed_bids:
            bid_barrier_score = self._calculate_bid_barrier_score(detailed_bids)
        else:
            bid = ad_data.get("avg_bid", 0)
            high_bid_threshold = self.thresholds.get("high_bid_barrier", 2.50)
            bid_barrier_score = min(100, (bid / high_bid_threshold) * 100) if bid > 0 else 50

        # Combined score:
        # 40% Dependency (how hard BSR winners rely on paid traffic)
        # 60% Bid Barrier (capital requirement to displace winners)
        return (dependency_score * 0.4) + (bid_barrier_score * 0.6)

    def _calculate_bid_barrier_score(self, detailed_bids: dict[str, Any]) -> float:
        """
        Calculates a barrier score based on multiple keywords and match types.
        Identifies high-barrier keywords.
        """
        all_suggested_bids = []
        high_barrier_keywords = []

        # Process Legacy for Sales as the most conservative/baseline strategy
        legacy_recs = detailed_bids.get("LEGACY_FOR_SALES", [])
        for rec in legacy_recs:
            for expr in rec.get("bidRecommendationsForTargetingExpressions", []):
                # v5 API: bids are in bidValues[].suggestedBid (float)
                bid_values = [
                    float(b["suggestedBid"])
                    for b in expr.get("bidValues", [])
                    if b.get("suggestedBid")
                ]
                bid = statistics.median(bid_values) if bid_values else 0
                kw = expr.get("targetingExpression", {}).get("value", "unknown")
                m_type = expr.get("targetingExpression", {}).get("type", "unknown")

                if bid > 0:
                    all_suggested_bids.append(bid)
                    # Threshold for a single high-barrier keyword
                    if bid > 2.80:
                        high_barrier_keywords.append(f"{kw} ({m_type}): ${bid:.2f}")

        if not all_suggested_bids:
            return 50.0

        avg_bid = statistics.mean(all_suggested_bids)
        # 3.0 USD as a benchmark for high-competition barrier in US marketplace
        barrier_threshold = self.thresholds.get("high_bid_barrier", 3.0)

        score = min(100, (avg_bid / barrier_threshold) * 100)
        # Bonus penalty if multiple keywords are high-barrier
        if len(set(high_barrier_keywords)) >= 2:
            score = min(100, score + 15)

        return score

    def _analyze_external_intensity(
        self, external_data: dict[str, Any] | None
    ) -> tuple[float, float]:
        if not external_data:
            return 50.0, 50.0
        social_psi = external_data.get("social_psi", 0)
        social_score = min(100, social_psi)
        deal_intensity = external_data.get("deal_intensity", 0)
        deal_score = min(100, deal_intensity * 10)
        return social_score, deal_score

    def _analyze_review_integrity(self, products: list[dict[str, Any]]) -> float:
        """
        Review manipulation moat: incumbents who boosted review counts via brushing
        (written/global ratio far above ~10% natural rate) create a barrier that a
        genuine-quality entrant cannot match through authentic reviews alone.

        Uses review_ratio (written_reviews / global_ratings) from the top-10 BSR products.
        Ceiling threshold: 15% (review_ratio_ceiling in config) — above this the moat is
        likely artificial. Returns neutral 50 when data is unavailable.
        """
        eligible = [p for p in products[:10] if p.get("review_ratio") is not None]
        if not eligible:
            return 50.0
        avg_ratio = statistics.mean(p["review_ratio"] for p in eligible)
        ceiling = self.thresholds.get("review_ratio_ceiling", 0.15)
        return min(100.0, (avg_ratio / ceiling) * 100)

    def _analyze_seller_concentration(self, products: list[dict[str, Any]]) -> float:
        """
        Seller-level CR3 (top-3 sellers' share of BSR position slots). A seller running
        multiple brands can appear fragmented at brand level while being highly concentrated
        at seller level. Returns neutral 50 when seller_id coverage is below 30% — too
        sparse to compute a reliable CR3.
        """
        sellers = [p.get("seller_id") for p in products if p.get("seller_id")]
        coverage = len(sellers) / max(len(products), 1)
        if coverage < 0.30:
            return 50.0
        counter: Counter = Counter(sellers)
        total = sum(counter.values())
        cr3 = sum(v for _, v in counter.most_common(3)) / total
        limit = self.thresholds.get("seller_cr3_limit", 0.40)
        return min(100.0, (cr3 / limit) * 100)

    def _analyze_ad_burden(self, acos_data: dict[str, Any] | None) -> float:
        """
        Structural ad-profitability barrier: estimated ACOS / breakeven ACOS.
        Ratio ≥ 1.0 means ads consume all margin — a capital barrier on top of the
        bid-cost barrier already captured in ad_traffic_ratio. Returns neutral 50
        when CPC or CVR data is unavailable.
        """
        if not acos_data:
            return 50.0
        estimated = acos_data.get("estimated_acos")
        breakeven = acos_data.get("breakeven_acos")
        if not estimated or not breakeven or breakeven <= 0:
            return 50.0
        return min(100.0, (estimated / breakeven) * 100)

    def _analyze_demand_trajectory(self, seasonality_result: dict[str, Any]) -> float:
        """
        YOY demand trend from ABA weekly search volume (keyword_weekly_trends source only).
        A growing market lowers entry difficulty — room for new players and incumbents are
        less defensive. A declining market raises it — incumbents fight harder for shrinking
        share, and capital ROI is at risk. Returns neutral 50 when keyword trends data is
        unavailable or covers fewer than 2 full years (required for reliable YOY comparison).

        Score formula: 50 − yoy_pct × 0.5
          yoy = +40% → score 30  (strong growth = lower barrier)
          yoy =   0% → score 50  (stable)
          yoy = −40% → score 70  (declining = incumbents entrench)
          yoy = −80% → score 90  (collapsing)
        """
        if not seasonality_result or seasonality_result.get("source") != "keyword_weekly_trends":
            return 50.0
        yoy = seasonality_result.get("yoy_peak_1y_pct") or seasonality_result.get("yoy_1y_pct")
        if yoy is None:
            return 50.0
        return max(0.0, min(100.0, 50.0 - yoy * 0.5))

    def _analyze_bsr_metabolism(
        self,
        bsr_churn_result: dict[str, Any],
        new_entrant_ratio: float | None,
    ) -> float:
        """
        BSR listing metabolism: how hostile is rank turnover to building a durable position?

        Uses the churn label (the qualitative synthesis of 3m/6m/12m rates already computed by
        _analyze_bsr_churn) as the primary anchor, then shifts ±20/15 points based on
        new_entrant_ratio — the fraction of current Top-100 ASINs listed in the past 12 months.

        High score (>70): hard to sustain — either incumbents are entrenched (mature_stable)
                          or the market is too volatile to hold rank (fomo_spike_die).
        Low score (<30):  positions are open AND new entrants can hold them (blue_ocean).

        Label anchors:
          fomo_spike_die     → 88  (spike-and-die: enter easily, cannot sustain)
          mature_stable      → 82  (incumbent lock-in: hard to displace; refined by c12)
          high_churn         → 55  (significant rotation but some sustainability possible)
          moderate_competitive → 45 (normal market)
          blue_ocean         → 20  (positions open and durable; refined by c12)
          unknown            → 50  (no snapshot data)

        new_entrant_ratio adjustment (±15/20 pts):
          high ratio (>20%): recent products ARE breaking in → barrier lower
          low ratio (<20%):  only veterans survive → barrier higher
        """
        if not bsr_churn_result or not bsr_churn_result.get("snapshots_available"):
            return 50.0

        label = bsr_churn_result.get("label", "unknown")
        c12 = bsr_churn_result.get("churn_12m") or bsr_churn_result.get("churn_6m")

        label_base: dict[str, float] = {
            "fomo_spike_die": 88.0,
            "mature_stable": 82.0,
            "high_churn": 55.0,
            "moderate_competitive": 45.0,
            "blue_ocean": 20.0,
            "unknown": 50.0,
        }
        base = label_base.get(label, 50.0)

        # Within mature_stable and blue_ocean, c12 refines the within-label score
        if label == "mature_stable" and c12 is not None:
            base = 90.0 - (c12 / 0.30) * 15.0  # c12=0 → 90, c12=0.30 → 75
        elif label == "blue_ocean" and c12 is not None:
            base = 18.0 + ((c12 - 0.30) / 0.25) * 12.0  # c12=0.30 → 18, c12=0.55 → 30

        # new_entrant_ratio: 20% is neutral; above = more open (discount); below = more locked (add)
        if new_entrant_ratio is not None:
            adj = -(new_entrant_ratio - 0.20) * 50.0
            base += max(-20.0, min(15.0, adj))

        return max(0.0, min(100.0, base))

    def _analyze_brand_search_monopoly(
        self,
        brand_search_data: list[dict[str, Any]] | None,
    ) -> float:
        """
        Search-traffic moat from ABA keyword click-share leadership.

        The top brand's keyword_lead_share (fraction of core keywords where that brand
        holds the #1-click-share ASIN) approximates the PPC moat independently of BSR
        concentration. A brand leading ≥50% of keywords holds a search monopoly;
        challengers must outbid them on every core term just to appear.

        Scoring:
          keyword_lead_share ≥ 0.50  → score 100 (search monopoly)
          keyword_lead_share = 0.25  → score 50
          keyword_lead_share = 0.00  → score 0  (fully fragmented)

        Returns 50.0 (neutral) when keyword data was not fetched.
        Returns 0.0 when data was fetched but no brand leads any keyword.
        """
        if brand_search_data is None:
            return 50.0
        if not brand_search_data:
            return 0.0
        top_share = brand_search_data[0].get("keyword_lead_share", 0.0)
        return min(100.0, (top_share / 0.50) * 100.0)

    def _analyze_bsr_churn(
        self,
        snapshots: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Any]:
        """
        Calculate BSR listing churn rate by comparing Top-100 ASIN
        sets across 4 monthly snapshots: T, T-3, T-6, T-12.

        Input: Dict[YYYYMM, List[{"asin": str, "rank": int, "brand": str}]]
               from _fetch_sellersprite_bsr (4 snapshots).

        Churn rate definition
        ─────────────────────────────────────────────────────────────────
        churn_Nm = fraction of ASINs in snapshot T (most recent) that were
                   NOT present in snapshot T-N.

        A high churn_12m means the category rotates most of its Top-100
        within a year — classic Die Quickly.
        A low churn_12m means incumbents dominate and rarely get displaced.

        Category labels
        ─────────────────────────────────────────────────────────────────
        fomo_spike_die   churn_3m > 0.40 AND churn_12m > 0.65
                         Fast rotation at every time scale. Products flood
                         in, spike, then vanish.

        high_churn       churn_12m > 0.55
                         Significant annual turnover even without extreme
                         short-term spike.

        blue_ocean       0.30 ≤ churn_12m ≤ 0.55 AND churn_3m < 0.25
                         Recent list is stable (new entrants can stay) but
                         historically the door IS open — not yet locked by
                         incumbents.

        mature_stable    churn_12m < 0.30
                         Incumbents dominate; hard to enter and displace.

        moderate_competitive  all other cases.
        """
        _empty: dict[str, Any] = {
            "churn_3m": None,
            "churn_6m": None,
            "churn_12m": None,
            "label": "unknown",
            "snapshots_available": [],
        }

        if not snapshots:
            return _empty

        sorted_months = sorted(
            snapshots.keys()
        )  # chronological, e.g. ["202502","202508","202511","202602"]
        latest = sorted_months[-1]
        latest_set = {p["asin"] for p in snapshots[latest] if p.get("asin")}
        if not latest_set:
            return _empty

        latest_y, latest_mo = int(latest[:4]), int(latest[4:])

        def churn_vs(older_ym: str) -> float | None:
            if older_ym not in snapshots:
                return None
            older_set = {p["asin"] for p in snapshots[older_ym] if p.get("asin")}
            if not older_set:
                return None
            new_in_latest = latest_set - older_set
            return len(new_in_latest) / len(latest_set)

        # Map each older snapshot to its approximate time gap
        churn_3m = churn_6m = churn_12m = None
        for ym in sorted_months[:-1]:
            y, mo = int(ym[:4]), int(ym[4:])
            gap = (latest_y * 12 + latest_mo) - (y * 12 + mo)
            rate = churn_vs(ym)
            if rate is None:
                continue
            if abs(gap - 3) <= 1:
                churn_3m = rate
            elif abs(gap - 6) <= 1:
                churn_6m = rate
            elif abs(gap - 12) <= 1:
                churn_12m = rate

        # Require at least one longer-horizon rate (6m or 12m) to label.
        # Without it c12 would default to 0.0, triggering a false "mature_stable"
        # even when only a single snapshot exists and no comparison is possible.
        if churn_12m is None and churn_6m is None:
            label = "unknown"
        else:
            c12 = churn_12m if churn_12m is not None else churn_6m  # type: ignore[assignment]
            c3 = churn_3m or 0.0
            if c3 > 0.40 and c12 > 0.65:
                label = "fomo_spike_die"
            elif c12 > 0.55:
                label = "high_churn"
            elif c12 < 0.30:
                label = "mature_stable"
            elif 0.30 <= c12 <= 0.55 and c3 < 0.25:
                label = "blue_ocean"
            else:
                label = "moderate_competitive"

        return {
            "churn_3m": round(churn_3m, 3) if churn_3m is not None else None,
            "churn_6m": round(churn_6m, 3) if churn_6m is not None else None,
            "churn_12m": round(churn_12m, 3) if churn_12m is not None else None,
            "label": label,
            "snapshots_available": sorted_months,
            "latest_snapshot": latest,
        }

    def _analyze_market_churn(
        self,
        products: list[dict[str, Any]],
        historical_data: dict[str, list[dict[str, Any]]] | None,
    ) -> dict[str, Any]:
        """
        Detect high-mortality / predatory competition patterns.

        Signals:
          - Rating collapse rate: % of tracked ASINs whose rating dropped >0.3 stars
            in the second half of their tracked period (from get_asin_daily_trends).
          - New product flood: % of current Top-100 products with review_count < 50.
          - Category rating depression: avg rating < 4.0 indicates systemic quality
            issues or sustained review-bombing.

        Pattern classification:
          - predatory_competition: high flood + high collapse rate
          - lemon_market: high flood + depressed avg rating
          - rating_attack: high collapse rate without flood (targeted bombing)
          - normal: no abnormal signals
        """
        # Coerce None → 0 / 0.0 so comparisons never raise TypeError
        new_product_ratio = sum(1 for p in products if (p.get("review_count") or 0) < 50) / max(
            len(products), 1
        )

        raw_ratings = [float(p["rating"]) for p in products if (p.get("rating") or 0) > 0]
        avg_rating = statistics.mean(raw_ratings) if raw_ratings else 4.0

        collapse_count = 0
        total_tracked = 0

        if historical_data:
            for daily_records in historical_data.values():
                if len(daily_records) < 14:
                    continue
                total_tracked += 1
                records = sorted(daily_records, key=lambda x: x.get("date", ""))
                ratings = [
                    r.get("stars") or r.get("rating")
                    for r in records
                    if r.get("stars") or r.get("rating")
                ]
                if len(ratings) >= 14:
                    mid = len(ratings) // 2
                    peak = max(ratings[:mid])
                    recent_avg = statistics.mean(ratings[mid:])
                    if peak - recent_avg > self.thresholds.get("rating_collapse_threshold", 0.3):
                        collapse_count += 1

        collapse_rate = collapse_count / total_tracked if total_tracked else 0.0
        rating_depression = max(0.0, (4.0 - avg_rating) / 0.5 * 50) if avg_rating < 4.0 else 0.0

        churn_score = min(
            100, collapse_rate * 60 + new_product_ratio * 100 * 0.3 + rating_depression * 0.1
        )

        if new_product_ratio > 0.4 and collapse_rate > 0.3:
            pattern = "predatory_competition"
        elif new_product_ratio > 0.4 and avg_rating < 3.8:
            pattern = "lemon_market"
        elif collapse_rate > 0.4:
            pattern = "rating_attack"
        else:
            pattern = "normal"

        return {
            "churn_score": round(churn_score, 2),
            "pattern": pattern,
            "collapse_rate": round(collapse_rate, 3),
            "new_product_ratio": round(new_product_ratio, 3),
            "avg_category_rating": round(avg_rating, 2),
            "tracked_asins": total_tracked,
        }

    def _analyze_seasonality(
        self,
        historical_data: dict[str, list[dict[str, Any]]] | None,
    ) -> dict[str, Any]:
        """
        Detect organic seasonal demand patterns from BSR time series.

        Design decisions:
          1. log(BSR) transform   — BSR is a rank, not a linear scale.  A move from
             BSR 10→20 is ~50% sales drop; BSR 10000→10010 is negligible.  Log space
             makes both comparable.
          2. Linear detrending    — A growing blue-ocean product has a falling BSR
             trend.  Without detrending, monotone improvement looks like high variance
             and gets mis-scored as "strongly seasonal".  We fit OLS on the monthly
             time-series and measure seasonality on the residuals only.
          3. Platform-event dampening — Prime Day (July) and Black Friday (November)
             cause category-wide BSR spikes driven by platform promotions, not organic
             demand.  Residuals for those months are weighted down to 0.3 so they
             cannot single-handedly define a "peak season".
          4. Circular arc span    — Winter products peak in Nov/Dec/Jan.  The naive
             max-min formula gives 12-1=11 (wrong).  We find the largest gap between
             consecutive peak months on the 12-month circle and subtract from 12 to
             get the true minimum arc length.

        Pattern classification:
          - evergreen:           amplitude < 0.20 (< ~22% BSR swing in log space)
          - mild_seasonal:       amplitude 0.20–0.49
          - strong_seasonal:     amplitude ≥ 0.50 and peak months span ≤ 3 months
          - multi_peak_seasonal: amplitude ≥ 0.50 and peak months span > 3 months
        """
        _no_data: dict[str, Any] = {
            "seasonality_score": 0,
            "is_seasonal": False,
            "peak_months": [],
            "pattern": "unknown",
            "monthly_amplitude": 0,
            "platform_event_dampened": [],
            "platform_event_in_peak": False,
            "source": "bsr_daily_trends",
            "n_data_points": 0,
        }
        if not historical_data:
            return _no_data

        PLATFORM_EVENT_MONTHS = {7, 11}  # Prime Day, Black Friday

        # ── Step 1: aggregate log(BSR) by (year, month) ───────────────────
        monthly_log_bsr: dict[tuple[int, int], list[float]] = {}
        for daily_records in historical_data.values():
            for record in daily_records:
                bsr = record.get("bsr") or record.get("bestSellerRank")
                date_str = record.get("date", "")
                if not bsr or bsr <= 0 or not date_str:
                    continue
                try:
                    year, month = int(date_str[:4]), int(date_str[5:7])
                    monthly_log_bsr.setdefault((year, month), []).append(math.log(float(bsr)))
                except (ValueError, IndexError):
                    continue

        sorted_keys = sorted(monthly_log_bsr.keys())
        if len(sorted_keys) < 6:
            return {**_no_data, "pattern": "insufficient_data"}

        # ── Step 2: monthly median log-BSR ────────────────────────────────
        monthly_median = {k: statistics.median(v) for k, v in monthly_log_bsr.items()}

        # ── Step 3: linear detrend (OLS) ──────────────────────────────────
        n = len(sorted_keys)
        t_vals = list(range(n))
        v_vals = [monthly_median[k] for k in sorted_keys]
        mean_t, mean_v = statistics.mean(t_vals), statistics.mean(v_vals)
        cov_tv = sum((t - mean_t) * (v - mean_v) for t, v in zip(t_vals, v_vals, strict=False))
        var_t = sum((t - mean_t) ** 2 for t in t_vals) or 1.0
        slope = cov_tv / var_t
        intercept = mean_v - slope * mean_t
        detrended = {
            k: monthly_median[k] - (slope * i + intercept) for i, k in enumerate(sorted_keys)
        }

        # ── Step 4: group by calendar month; dampen platform event months ─
        calendar_residuals: dict[int, list[float]] = {}
        for (_, month), residual in detrended.items():
            weight = 0.3 if month in PLATFORM_EVENT_MONTHS else 1.0
            calendar_residuals.setdefault(month, []).append(residual * weight)

        if len(calendar_residuals) < 6:
            return {**_no_data, "pattern": "insufficient_data"}

        avg_by_month = {m: statistics.mean(v) for m, v in calendar_residuals.items()}

        # ── Step 5: seasonality amplitude in log-BSR space ────────────────
        detrended_vals = list(avg_by_month.values())
        amplitude = max(detrended_vals) - min(detrended_vals)
        # e^0.693 ≈ 2×, e^1.386 ≈ 4×;  map 4× swing → score 100
        seasonality_score = min(100, amplitude / 1.386 * 100)

        # ── Step 6: peak months (lower log-BSR = better rank = more sales) ─
        mean_d = statistics.mean(detrended_vals)
        std_d = statistics.stdev(detrended_vals) if len(detrended_vals) >= 2 else 0.1
        peak_threshold = mean_d - 0.5 * std_d
        peak_months = sorted(m for m, v in avg_by_month.items() if v < peak_threshold)

        # ── Step 7: circular arc span for winter-product correctness ───────
        arc = self._circular_arc_span(peak_months)

        if amplitude < 0.20:
            pattern = "evergreen"
        elif amplitude < 0.50:
            pattern = "mild_seasonal"
        elif arc <= 3:
            pattern = "strong_seasonal"
        else:
            pattern = "multi_peak_seasonal"

        platform_in_peak = bool(PLATFORM_EVENT_MONTHS & set(peak_months))

        return {
            "seasonality_score": round(seasonality_score, 2),
            "is_seasonal": seasonality_score >= 20,
            "peak_months": peak_months,
            "pattern": pattern,
            "monthly_amplitude": round(amplitude, 3),
            "platform_event_dampened": sorted(PLATFORM_EVENT_MONTHS),
            "platform_event_in_peak": platform_in_peak,
            "source": "bsr_daily_trends",
            "n_data_points": len(sorted_keys),
        }

    def _analyze_seasonality_from_keyword_trends(
        self,
        keyword_weekly_trends: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Detect organic seasonal demand patterns from ABA weekly search-volume data.

        This is a more direct signal than BSR-based seasonality: it reflects
        consumer *intent* (search demand), not competitive dynamics.

        Input: raw response from XiyouZhaociAPI.get_search_term_trends()
            {
              "searchTerms": [{
                "trends": {
                  "weekSearch": [5630, 6205, ...]   # oldest → newest, ~150 values
                }
              }]
            }

        Design decisions (mirrors _analyze_seasonality):
          1. log(weekSearch) transform — search volume spans multiple orders of
             magnitude; log space makes comparisons proportional.
          2. Linear detrending (OLS) — growing niches have rising search trends.
             Without detrending, a steadily growing keyword looks "seasonal".
          3. Platform-event dampening — July (Prime Day) and November (Black
             Friday) inflate search volume via platform promotions, not organic
             intent. Dampened to weight 0.3.
          4. Direction: HIGH residual = peak season (opposite of BSR where LOW
             residual = good rank = peak sales).
          5. Circular arc span — same winter-product fix as _analyze_seasonality.
        """
        _no_data: dict[str, Any] = {
            "seasonality_score": 0,
            "is_seasonal": False,
            "peak_months": [],
            "pattern": "unknown",
            "monthly_amplitude": 0,
            "platform_event_dampened": [],
            "platform_event_in_peak": False,
            "source": "keyword_weekly_trends",
            "n_data_points": 0,
            "demand_trend": "unknown",
            "yoy_1y_pct": None,
            "yoy_2y_pct": None,
            "yoy_peak_1y_pct": None,
        }

        try:
            terms = keyword_weekly_trends.get("searchTerms") or []
            if not terms:
                return _no_data
            week_search: list[float] = [
                float(v)
                for v in (terms[0].get("trends") or {}).get("weekSearch") or []
                if v is not None
            ]
        except (KeyError, IndexError, TypeError):
            return _no_data

        if len(week_search) < 26:  # need at least 6 months of weekly data
            return {**_no_data, "pattern": "insufficient_data"}

        PLATFORM_EVENT_MONTHS = {7, 11}
        today = datetime.date.today()
        n = len(week_search)

        # ── Step 1: assign calendar dates to each weekly bucket ───────────
        # Position 0 is oldest; position n-1 is this week.
        # date(i) = today − (n − 1 − i) × 7 days
        monthly_log_vol: dict[tuple[int, int], list[float]] = {}
        for i, vol in enumerate(week_search):
            if vol <= 0:
                continue
            week_date = today - datetime.timedelta(days=(n - 1 - i) * 7)
            key = (week_date.year, week_date.month)
            monthly_log_vol.setdefault(key, []).append(math.log(vol))

        sorted_keys = sorted(monthly_log_vol.keys())
        if len(sorted_keys) < 6:
            return {**_no_data, "pattern": "insufficient_data"}

        # ── Step 2: monthly median log-volume ─────────────────────────────
        monthly_median = {k: statistics.median(v) for k, v in monthly_log_vol.items()}

        # ── Step 3: linear detrend (OLS) ──────────────────────────────────
        nk = len(sorted_keys)
        t_vals = list(range(nk))
        v_vals = [monthly_median[k] for k in sorted_keys]
        mean_t, mean_v = statistics.mean(t_vals), statistics.mean(v_vals)
        cov_tv = sum((t - mean_t) * (v - mean_v) for t, v in zip(t_vals, v_vals, strict=False))
        var_t = sum((t - mean_t) ** 2 for t in t_vals) or 1.0
        slope = cov_tv / var_t
        intercept = mean_v - slope * mean_t
        detrended = {
            k: monthly_median[k] - (slope * i + intercept) for i, k in enumerate(sorted_keys)
        }

        # ── Step 4: group by calendar month; dampen platform event months ─
        calendar_residuals: dict[int, list[float]] = {}
        for (_, month), residual in detrended.items():
            weight = 0.3 if month in PLATFORM_EVENT_MONTHS else 1.0
            calendar_residuals.setdefault(month, []).append(residual * weight)

        if len(calendar_residuals) < 6:
            return {**_no_data, "pattern": "insufficient_data"}

        avg_by_month = {m: statistics.mean(v) for m, v in calendar_residuals.items()}

        # ── Step 5: amplitude ─────────────────────────────────────────────
        detrended_vals = list(avg_by_month.values())
        amplitude = max(detrended_vals) - min(detrended_vals)
        seasonality_score = min(100, amplitude / 1.386 * 100)

        # ── Step 6: peak months (HIGH log-volume residual = peak demand) ──
        # Opposite sign convention from BSR (where LOW residual = peak rank).
        mean_d = statistics.mean(detrended_vals)
        std_d = statistics.stdev(detrended_vals) if len(detrended_vals) >= 2 else 0.1
        peak_threshold = mean_d + 0.5 * std_d
        peak_months = sorted(m for m, v in avg_by_month.items() if v > peak_threshold)

        # ── Step 7: circular arc span ─────────────────────────────────────
        arc = self._circular_arc_span(peak_months)

        if amplitude < 0.20:
            pattern = "evergreen"
        elif amplitude < 0.50:
            pattern = "mild_seasonal"
        elif arc <= 3:
            pattern = "strong_seasonal"
        else:
            pattern = "multi_peak_seasonal"

        platform_in_peak = bool(PLATFORM_EVENT_MONTHS & set(peak_months))

        # ── Step 8: YOY demand trend ──────────────────────────────────────────
        # Compare raw search-volume means across 52-week yearly windows to detect
        # structural growth or decline independent of seasonal shape.
        # Uses peak-window weeks when peak_months are detected (more sensitive
        # for seasonal categories); falls back to full-year mean for evergreen.
        yoy_1y_pct: float | None = None
        yoy_2y_pct: float | None = None
        yoy_peak_1y_pct: float | None = None
        demand_trend = "unknown"

        if n >= 104:  # need at least 2 full years
            week_dates_list = [today - datetime.timedelta(days=(n - 1 - i) * 7) for i in range(n)]
            peak_month_set = set(peak_months)

            def _window_mean(start: int, end: int, months: set) -> float | None:
                vols = [
                    week_search[i]
                    for i in range(start, end)
                    if week_search[i] > 0 and (not months or week_dates_list[i].month in months)
                ]
                return statistics.mean(vols) if vols else None

            y3_s, y3_e = max(0, n - 52), n
            y2_s, y2_e = max(0, n - 104), y3_s
            y1_s, y1_e = max(0, n - 156), y2_s

            y3_annual = _window_mean(y3_s, y3_e, set())
            y2_annual = _window_mean(y2_s, y2_e, set())
            y1_annual = _window_mean(y1_s, y1_e, set()) if n >= 156 else None

            if y3_annual and y2_annual:
                yoy_1y_pct = round((y3_annual / y2_annual - 1) * 100, 1)
            if y3_annual and y1_annual:
                yoy_2y_pct = round((y3_annual / y1_annual - 1) * 100, 1)

            if peak_month_set:
                y3_peak = _window_mean(y3_s, y3_e, peak_month_set)
                y2_peak = _window_mean(y2_s, y2_e, peak_month_set)
                if y3_peak and y2_peak:
                    yoy_peak_1y_pct = round((y3_peak / y2_peak - 1) * 100, 1)

            _primary = yoy_peak_1y_pct if yoy_peak_1y_pct is not None else yoy_1y_pct
            if _primary is not None:
                if _primary <= -40:
                    demand_trend = "collapsing"
                elif _primary <= -15:
                    demand_trend = "declining"
                elif _primary >= 20:
                    demand_trend = "growing"
                else:
                    demand_trend = "stable"

        return {
            "seasonality_score": round(seasonality_score, 2),
            "is_seasonal": seasonality_score >= 20,
            "peak_months": peak_months,
            "pattern": pattern,
            "monthly_amplitude": round(amplitude, 3),
            "platform_event_dampened": sorted(PLATFORM_EVENT_MONTHS),
            "platform_event_in_peak": platform_in_peak,
            "source": "keyword_weekly_trends",
            "n_data_points": len(week_search),
            "demand_trend": demand_trend,
            "yoy_1y_pct": yoy_1y_pct,
            "yoy_2y_pct": yoy_2y_pct,
            "yoy_peak_1y_pct": yoy_peak_1y_pct,
        }

    @staticmethod
    def _circular_arc_span(months: list[int]) -> int:
        """
        Minimum arc length (in month-steps) on a circular 12-month calendar.

        Examples:
          [11, 12, 1]  →  2  (Nov→Dec→Jan spans 2 steps, correctly ≤ 3)
          [1, 4, 7, 10] →  9  (quarterly distribution, correctly > 3)
          [6, 7]        →  1
        """
        if not months:
            return 0
        s = sorted(set(months))
        n = len(s)
        # Gaps between every consecutive pair (circular)
        gaps = [(s[(i + 1) % n] - s[i]) % 12 for i in range(n)]
        # Minimum arc = 12 minus the largest empty gap
        return 12 - max(gaps)
