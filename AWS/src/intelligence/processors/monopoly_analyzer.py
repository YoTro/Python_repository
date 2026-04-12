from __future__ import annotations
import datetime
import logging
import math
import statistics
from typing import List, Dict, Any, Optional, Tuple
from src.workflows.config import merge_config

logger = logging.getLogger(__name__)

class CategoryMonopolyAnalyzer:
    """
    Processor to calculate monopoly and competition scores for an Amazon category
    based on Top 100 BSR data, seller details, and keyword traffic.
    """

    def __init__(self, custom_weights: Optional[Dict[str, float]] = None):
        config = merge_config("category_monopoly_analysis")
        self.weights = custom_weights or config.get("weights", {})
        self.thresholds = config.get("thresholds", {})

    def analyze(self,
                products: List[Dict[str, Any]],
                keyword_data: Optional[Dict[str, Any]] = None,
                ad_data: Optional[Dict[str, Any]] = None,
                external_data: Optional[Dict[str, Any]] = None,
                historical_data: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                bsr_snapshots: Optional[Dict[str, List[Dict[str, Any]]]] = None,
                keyword_weekly_trends: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Main analysis entry point.

        :param historical_data: ASIN → list of daily records from XiyouZhaociAPI.get_asin_daily_trends().
                                Each record: {"date": "YYYY-MM-DD", "price": float,
                                              "stars": float, "ratings": int, "bsr": int}
        :param bsr_snapshots: Dict[YYYYMM, List[{"asin", "rank", "brand"}]] — 4 monthly BSR
                              snapshots (T, T-3, T-6, T-12) from sellersprite_competing_lookup.
                              Used to calculate true list churn ratevia ASIN set comparison.
        :param keyword_weekly_trends: Raw response from XiyouZhaociAPI.get_search_term_trends().
                                      When provided, keyword-based seasonality replaces the
                                      BSR-proxy method (more direct demand signal).
        """
        if not products:
            return {"error": "No product data provided for analysis."}

        sorted_products = sorted(products, key=lambda x: x.get("rank", 999))

        sales_scores = self._analyze_sales_distribution(sorted_products)
        brand_score = self._analyze_brand_concentration(sorted_products)
        seller_score = self._analyze_seller_background(sorted_products)
        review_score = self._analyze_review_barrier(sorted_products)
        price_score = self._analyze_price_convergence(sorted_products)
        keyword_score = self._analyze_keyword_monopoly(keyword_data)
        ad_score = self._analyze_ad_competition(ad_data)
        social_score, deal_score = self._analyze_external_intensity(external_data)
        churn_result = self._analyze_market_churn(sorted_products, historical_data)
        if keyword_weekly_trends:
            seasonality_result = self._analyze_seasonality_from_keyword_trends(keyword_weekly_trends)
        else:
            seasonality_result = self._analyze_seasonality(historical_data)
        bsr_churn_result = self._analyze_bsr_churn(bsr_snapshots or {})

        metrics = {
            "sales_curve_top3": sales_scores["top3_concentration"], "sales_survival_space": sales_scores["survival_space"],
            "brand_concentration": brand_score, "seller_background": seller_score, "review_curve": review_score,
            "keyword_traffic": keyword_score, "price_compression": price_score, "ad_traffic_ratio": ad_score,
            "social_promotion_intensity": social_score, "deal_promotion_intensity": deal_score
        }

        total_score, details = self._calculate_weighted_score(metrics)
        status = self._interpret_score(total_score, churn_result, seasonality_result, bsr_churn_result)

        return {
            "overall_score": round(total_score, 2), "status": status, "dimension_details": details,
            "summary_metrics": {
                "cr3": sales_scores.get("cr3"), "price_cv": sales_scores.get("price_cv"),
                "avg_rating": sales_scores.get("avg_rating")
            },
            "niche_benchmarks": {
                "median_price": statistics.median([p.get("price", 0) for p in products if p.get("price", 0) > 0]) if products else 0,
                "avg_reviews_top10": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[:10]])) if len(sorted_products) >= 10 else 0,
                "avg_reviews_bottom50": int(statistics.mean([p.get("review_count", 0) for p in sorted_products[50:]])) if len(sorted_products) > 50 else 0,
                "total_estimated_monthly_sales": int(sum(p.get("sales", 0) for p in products))
            },
            "market_churn": churn_result,
            "seasonality": seasonality_result,
            "bsr_churn": bsr_churn_result,
        }

    def _calculate_weighted_score(self, metrics: Dict[str, float]) -> tuple[float, Dict[str, Any]]:
        total_score, details = 0.0, {}
        for key, value in metrics.items():
            weight = self.weights.get(key, 0.0)
            contribution = value * weight
            total_score += contribution
            details[key] = {"raw_score": round(value, 2), "weight": weight, "weighted_contribution": round(contribution, 2)}
        return total_score, details

    def _interpret_score(
        self,
        score: float,
        churn_result: Optional[Dict[str, Any]] = None,
        seasonality_result: Optional[Dict[str, Any]] = None,
        bsr_churn_result: Optional[Dict[str, Any]] = None,
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

        if score >= self.thresholds.get("high_monopoly_score", 75):
            base = "High Monopoly (Red Ocean)"
        elif score <= self.thresholds.get("opportunity_score", 40):
            base = "Low Monopoly (Blue Ocean/Opportunity)"
        else:
            base = "Medium Competition"

        if seasonality_result and seasonality_result.get("is_seasonal"):
            return f"{base} + Seasonal ({seasonality_result.get('pattern', '')})"
        return base

    def _analyze_sales_distribution(self, products: List[Dict[str, Any]]) -> Dict[str, float]:
        total_sales = sum(p.get("sales") or 0 for p in products) or 1
        top3_sales = sum(p.get("sales") or 0 for p in products[:3])
        survival_sales = sum(p.get("sales") or 0 for p in products[19:50])
        cr3 = top3_sales / total_sales
        cr3_limit = self.thresholds.get("cr3_monopoly_limit", 0.60)
        conc_score = min(100, (cr3 / cr3_limit) * 100)
        survival_ratio = survival_sales / total_sales
        survival_score = max(0, 100 - (survival_ratio / 0.20) * 100)
        return {"top3_concentration": conc_score, "survival_space": survival_score, "cr3": round(cr3, 4)}

    def _analyze_brand_concentration(self, products: List[Dict[str, Any]]) -> float:
        brands = [p.get("brand") for p in products if p.get("brand")]
        if not brands: return 50
        counts = {b: brands.count(b) for b in set(brands)}
        brand_ratio = len(counts) / len(products)
        return max(0, 100 - (brand_ratio * 150))

    def _analyze_seller_background(self, products: List[Dict[str, Any]]) -> float:
        amazon_count = sum(1 for p in products if p.get("seller_type") in ["Amazon", "AMZ", "Retail"])
        mega_seller_feedback = self.thresholds.get("mega_seller_feedback", 10000)
        large_seller_count = sum(1 for p in products if p.get("feedback_count", 0) > mega_seller_feedback)
        amz_ratio = amazon_count / len(products)
        large_ratio = large_seller_count / len(products)
        return min(100, (amz_ratio * 300) + (large_ratio * 100))

    def _analyze_review_barrier(self, products: List[Dict[str, Any]]) -> float:
        if len(products) < 20: return 50
        top_10 = products[:10]
        bottom_50 = products[49:] if len(products) > 50 else products[len(products)//2:]
        avg_reviews_top = statistics.mean([p.get("review_count", 0) for p in top_10]) or 1
        avg_reviews_bottom = statistics.mean([p.get("review_count", 0) for p in bottom_50]) or 1
        review_disparity = avg_reviews_top / avg_reviews_bottom
        disparity_threshold = self.thresholds.get("review_disparity_threshold", 5.0)
        review_score = min(100, (review_disparity / disparity_threshold) * 100)
        avg_rating_top = statistics.mean([p.get("rating", 0) for p in top_10]) or 4.0
        rating_cap = self.thresholds.get("rating_hard_barrier", 4.5)
        rating_score = max(0, (avg_rating_top - 4.0) / (rating_cap - 4.0) * 100) if avg_rating_top > 4.0 else 0
        return (review_score * 0.7) + (rating_score * 0.3)

    def _analyze_price_convergence(self, products: List[Dict[str, Any]]) -> float:
        prices = [p.get("price") for p in products if p.get("price")]
        if len(prices) < 5: return 50
        avg_price, std_dev = statistics.mean(prices), statistics.stdev(prices)
        cv = std_dev / avg_price
        cv_threshold = self.thresholds.get("price_cv_compression", 0.15)
        if cv < cv_threshold: return 100
        return max(0, 100 - (cv / 0.6 * 100))

    def _analyze_keyword_monopoly(self, keyword_data: Optional[Dict[str, Any]]) -> float:
        if not keyword_data or "top_asins" not in keyword_data: return 50
        top3_shares = sum(item.get("clickShare", 0) for item in keyword_data["top_asins"][:3])
        return min(100, (top3_shares / 0.50) * 100)

    def _analyze_ad_competition(self, ad_data: Optional[Dict[str, Any]]) -> float:
        if not ad_data: return 50
        
        # 1. Search Result Ad Ratio (Visibility Competition)
        # Reflects how crowded the first page is with ads
        ratio = ad_data.get("ad_ratio") or 0.3
        danger_zone = self.thresholds.get("ad_ratio_danger_zone", 0.40)
        visibility_score = min(100, (ratio / danger_zone) * 100)

        # 2. BSR Winners Ad Dependency (Actual Sales Driver)
        # If winners rely heavily on ads, the moat for natural search is weak
        # or the CAC is high for everyone.
        bsr_ad_ratio = ad_data.get("actual_bsr_ad_ratio") or ratio  # Fallback to search ratio
        dependency_score = min(100, (bsr_ad_ratio / 0.50) * 100)  # 50% dependency is critical

        # 3. Detailed Bid Analysis (Capital barrier)
        detailed_bids = ad_data.get("detailed_bids", {})
        if detailed_bids:
            bid_barrier_score = self._calculate_bid_barrier_score(detailed_bids)
        else:
            # Fallback to single avg_bid if detailed data is missing
            bid = ad_data.get("avg_bid", 0)
            high_bid_threshold = self.thresholds.get("high_bid_barrier", 2.50)
            bid_barrier_score = min(100, (bid / high_bid_threshold) * 100) if bid > 0 else 50
        
        # Combined score: 
        # 20% Visibility (Current heat)
        # 20% Dependency (How hard winners have to pay)
        # 60% Bid Barrier (Capital requirement to displace winners)
        return (visibility_score * 0.2) + (dependency_score * 0.2) + (bid_barrier_score * 0.6)

    def _calculate_bid_barrier_score(self, detailed_bids: Dict[str, Any]) -> float:
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
                bid = expr.get("suggestedBid", {}).get("amount", 0)
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

    def _analyze_external_intensity(self, external_data: Optional[Dict[str, Any]]) -> tuple[float, float]:
        if not external_data: return 50.0, 50.0
        social_psi = external_data.get("social_psi", 0)
        social_score = min(100, social_psi)
        deal_intensity = external_data.get("deal_intensity", 0)
        deal_score = min(100, deal_intensity * 10)
        return social_score, deal_score

    def _analyze_bsr_churn(
        self,
        snapshots: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
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
        _empty: Dict[str, Any] = {
            "churn_3m": None,
            "churn_6m": None,
            "churn_12m": None,
            "label": "unknown",
            "snapshots_available": [],
        }

        if not snapshots:
            return _empty

        sorted_months = sorted(snapshots.keys())  # chronological, e.g. ["202502","202508","202511","202602"]
        latest = sorted_months[-1]
        latest_set = {p["asin"] for p in snapshots[latest] if p.get("asin")}
        if not latest_set:
            return _empty

        latest_y, latest_mo = int(latest[:4]), int(latest[4:])

        def churn_vs(older_ym: str) -> Optional[float]:
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

        # Use best available values for labelling
        c12 = churn_12m if churn_12m is not None else (churn_6m or 0.0)
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
        products: List[Dict[str, Any]],
        historical_data: Optional[Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:
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
        new_product_ratio = sum(
            1 for p in products if (p.get("review_count") or 0) < 50
        ) / max(len(products), 1)

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
                    recent_avg = statistics.mean(ratings[-14:])
                    if peak - recent_avg > self.thresholds.get("rating_collapse_threshold", 0.3):
                        collapse_count += 1

        collapse_rate = collapse_count / total_tracked if total_tracked else 0.0
        rating_depression = max(0.0, (4.0 - avg_rating) / 0.5 * 50) if avg_rating < 4.0 else 0.0

        churn_score = min(100,
            collapse_rate * 60
            + new_product_ratio * 100 * 0.3
            + rating_depression * 0.1
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
        historical_data: Optional[Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:
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
        _no_data: Dict[str, Any] = {
            "seasonality_score": 0, "is_seasonal": False, "peak_months": [],
            "pattern": "unknown", "monthly_amplitude": 0,
            "platform_event_dampened": [], "platform_event_in_peak": False,
        }
        if not historical_data:
            return _no_data

        PLATFORM_EVENT_MONTHS = {7, 11}  # Prime Day, Black Friday

        # ── Step 1: aggregate log(BSR) by (year, month) ───────────────────
        monthly_log_bsr: Dict[Tuple[int, int], List[float]] = {}
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
        cov_tv = sum((t - mean_t) * (v - mean_v) for t, v in zip(t_vals, v_vals))
        var_t = sum((t - mean_t) ** 2 for t in t_vals) or 1.0
        slope = cov_tv / var_t
        intercept = mean_v - slope * mean_t
        detrended = {k: monthly_median[k] - (slope * i + intercept)
                     for i, k in enumerate(sorted_keys)}

        # ── Step 4: group by calendar month; dampen platform event months ─
        calendar_residuals: Dict[int, List[float]] = {}
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
        }

    def _analyze_seasonality_from_keyword_trends(
        self,
        keyword_weekly_trends: Dict[str, Any],
    ) -> Dict[str, Any]:
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
        _no_data: Dict[str, Any] = {
            "seasonality_score": 0, "is_seasonal": False, "peak_months": [],
            "pattern": "unknown", "monthly_amplitude": 0,
            "platform_event_dampened": [], "platform_event_in_peak": False,
            "source": "keyword_weekly_trends",
        }

        try:
            terms = keyword_weekly_trends.get("searchTerms") or []
            if not terms:
                return _no_data
            week_search: List[float] = [
                float(v) for v in (terms[0].get("trends") or {}).get("weekSearch") or []
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
        monthly_log_vol: Dict[Tuple[int, int], List[float]] = {}
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
        cov_tv = sum((t - mean_t) * (v - mean_v) for t, v in zip(t_vals, v_vals))
        var_t = sum((t - mean_t) ** 2 for t in t_vals) or 1.0
        slope = cov_tv / var_t
        intercept = mean_v - slope * mean_t
        detrended = {k: monthly_median[k] - (slope * i + intercept)
                     for i, k in enumerate(sorted_keys)}

        # ── Step 4: group by calendar month; dampen platform event months ─
        calendar_residuals: Dict[int, List[float]] = {}
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

        return {
            "seasonality_score": round(seasonality_score, 2),
            "is_seasonal": seasonality_score >= 20,
            "peak_months": peak_months,
            "pattern": pattern,
            "monthly_amplitude": round(amplitude, 3),
            "platform_event_dampened": sorted(PLATFORM_EVENT_MONTHS),
            "platform_event_in_peak": platform_in_peak,
            "source": "keyword_weekly_trends",
        }

    @staticmethod
    def _circular_arc_span(months: List[int]) -> int:
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
