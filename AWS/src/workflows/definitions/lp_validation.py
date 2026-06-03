"""
LP Validation workflow — Predictive Accuracy Score (PAS) computation.

Compares LP optimizer predictions (stored in a snapshot at T₀) against actual
post-period performance to measure model calibration fidelity.

CLI:
    python main.py --workflow lp_validation \\
        --params '{"asin":"B0FXFGMD7Z","snapshot_date":"2026-05-29",
                   "n_days":28,"store_id":"<profile_id>"}'

Config keys:
    asin          str   ASIN whose snapshot to validate
    snapshot_date str   YYYY-MM-DD when the LP snapshot was saved
    n_days        int   post-period length (default 28; minimum recommended 14)
    store_id      str   Amazon Ads profile / store ID
    region        str   NA | EU | FE  (default NA)
"""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import date as _date_cls
from datetime import timedelta

import numpy as np

from src.workflows.engine import Workflow, WorkflowContext
from src.workflows.registry import WorkflowRegistry
from src.workflows.steps.enrich import EnrichStep

logger = logging.getLogger(__name__)

_SNAP_ROOT = os.path.join("data", "intelligence", "lp_snapshots")
_MIN_ITS_PRE = 7  # minimum pre-period days for ITS trend fit
_DEAD_BAND_BASE = 0.20  # half-width at n_keywords == 10
_IMPL_WARN_THR = 0.70  # widen dead band when mean impl_ratio < this
# Campaigns whose impl_ratio falls below this are treated as "not implemented":
# their keywords are excluded from sum_pred_adj (denominator) entirely.
# A campaign that was blocked by inventory gate or user override will have
# actual_daily ≈ historical_daily << lp_recommended_daily, giving ir < this.
_IMPL_FLOOR = 0.10
# Maximum absolute daily trend slope as a fraction of pre-period mean.
# Prevents organic-growth trends (launch phase) from over-extrapolating the
# ITS counterfactual and biasing the PAS numerator.
_MAX_TREND_FRAC = 0.50


# ---------------------------------------------------------------------------
# Snapshot I/O
# ---------------------------------------------------------------------------


def _snap_path(asin: str, snapshot_date: str) -> str:
    return os.path.join(_SNAP_ROOT, asin.upper(), f"{snapshot_date}.json")


def _load_snap(asin: str, snapshot_date: str) -> dict:
    path = _snap_path(asin, snapshot_date)
    if not os.path.exists(path):
        asin_dir = os.path.join(_SNAP_ROOT, asin.upper())
        try:
            available = sorted(f.stem for f in __import__("pathlib").Path(asin_dir).glob("*.json"))
        except Exception:
            available = []
        hint = f"; available dates: {available}" if available else ""
        raise FileNotFoundError(f"LP snapshot not found: {path}{hint}")
    with open(path) as f:
        return json.load(f)


def _write_snap(snap: dict) -> None:
    path = _snap_path(snap["asin"], snap["run_date"])
    with open(path, "w") as f:
        json.dump(snap, f, indent=2)


# ---------------------------------------------------------------------------
# ITS — aggregate LP-scope trend adjustment
# ---------------------------------------------------------------------------


def _its_counterfactual(
    pre_daily: list[dict],
    n_post: int,
) -> tuple[float, float, str]:
    """Fit linear OLS on pre-period LP-scope kw orders and extrapolate n_post days.

    Returns (counterfactual_mean_over_post, pre_period_mean, status).
    status: "ok" | "insufficient_data" | "flat_trend" | "trend_clamped"

    The raw OLS slope is capped at ±_MAX_TREND_FRAC * pre_mean / T to prevent
    high-organic-growth periods (e.g., product launch) from extrapolating an
    unrealistically steep counterfactual that would bias the PAS numerator.
    """
    if len(pre_daily) < _MIN_ITS_PRE:
        pre_orders = [r["orders"] for r in pre_daily]
        pre_mean = float(np.mean(pre_orders)) if pre_orders else 0.0
        return pre_mean, pre_mean, "insufficient_data"

    orders = np.array(
        [r["orders"] for r in sorted(pre_daily, key=lambda x: x["date"])],
        dtype=float,
    )
    T = len(orders)
    t = np.arange(T, dtype=float)
    b, a = np.polyfit(t, orders, 1)  # orders ≈ a + b·t
    pre_mean = float(np.mean(orders))

    # Cap slope: max daily drift = _MAX_TREND_FRAC × pre_mean / T
    # (i.e., cumulative pre-period drift ≤ _MAX_TREND_FRAC × pre_mean)
    b_cap = _MAX_TREND_FRAC * max(pre_mean, 1.0) / T
    b_clamped = max(min(b, b_cap), -b_cap)
    clamped = abs(b) > b_cap

    post_t = np.arange(T, T + n_post, dtype=float)
    cf_mean = float(np.mean(a + b_clamped * post_t))

    if abs(b_clamped) < 1e-4:
        status = "flat_trend"
    elif clamped:
        status = "trend_clamped"
    else:
        status = "ok"

    return cf_mean, pre_mean, status


# ---------------------------------------------------------------------------
# Implementation ratio
# ---------------------------------------------------------------------------


def _impl_ratios(
    post_camp_spend: dict[str, float],
    lp_spend_per_cid: dict[str, float],
    n_actual: int,
) -> dict[str, float]:
    """Delivery-based impl_ratio = actual_daily_spend / lp_recommended_daily_spend.

    n_actual is the number of days actually covered by post_camp_spend data
    (not the configured n_post window).  Using the observed day count prevents
    under-stating impl_ratio when validation is run before the full window elapses.

    Clamped to [0, 2.0].  Missing campaigns default to 1.0 (assume full delivery).
    """
    ratios: dict[str, float] = {}
    for cid, lp_sp in lp_spend_per_cid.items():
        if lp_sp <= 0:
            continue
        actual_daily = post_camp_spend.get(cid, 0.0) / max(n_actual, 1)
        ratios[cid] = min(round(actual_daily / lp_sp, 4), 2.0)
    return ratios


# ---------------------------------------------------------------------------
# Dead band
# ---------------------------------------------------------------------------


def _dead_band(n_keywords: int, mean_impl: float) -> tuple[float, float]:
    """Adaptive dead band [1-w, 1+w].

    w = base / sqrt(n_keywords / 10), capped at ±0.40.
    Widened by 50% when mean impl_ratio < threshold (sparse implementation).
    """
    w = _DEAD_BAND_BASE / math.sqrt(max(n_keywords, 1) / 10.0)
    w = min(w, 0.40)
    if mean_impl < _IMPL_WARN_THR:
        w = min(w * 1.5, 0.40)
    return round(1.0 - w, 4), round(1.0 + w, 4)


# ---------------------------------------------------------------------------
# PAS prerequisite warnings (post-validation, from confirmed measurements)
# ---------------------------------------------------------------------------


def _build_pas_warnings(result: dict) -> list[str]:
    """Return warning strings from confirmed post-validation measurements.

    These supplement the ad_diagnosis prerequisite checks, which can only
    proxy risks from historical data.  Here we have the actual numbers.

    All inputs come from the result dict — no secondary raw-data parameters —
    so there is a single source of truth and no risk of divergence.
    """
    warnings: list[str] = []

    # 1. Low implementation ratio — LP plan was not executed at plan level
    mean_impl = result.get("mean_impl_ratio", 1.0)
    if mean_impl < _IMPL_WARN_THR:
        warnings.append(
            f"low_impl_ratio: mean_impl_ratio={mean_impl:.2f} < {_IMPL_WARN_THR} — "
            "LP spend plan was under-executed; PAS reflects partial-execution accuracy, "
            "not the accuracy of the full LP plan."
        )

    # 2. ITS insufficient data — confirmed flat-baseline fallback
    its_status = result.get("its_status")
    if its_status == "insufficient_data":
        n_pre = result.get("n_pre_days", 0)
        warnings.append(
            f"insufficient_its_data: {n_pre}d pre-period < {_MIN_ITS_PRE}d required — "
            "ITS counterfactual uses a flat baseline; PAS numerator (its_actual_delta) "
            "absorbs organic trend noise and is less reliable."
        )

    # 3. ITS trend clamped — explosive growth biases counterfactual downward
    elif its_status == "trend_clamped":
        warnings.append(
            "trend_clamped: OLS slope exceeded _MAX_TREND_FRAC limit — "
            "product may be in an explosive growth phase; the truncated counterfactual "
            "understates the no-ad baseline, making PAS appear more conservative than it is."
        )

    # 4. PAS indeterminate — denominator collapsed to zero
    if result.get("pas_status") == "indeterminate":
        n_excl = result.get("n_keywords_excluded", 0)
        n_kw = result.get("n_keywords", 0)
        warnings.append(
            f"pas_indeterminate: sum_pred_delta_adj ≈ 0 "
            f"(n_keywords_excluded={n_excl}/{n_kw}) — "
            "all LP-scope budget was excluded by impl_ratio < _IMPL_FLOOR; "
            "no valid denominator for PAS computation."
        )

    # 5. High keyword exclusion rate — denominator based on small subset of LP plan
    n_excl = result.get("n_keywords_excluded", 0)
    n_kw = result.get("n_keywords", 0)
    if n_kw > 0 and result.get("pas_status") != "indeterminate":
        excl_rate = n_excl / n_kw
        if excl_rate > 0.30:
            warnings.append(
                f"high_exclusion_rate: {n_excl}/{n_kw} keywords excluded "
                f"({excl_rate:.0%}, impl_ratio < _IMPL_FLOOR={_IMPL_FLOOR}) — "
                "PAS denominator represents only the delivered fraction of the LP plan."
            )

    # 6. Partial validation window — PAS based on incomplete post-period
    n_actual = result.get("n_actual_days", 0)
    n_post = result.get("n_post_days", 0)
    if n_post > 0 and n_actual < n_post * 0.70:
        warnings.append(
            f"partial_window: {n_actual}d of {n_post}d post-period elapsed ({n_actual / n_post:.0%}) — "
            "PAS is preliminary; rerun after the full window elapses for a stable score."
        )

    # 7. Low pre-period base rate — tiny absolute deltas produce unstable PAS
    pre_mean = result.get("pre_mean_kw_orders_day", 0.0)
    if pre_mean < 1.0 and result.get("pas_status") == "computed":
        warnings.append(
            f"low_base_rate: pre_mean_kw_orders_day={pre_mean:.3f} — "
            "LP-scope keyword orders are near zero; small absolute its_actual_delta "
            "values cause PAS to swing widely on noise; treat this score as low-confidence."
        )

    return warnings


# ---------------------------------------------------------------------------
# PAS computation
# ---------------------------------------------------------------------------


def _compute_pas(
    snap: dict,
    post_kw_daily: list[dict],
    post_camp_spend: dict[str, float],
    n_post: int,
    n_actual: int | None = None,
) -> dict:
    """Compute Predictive Accuracy Score and dead-band classification.

    PAS = ITS-adjusted actual delta / impl-ratio-adjusted predicted delta

    A PAS of 1.0 means the LP model perfectly predicted incremental orders.
    PAS < dead_band_lo  → over-optimistic (recalibrate: increase k_max).
    PAS > dead_band_hi  → conservative   (recalibrate: decrease k_max).

    n_post   — configured validation window length; used as ITS extrapolation horizon.
    n_actual — actual days covered by data; used to normalise campaign spend to daily.
               Defaults to n_post when not supplied.
    """
    if n_actual is None:
        n_actual = n_post

    keywords = snap.get("keywords") or []
    lp_spend_cid = snap.get("lp_spend_per_cid") or {}

    # impl_ratio per campaign — normalise by actual observed days, not configured window
    impl_ratio_map = _impl_ratios(post_camp_spend, lp_spend_cid, n_actual)
    mean_impl = float(np.mean(list(impl_ratio_map.values()))) if impl_ratio_map else 1.0

    # ITS counterfactual on pre-period LP-scope kw orders
    pre_daily = snap.get("pre_period_kw_daily") or []
    cf_mean, pre_mean, its_status = _its_counterfactual(pre_daily, n_post)

    # Actual post mean (over observed days only — future zeros already excluded)
    post_orders = [r["orders"] for r in post_kw_daily]
    actual_post_mean = float(np.mean(post_orders)) if post_orders else 0.0
    its_delta = actual_post_mean - cf_mean

    # Predicted delta (raw, without impl_ratio) — diagnostic reference only
    sum_pred_raw = sum(
        (kw.get("optimized_clicks") or 0.0) - (kw.get("historical_clicks") or 0.0)
        for kw in keywords
        if kw.get("raw_cvr")
    )

    # Predicted delta adjusted by per-campaign impl_ratio.
    # Keywords from campaigns with ir < _IMPL_FLOOR are excluded: their
    # LP-recommended budget was not implemented (e.g., inventory gate blocked the
    # action), so including them would inflate the denominator and falsely pull
    # PAS toward zero ("over-optimistic" misclassification).
    sum_pred_adj = 0.0
    n_excl = 0
    excl_cids: set = set()

    for kw in keywords:
        raw_cvr = kw.get("raw_cvr") or 0.0
        opt_c = kw.get("optimized_clicks") or 0.0
        hist_c = kw.get("historical_clicks") or 0.0
        cid = kw.get("campaign_id") or ""
        ir = impl_ratio_map.get(cid, 1.0)

        # Only exclude when we have an explicit impl measurement showing non-delivery.
        # cid not in impl_ratio_map → no spend data → default 1.0 → include.
        if cid in impl_ratio_map and ir < _IMPL_FLOOR:
            n_excl += 1
            excl_cids.add(cid)
            continue

        sum_pred_adj += (opt_c - hist_c) * raw_cvr * ir

    n_included = len(keywords) - n_excl

    # PAS
    if abs(sum_pred_adj) < 1e-6:
        pas, pas_status = None, "indeterminate"
    else:
        pas = round(its_delta / sum_pred_adj, 4)
        pas_status = "computed"

    lo, hi = _dead_band(n_included, mean_impl)
    if pas is None:
        band = "indeterminate"
    elif lo <= pas <= hi:
        band = "within_band"
    elif pas < lo:
        band = "over_optimistic"
    else:
        band = "conservative"

    result = {
        "pas": pas,
        "pas_status": pas_status,
        "band_result": band,
        "dead_band_lo": lo,
        "dead_band_hi": hi,
        "its_status": its_status,
        "its_counterfactual_mean": round(cf_mean, 4),
        "pre_mean_kw_orders_day": round(pre_mean, 4),
        "actual_post_mean_kw_orders_day": round(actual_post_mean, 4),
        "its_actual_delta": round(its_delta, 4),
        "sum_pred_delta_adj": round(sum_pred_adj, 4),
        "sum_pred_delta_raw": round(sum_pred_raw, 4),
        "mean_impl_ratio": round(mean_impl, 4),
        "impl_ratios": impl_ratio_map,
        "n_post_days": n_post,
        "n_actual_days": n_actual,
        "n_keywords": len(keywords),
        "n_keywords_excluded": n_excl,
        "excluded_cids": list(excl_cids),
        "warnings": _build_pas_warnings(
            {
                "pas_status": pas_status,
                "its_status": its_status,
                "mean_impl_ratio": round(mean_impl, 4),
                "n_keywords": len(keywords),
                "n_keywords_excluded": n_excl,
                "n_actual_days": n_actual,
                "n_post_days": n_post,
                "n_pre_days": len(pre_daily),
                "pre_mean_kw_orders_day": round(pre_mean, 4),
            },
        ),
    }
    return result


# ---------------------------------------------------------------------------
# Workflow steps
# ---------------------------------------------------------------------------


async def _step_load_snapshot(item: dict, ctx: WorkflowContext) -> dict:
    asin = item["asin"]
    snapshot_date = ctx.config.get("snapshot_date") or ""
    n_days = int(ctx.config.get("n_days") or 28)

    snap = _load_snap(asin, snapshot_date)
    snap_dt = _date_cls.fromisoformat(snapshot_date)
    today = _date_cls.today()
    elapsed = (today - snap_dt).days

    if elapsed < n_days:
        logger.warning(
            "Post-period incomplete for %s: %dd elapsed, need %dd — PAS may be unreliable",
            asin,
            elapsed,
            n_days,
        )

    post_start = str(snap_dt + timedelta(days=1))
    post_end = str(snap_dt + timedelta(days=n_days))

    logger.info(
        "Loaded snapshot %s/%s — %d keywords, order_gap=%.2f, post window %s→%s",
        asin,
        snapshot_date,
        len(snap.get("keywords") or []),
        snap.get("order_gap") or 0,
        post_start,
        post_end,
    )
    return {
        "snapshot": snap,
        "days_elapsed": elapsed,
        "post_start": post_start,
        "post_end": post_end,
    }


async def _step_fetch_post_period(item: dict, ctx: WorkflowContext) -> dict:
    if "snapshot" not in item:
        raise RuntimeError(
            "prerequisite 'snapshot' missing — load_snapshot step did not complete; "
            "check the snapshot_date and verify the file exists under data/intelligence/lp_snapshots/"
        )

    from src.mcp.servers.amazon.ads.client import AmazonAdsClient

    snap = item["snapshot"]
    lp_cids = set(snap.get("lp_scoped_cids") or [])
    post_start = item["post_start"]
    post_end = item["post_end"]
    fetch_days = item["days_elapsed"] + 3  # buffer to ensure full API coverage

    ads = AmazonAdsClient(
        store_id=ctx.config.get("store_id"),
        region=ctx.config.get("region", "NA"),
    )

    # spSearchTerm daily — LP-scope keyword orders
    kw_records = await ads.get_performance_report(
        report_type="spSearchTerm", days=fetch_days, time_unit="DAILY"
    )
    kw_day: dict[str, float] = {}
    for r in kw_records:
        cid = str(r.get("campaign_id", ""))
        date = r.get("date") or ""
        if cid in lp_cids and post_start <= date <= post_end:
            kw_day[date] = kw_day.get(date, 0.0) + float(r.get("orders", 0) or 0)

    # Fill post-window dates up to yesterday only.
    # Capping at today-1 prevents padding future dates with 0 orders, which would
    # dilute actual_post_mean when validation is run before the full window elapses.
    today = _date_cls.today()
    d = _date_cls.fromisoformat(post_start)
    end_cfg = _date_cls.fromisoformat(post_end)
    end_data = min(end_cfg, today - timedelta(days=1))

    post_kw_daily: list[dict] = []
    while d <= end_data:
        post_kw_daily.append(
            {
                "date": d.isoformat(),
                "orders": round(kw_day.get(d.isoformat(), 0.0), 4),
            }
        )
        d += timedelta(days=1)

    # spCampaigns daily — LP-scope spend for impl_ratio
    camp_records = await ads.get_performance_report(
        report_type="spCampaigns", days=fetch_days, time_unit="DAILY"
    )
    post_camp_spend: dict[str, float] = {}
    for r in camp_records:
        cid = str(r.get("campaign_id", ""))
        date = r.get("date") or ""
        if cid in lp_cids and post_start <= date <= end_data.isoformat():
            post_camp_spend[cid] = post_camp_spend.get(cid, 0.0) + float(r.get("spend", 0) or 0)

    logger.info(
        "Post-period data: %d days (%s→%s, capped at %s), %d LP-scope campaigns with spend",
        len(post_kw_daily),
        post_start,
        post_end,
        end_data.isoformat(),
        len(post_camp_spend),
    )
    return {"post_kw_daily": post_kw_daily, "post_camp_spend": post_camp_spend}


async def _step_compute_pas(item: dict, ctx: WorkflowContext) -> dict:
    missing = [k for k in ("snapshot", "post_kw_daily", "post_camp_spend") if k not in item]
    if missing:
        raise RuntimeError(
            f"prerequisite keys missing from item: {missing} — "
            "one or more upstream steps (load_snapshot, fetch_post_period) did not complete"
        )

    from src.intelligence.processors.lp_calibration import record_pas

    snap = item["snapshot"]
    post_kw_daily = item["post_kw_daily"]
    post_camp_spend = item["post_camp_spend"]
    n_post = int(ctx.config.get("n_days") or 28)  # configured window — ITS extrapolation horizon
    n_actual = len(post_kw_daily)  # observed days — impl_ratio denominator

    result = _compute_pas(snap, post_kw_daily, post_camp_spend, n_post, n_actual=n_actual)

    snap["validation"] = result
    _write_snap(snap)

    logger.info(
        "PAS %s/%s: PAS=%.3f [%s] impl=%.2f dead_band=[%.2f,%.2f] excl_kw=%d/%d its=%s",
        snap["asin"],
        snap["run_date"],
        result["pas"] if result["pas"] is not None else float("nan"),
        result["band_result"],
        result["mean_impl_ratio"],
        result["dead_band_lo"],
        result["dead_band_hi"],
        result["n_keywords_excluded"],
        result["n_keywords"],
        result["its_status"],
    )

    for w in result.get("warnings") or []:
        logger.warning("PAS prerequisite [%s/%s]: %s", snap["asin"], snap["run_date"], w)

    record_pas(
        asin=snap["asin"],
        run_date=snap["run_date"],
        pas=result["pas"],
        band_result=result["band_result"],
        n_keywords=result["n_keywords"],
        mean_impl_ratio=result["mean_impl_ratio"],
        its_status=result["its_status"],
    )
    return {"pas_result": result}


# ---------------------------------------------------------------------------
# Workflow builder
# ---------------------------------------------------------------------------


@WorkflowRegistry.register("lp_validation")
def build_lp_validation(config: dict) -> Workflow:
    """Validate LP allocation predictions against actual post-period performance.

    Required: asin, snapshot_date, store_id
    Optional: n_days (default 28), region (default NA)
    """
    asin = (config.get("asin") or "").upper()
    snapshot_date = config.get("snapshot_date") or ""

    if not asin or not snapshot_date:
        raise ValueError("lp_validation requires 'asin' and 'snapshot_date' in config")

    return Workflow(
        name="lp_validation",
        steps=[
            EnrichStep(name="load_snapshot", extractor_fn=_step_load_snapshot),
            EnrichStep(name="fetch_post_period", extractor_fn=_step_fetch_post_period),
            EnrichStep(name="compute_pas", extractor_fn=_step_compute_pas),
        ],
    )
