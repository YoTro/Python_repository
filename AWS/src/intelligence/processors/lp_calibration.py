"""
LP calibration parameter store and PAS telemetry.

Records PAS observations per ASIN and updates Beta-CVR prior parameters
(k_max per match type) when the moving average trigger fires.

File layout:
    config/lp_calibration/_global.yaml       — seed defaults
    config/lp_calibration/{ASIN}.yaml        — per-ASIN overrides
    data/intelligence/lp_snapshots/{ASIN}/pas_history.jsonl — PAS log

Phase 1 (current): record_pas() writes telemetry only.
Phase 3: compute_update() is called after 3+ consecutive out-of-band PAS
         observations and writes proportional k_max adjustments.
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import threading
from datetime import date as _date_cls

import yaml

logger = logging.getLogger(__name__)

# Per-ASIN locks serialise the read-compute-write cycle in _maybe_update.
# Without these, two threads can both read the same last_trigger_at, compute
# the same window, and each apply *1.1 — doubling the intended adjustment.
_ASIN_LOCKS: dict[str, threading.Lock] = {}
_ASIN_LOCKS_GUARD = threading.Lock()

_CAL_DIR = os.path.join("config", "lp_calibration")
_SNAP_ROOT = os.path.join("data", "intelligence", "lp_snapshots")
_GLOBAL_SEED = os.path.join(_CAL_DIR, "_global.yaml")

# Proportional correction step size per trigger event.
# PAS > hi → conservative → k_max too large  → decrease by _STEP_FACTOR
# PAS < lo → over-optimistic → k_max too small → increase by _STEP_FACTOR
_STEP_FACTOR = 0.10  # 10% adjustment per trigger
_TRIGGER_WINDOW = 3  # consecutive out-of-band PAS observations required
_K_MAX_FLOOR = 0.5  # never shrink k_max below this
_K_MAX_CEILING = 10.0  # never grow k_max above this


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------


def _cal_path(asin: str) -> str:
    return os.path.join(_CAL_DIR, f"{asin.upper()}.yaml")


def _pas_log_path(asin: str) -> str:
    return os.path.join(_SNAP_ROOT, asin.upper(), "pas_history.jsonl")


def _deep_merge(base: dict, overrides: dict) -> dict:
    """Merge overrides into base, recursively merging nested dicts.

    Prevents shallow dict.update() from clobbering entire nested mappings
    (e.g., k_by_match_type) when the override only specifies one match type.
    """
    result = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_calibration(asin: str) -> dict:
    """Return calibration params for asin, deep-merging over _global.yaml."""
    params: dict = {}
    if os.path.exists(_GLOBAL_SEED):
        with open(_GLOBAL_SEED) as f:
            params = yaml.safe_load(f) or {}

    per_asin = _cal_path(asin)
    if os.path.exists(per_asin):
        with open(per_asin) as f:
            overrides = yaml.safe_load(f) or {}
        params = _deep_merge(params, overrides)

    return params


def save_calibration(asin: str, params: dict) -> None:
    """Persist updated calibration params for asin."""
    os.makedirs(_CAL_DIR, exist_ok=True)
    path = _cal_path(asin)
    with open(path, "w") as f:
        yaml.safe_dump(params, f, default_flow_style=False, sort_keys=True)
    logger.info("Calibration written → %s", path)


def _append_pas_log(asin: str, entry: dict) -> None:
    log_path = _pas_log_path(asin)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(json.dumps(entry) + "\n")
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _load_pas_log(asin: str) -> list[dict]:
    log_path = _pas_log_path(asin)
    if not os.path.exists(log_path):
        return []
    entries: list[dict] = []
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Skipping unparsable PAS log line for %s: %s — line: %.120r",
                        asin,
                        exc,
                        line,
                    )
    return entries


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def record_pas(
    asin: str,
    run_date: str,
    pas: float | None,
    band_result: str,
    n_keywords: int,
    mean_impl_ratio: float,
    its_status: str,
) -> None:
    """Append a PAS observation to the telemetry log and trigger calibration
    update if _TRIGGER_WINDOW consecutive out-of-band results are seen.
    """
    entry = {
        "date": run_date,
        "pas": pas,
        "band_result": band_result,
        "n_keywords": n_keywords,
        "mean_impl_ratio": mean_impl_ratio,
        "its_status": its_status,
    }
    _append_pas_log(asin, entry)
    logger.info(
        "PAS recorded for %s/%s: PAS=%s band=%s",
        asin,
        run_date,
        pas,
        band_result,
    )

    if band_result in ("over_optimistic", "conservative") and pas is not None:
        _maybe_update(asin)


def compute_update(asin: str, current_params: dict | None = None) -> dict | None:
    """Check whether the trigger has fired and return updated params if so.

    Returns updated params dict when:
      - The trigger fires (k_cvr_max adjusted, last_trigger_at advanced), OR
      - The evaluated window is mixed-band (last_trigger_at advanced only,
        k_cvr_max unchanged) — caller must still persist to unblock future calls.
    Returns None only when fewer than _TRIGGER_WINDOW new observations exist.
    Does NOT persist — caller must call save_calibration() if desired.

    Trigger logic: requires _TRIGGER_WINDOW *new* observations since the last
    trigger (tracked via last_trigger_at in the calibration YAML).  This
    prevents runaway drift when the model stays out-of-band for many weeks —
    each trigger fires exactly once per fresh _TRIGGER_WINDOW-observation run.

    Mixed-window advancement: when a window contains mixed band results, the
    watermark advances past it so future consistent observations are not
    permanently blocked by stale mixed data anchoring the window at index 0.
    """
    history = _load_pas_log(asin)
    if len(history) < _TRIGGER_WINDOW:
        return None

    params = current_params if current_params is not None else load_calibration(asin)

    # Only consider observations that arrived after the last trigger.
    last_trigger_at = int(params.get("last_trigger_at", 0))
    new_obs_count = len(history) - last_trigger_at
    if new_obs_count < _TRIGGER_WINDOW:
        return None

    # Evaluate the most recent _TRIGGER_WINDOW observations in the new window.
    recent = history[last_trigger_at : last_trigger_at + _TRIGGER_WINDOW]
    bands = [e["band_result"] for e in recent]

    all_conservative = all(b == "conservative" for b in bands)
    all_over_optimistic = all(b == "over_optimistic" for b in bands)

    if not (all_conservative or all_over_optimistic):
        # Mixed window — advance watermark past it so future consistent
        # observations are not permanently blocked by stale mixed data.
        params["last_trigger_at"] = last_trigger_at + _TRIGGER_WINDOW
        logger.debug(
            "Calibration window mixed for %s (obs %d–%d) — advancing watermark to %d",
            asin,
            last_trigger_at,
            last_trigger_at + _TRIGGER_WINDOW - 1,
            last_trigger_at + _TRIGGER_WINDOW,
        )
        return params

    k_max = float(params.get("k_cvr_max", 3.0))

    if all_conservative:
        new_k = max(k_max * (1.0 - _STEP_FACTOR), _K_MAX_FLOOR)
        direction = "decrease"
    else:
        new_k = min(k_max * (1.0 + _STEP_FACTOR), _K_MAX_CEILING)
        direction = "increase"

    new_k = round(new_k, 4)
    if new_k == k_max:
        return None

    params["k_cvr_max"] = new_k
    params["last_updated"] = _date_cls.today().isoformat()
    params["last_trigger"] = direction
    # Advance the watermark so the next trigger requires a fresh window.
    params["last_trigger_at"] = last_trigger_at + _TRIGGER_WINDOW

    logger.info(
        "Calibration trigger fired for %s: k_cvr_max %s → %s (%s, obs %d–%d)",
        asin,
        k_max,
        new_k,
        direction,
        last_trigger_at,
        last_trigger_at + _TRIGGER_WINDOW - 1,
    )
    return params


def _asin_lock(asin: str) -> threading.Lock:
    with _ASIN_LOCKS_GUARD:
        if asin not in _ASIN_LOCKS:
            _ASIN_LOCKS[asin] = threading.Lock()
        return _ASIN_LOCKS[asin]


def _maybe_update(asin: str) -> None:
    """Internal: compute and persist update if trigger has fired."""
    with _asin_lock(asin):
        new_params = compute_update(asin)
        if new_params is not None:
            save_calibration(asin, new_params)
