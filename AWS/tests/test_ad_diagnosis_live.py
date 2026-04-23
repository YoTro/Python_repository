"""
Live integration test for the ad_diagnosis workflow.
Fetches real data for ASIN B0FXFGMD7Z and runs the full diagnostic pipeline.

Two execution modes
-------------------
Default (JobManager mode):
    Routes through JobManager → supports the full batch suspend/resume lifecycle.
    When ad_diagnosis_llm submits a provider batch job the test prints the batch ID,
    polls every --poll-interval seconds, and waits up to --timeout seconds for the
    provider to complete the batch.  Use this mode to test the Batch API end-to-end.

--direct mode:
    Calls workflow.execute() directly — bypasses JobManager and BatchPoller.
    Combine with --no-llm for fast data-collection-only validation.
    If the LLM step is enabled in direct mode, BatchPendingError will propagate
    and the test will stop after printing the submitted batch handle.

Checkpoint support
------------------
Each step is checkpointed after completion.  Re-running with the same --job-id
resumes from the last completed step, skipping expensive API report fetches.

Usage
-----
    # Full end-to-end (default — JobManager + BatchPoller):
    venv311/bin/python3 tests/test_ad_diagnosis_live.py

    # Data-collection only (no LLM, direct mode):
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --direct --no-llm

    # Resume suspended batch job that was checkpointed:
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id ad-diag-B0FXFGMD7Z-dev

    # Reset checkpoint and start fresh:
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id ad-diag-B0FXFGMD7Z-dev --reset

    # LLM-only: compress existing checkpoint data and run just the LLM step via JobManager:
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --llm-only --job-id ad-diag-B0FXFGMD7Z-dev
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import os
import argparse
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("ad_diagnosis_test")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Live ad_diagnosis workflow test")
    p.add_argument("--asin",     default="B0FXFGMD7Z", help="ASIN to diagnose")
    p.add_argument("--store-id", default="US",  help="Store/marketplace ID (default: US)")
    p.add_argument("--region",   default="NA",  help="Ads API region (default: NA)")
    p.add_argument("--days",     type=int, default=30, help="Report lookback days")
    p.add_argument("--no-xiyou", action="store_true", help="Skip Xiyouzhaoci enrichment")
    p.add_argument("--no-llm",   action="store_true", help="Skip LLM diagnostic step")
    p.add_argument(
        "--direct",
        action="store_true",
        help=(
            "Bypass JobManager and call workflow.execute() directly. "
            "Use with --no-llm for fast data-only runs. "
            "BatchPendingError will surface if LLM step is enabled."
        ),
    )
    p.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Seconds between status polls in JobManager mode (default: 30)",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=7200,
        help="Max seconds to wait for batch completion in JobManager mode (default: 7200)",
    )
    p.add_argument(
        "--job-id",
        default=None,
        help=(
            "Fixed job ID for checkpoint resume. "
            "Defaults to 'ad-diag-<ASIN>-dev' (stable across runs). "
            "Use a timestamp suffix for a one-off fresh run."
        ),
    )
    p.add_argument(
        "--reset",
        action="store_true",
        help="Clear existing checkpoint before running (forces full re-fetch).",
    )
    p.add_argument(
        "--llm-only",
        action="store_true",
        help=(
            "Load an existing --job-id checkpoint, compress the items to LLM-relevant "
            "fields only, save a new checkpoint at job_id+'-llm', then run just the "
            "ad_diagnosis_llm step via JobManager (tests the full Batch API flow)."
        ),
    )
    return p.parse_args()


# ── Helpers ───────────────────────────────────────────────────────────────────

# Fields kept from the raw item for LLM context.
# Drop campaigns, campaign_ids, performance_records, keywords — large raw arrays
# whose summaries are already captured in other fields.
_LLM_SCALAR_FIELDS = [
    "asin", "title", "brand", "size", "bullet_point_count",
    "total_available", "can_sell_days", "inventory_risk",
    "total_daily_budget", "bidding_strategies",
    "total_spend", "total_sales", "total_orders", "total_clicks", "account_acos",
    "budget_exhaustion_pct", "budget_likely_exhausted",
    "keyword_count", "avg_bid", "min_bid", "max_bid", "match_type_dist",
    "placement_performance", "placement_configured_pcts",
    "change_event_count", "has_compound_change",
    "lp_summary", "lp_top_allocations", "lp_zero_keywords", "lp_maxed_keywords",
    # Xiyou traffic scores
    "ad_traffic_ratio", "organic_traffic_ratio", "traffic_growth_7d",
    # Keyword signals (merged from fetch_keyword_signals)
    "rank_tracked_keywords", "market_trends_meta",
]


def _compress_item_for_llm(item: dict) -> dict:
    """Retain only LLM-relevant fields; truncate large lists."""
    compressed = {k: item[k] for k in _LLM_SCALAR_FIELDS if k in item}

    # Top-8 high-ACOS campaigns
    if "high_acos_campaigns" in item:
        compressed["high_acos_campaigns"] = item["high_acos_campaigns"][:8]

    # Full keyword_performance (small enough as-is)
    if "keyword_performance" in item:
        compressed["keyword_performance"] = item["keyword_performance"]

    # Most recent 20 change events (already truncated to 50 upstream)
    if "change_events" in item:
        compressed["change_events"] = item["change_events"][:20]

    # change_attributions now embeds ITS/CausalImpact/DML/consensus per event
    if item.get("change_attributions"):
        compressed["change_attributions"] = item["change_attributions"]

    # natural_rank_series: daily organic rank per keyword (needed for LLM dim 5)
    if item.get("natural_rank_series"):
        compressed["natural_rank_series"] = item["natural_rank_series"]

    # market_trends: weekly SFR per keyword (needed for LLM dim 5 + dim 10)
    if item.get("market_trends"):
        compressed["market_trends"] = item["market_trends"]

    # competitor_price_summary: used in causal covariate annotation
    if item.get("competitor_price_summary"):
        compressed["competitor_price_summary"] = item["competitor_price_summary"]

    return compressed


def _build_config(args: argparse.Namespace) -> dict:
    return {
        "store_id":              args.store_id,
        "region":                args.region,
        "days":                  args.days,
        "enable_xiyou":          not args.no_xiyou,
        "inventory_risk_days":   30,
        "acos_warn_threshold":   0.30,
        "acos_crit_threshold":   0.50,
        "budget_exhaustion_pct": 0.90,
        "min_clicks_for_cvr":    5,
        "lp_headroom_factor":    3.0,
    }


def _print_result(result, workflow_steps) -> None:
    print(f"\n{'='*60}")
    print(f"Workflow completed in {result.total_duration_ms}ms")
    print(f"Steps executed: {len(result.step_reports)}")
    print(f"{'='*60}\n")

    print("── Step Reports ──")
    for r in result.step_reports:
        print(
            f"  [{r.step_index+1}] {r.step_name:<30} "
            f"{r.input_count} → {r.output_count} items  "
            f"({r.duration_ms}ms)"
        )

    print(f"\n── Final Item Preview (truncated) ──")
    if result.final_items:
        item = result.final_items[0]
        highlights = {
            "asin":                    item.get("asin"),
            "title":                   item.get("title"),
            "brand":                   item.get("brand"),
            "total_available":         item.get("total_available"),
            "can_sell_days":           item.get("can_sell_days"),
            "inventory_risk":          item.get("inventory_risk"),
            "campaign_count":          len(item.get("campaigns", [])),
            "total_daily_budget":      item.get("total_daily_budget"),
            "bidding_strategies":      item.get("bidding_strategies"),
            "total_spend":             item.get("total_spend"),
            "total_sales":             item.get("total_sales"),
            "account_acos":            item.get("account_acos"),
            "budget_exhaustion_pct":   item.get("budget_exhaustion_pct"),
            "budget_likely_exhausted": item.get("budget_likely_exhausted"),
            "keyword_count":           item.get("keyword_count"),
            "avg_bid":                 item.get("avg_bid"),
            "match_type_dist":         item.get("match_type_dist"),
            "kw_performance_count":    len(item.get("keyword_performance", [])),
            "lp_summary":              item.get("lp_summary"),
            "lp_top_allocations":      item.get("lp_top_allocations", [])[:3],
            "lp_zero_keywords":        item.get("lp_zero_keywords", [])[:5],
            "lp_maxed_keywords":       item.get("lp_maxed_keywords", [])[:5],
            "ad_traffic_ratio":        item.get("ad_traffic_ratio"),
            "organic_traffic_ratio":   item.get("organic_traffic_ratio"),
            "rank_tracked_keywords":   item.get("rank_tracked_keywords"),
            "rank_series_days":        len(next(iter((item.get("natural_rank_series") or {}).values()), {})),
            "market_trends_keywords":  list((item.get("market_trends") or {}).keys()),
            "change_attributions":     len(item.get("change_attributions") or []),
            "causal_consensus_sample": (item.get("change_attributions") or [{}])[0].get("consensus"),
        }
        for k, v in highlights.items():
            print(f"  {k:<30} {json.dumps(v, ensure_ascii=False)}")

        llm_output = (
            item.get("ad_diagnosis_llm")
            or item.get("llm_output")
            or item.get("diagnosis")
        )
        if llm_output:
            print(f"\n── LLM Diagnosis ──\n{llm_output}\n")

    out_path = f"/tmp/ad_diagnosis_{result.final_items[0].get('asin', 'unknown')}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result.final_items, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nFull result saved to: {out_path}")


# ── Execution modes ───────────────────────────────────────────────────────────

async def _run_direct(args: argparse.Namespace, job_id: str) -> None:
    """Bypass JobManager — direct workflow.execute() call."""
    from src.workflows.registry import WorkflowRegistry
    from src.workflows.steps.base import WorkflowContext
    from src.jobs.checkpoint import CheckpointManager
    from src.intelligence.router import IntelligenceRouter
    from src.core.errors.exceptions import BatchPendingError
    import src.workflows.definitions.ad_diagnosis  # noqa: F401

    checkpoint_mgr = CheckpointManager()
    if args.reset:
        checkpoint_mgr.clear(job_id)
        logger.info(f"Checkpoint cleared for job_id={job_id}")

    existing = checkpoint_mgr.load(job_id)
    if existing:
        logger.info(
            f"Resuming from checkpoint: last completed step [{existing.step_index}] "
            f"'{existing.step_name}', {len(existing.items)} items, "
            f"{len(existing.ctx_cache)} cache keys"
        )
    else:
        logger.info(f"No checkpoint found for job_id={job_id}, starting fresh")

    config = _build_config(args)
    ctx = WorkflowContext(
        job_id=job_id,
        tenant_id="default",
        config=config,
        router=IntelligenceRouter(),
    )

    params = {
        "initial_items": [{"asin": args.asin}],
        "no_llm": args.no_llm,
        **config,
    }
    workflow = WorkflowRegistry.build("ad_diagnosis", params)
    if args.no_llm:
        logger.info("LLM step disabled — running data-collection steps only")

    try:
        result = await workflow.execute(
            job_id=job_id,
            params=params,
            ctx=ctx,
            checkpoint_mgr=checkpoint_mgr,
        )
    except BatchPendingError as e:
        logger.warning(
            f"\n[DIRECT MODE] BatchPendingError raised — batch submitted but cannot be "
            f"polled in direct mode.\n"
            f"  batch_id  : {e.batch_job_id}\n"
            f"  provider  : {getattr(e.handle, 'provider', 'unknown')}\n"
            f"  requests  : {len(e.requests)}\n"
            f"\nRe-run WITHOUT --direct to poll via JobManager, or wait for the batch "
            f"to complete and resume with:\n"
            f"  venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id {job_id}"
        )
        return

    # Preserve checkpoint in dev mode so data-collection steps are not re-run
    checkpoint_mgr.save(
        job_id=job_id,
        step_index=len(workflow.steps) - 1,
        step_name=workflow.steps[-1].name,
        items=result.final_items,
        workflow_name="ad_diagnosis",
        workflow_params=params,
        ctx_cache=dict(ctx.cache),
    )
    logger.info(
        f"Checkpoint preserved at job_id={job_id} "
        f"({len(ctx.cache)} cache keys). Use --reset to start fresh."
    )
    _print_result(result, workflow.steps)


async def _run_via_job_manager(args: argparse.Namespace, job_id: str) -> None:
    """Route through JobManager — handles SUSPENDED → BatchPoller → resume lifecycle."""
    from src.jobs.manager import JobManager, JobStatus
    from src.jobs.checkpoint import CheckpointManager
    from src.core.models.request import UnifiedRequest
    import src.workflows.definitions.ad_diagnosis  # noqa: F401

    checkpoint_mgr = CheckpointManager()
    if args.reset:
        checkpoint_mgr.clear(job_id)
        logger.info(f"Checkpoint cleared for job_id={job_id}")

    config = _build_config(args)
    if args.no_llm:
        logger.info("LLM step disabled — running data-collection steps only")

    # Inject no_llm flag into params so the workflow definition can act on it
    params = {
        "initial_items": [{"asin": args.asin}],
        "no_llm": args.no_llm,
        **config,
    }

    job_mgr = JobManager(max_workers=1)

    existing = checkpoint_mgr.load(job_id)
    if existing:
        logger.info(
            f"Checkpoint found: last completed step [{existing.step_index}] "
            f"'{existing.step_name}', {len(existing.items)} items — resuming"
        )
        job_mgr.resume_from_checkpoint(job_id)
    else:
        request = UnifiedRequest(workflow_name="ad_diagnosis", params=params)
        job_mgr.submit(request, job_id=job_id)
        logger.info(f"Job submitted: {job_id}")

    # ── Poll loop ─────────────────────────────────────────────────────────────
    deadline = time.monotonic() + args.timeout
    last_status = None
    suspended_since: float | None = None

    print(f"\n{'='*60}")
    print(f"  Polling job {job_id} every {args.poll_interval}s (timeout {args.timeout}s)")
    print(f"{'='*60}")

    while True:
        record = job_mgr.get_status(job_id)
        if record is None:
            logger.error(f"Job record not found for job_id={job_id}")
            break

        status = record.status

        if status != last_status:
            elapsed = f"{time.monotonic() - (suspended_since or 0):.0f}s" if suspended_since else ""
            logger.info(f"Job status → {status.value.upper()}  {elapsed}")

            if status == JobStatus.SUSPENDED:
                suspended_since = time.monotonic()
                # Extract batch_id from checkpoint events for visibility
                cp = checkpoint_mgr.load(job_id)
                batch_id = None
                provider = None
                if cp:
                    for ev in reversed(cp.events):
                        if ev.event_type == "BATCH_SUBMITTED":
                            batch_id = ev.payload.get("handle", {}).get("job_id")
                            provider = ev.payload.get("handle", {}).get("provider", "").upper()
                            break
                print(
                    f"\n  ┌─ BATCH SUBMITTED ────────────────────────────────┐\n"
                    f"  │  provider : {provider or 'unknown':<38}│\n"
                    f"  │  batch_id : {(batch_id or 'unknown'):<38}│\n"
                    f"  │  timeout  : {args.timeout}s                              │\n"
                    f"  │  BatchPoller polls every 60s — waiting...        │\n"
                    f"  └──────────────────────────────────────────────────┘\n"
                )

            last_status = status

        if status == JobStatus.COMPLETED:
            result = record.result
            if result is None:
                logger.error("Job completed but result is None")
            else:
                _print_result(result, [])
            break

        if status in (JobStatus.FAILED, JobStatus.CANCELLED):
            logger.error(
                f"Job ended with status={status.value}: {record.error or 'no error detail'}"
            )
            break

        if time.monotonic() > deadline:
            logger.error(
                f"Timed out after {args.timeout}s waiting for job {job_id}. "
                f"The batch may still be running. Re-run with the same --job-id to resume."
            )
            break

        if status == JobStatus.SUSPENDED:
            waited = time.monotonic() - (suspended_since or time.monotonic())
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] still SUSPENDED — "
                f"waited {waited:.0f}s / {args.timeout}s ...",
                flush=True,
            )

        await asyncio.sleep(args.poll_interval)

    # Clean up background tasks
    for w in job_mgr._workers:
        w.cancel()
    if job_mgr._reaper_task:
        job_mgr._reaper_task.cancel()
    if job_mgr._batch_poller:
        job_mgr._batch_poller.stop()


# ── LLM-only mode ────────────────────────────────────────────────────────────

async def _run_llm_only(args: argparse.Namespace, source_job_id: str) -> None:
    """
    Load an existing checkpoint, compress items, save a fresh checkpoint at
    <source_job_id>-llm, then run only ad_diagnosis_llm via JobManager.
    The original checkpoint is never modified.
    """
    from src.jobs.checkpoint import CheckpointManager, WorkflowEvent
    from src.jobs.manager import JobManager, JobStatus
    from src.core.models.request import UnifiedRequest
    import src.workflows.definitions.ad_diagnosis  # noqa: F401

    checkpoint_mgr = CheckpointManager()

    # 1. Load source checkpoint
    source = checkpoint_mgr.load(source_job_id)
    if not source:
        logger.error(f"No checkpoint found for job_id={source_job_id}. Run data collection first.")
        return

    logger.info(
        f"Loaded checkpoint: step={source.step_index} ({source.step_name}), "
        f"{len(source.items)} items, {len(source.ctx_cache)} cache keys"
    )

    # 2. Compress items
    compressed_items = [_compress_item_for_llm(item) for item in source.items]
    compressed_json_size = len(json.dumps(compressed_items, ensure_ascii=False))
    estimated_tokens = compressed_json_size // 4
    logger.info(
        f"Compressed {len(source.items)} items: "
        f"{compressed_json_size:,} chars ≈ {estimated_tokens:,} tokens "
        f"(was {sum(len(json.dumps(i, ensure_ascii=False)) for i in source.items):,} chars)"
    )

    # 3. Save a new checkpoint at the source step_index with compressed items
    #    so the engine resumes at the next step (ad_diagnosis_llm).
    llm_job_id = f"{source_job_id}-llm"
    # Merge source params but force no_llm=False so ad_diagnosis_llm is not filtered out
    llm_params = {**(source.workflow_params or {}), "no_llm": False}

    checkpoint_mgr.clear(llm_job_id)
    checkpoint_mgr.save(
        job_id=llm_job_id,
        step_index=source.step_index,
        step_name=source.step_name,
        items=compressed_items,
        workflow_name=source.workflow_name or "ad_diagnosis",
        workflow_params=llm_params,
        ctx_cache=source.ctx_cache,
    )
    logger.info(f"New checkpoint saved: job_id={llm_job_id} (compressed items)")

    # 4. Submit to JobManager and poll
    config = _build_config(args)
    params = {
        "initial_items": compressed_items,
        **config,
    }
    request = UnifiedRequest(workflow_name="ad_diagnosis", params=params)

    job_mgr = JobManager(max_workers=1)
    job_mgr.resume_from_checkpoint(llm_job_id)
    logger.info(f"Job {llm_job_id} queued via resume_from_checkpoint")

    # ── Poll loop ─────────────────────────────────────────────────────────────
    deadline = time.monotonic() + args.timeout
    last_status = None
    suspended_since: float | None = None

    print(f"\n{'='*60}")
    print(f"  Polling {llm_job_id} every {args.poll_interval}s")
    print(f"{'='*60}")

    while True:
        record = job_mgr.get_status(llm_job_id)
        if record is None:
            logger.error(f"Job record not found for {llm_job_id}")
            break

        status = record.status

        if status != last_status:
            logger.info(f"Job status → {status.value.upper()}")

            if status == JobStatus.SUSPENDED:
                suspended_since = time.monotonic()
                cp = checkpoint_mgr.load(llm_job_id)
                batch_id = provider = None
                if cp:
                    for ev in reversed(cp.events):
                        if ev.event_type == "BATCH_SUBMITTED":
                            batch_id = ev.payload.get("handle", {}).get("job_id")
                            provider = ev.payload.get("handle", {}).get("provider", "").upper()
                            break
                print(
                    f"\n  ┌─ BATCH SUBMITTED ────────────────────────────────┐\n"
                    f"  │  provider : {provider or 'unknown':<38}│\n"
                    f"  │  batch_id : {(batch_id or 'unknown'):<38}│\n"
                    f"  │  BatchPoller polls every 60s — waiting...        │\n"
                    f"  └──────────────────────────────────────────────────┘\n"
                )

            last_status = status

        if status == JobStatus.COMPLETED:
            _print_result(record.result, [])
            break

        if status in (JobStatus.FAILED, JobStatus.CANCELLED):
            logger.error(f"Job ended with status={status.value}: {record.error or 'no detail'}")
            break

        if time.monotonic() > deadline:
            logger.error(
                f"Timed out after {args.timeout}s. Batch may still be running. "
                f"Re-run with: --llm-only --job-id {source_job_id}"
            )
            break

        if status == JobStatus.SUSPENDED and suspended_since:
            waited = time.monotonic() - suspended_since
            print(
                f"  [{datetime.now().strftime('%H:%M:%S')}] SUSPENDED — "
                f"waited {waited:.0f}s / {args.timeout}s ...",
                flush=True,
            )

        await asyncio.sleep(args.poll_interval)

    for w in job_mgr._workers:
        w.cancel()
    if job_mgr._reaper_task:
        job_mgr._reaper_task.cancel()
    if job_mgr._batch_poller:
        job_mgr._batch_poller.stop()


# ── Entry point ───────────────────────────────────────────────────────────────

async def run_test(args: argparse.Namespace) -> None:
    job_id = args.job_id or f"ad-diag-{args.asin}-dev"

    logger.info(
        f"\n{'='*60}\n"
        f"  ad_diagnosis workflow — live test\n"
        f"  ASIN      : {args.asin}\n"
        f"  Job ID    : {job_id}\n"
        f"  Store     : {args.store_id} / {args.region}\n"
        f"  Days      : {args.days}\n"
        f"  Xiyou     : {'enabled' if not args.no_xiyou else 'disabled'}\n"
        f"  LLM step  : {'enabled' if not args.no_llm else 'disabled'}\n"
        f"  Mode      : {'direct (workflow.execute)' if args.direct else 'JobManager + BatchPoller'}\n"
        f"{'='*60}"
    )

    if args.llm_only:
        await _run_llm_only(args, job_id)
    elif args.direct:
        await _run_direct(args, job_id)
    else:
        await _run_via_job_manager(args, job_id)


if __name__ == "__main__":
    asyncio.run(run_test(_parse_args()))
