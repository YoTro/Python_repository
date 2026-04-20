"""
Live integration test for the ad_diagnosis workflow.
Fetches real data for ASIN B0FXFGMD7Z and runs the full diagnostic pipeline.

Checkpoint support:
  Each step is checkpointed after completion. Re-running with the same --job-id
  resumes from the last completed step — skipping expensive API report fetches.

Usage:
    # First run (or fresh run):
    venv311/bin/python3 tests/test_ad_diagnosis_live.py

    # Resume from last checkpoint (same job-id):
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id ad-diag-B0FXFGMD7Z-dev

    # Reset checkpoint and start over:
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id ad-diag-B0FXFGMD7Z-dev --reset

    # Skip LLM to validate data only:
    venv311/bin/python3 tests/test_ad_diagnosis_live.py --job-id ad-diag-B0FXFGMD7Z-dev --no-llm
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import os
import argparse
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
    return p.parse_args()


async def run_test(args: argparse.Namespace) -> None:
    from src.workflows.registry import WorkflowRegistry
    from src.workflows.steps.base import WorkflowContext
    from src.jobs.checkpoint import CheckpointManager
    from src.intelligence.router import IntelligenceRouter
    import src.workflows.definitions.ad_diagnosis  # noqa: F401 — registers the workflow

    job_id = args.job_id or f"ad-diag-{args.asin}-dev"

    checkpoint_mgr = CheckpointManager()

    if args.reset:
        checkpoint_mgr.clear(job_id)
        logger.info(f"Checkpoint cleared for job_id={job_id}")

    # Show existing checkpoint state before running
    existing = checkpoint_mgr.load(job_id)
    if existing:
        logger.info(
            f"Resuming from checkpoint: last completed step [{existing.step_index}] "
            f"'{existing.step_name}', {len(existing.items)} items, "
            f"{len(existing.ctx_cache)} cache keys"
        )
    else:
        logger.info(f"No checkpoint found for job_id={job_id}, starting fresh")

    config = {
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

    router = IntelligenceRouter()

    ctx = WorkflowContext(
        job_id=job_id,
        tenant_id="default",
        config=config,
        router=router,
    )

    initial_items = [{"asin": args.asin}]
    params = {"initial_items": initial_items}

    workflow = WorkflowRegistry.build("ad_diagnosis", config)

    if args.no_llm:
        workflow.steps = [s for s in workflow.steps if s.name != "ad_diagnosis_llm"]
        logger.info("LLM step disabled — running data-collection steps only")

    logger.info(
        f"\n{'='*60}\n"
        f"  ad_diagnosis workflow — live test\n"
        f"  ASIN      : {args.asin}\n"
        f"  Job ID    : {job_id}\n"
        f"  Store     : {args.store_id} / {args.region}\n"
        f"  Days      : {args.days}\n"
        f"  Xiyou     : {'enabled' if not args.no_xiyou else 'disabled'}\n"
        f"  LLM step  : {'enabled' if not args.no_llm else 'disabled'}\n"
        f"{'='*60}"
    )

    result = await workflow.execute(
        job_id=job_id,
        params=params,
        ctx=ctx,
        checkpoint_mgr=checkpoint_mgr,
    )

    # Keep checkpoint on success in dev mode so LLM-only reruns don't re-fetch reports.
    # The engine normally clears it; we re-save the final state instead.
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
        }

        for k, v in highlights.items():
            print(f"  {k:<30} {json.dumps(v, ensure_ascii=False)}")

        llm_output = item.get("ad_diagnosis_llm") or item.get("llm_output") or item.get("diagnosis")
        if llm_output:
            print(f"\n── LLM Diagnosis ──\n{llm_output}\n")

    out_path = f"/tmp/ad_diagnosis_{args.asin}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result.final_items, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nFull result saved to: {out_path}")


if __name__ == "__main__":
    asyncio.run(run_test(_parse_args()))
