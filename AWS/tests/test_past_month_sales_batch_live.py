"""
Live probe: batch vs single-product past-month-sales fetching.

Steps:
  1. Fetch top-100 BSR ASINs from a real category page.
  2. Run get_batch_sales() in chunks of 10 (10 HTTP requests for 100 ASINs).
  3. For every ASIN that returned None in batch, fall back to single-product fetch.
  4. Print hit-rate summary.

Run:
    python3 tests/test_past_month_sales_batch_live.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_ROOT, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
for lib in ("urllib3", "requests", "charset_normalizer", "hpack", "httpx"):
    logging.getLogger(lib).setLevel(logging.WARNING)

logger = logging.getLogger("probe")

BSR_URL = "https://www.amazon.com/gp/bestsellers/kitchen/ref=zg_bs_nav_kitchen_0"
BATCH_SIZE = 10


def _divider(title: str) -> None:
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


# ── Step 1: collect ASINs from BSR ──────────────────────────────────────────

async def collect_asins(max_asins: int = 100) -> list[str]:
    from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
    _divider(f"Step 1 — fetch BSR ASINs (target={max_asins})")
    products = await BestSellersExtractor().get_bestsellers(BSR_URL, max_pages=2)
    asins = [
        (p.get("ASIN") or p.get("asin") or "").strip().upper()
        for p in products
        if (p.get("ASIN") or p.get("asin"))
    ][:max_asins]
    print(f"  collected {len(asins)} ASINs")
    print(f"  sample: {asins[:5]}")
    return asins


# ── Step 2: batch fetch ──────────────────────────────────────────────────────

async def run_batch(asins: list[str]) -> dict[str, int | None]:
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    _divider(f"Step 2 — batch fetch ({len(asins)} ASINs, chunk={BATCH_SIZE})")
    extractor = PastMonthSalesExtractor()
    t0 = time.perf_counter()
    results = await extractor.get_batch_sales(asins)
    elapsed = time.perf_counter() - t0

    hits   = {k: v for k, v in results.items() if v is not None}
    misses = [k for k, v in results.items() if v is None]
    print(f"  elapsed : {elapsed:.1f}s")
    print(f"  hit     : {len(hits)}/{len(asins)}  ({len(hits)/len(asins):.0%})")
    print(f"  miss    : {len(misses)}")
    print(f"\n  Top hits:")
    for asin, sales in sorted(hits.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"    {asin}  {sales:,}")
    if misses:
        print(f"\n  Sample misses: {misses[:10]}")
    return results


# ── Step 3: single-product fallback for misses ───────────────────────────────

async def run_fallback(misses: list[str], sample: int = 5) -> dict[str, int | None]:
    from src.mcp.servers.amazon.extractors.past_month_sales import PastMonthSalesExtractor
    if not misses:
        _divider("Step 3 — fallback: no misses, skipping")
        return {}

    probe = misses[:sample]
    _divider(f"Step 3 — single-product fallback (probing {len(probe)} of {len(misses)} misses)")
    extractor = PastMonthSalesExtractor()
    fallback: dict[str, int | None] = {}
    for asin in probe:
        batch = await extractor.get_batch_past_month_sales([asin])
        fallback[asin] = batch.get(asin.upper())
        status = f"{fallback[asin]:,}" if fallback[asin] is not None else "None"
        print(f"  {asin}  ->  {status}")

    recovered = sum(1 for v in fallback.values() if v is not None)
    print(f"\n  Recovered {recovered}/{len(probe)} via fallback")
    return fallback


# ── Runner ───────────────────────────────────────────────────────────────────

async def main() -> None:
    asins = await collect_asins(max_asins=100)
    if not asins:
        print("ERROR: no ASINs collected — check BSR URL or cookies")
        return

    batch_results = await run_batch(asins)
    misses = [k for k, v in batch_results.items() if v is None]
    await run_fallback(misses, sample=5)

    _divider("Summary")
    hits = sum(1 for v in batch_results.values() if v is not None)
    print(f"  Batch hit rate : {hits}/{len(asins)} ({hits/len(asins):.0%})")
    print(f"  Misses         : {len(misses)}")
    print(
        "  Note: misses may be products without 'bought in past month' badge "
        "(low-volume / new listings)"
    )


if __name__ == "__main__":
    asyncio.run(main())
