"""
Probe: find the optimal batch size for get_batch_past_month_sales.

Uses the same 100 Kitchen BSR ASINs for each batch size so results are comparable.
Measures hit rate and total elapsed time.

Run:
    python3 tests/test_past_month_sales_batch_size.py
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

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s %(message)s")

BSR_URL = "https://www.amazon.com/gp/bestsellers/kitchen/ref=zg_bs_nav_kitchen_0"
BATCH_SIZES = [10, 20, 50, 100]


async def collect_asins(max_asins: int = 100) -> list[str]:
    from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
    products = await BestSellersExtractor().get_bestsellers(BSR_URL, max_pages=2)
    return [
        (p.get("ASIN") or p.get("asin") or "").strip().upper()
        for p in products
        if (p.get("ASIN") or p.get("asin"))
    ][:max_asins]


async def run_with_batch_size(asins: list[str], batch_size: int) -> tuple[int, float]:
    """Returns (hit_count, elapsed_seconds)."""
    from src.mcp.servers.amazon.extractors.past_month_sales import (
        PastMonthSalesExtractor,
        _parse_search_page,
    )
    from bs4 import BeautifulSoup

    extractor = PastMonthSalesExtractor()
    results = {a.upper(): None for a in asins}
    t0 = time.perf_counter()

    for i in range(0, len(asins), batch_size):
        chunk = [a.upper() for a in asins[i : i + batch_size]]
        query = "%7C+".join(chunk)
        url = f"https://www.amazon.com/s/?k={query}&ref=nb_sb_noss"
        html = await extractor.fetch(url)
        if html:
            _parse_search_page(BeautifulSoup(html, "html.parser"), results)

    elapsed = time.perf_counter() - t0
    hits = sum(1 for v in results.values() if v is not None)
    return hits, elapsed


async def main() -> None:
    print("Collecting 100 ASINs from Kitchen BSR...")
    asins = await collect_asins(100)
    print(f"Got {len(asins)} ASINs\n")

    print(f"{'Batch':>6}  {'Requests':>9}  {'Hits':>8}  {'Hit%':>6}  {'Time(s)':>8}")
    print("-" * 48)

    for size in BATCH_SIZES:
        n_requests = -(-len(asins) // size)  # ceil division
        hits, elapsed = await run_with_batch_size(asins, size)
        pct = hits / len(asins) * 100
        print(f"{size:>6}  {n_requests:>9}  {hits:>8}  {pct:>5.0f}%  {elapsed:>8.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
