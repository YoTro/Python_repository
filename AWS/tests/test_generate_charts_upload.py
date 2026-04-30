"""
Integration test for _generate_charts step.
Loads real item data from ad-diag-B0FXFGMD7Z-dev.json, runs chart generation,
and uploads to R2. Prints chart_urls on success.
"""
import json
import logging
import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

DEV_JSON = os.path.join(os.path.dirname(__file__), "../../ad-diag-B0FXFGMD7Z-dev.json")


class _FakeCache(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class _FakeCtx:
    def __init__(self, cache_dict):
        self.cache = _FakeCache(cache_dict)
        self.config = {}


def main():
    with open(DEV_JSON, encoding="utf-8") as f:
        data = json.load(f)

    items = data["items"]
    ctx_cache = data.get("ctx_cache", {})

    # _generate_charts looks up "ad_diag:daily_performance:{ASIN}" but dev.json
    # stores the aggregated list under "ad_diag:daily_performance" (no suffix).
    # Mirror it under the per-ASIN key so the cache hit works.
    for item in items:
        asin = (item.get("asin") or "").upper()
        base_key = "ad_diag:daily_performance"
        if base_key in ctx_cache:
            ctx_cache[f"{base_key}:{asin}"] = ctx_cache[base_key]

    ctx = _FakeCtx(ctx_cache)

    from src.workflows.definitions.ad_diagnosis import _generate_charts

    result_items = _generate_charts(items, ctx)

    for item in result_items:
        asin = item.get("asin", "?")
        chart_urls = item.get("chart_urls", {})
        print(f"\n=== {asin}: {len(chart_urls)}/6 charts ===")
        for name, url in chart_urls.items():
            print(f"  {name}: {url}")
        if not chart_urls:
            print("  (no charts generated — check logs above for reasons)")


if __name__ == "__main__":
    main()
