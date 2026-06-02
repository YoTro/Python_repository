"""
Global pytest configuration.

Markers
-------
live    Requires live external API credentials or network access.
        Auto-applied to *_live.py files and explicitly named files below.
        Skip with: pytest -m "not live"

redis   Requires a running Redis instance (REDIS_URL env var).
        Auto-applied to *redis*.py files and explicitly named files below.
        Skip with: pytest -m "not redis"

Run only live tests:  pytest -m live
Run only redis tests: pytest -m redis
CI-safe subset:       pytest -m "not live and not redis"
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Script-style files — run directly (python3 tests/<file>), not via pytest.
# They execute Redis/API calls at module level and call sys.exit(), which
# causes an INTERNALERROR during collection. Exclude them permanently.
# ---------------------------------------------------------------------------
collect_ignore: list[str] = [
    "test_beta_cvr_redis.py",    # module-level Redis reads + sys.exit
    "test_inventory_gate.py",    # module-level Redis reads + sys.exit
    "test_summary_snapshot.py",  # imports removed private symbol _build_kw_to_campaign_map
]


# ---------------------------------------------------------------------------
# Files that need external services but don't follow *_live.py / *redis* naming
# ---------------------------------------------------------------------------

# live marker: needs credentials or real network calls
_LIVE_FILES: frozenset[str] = frozenset({
    "test_amazon_ads_full_bids.py",       # Amazon Ads API credentials
    "test_erp_sp_campaign_ad_report.py",  # Lingxing ERP credentials
    "test_feishu_upload_actual.py",       # Feishu credentials + real upload
    "test_generate_charts_upload.py",     # R2 upload + real item data
    "test_comments_with_login.py",        # browser login flow
    "test_deal_history_debug.py",         # live Amazon scraping
    "test_xiyou_daily_cycle.py",          # Xiyouzhaoci live session
    "test_profitability_search.py",       # live Amazon scraping
})

# redis marker: needs a Redis server with real cached data
_REDIS_FILES: frozenset[str] = frozenset({
    "test_summary_snapshot.py",   # reads from aws:cache:ad_diag:* keys
    "test_inventory_gate.py",     # reads campaign/kw_perf from Redis
})


# ---------------------------------------------------------------------------
# Auto-marker hook
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        filename = item.path.name if hasattr(item, "path") else ""

        if filename.endswith("_live.py") or filename in _LIVE_FILES:
            item.add_marker(pytest.mark.live)

        if "redis" in filename or filename in _REDIS_FILES:
            item.add_marker(pytest.mark.redis)
