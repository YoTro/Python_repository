"""
Live integration test for sellersprite_competing_lookup end-to-end flow.

Token + session cookies must be saved in config/auth/sellersprite_default_token.json.
If running from a datacenter IP (where the extension signin is blocked), inject
browser-captured cookies first:

    venv311/bin/python - <<'EOF'
    from src.mcp.servers.market.sellersprite.auth import SellerspriteAuth
    SellerspriteAuth().save_cookies(token="...", cookies={...})
    EOF

Run:
    venv311/bin/python tests/test_sellersprite_live.py
or via pytest:
    venv311/bin/python -m pytest tests/test_sellersprite_live.py -v -s
"""
from __future__ import annotations

import json
import sys
import os

_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _ROOT)

from dotenv import load_dotenv
load_dotenv(os.path.join(_ROOT, ".env"))

AMAZON_URL = "https://www.amazon.com/gp/bestsellers/industrial/8297518011/ref=pd_zg_hrsr_industrial"
MONTH      = "2025-06"
NODE_ID    = "8297518011"
MARKET     = "US"
MARKET_ID  = 1
TABLE      = "bsr_sales_monthly_202506"


def _divider(title: str) -> None:
    print(f"\n{'=' * 60}\n  {title}\n{'=' * 60}")


def _setup_logging() -> None:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(name)s  %(message)s")
    for lib in ("urllib3", "requests", "charset_normalizer"):
        logging.getLogger(lib).setLevel(logging.WARNING)


# ── Step 1: resolve node path ────────────────────────────────────────────────

def test_resolve_node_path() -> list[dict]:
    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
    api = SellerspriteAPI()

    _divider(f"resolve_node_path  node={NODE_ID}  table={TABLE}")
    nodes = api.resolve_node_path(market_id=MARKET_ID, table=TABLE, query=NODE_ID)

    print(f"  returned {len(nodes)} node(s)")
    for n in nodes[:3]:
        print(f"    id={n.get('id')}  label={n.get('label')}  products={n.get('products')}")

    assert nodes, "resolve_node_path returned empty — check token file"
    assert nodes[0].get("id"), "first node has no 'id' field"
    return nodes


# ── Step 2: competing lookup ─────────────────────────────────────────────────

def test_competing_lookup(node_id_path: str) -> dict:
    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
    api = SellerspriteAPI()

    _divider(f"get_competing_lookup  nodes=[{node_id_path}]  month={TABLE}")
    result = api.get_competing_lookup(
        market=MARKET,
        month_name=TABLE,
        node_id_paths=[node_id_path],
        page=1,
        size=10,
    )

    total = result.get("total", 0)
    items = result.get("items") or []
    print(f"  total={total}  returned={len(items)}")
    for i, item in enumerate(items[:5], 1):
        asin  = item.get("asin", "?")
        rank  = item.get("bsrRank") or item.get("rank") or item.get("rankingPosition", "?")
        brand = item.get("brandName") or item.get("brand", "?")
        price = item.get("price", "?")
        print(f"    [{i:02d}] ASIN={asin}  rank={rank}  brand={brand}  price=${price}")

    assert items, "get_competing_lookup returned no items"
    assert items[0].get("asin"), "first item has no 'asin' field"
    return result


# ── Step 3: MCP tool end-to-end ──────────────────────────────────────────────

def test_mcp_tool_end_to_end() -> None:
    import asyncio
    from src.mcp.servers.market.tools import handle_market_tool

    _divider(f"MCP tool: sellersprite_competing_lookup  month={MONTH}")
    result_contents = asyncio.run(handle_market_tool(
        "sellersprite_competing_lookup",
        {
            "amazon_url": AMAZON_URL,
            "month_name": MONTH,
            "market": "us",   # intentionally lowercase — tests .upper() normalisation
        },
    ))

    assert result_contents, "MCP tool returned empty list"
    payload = json.loads(result_contents[0].text)

    print(f"  snapshot              = {payload.get('snapshot')}")
    print(f"  today                 = {payload.get('today')}")
    print(f"  latest_available      = {payload.get('latest_available_snapshot')}")
    print(f"  total                 = {payload.get('total')}")
    print(f"  returned              = {payload.get('returned')}")

    if "error" in payload:
        raise AssertionError(f"MCP tool returned error: {payload['error']}")

    items = payload.get("items") or []
    print(f"\n  First 5 slim items:")
    for i, item in enumerate(items[:5], 1):
        print(f"    [{i:02d}] {item}")

    assert payload.get("snapshot") == "bsr_sales_monthly_202506"
    assert items, "MCP tool returned no items"


# ── Runner ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _setup_logging()

    nodes        = test_resolve_node_path()
    node_id_path = nodes[0]["id"]

    test_competing_lookup(node_id_path)
    test_mcp_tool_end_to_end()

    _divider("ALL STEPS PASSED")
