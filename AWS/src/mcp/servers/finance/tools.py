import json
import logging
import os
from typing import Dict, Any, Optional
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.core.data_cache import data_cache

logger = logging.getLogger("mcp-finance")

# Load fee data
BASE_DIR = os.path.dirname(__file__)
FBA_FEE_PATH          = os.path.join(BASE_DIR, "fba_fee.json")
REFERRAL_FEE_PATH     = os.path.join(BASE_DIR, "referral_fee_rates.json")
CATEGORY_METRICS_PATH = os.path.join(BASE_DIR, "us_category_metrics.json")

def load_json_config(path: str) -> Dict[str, Any]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config {path}: {e}")
    return {}

FBA_CONFIG      = load_json_config(FBA_FEE_PATH)
REFERRAL_CONFIG = load_json_config(REFERRAL_FEE_PATH)
_CATEGORY_METRICS = load_json_config(CATEGORY_METRICS_PATH).get("categories", {})

def get_referral_rate(category: str, price: float) -> float:
    """Find the referral fee rate for a category and price."""
    fees = REFERRAL_CONFIG.get("referral_fees", [])
    default_rate = 0.15
    for item in fees:
        if item["category"].lower() in category.lower() or category.lower() in item["category"].lower():
            tiers = item.get("tiers", [])
            if not tiers: return default_rate
            for tier in tiers:
                if tier["price_range"] == "All prices": return tier["rate_pct"] / 100.0
                if "≤" in tier["price_range"]:
                    limit = float(tier["price_range"].replace("≤", "").replace("$", "").strip())
                    if price <= limit: return tier["rate_pct"] / 100.0
                if ">" in tier["price_range"]:
                    limit = float(tier["price_range"].replace(">", "").replace("$", "").strip())
                    if price > limit: return tier["rate_pct"] / 100.0
            return tiers[0]["rate_pct"] / 100.0
    return default_rate

def _cat_metrics_from_items(c: dict) -> Dict[str, Any]:
    """Compute avg return_rate and search_to_buy_ratio from subcategory items."""
    items = c.get("items", [])
    rr_vals  = [i["return_rate_pct"]        for i in items if isinstance(i.get("return_rate_pct"),        (int, float))]
    stb_vals = [i["search_to_buy_ratio_pm"] for i in items if isinstance(i.get("search_to_buy_ratio_pm"), (int, float))]
    return {
        "label":                c["label"],
        "avg_return_rate_pct":  round(sum(rr_vals)  / len(rr_vals),  2) if rr_vals  else None,
        "avg_search_to_buy_pm": round(sum(stb_vals) / len(stb_vals), 2) if stb_vals else None,
    }


def get_category_metrics(node_id: str = None, category: str = None) -> Dict[str, Any]:
    """
    Return avg return_rate (%) and avg search_to_buy_ratio (‰) for a top-level category.

    Resolution order:
      1. Exact node_id match against us_category_metrics.json top-level keys
      2. node_id from referral_fee_rates.json browse_node_hint (via category label)
      3. Partial label match against us_category_metrics.json labels
    Returns dict with keys avg_return_rate_pct, avg_search_to_buy_pm, label (or empty dict).
    """
    # 1. Direct node_id lookup
    if node_id and node_id in _CATEGORY_METRICS:
        return _cat_metrics_from_items(_CATEGORY_METRICS[node_id])

    # 2. Resolve node_id via referral_fee browse_node_hint
    if category:
        cat_lower = category.lower()
        for fee_item in REFERRAL_CONFIG.get("referral_fees", []):
            if fee_item.get("node_id") and (
                fee_item["category"].lower() in cat_lower or cat_lower in fee_item["category"].lower()
            ):
                resolved = fee_item["node_id"]
                if resolved in _CATEGORY_METRICS:
                    return _cat_metrics_from_items(_CATEGORY_METRICS[resolved])

        # 3. Partial label match in metrics
        for c in _CATEGORY_METRICS.values():
            lbl = c["label"].lower()
            if lbl in cat_lower or cat_lower in lbl:
                return _cat_metrics_from_items(c)

    return {}


def estimate_fba_fee_from_dims(weight_lb: float, is_apparel: bool = False) -> float:
    section = "apparel" if is_apparel else "standard_non_apparel"
    tiers = FBA_CONFIG.get("fba_fulfillment_fees", {}).get(section, {}).get("tiers", [])
    if not tiers: return 4.50
    weight_oz = weight_lb * 16.0
    for tier in tiers:
        if "Small Standard" in tier.get("size_tier", ""):
            if weight_oz <= 16: return tier.get("fee_usd", 3.11)
        if "Large Standard" in tier.get("size_tier", ""):
            if weight_lb <= 1.0: return 4.20
            if weight_lb <= 2.0: return 5.50
            if weight_lb <= 3.0: return 6.50
    return 7.00

def calculate_amazon_refund_admin_fee(referral_fee: float) -> float:
    """Calculates standard refund administration fee: lesser of $5 or 20% of referral fee."""
    return round(min(5.0, 0.20 * referral_fee), 2)

def calculate_high_return_rate_fee(category: str, weight_lb: float, return_rate: float) -> float:
    """
    Calculates additional fee for returns exceeding category threshold.
    Returns estimated fee per TOTAL sold unit (spread across all sales).
    """
    hrr_config = FBA_CONFIG.get("high_return_rate_fees", {})
    thresholds = hrr_config.get("thresholds", {})
    
    # 1. Get threshold (default to 10% if category not matched)
    category_matched = next((k for k in thresholds if k.lower() in category.lower()), None)
    threshold = thresholds.get(category_matched, 10.0) / 100.0
    
    # 2. Check if fee applies
    is_apparel = "clothing" in category.lower() or "shoe" in category.lower()
    if not is_apparel and return_rate <= threshold:
        return 0.0

    # 3. Determine Rate Card
    section = "apparel_and_shoes" if is_apparel else "other_products"
    rate_tiers = hrr_config.get("rate_cards", {}).get(section, {}).get("tiers", [])
    
    # 4. Find Tier Fee
    per_return_fee = 2.50 # Fallback
    weight_oz = weight_lb * 16.0
    for tier in rate_tiers:
        if "Small standard" in tier.get("size_tier", "") and weight_oz <= 16:
            per_return_fee = tier.get("fee_usd", 2.00)
            break
        if "Large standard" in tier.get("size_tier", "") and weight_lb <= 3.0:
            per_return_fee = tier.get("fee_usd", 3.50)
            break

    # 5. Calculate impact per sold unit
    if is_apparel:
        # Every return is charged
        return round(per_return_fee * return_rate, 2)
    else:
        # Only returns ABOVE threshold are charged
        excess_rate = max(0, return_rate - threshold)
        return round(per_return_fee * excess_rate, 2)

async def handle_finance_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "calc_profit":
        asin = arguments.get("asin")
        cost = arguments.get("estimated_cost", 0)

        product_data = data_cache.get("amazon", asin) or {}
        price    = float(arguments.get("current_price") or product_data.get("price") or 0)
        category = arguments.get("category") or product_data.get("category", "")
        weight   = product_data.get("weight_lb") or product_data.get("weight", 1.0)

        if price <= 0:
            return [TextContent(type="text", text=json.dumps({"error": "Price not found. Pass current_price or call get_product_details first.", "asin": asin}))]

        # Resolve category-level benchmarks from us_category_metrics.json
        # Prefer top_level_node_id from cache (set by get_bsr_rank) for precise lookup
        node_id = product_data.get("top_level_node_id")
        cat_metrics = get_category_metrics(node_id=node_id, category=category)
        category_avg_return_pct = cat_metrics.get("avg_return_rate_pct")  # e.g. 4.2
        category_avg_stb_pm     = cat_metrics.get("avg_search_to_buy_pm") # e.g. 18.5 ‰

        # Use caller-supplied return_rate; fall back to category avg, then 5%
        if "return_rate" in arguments:
            est_return_rate = float(arguments["return_rate"])
        elif category_avg_return_pct is not None:
            est_return_rate = category_avg_return_pct / 100.0
        else:
            est_return_rate = 0.05

        # Base Fees
        ref_rate = get_referral_rate(category, price)
        referral_fee = price * ref_rate
        fba_fee = estimate_fba_fee_from_dims(weight if isinstance(weight, (int, float)) else 1.0)

        # Return-related Fees
        admin_fee_per_return = calculate_amazon_refund_admin_fee(referral_fee)
        high_return_fee_per_unit = calculate_high_return_rate_fee(category, weight, est_return_rate)

        total_fees = referral_fee + fba_fee + (est_return_rate * admin_fee_per_return) + high_return_fee_per_unit
        net_profit = price - cost - total_fees

        result: Dict[str, Any] = {
            "asin": asin,
            "price": price,
            "cost": cost,
            "return_rate": f"{est_return_rate:.1%}",
            "fee_breakdown": {
                "referral_fee": round(referral_fee, 2),
                "fba_fulfillment_fee": round(fba_fee, 2),
                "refund_admin_fee_impact": round(est_return_rate * admin_fee_per_return, 2),
                "high_return_rate_fee_impact": round(high_return_fee_per_unit, 2),
            },
            "profitability": {
                "net_profit": round(net_profit, 2),
                "margin": round(net_profit / price, 4),
                "roi": round(net_profit / cost, 4) if cost > 0 else 0,
            },
        }
        if cat_metrics:
            result["category_benchmarks"] = {
                "matched_category": cat_metrics.get("label"),
                "avg_return_rate_pct": category_avg_return_pct,
                "avg_search_to_buy_pm": category_avg_stb_pm,
                "return_rate_source": "category_avg" if "return_rate" not in arguments else "caller_supplied",
            }
        return _json_response(result)

    elif name == "calc_fba_fee":
        asin = arguments.get("asin")
        weight = arguments.get("weight_lb")
        if asin and not weight:
            product_data = data_cache.get("amazon", asin) or {}
            weight = product_data.get("weight_lb") or product_data.get("weight", 1.0)
        fee = estimate_fba_fee_from_dims(float(weight) if weight else 1.0)
        return _json_response({"asin": asin, "fba_fee": fee})

    return [TextContent(type="text", text=f"Unknown tool: {name}")]

def _json_response(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False))]

finance_tools = [
    Tool(
        name="calc_profit",
        description=(
            "Comprehensive Amazon profit analysis. Requires get_product_details or get_bsr_rank "
            "to be called first so price, category, and top_level_node_id are in DataCache. "
            "Returns: asin, price, cost, return_rate, "
            "fee_breakdown {referral_fee, fba_fulfillment_fee, refund_admin_fee_impact, high_return_rate_fee_impact}, "
            "profitability {net_profit, margin, roi}, "
            "category_benchmarks {matched_category, avg_return_rate_pct (%), avg_search_to_buy_pm (‰), return_rate_source}. "
            "Return rate resolves in order: caller-supplied → category average (us_category_metrics.json) → 5% fallback."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin":           {"type": "string", "description": "Product ASIN"},
                "estimated_cost": {"type": "number", "description": "COGS in USD (landed cost per unit)"},
                "current_price":  {"type": "number", "description": "Selling price (USD). Falls back to DataCache if omitted."},
                "category":       {"type": "string", "description": "Amazon category name (e.g. 'Tools & Home Improvement'). Falls back to DataCache if omitted."},
                "return_rate":    {"type": "number", "description": "Estimated return rate fraction (e.g. 0.05 for 5%). Defaults to category average from us_category_metrics.json."},
            },
            "required": ["asin", "estimated_cost"]
        }
    ),
    Tool(
        name="calc_fba_fee",
        description=(
            "Estimate the FBA fulfillment fee from product weight. "
            "Returns: {asin, fba_fee (USD)}. "
            "Weight is read from DataCache if asin is provided and weight_lb is omitted. "
            "Uses size-tier logic: Small Standard (≤16 oz) ~$3.11, Large Standard scales with weight."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin":      {"type": "string", "description": "Product ASIN (used to look up weight from DataCache)"},
                "weight_lb": {"type": "number", "description": "Product weight in pounds. Falls back to DataCache if omitted."}
            }
        }
    )
]

for tool in finance_tools:
    tool_registry.register_tool(tool, handle_finance_tool, category="COMPUTE", returns="JSON report")
