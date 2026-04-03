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
FBA_FEE_PATH = os.path.join(BASE_DIR, "fba_fee.json")
REFERRAL_FEE_PATH = os.path.join(BASE_DIR, "referral_fee_rates.json")

def load_json_config(path: str) -> Dict[str, Any]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config {path}: {e}")
    return {}

FBA_CONFIG = load_json_config(FBA_FEE_PATH)
REFERRAL_CONFIG = load_json_config(REFERRAL_FEE_PATH)

def get_referral_rate(category: str, price: float) -> float:
    """Find the referral fee rate for a category and price."""
    fees = REFERRAL_CONFIG.get("referral_fees", [])
    # Default to 15% if no match
    default_rate = 0.15
    
    for item in fees:
        if item["category"].lower() in category.lower() or category.lower() in item["category"].lower():
            tiers = item.get("tiers", [])
            if not tiers:
                return default_rate
            
            # Simple matching for price-based tiers
            # (In a real scenario, we'd parse the price_range string properly)
            for tier in tiers:
                if tier["price_range"] == "All prices":
                    return tier["rate_pct"] / 100.0
                if "≤" in tier["price_range"]:
                    limit = float(tier["price_range"].replace("≤", "").replace("$", "").strip())
                    if price <= limit:
                        return tier["rate_pct"] / 100.0
                if ">" in tier["price_range"]:
                    limit = float(tier["price_range"].replace(">", "").replace("$", "").strip())
                    if price > limit:
                        return tier["rate_pct"] / 100.0
            
            # Fallback to the first tier's rate if provided
            return tiers[0]["rate_pct"] / 100.0
            
    return default_rate

def estimate_fba_fee_from_dims(weight_lb: float, is_apparel: bool = False) -> float:
    """
    Very simplified FBA fee estimator based on weight.
    Real Amazon logic uses dimensions + weight to determine size tier first.
    """
    section = "apparel" if is_apparel else "standard_non_apparel"
    tiers = FBA_CONFIG.get("fba_fulfillment_fees", {}).get(section, {}).get("tiers", [])
    
    if not tiers:
        return 4.50 # Generic fallback
    
    # Weight in oz for small tiers
    weight_oz = weight_lb * 16.0
    
    for tier in tiers:
        if "Small Standard" in tier.get("size_tier", ""):
            range_str = tier.get("weight_range", "")
            if "oz" in range_str:
                # Logic to parse "2+ to 4 oz" etc
                # Simplified: just return the fee if weight is low
                if weight_oz <= 16:
                    return tier.get("fee_usd", 3.50)
        
        if "Large Standard" in tier.get("size_tier", ""):
            if weight_lb <= 1.0:
                return 4.20
            if weight_lb <= 2.0:
                return 5.50
            if weight_lb <= 3.0:
                return 6.50
                
    return 7.00 # Bulky/Default

async def handle_finance_tool(name: str, arguments: dict) -> list[TextContent]:
    """Dispatcher for finance and profitability tools."""
    
    if name == "calc_referral_fee":
        asin = arguments.get("asin")
        price = arguments.get("price")
        category = arguments.get("category", "")
        
        # Try to enrich from cache
        if asin and not price:
            product_data = data_cache.get("amazon", asin) or {}
            price = product_data.get("price", 0)
        if asin and not category:
            product_data = data_cache.get("amazon", asin) or {}
            category = product_data.get("category", "")
            
        if not price or price <= 0:
            return [TextContent(type="text", text=json.dumps({"error": "Price required", "asin": asin}))]
            
        rate = get_referral_rate(category, price)
        fee = price * rate
        
        return _json_response({
            "asin": asin,
            "category": category,
            "price": price,
            "referral_fee_rate": rate,
            "referral_fee_amount": round(fee, 2)
        })

    elif name == "calc_fba_fee":
        asin = arguments.get("asin")
        weight = arguments.get("weight_lb")
        
        # Try to enrich from cache
        if asin and not weight:
            product_data = data_cache.get("amazon", asin) or {}
            # Dimensions extractor might store 'weight_lb' or similar
            weight = product_data.get("weight_lb") or product_data.get("weight", 1.0)
            if isinstance(weight, str):
                # Simple extraction if it's like "1.2 pounds"
                try: weight = float(weight.split()[0])
                except: weight = 1.0
        
        fee = estimate_fba_fee_from_dims(weight or 1.0)
        
        return _json_response({
            "asin": asin,
            "weight_lb": weight,
            "fba_fee": fee,
            "status": "estimated_from_weight"
        })

    elif name == "calc_profit":
        asin = arguments.get("asin")
        cost = arguments.get("estimated_cost", 0)
        
        product_data = data_cache.get("amazon", asin) or {}
        price = product_data.get("price", 0)
        category = product_data.get("category", "")
        weight = product_data.get("weight_lb") or product_data.get("weight", 1.0)
        
        if price <= 0:
            return [TextContent(type="text", text=json.dumps({"error": "Price not found in cache for ASIN. Please provide price manually or search product first.", "asin": asin}))]

        # Component calculations
        ref_rate = get_referral_rate(category, price)
        referral_fee = price * ref_rate
        fba_fee = estimate_fba_fee_from_dims(weight if isinstance(weight, (int, float)) else 1.0)
        
        net_profit = price - cost - referral_fee - fba_fee
        margin = net_profit / price if price > 0 else 0
        roi = net_profit / cost if cost > 0 else 0
        
        return _json_response({
            "asin": asin,
            "price": price,
            "cost": cost,
            "fees": {
                "referral_fee": round(referral_fee, 2),
                "fba_fee": round(fba_fee, 2),
                "total_fees": round(referral_fee + fba_fee, 2)
            },
            "profitability": {
                "net_profit": round(net_profit, 2),
                "margin": round(margin, 4),
                "roi": round(roi, 4)
            }
        })

    elif name == "estimate_cost":
        # Placeholder for more complex logic (e.g. 25% of price)
        asin = arguments.get("asin")
        product_data = data_cache.get("amazon", asin) or {}
        price = product_data.get("price", 40.0)
        return _json_response({"asin": asin, "estimated_cost": round(price * 0.25, 2), "method": "percentage_of_price"})

    return [TextContent(type="text", text=f"Unknown tool: {name}")]

def _to_serializable(data):
    if hasattr(data, "model_dump"): return data.model_dump()
    return data

def _json_response(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False))]

finance_tools = [
    Tool(
        name="calc_profit",
        description="Comprehensive profit analysis including referral fees, FBA fees, and COGS.",
        inputSchema={
            "type": "object", 
            "properties": {
                "asin": {"type": "string", "description": "ASIN of the product"}, 
                "estimated_cost": {"type": "number", "description": "Manufacturing and shipping cost (COGS)"}
            }, 
            "required": ["asin", "estimated_cost"]
        }
    ),
    Tool(
        name="calc_referral_fee",
        description="Calculate Amazon referral fee (commission) based on category and price.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string"},
                "price": {"type": "number"},
                "category": {"type": "string"}
            }
        }
    ),
    Tool(
        name="calc_fba_fee",
        description="Calculate FBA fulfillment fee based on product weight and size.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string"},
                "weight_lb": {"type": "number"}
            }
        }
    ),
    Tool(
        name="estimate_cost",
        description="Estimate the manufacturing cost of a product based on market price.",
        inputSchema={"type": "object", "properties": {"asin": {"type": "string"}}, "required": ["asin"]}
    )
]

_FINANCE_META = {
    "calc_profit": ("COMPUTE", "detailed profitability report"),
    "calc_referral_fee": ("COMPUTE", "commission amount and rate"),
    "calc_fba_fee": ("COMPUTE", "FBA fulfillment fee"),
    "estimate_cost": ("COMPUTE", "estimated COGS"),
}

for tool in finance_tools:
    cat, ret = _FINANCE_META.get(tool.name, ("COMPUTE", ""))
    tool_registry.register_tool(tool, handle_finance_tool, category=cat, returns=ret)
