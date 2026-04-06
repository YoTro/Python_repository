from __future__ import annotations
import json
import logging
import os
from typing import Dict, Any, Optional, List
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.core.data_cache import data_cache
from src.core.utils.context import ContextPropagator

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

def _to_serializable(data: Any) -> Any:
    """Convert Pydantic models, dataclasses, or plain dicts to JSON-safe structures."""
    if hasattr(data, "model_dump"):
        return data.model_dump()
    if hasattr(data, "__dataclass_fields__"):
        from dataclasses import asdict
        return asdict(data)
    if isinstance(data, list):
        return [_to_serializable(item) for item in data]
    if isinstance(data, dict):
        return {k: _to_serializable(v) for k, v in data.items()}
    return data

def _json_response(data: Any) -> List[TextContent]:
    """Serialize data to JSON TextContent."""
    return [TextContent(type="text", text=json.dumps(_to_serializable(data), indent=2, ensure_ascii=False, default=str))]

def get_referral_rate(category: str, price: float) -> float:
    """Find the referral fee rate for a category and price."""
    fees = REFERRAL_CONFIG.get("referral_fees", [])
    # Default to 15% if no match
    default_rate = 0.15
    
    for item in fees:
        cat_name = item["category"].lower()
        if category and (cat_name in category.lower() or category.lower() in cat_name):
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

    # Implicit ASIN resolution from context if not provided
    asin = arguments.get("asin") or ContextPropagator.get("asin")
    
    if name == "calc_referral_fee":
        price = arguments.get("price")
        category = arguments.get("category", "")
        
        # Try to enrich from cache
        if asin:
            product_data = data_cache.get("amazon", asin) or {}
            if not price:
                price = product_data.get("price", 0)
            if not category:
                category = product_data.get("category", "")
            
        if not price or price <= 0:
            return _json_response({"success": False, "error": "Price required. Provide 'price' manually or ensure product data is in cache for ASIN.", "asin": asin})
            
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
        weight = arguments.get("weight_lb")
        
        # Try to enrich from cache
        if asin and not weight:
            product_data = data_cache.get("amazon", asin) or {}
            weight = product_data.get("weight_lb") or product_data.get("weight", 1.0)
            if isinstance(weight, str):
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
        cost = arguments.get("estimated_cost", 0)
        
        if not asin:
             return _json_response({"success": False, "error": "ASIN required for profit analysis."})

        product_data = data_cache.get("amazon", asin) or {}
        price = product_data.get("price", 0)
        category = product_data.get("category", "")
        weight = product_data.get("weight_lb") or product_data.get("weight", 1.0)
        
        if price <= 0:
            return _json_response({
                "success": False, 
                "error": "Price not found in cache for ASIN. Please provide price manually or search product first.", 
                "asin": asin
            })

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
        if not asin:
             return _json_response({"success": False, "error": "ASIN required for cost estimation."})
             
        product_data = data_cache.get("amazon", asin) or {}
        price = product_data.get("price", 40.0)
        return _json_response({"asin": asin, "estimated_cost": round(price * 0.25, 2), "method": "percentage_of_price"})

    return [TextContent(type="text", text=f"Unknown finance tool: {name}")]

finance_tools = [
    Tool(
        name="calc_profit",
        description="Comprehensive profit analysis including referral fees, FBA fees, and COGS. If ASIN is not provided, it will attempt to use the ASIN from the current conversation context.",
        inputSchema={
            "type": "object", 
            "properties": {
                "asin": {"type": "string", "description": "Optional. ASIN of the product. If not provided, attempts to resolve from context."}, 
                "estimated_cost": {"type": "number", "description": "Manufacturing and shipping cost (COGS)"}
            }, 
            "required": ["estimated_cost"]
        }
    ),
    Tool(
        name="calc_referral_fee",
        description="Calculate Amazon referral fee (commission) based on category and price. If ASIN is not provided, it will attempt to use the ASIN from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Optional. Product ASIN."},
                "price": {"type": "number", "description": "Optional. If omitted, will be pulled from cache for ASIN."},
                "category": {"type": "string", "description": "Optional. If omitted, will be pulled from cache for ASIN."}
            }
        }
    ),
    Tool(
        name="calc_fba_fee",
        description="Calculate FBA fulfillment fee based on product weight and size. If ASIN is not provided, it will attempt to use the ASIN from the current conversation context.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Optional. Product ASIN."},
                "weight_lb": {"type": "number", "description": "Optional. If omitted, will be pulled from cache for ASIN."}
            }
        }
    ),
    Tool(
        name="estimate_cost",
        description="Estimate the manufacturing cost of a product based on market price. If ASIN is not provided, it will attempt to use the ASIN from the current conversation context.",
        inputSchema={
            "type": "object", 
            "properties": {
                "asin": {"type": "string", "description": "Optional. Product ASIN."}
            }
        }
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
