import json
import logging
import asyncio
import os
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-market")


def _get_xiyou_api():
    """Lazy-load XiyouZhaociAPI singleton."""
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    return XiyouZhaociAPI()


def _xiyou_auth_required(api) -> list[TextContent] | None:
    """
    If token is missing, auto-send SMS and return an auth-needed response.
    Returns None if auth is valid.
    """
    if not api.needs_auth:
        return None

    phone = os.getenv("XIYOUZHAOCI_PHONE", "")
    if not phone:
        return [TextContent(type="text", text=json.dumps({
            "status": "auth_required",
            "error": "Xiyouzhaoci token not found and XIYOUZHAOCI_PHONE env var is not set. "
                     "Set the env var and retry, or call xiyou_verify_sms after sending code manually.",
        }))]

    sent = api.request_sms_code(phone)
    masked = phone[:3] + "****" + phone[-4:]
    if sent:
        return [TextContent(type="text", text=json.dumps({
            "status": "sms_sent",
            "phone": masked,
            "message": f"SMS verification code sent to {masked}. "
                       f"Call xiyou_verify_sms with the code to complete authentication.",
        }))]
    return [TextContent(type="text", text=json.dumps({
        "status": "sms_failed",
        "phone": masked,
        "error": "Failed to send SMS code. Check logs for details.",
    }))]


async def handle_market_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "seller_analysis":
        return [TextContent(type="text", text=json.dumps({"us_seller_percentage": 0.62, "market_concentration": "high"}))]

    elif name == "keyword_data":
        return [TextContent(type="text", text=json.dumps({"search_volume": 150000, "cpc_bid": 1.25}))]

    elif name == "get_ad_traffic":
        return [TextContent(type="text", text=json.dumps({"ad_spend": 5000, "roas": 2.1}))]

    # ── Xiyouzhaoci auth ─────────────────────────────────────────────────

    elif name == "xiyou_send_sms":
        api = _get_xiyou_api()
        phone = arguments.get("phone") or os.getenv("XIYOUZHAOCI_PHONE", "")
        if not phone:
            return [TextContent(type="text", text=json.dumps({
                "status": "error",
                "error": "No phone number provided and XIYOUZHAOCI_PHONE env var is not set.",
            }))]
        sent = await asyncio.to_thread(api.request_sms_code, phone)
        masked = phone[:3] + "****" + phone[-4:]
        if sent:
            return [TextContent(type="text", text=json.dumps({
                "status": "sms_sent",
                "phone": masked,
                "message": f"SMS code sent to {masked}. Call xiyou_verify_sms with the code.",
            }))]
        return [TextContent(type="text", text=json.dumps({
            "status": "sms_failed",
            "error": "Failed to send SMS code.",
        }))]

    elif name == "xiyou_verify_sms":
        api = _get_xiyou_api()
        sms_code = arguments["sms_code"]
        phone = arguments.get("phone") or os.getenv("XIYOUZHAOCI_PHONE", "")
        success = await asyncio.to_thread(api.verify_sms_code, sms_code, phone)
        if success:
            return [TextContent(type="text", text=json.dumps({
                "status": "authenticated",
                "message": "Xiyouzhaoci login successful. Token saved. You can now use keyword analysis tools.",
            }))]
        return [TextContent(type="text", text=json.dumps({
            "status": "auth_failed",
            "error": "SMS verification failed. The code may be incorrect or expired.",
        }))]

    # ── Xiyouzhaoci data tools ───────────────────────────────────────────

    elif name == "xiyou_keyword_analysis":
        api = _get_xiyou_api()
        auth_response = _xiyou_auth_required(api)
        if auth_response:
            return auth_response

        country = arguments.get("country", "US")
        keyword = arguments["keyword"]
        output_dir = arguments.get("output_dir", "data")

        file_path = await asyncio.to_thread(api.export_keyword_data, country, keyword, output_dir)
        if file_path:
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "keyword": keyword,
                "country": country,
                "file_path": file_path,
            }))]
        return [TextContent(type="text", text=json.dumps({
            "status": "failed",
            "keyword": keyword,
            "country": country,
            "error": "Export failed. Check logs for details.",
        }))]

    elif name == "xiyou_asin_lookup":
        api = _get_xiyou_api()
        auth_response = _xiyou_auth_required(api)
        if auth_response:
            return auth_response

        country = arguments.get("country", "US")
        asin = arguments["asin"]
        output_dir = arguments.get("output_dir", "data")

        file_path = await asyncio.to_thread(api.export_asin_data, country, asin, output_dir)
        if file_path:
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "asin": asin,
                "country": country,
                "file_path": file_path,
            }))]
        return [TextContent(type="text", text=json.dumps({
            "status": "failed",
            "asin": asin,
            "country": country,
            "error": "Export failed. Check logs for details.",
        }))]

    return [TextContent(type="text", text=f"Unknown tool: {name}")]


market_tools = [
    Tool(
        name="seller_analysis",
        description="Analyze seller demographics for a category or keyword.",
        inputSchema={
            "type": "object",
            "properties": {"category_url": {"type": "string"}},
            "required": ["category_url"],
        },
    ),
    Tool(
        name="keyword_data",
        description="Get search volume and CPC data for a keyword.",
        inputSchema={
            "type": "object",
            "properties": {"keyword": {"type": "string"}},
            "required": ["keyword"],
        },
    ),
    Tool(
        name="get_ad_traffic",
        description="Get advertising traffic estimates for an ASIN.",
        inputSchema={
            "type": "object",
            "properties": {"asin": {"type": "string"}},
            "required": ["asin"],
        },
    ),
    Tool(
        name="xiyou_send_sms",
        description="Send SMS verification code for Xiyouzhaoci authentication. Usually auto-triggered when token is missing.",
        inputSchema={
            "type": "object",
            "properties": {
                "phone": {"type": "string", "description": "Phone number (defaults to XIYOUZHAOCI_PHONE env var)"},
            },
        },
    ),
    Tool(
        name="xiyou_verify_sms",
        description="Verify SMS code to complete Xiyouzhaoci authentication. Call this after receiving the SMS code.",
        inputSchema={
            "type": "object",
            "properties": {
                "sms_code": {"type": "string", "description": "The SMS verification code received"},
                "phone": {"type": "string", "description": "Phone number (defaults to XIYOUZHAOCI_PHONE env var)"},
            },
            "required": ["sms_code"],
        },
    ),
    Tool(
        name="xiyou_keyword_analysis",
        description="[Third-party Xiyouzhaoci tool, NOT Amazon search] Analyze a keyword via Xiyouzhaoci's database: returns a local xlsx file with ASINs, traffic data, and ranking trends. Do NOT use this for direct Amazon search — use search_products instead.",
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "The search term to analyze (e.g. 'iphone case')"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="xiyou_asin_lookup",
        description="[Third-party Xiyouzhaoci tool] Reverse-lookup keywords for an ASIN via Xiyouzhaoci's database: returns a local xlsx file. Do NOT use this for Amazon product details — use get_product_details instead.",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "The Amazon ASIN to look up"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["asin"],
        },
    ),
]

_MARKET_META = {
    "seller_analysis": ("DATA", "seller demographics: US seller %, market concentration"),
    "keyword_data": ("DATA", "search volume and CPC bid"),
    "get_ad_traffic": ("DATA", "ad spend and ROAS estimates"),
    "xiyou_send_sms": ("DATA", "SMS send confirmation"),
    "xiyou_verify_sms": ("DATA", "authentication status"),
    "xiyou_keyword_analysis": ("DATA", "xlsx file with ASINs, traffic data, ranking trends (third-party)"),
    "xiyou_asin_lookup": ("DATA", "xlsx file with reverse-lookup keywords for an ASIN (third-party)"),
}

for tool in market_tools:
    cat, ret = _MARKET_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_market_tool, category=cat, returns=ret)
