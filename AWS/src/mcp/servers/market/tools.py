import json
import logging
import asyncio
import os
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-market")

def _get_xiyou_api(tenant_id: str = "default"):
    """Lazy-load a tenant-specific XiyouZhaociAPI instance."""
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    return XiyouZhaociAPI(tenant_id=tenant_id)


async def handle_market_tool(name: str, arguments: dict) -> list[TextContent]:
    # Use context propagation to resolve identity
    from src.core.utils.context import ContextPropagator
    tenant_id = ContextPropagator.get("tenant_id", "default")

    if name == "seller_analysis":
        return [TextContent(type="text", text=json.dumps({"us_seller_percentage": 0.62, "market_concentration": "high"}))]

    elif name == "keyword_data":
        return [TextContent(type="text", text=json.dumps({"search_volume": 150000, "cpc_bid": 1.25}))]

    elif name == "get_ad_traffic":
        return [TextContent(type="text", text=json.dumps({"ad_spend": 5000, "roas": 2.1}))]

    elif name == "get_deal_history":
        from src.mcp.servers.market.deals.client import DealHistoryClient
        client = DealHistoryClient()
        asin = arguments["asin"]
        keyword = arguments.get("keyword", "")
        max_pages = arguments.get("max_pages", 3)
        deals = await client.get_deal_history(asin, keyword=keyword, max_pages=max_pages)
        return [TextContent(type="text", text=json.dumps(deals, ensure_ascii=False))]

    elif name == "analyze_promotions":
        from src.intelligence.processors.promo_analyzer import PromoAnalyzer
        analyzer = PromoAnalyzer()
        current_price = arguments.get("current_price", 0.0)
        deals = arguments.get("deals", [])
        result = analyzer.analyze(current_price, deals)
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    # ── Xiyouzhaoci NEW WeChat QR Auth Tools ─────────────────────────────

    elif name == "xiyou_get_login_qr":
        api = _get_xiyou_api(tenant_id)
        qr_data = await asyncio.to_thread(api.get_login_qr)
        
        if "url" in qr_data:
            # Return a standardized cross-platform interaction signal
            signal = {
                "_type": "INTERACTION_REQUIRED",
                "interaction_type": "AUTH_QR_SCAN",
                "required_capabilities": ["IMAGE_DISPLAY", "INTERACTIVE_BUTTONS"],
                "ui_config": {
                    "title": "🔐 需要认证 (西柚找词)",
                    "description": "由于 Token 已过期，请扫描下方二维码完成登录。",
                    "button_text": "我已确认扫码",
                    "action": "VERIFY_XIYOU_LOGIN"
                },
                "data": {
                    "url": qr_data["url"],
                    "expires_in": qr_data.get("expires_in", 120)
                },
                "context": {
                    "tenant_id": tenant_id,
                    "job_id": ContextPropagator.get("job_id")
                },
                "fallback_text": f"Please scan this QR code to login to Xiyouzhaoci (valid for 120s): {qr_data['url']}. Reply 'I have scanned' when done."
            }
            return [TextContent(type="text", text=json.dumps(signal))]
        return [TextContent(type="text", text=json.dumps(qr_data))]

    elif name == "xiyou_check_login_status":
        api = _get_xiyou_api(tenant_id)
        result = await asyncio.to_thread(api.check_qr_login_status)
        return [TextContent(type="text", text=json.dumps(result))]

    # ── Xiyouzhaoci Legacy SMS Auth Tools ───────────────────────────────

    elif name == "xiyou_send_sms":
        api = _get_xiyou_api(tenant_id)
        phone = arguments.get("phone") or os.getenv("XIYOUZHAOCI_PHONE", "")
        if not phone:
            return [TextContent(type="text", text=json.dumps({"status": "error", "error": "Phone number not provided."}))]
        sent = await asyncio.to_thread(api.request_sms_code, phone)
        masked = f"{phone[:3]}****{phone[-4:]}"
        if sent:
            return [TextContent(type="text", text=json.dumps({"status": "sms_sent", "phone": masked, "message": f"SMS sent to {masked}. Call xiyou_verify_sms."}))]
        return [TextContent(type="text", text=json.dumps({"status": "sms_failed", "error": "Failed to send SMS."}))]

    elif name == "xiyou_verify_sms":
        api = _get_xiyou_api(tenant_id)
        sms_code = arguments["sms_code"]
        phone = arguments.get("phone") or os.getenv("XIYOUZHAOCI_PHONE", "")
        success = await asyncio.to_thread(api.verify_sms_code, sms_code, phone)
        if success:
            return [TextContent(type="text", text=json.dumps({"status": "authenticated", "message": "Authentication successful."}))]
        return [TextContent(type="text", text=json.dumps({"status": "auth_failed", "error": "SMS verification failed."}))]

    # ── Xiyouzhaoci Data Tools (Now Auth-Aware) ──────────────────────────

    else:
        # Generic handler for all data tools to reduce repetition
        from src.mcp.servers.market.xiyouzhaoci.client import XiyouAuthRequiredError
        
        tool_map = {
            "xiyou_keyword_analysis": "export_keyword_data",
            "xiyou_asin_lookup": "export_asin_data",
            "xiyou_asin_compare_keywords": "export_compare_data",
            "xiyou_get_aba_top_asins": "get_aba_top_asins",
            "xiyou_get_search_terms_ranking": "get_search_terms_ranking",
        }

        if name in tool_map:
            try:
                api = _get_xiyou_api(tenant_id)
                method_to_call = getattr(api, tool_map[name])
                
                # The underlying methods in the client don't need _metadata
                # arguments.pop("_metadata", None) 
                
                result = await asyncio.to_thread(method_to_call, **arguments)
                
                # Wrap file path results in a standard JSON structure
                if isinstance(result, str) and os.path.exists(result):
                    return [TextContent(type="text", text=json.dumps({"status": "success", "file_path": result}))]
                
                return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

            except XiyouAuthRequiredError as e:
                logger.warning(f"Xiyou Auth required for tenant {tenant_id} on tool {name}.")
                return [TextContent(type="text", text=json.dumps({"status": "AUTH_REQUIRED", "message": str(e)}))]
            except Exception as e:
                logger.error(f"Error during {name} for tenant {tenant_id}: {e}")
                return [TextContent(type="text", text=json.dumps({"status": "ERROR", "message": str(e)}))]

    return [TextContent(type="text", text=f"Unknown market tool: {name}")]



market_tools = [
    Tool(
        name="xiyou_get_login_qr",
        description="Initiates WeChat QR code login for Xiyouzhaoci. Returns an image URL. You MUST display this URL to the user exactly as a Markdown image: ![WeChat QR](<url>) and tell them they have 120 seconds to scan and reply 'I have scanned'.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="xiyou_check_login_status",
        description="Checks the status of a pending WeChat QR code login for Xiyouzhaoci. Call this ONLY after the user confirms they have scanned the QR code.",
        inputSchema={"type": "object", "properties": {}},
    ),
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
        name="get_deal_history",
        description="Fetch off-Amazon deal history for an ASIN from target deal sites (e.g., Slickdeals, DealNews).",
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "The Amazon ASIN to look up"},
                "keyword": {"type": "string", "description": "Optional keyword override for the search"},
                "max_pages": {"type": "integer", "description": "Maximum number of pages to scrape (default: 3)"}
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="analyze_promotions",
        description="Analyze promotion frequency, all-time low price, and promo dependency score based on deal history.",
        inputSchema={
            "type": "object",
            "properties": {
                "current_price": {"type": "number", "description": "Current selling price of the product"},
                "deals": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "List of historical deals obtained from get_deal_history"
                }
            },
            "required": ["current_price", "deals"],
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
    Tool(
        name="xiyou_asin_compare_keywords",
        description="[Third-party Xiyouzhaoci tool] Compare multiple ASINs (up to 20) for common keywords and performance. Returns a local xlsx file.",
        inputSchema={
            "type": "object",
            "properties": {
                "asins": {"type": "array", "items": {"type": "string"}, "description": "List of Amazon ASINs to compare (max 20)"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "period": {"type": "string", "default": "last7days", "description": "Time period for data (e.g., 'last7days', 'last30days')"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["asins"],
        },
    ),
    Tool(
        name="xiyou_get_aba_top_asins",
        description="[Third-party Xiyouzhaoci tool] Query top ASINs and their click/conversion share for specific search terms based on ABA ranking data.",
        inputSchema={
            "type": "object",
            "properties": {
                "search_terms": {"type": "array", "items": {"type": "string"}, "description": "List of search terms to query (e.g., ['iphone case', 'charger'])"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
            },
            "required": ["search_terms"],
        },
    ),
    Tool(
        name="xiyou_get_search_terms_ranking",
        description="[Third-party Xiyouzhaoci tool] Query search terms ranking based on a root query string (e.g., finding top ranked variations of 'iphone').",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The root query string to search for (e.g., 'iphone')"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "page": {"type": "integer", "default": 1, "description": "Page number"},
                "page_size": {"type": "integer", "default": 100, "description": "Results per page (max 100)"},
                "field": {"type": "string", "default": "week", "description": "Time period field (e.g., 'week', 'month')"},
                "rank_pattern": {"type": "string", "default": "aba", "description": "Ranking pattern (e.g., 'aba')"},
            },
            "required": ["query"],
        },
    ),
]

_MARKET_META = {
    "xiyou_get_login_qr": ("DATA", "URL for WeChat login QR code"),
    "xiyou_check_login_status": ("DATA", "authentication status of pending QR scan"),
    "seller_analysis": ("DATA", "seller demographics: US seller %, market concentration"),
    "keyword_data": ("DATA", "search volume and CPC bid"),
    "get_ad_traffic": ("DATA", "ad spend and ROAS estimates"),
    "get_deal_history": ("DATA", "list of historical deals with dates, prices, and discounts"),
    "analyze_promotions": ("COMPUTE", "JSON containing promo frequency, all-time low, and dependency score"),
    "xiyou_send_sms": ("DATA", "SMS send confirmation"),
    "xiyou_verify_sms": ("DATA", "authentication status"),
    "xiyou_keyword_analysis": ("DATA", "xlsx file with ASINs, traffic data, ranking trends (third-party)"),
    "xiyou_asin_lookup": ("DATA", "xlsx file with reverse-lookup keywords for an ASIN (third-party)"),
    "xiyou_asin_compare_keywords": ("DATA", "xlsx file with multi-ASIN keyword comparison data (third-party)"),
    "xiyou_get_aba_top_asins": ("DATA", "JSON containing top ASINs and metrics for specified search terms"),
    "xiyou_get_search_terms_ranking": ("DATA", "JSON containing search frequency ranks and trends for variations of a query"),
}

for tool in market_tools:
    cat, ret = _MARKET_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_market_tool, category=cat, returns=ret)
