import json
import logging
import asyncio
import os
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-market")

def _get_sellersprite_api(tenant_id: str = "default"):
    from src.mcp.servers.market.sellersprite.client import SellerspriteAPI
    return SellerspriteAPI(tenant_id=tenant_id)


def _get_xiyou_api(tenant_id: str = "default"):
    """Lazy-load a tenant-specific XiyouZhaociAPI instance."""
    from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI
    return XiyouZhaociAPI(tenant_id=tenant_id)


async def handle_market_tool(name: str, arguments: dict) -> list[TextContent]:
    # Use context propagation to resolve identity
    from src.core.utils.context import ContextPropagator
    tenant_id = ContextPropagator.get("tenant_id", "default")

    if name == "sellersprite_competing_lookup":
        import re as _re
        from datetime import datetime as _dt

        api = _get_sellersprite_api(tenant_id)
        market = arguments.get("market", "US").upper()
        market_id = {"US": 1, "DE": 6, "JP": 8, "UK": 3, "FR": 4, "IT": 5, "ES": 7, "CA": 2}.get(market, 1)

        # Normalize month_name to "bsr_sales_monthly_YYYYMM" from any reasonable input.
        # Handles: omitted, "June 2025", "2025-06", "202506", "bsr_sales_monthly_202506"
        _MONTH_NAMES = {
            "january": 1, "february": 2, "march": 3, "april": 4,
            "may": 5, "june": 6, "july": 7, "august": 8,
            "september": 9, "october": 10, "november": 11, "december": 12,
        }

        def _normalize_month(raw: str | None) -> str:
            if not raw:
                now = _dt.now()
                y = now.year if now.month > 2 else now.year - 1
                m = now.month - 2 if now.month > 2 else now.month + 10
                return f"bsr_sales_monthly_{y:04d}{m:02d}"
            raw = raw.strip()
            if raw.startswith("bsr_sales_monthly_"):
                return raw
            # Pure YYYYMM: "202506"
            if _re.fullmatch(r"\d{6}", raw):
                return f"bsr_sales_monthly_{raw}"
            # YYYY-MM or YYYY/MM
            m2 = _re.fullmatch(r"(\d{4})[-/](\d{1,2})", raw)
            if m2:
                return f"bsr_sales_monthly_{int(m2.group(1)):04d}{int(m2.group(2)):02d}"
            # "June 2025" or "2025 June"
            parts = raw.replace(",", "").split()
            year = month = None
            for p in parts:
                if p.isdigit() and len(p) == 4:
                    year = int(p)
                elif p.lower() in _MONTH_NAMES:
                    month = _MONTH_NAMES[p.lower()]
            if year and month:
                return f"bsr_sales_monthly_{year:04d}{month:02d}"
            # Fallback: return as-is and let the API reject it with a clear error
            return raw

        now = _dt.now()
        # Latest published snapshot is 2 months prior to today
        latest_y = now.year if now.month > 2 else now.year - 1
        latest_m = now.month - 2 if now.month > 2 else now.month + 10
        latest_ym = latest_y * 100 + latest_m  # numeric YYYYMM for comparison

        month_name = _normalize_month(arguments.get("month_name"))

        # Validate: reject snapshots that haven't been published yet
        requested_ym = int(month_name.replace("bsr_sales_monthly_", "")) if month_name.startswith("bsr_sales_monthly_") else 0
        if requested_ym and requested_ym > latest_ym:
            return [TextContent(type="text", text=json.dumps({
                "error": (
                    f"Snapshot {month_name} is not yet published. "
                    f"Today is {now.strftime('%Y-%m-%d')}. "
                    f"Latest available snapshot: bsr_sales_monthly_{latest_y:04d}{latest_m:02d}."
                ),
                "today": now.strftime("%Y-%m-%d"),
                "latest_available_snapshot": f"bsr_sales_monthly_{latest_y:04d}{latest_m:02d}",
            }, ensure_ascii=False))]

        # Accept Amazon BSR URL in place of node_id_paths
        node_id_paths = arguments.get("node_id_paths")
        amazon_url = arguments.get("amazon_url", "")
        if not node_id_paths and amazon_url:
            m = _re.search(r"/(?:gp/bestsellers|zgbs)/[^/]+/(\d+)", amazon_url)
            if not m:
                return [TextContent(type="text", text=json.dumps(
                    {"error": f"Could not extract node ID from URL: {amazon_url}"}, ensure_ascii=False
                ))]
            node_id = m.group(1)
            nodes = await asyncio.to_thread(
                api.resolve_node_path, market_id=market_id, table=month_name, query=node_id
            )
            if not nodes:
                return [TextContent(type="text", text=json.dumps(
                    {"error": f"Could not resolve nodeIdPath for node_id={node_id} in table={month_name}"}
                ))]
            node_id_paths = [nodes[0]["id"]]

        if not node_id_paths:
            return [TextContent(type="text", text=json.dumps(
                {"error": "Either amazon_url or node_id_paths is required"}, ensure_ascii=False
            ))]

        try:
            raw = await asyncio.to_thread(
                api.get_competing_lookup,
                market=market,
                month_name=month_name,
                node_id_paths=node_id_paths,
                page=arguments.get("page", 1),
                size=arguments.get("size", 100),
                order=arguments.get("order"),
                symbol_flag=arguments.get("symbol_flag", True),
                low_price=arguments.get("low_price", "N"),
            )
        except Exception:
            logger.exception("[sellersprite] competing_lookup raised unexpectedly")
            raise

        # Slim response for LLM context — strip bulky fields (trends, images, etc.)
        # Full data is fetched directly via SellerspriteAPI in workflow steps.
        _KEEP = {"asin", "parentAsin", "rank", "rankingPosition", "price", "brand",
                 "brandName", "reviewCount", "rating", "bsr"}
        slim_items = [
            {k: v for k, v in item.items() if k in _KEEP}
            for item in (raw.get("items") or [])
        ]
        result = {
            "snapshot": month_name,
            "today": now.strftime("%Y-%m-%d"),
            "latest_available_snapshot": f"bsr_sales_monthly_{latest_y:04d}{latest_m:02d}",
            "total": raw.get("total", 0),
            "returned": len(slim_items),
            "items": slim_items,
        }
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    elif name == "sellersprite_resolve_node_path":
        import re as _re
        from datetime import datetime as _dt
        api = _get_sellersprite_api(tenant_id)
        raw_table = arguments.get("month_name") or arguments.get("table")
        if raw_table and _re.fullmatch(r"\d{6}", raw_table):
            table = f"bsr_sales_monthly_{raw_table}"
        elif raw_table and not raw_table.startswith("bsr_sales_monthly_"):
            # Try YYYY-MM
            m2 = _re.fullmatch(r"(\d{4})[-/](\d{1,2})", raw_table)
            table = f"bsr_sales_monthly_{int(m2.group(1)):04d}{int(m2.group(2)):02d}" if m2 else raw_table
        elif raw_table:
            table = raw_table
        else:
            _now = _dt.now()
            _y = _now.year if _now.month > 1 else _now.year - 1
            _m = _now.month - 1 if _now.month > 1 else 12
            table = f"bsr_sales_monthly_{_y:04d}{_m:02d}"
        result = await asyncio.to_thread(
            api.resolve_node_path,
            market_id=arguments.get("market_id", 1),
            table=table,
            query=arguments["query"],
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    elif name == "sellersprite_category_nodes":
        api = _get_sellersprite_api(tenant_id)
        result = await asyncio.to_thread(
            api.get_category_nodes,
            market_id=arguments.get("market_id", 1),
            table=arguments["table"],
            node_id_path=arguments["node_id_path"],
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

    elif name == "sellersprite_market_research":
        api = _get_sellersprite_api(tenant_id)
        result = await asyncio.to_thread(
            api.get_market_research,
            market_id=arguments.get("market_id", 1),
            node_id_path=arguments["node_id_path"],
            month_name=arguments.get("month_name", "bsr_sales_nearly"),
            size=arguments.get("size", 20),
            page=arguments.get("page", 1),
        )
        return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False))]

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
            "xiyou_get_traffic_scores": "get_traffic_scores",
            "xiyou_get_asin_daily_trends": "get_asin_daily_trends",
            "xiyou_get_search_term_trends": "get_search_term_trends",
            "xiyou_get_asin_keywords": "get_asin_keywords",
            "xiyou_get_asin_search_term_rank_trends": "get_asin_search_term_rank_trends",
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
        name="sellersprite_competing_lookup",
        description=(
            "[Sellersprite/卖家精灵] Fetch BSR-ranked competitor products for a category. "
            "Accepts an Amazon BSR URL directly via ``amazon_url`` — node ID extraction "
            "and path resolution are handled automatically. ``month_name`` defaults to "
            "the latest published snapshot (2 months prior) if omitted. "
            "Each item includes ASIN, price, rating, review count, BSR rank."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "amazon_url": {"type": "string", "description": "Amazon BSR category URL, e.g. 'https://www.amazon.com/gp/bestsellers/industrial/8297518011/'"},
                "market": {"type": "string", "default": "US", "description": "Marketplace code (US, DE, JP, …)"},
                "month_name": {"type": "string", "description": "BSR snapshot table name, e.g. 'bsr_sales_monthly_202602'. Defaults to latest published snapshot."},
                "node_id_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Colon-joined nodeIdPaths. Ignored when amazon_url is provided.",
                },
                "page": {"type": "integer", "default": 1, "description": "Page number (1-based)"},
                "size": {"type": "integer", "default": 100, "description": "Results per page (max 100)"},
                "order": {"type": "object", "description": "Sort spec, e.g. {\"field\": \"bsr_rank\", \"desc\": false}"},
                "symbol_flag": {"type": "boolean", "default": True, "description": "Include brand symbol filter"},
                "low_price": {"type": "string", "default": "N", "description": "Low-price filter flag (Y/N)"},
            },
            "required": [],
        },
    ),
    Tool(
        name="sellersprite_resolve_node_path",
        description=(
            "[Sellersprite] Search BSR category nodes by label (nodeLabelPath). "
            "``query`` accepts two forms:\n"
            "  1. Numeric node ID (e.g. '8297518011' from an Amazon BSR URL) → single exact match.\n"
            "  2. Category keyword (e.g. 'Traps') → multiple candidates ordered by product count.\n"
            "Returns a list of nodes, each with ``id`` (full nodeIdPath for competing_lookup), "
            "``label`` (English breadcrumb), ``nodeLabelLocale`` (Chinese name), and ``products`` count.\n"
            "When multiple results are returned, present them to the user for selection.\n"
            "``month_name`` is optional — omit it to default to the previous month's snapshot. "
            "Accepts: 'YYYY-MM', 'YYYYMM', or canonical 'bsr_sales_monthly_YYYYMM'."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "market_id":  {"type": "integer", "default": 1, "description": "Numeric market ID (1=US, 6=DE, …)"},
                "month_name": {"type": "string", "description": "BSR snapshot month, e.g. '2026-02' or '202602'. Defaults to previous month."},
                "query":      {"type": "string", "description": "Numeric node ID or category keyword to search"},
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="sellersprite_category_nodes",
        description=(
            "[Sellersprite/卖家精灵] Fetch child category nodes for a given BSR node path. "
            "Use this to walk the category tree. Pass the full colon-joined ``nodeIdPath`` "
            "(obtained from ``sellersprite_resolve_node_path``) to get its children."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "market_id": {"type": "integer", "default": 1, "description": "Numeric market ID (1=US, 6=DE, …)"},
                "table": {"type": "string", "description": "BSR snapshot table name, e.g. 'bsr_sales_monthly_202509'"},
                "node_id_path": {"type": "string", "description": "Colon-joined node path, e.g. '16310091:8297370011'"},
            },
            "required": ["table", "node_id_path"],
        },
    ),
    Tool(
        name="sellersprite_market_research",
        description=(
            "[Sellersprite/卖家精灵] Fetch subcategory market research data for a given category node. "
            "Returns each subcategory's return_rate_pct (%), avg_return_rate_pct (%), and "
            "search_to_buy_ratio_pm (‰) — the core signals for category entry evaluation. "
            "Paginated: use ``page`` to iterate through all subcategories (10 rows/page). "
            "``month_name`` defaults to 'bsr_sales_nearly' (latest rolling snapshot)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "node_id_path": {"type": "string", "description": "Top-level category node ID, e.g. '1055398' for Home & Kitchen"},
                "market_id": {"type": "integer", "default": 1, "description": "Numeric market ID (1=US, 6=DE, …)"},
                "month_name": {"type": "string", "default": "bsr_sales_nearly", "description": "Snapshot name, e.g. 'bsr_sales_nearly' or 'bsr_sales_monthly_202602'"},
                "page": {"type": "integer", "default": 1, "description": "Page number (1-based, 10 rows per page)"},
                "size": {"type": "integer", "default": 20, "description": "Rows requested per page (server returns ~10 regardless)"},
            },
            "required": ["node_id_path"],
        },
    ),
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
        name="get_ad_traffic",
        description=(
            "Get advertising traffic estimates for an ASIN. "
            "Returns: {ad_spend (USD), roas (return on ad spend)}. "
            "Note: currently returns stub data — wire to a live ad analytics source when available."
        ),
        inputSchema={
            "type": "object",
            "properties": {"asin": {"type": "string", "description": "Product ASIN"}},
            "required": ["asin"],
        },
    ),
    Tool(
        name="get_deal_history",
        description=(
            "Scrape off-Amazon deal history for an ASIN from Slickdeals and DealNews. "
            "Returns list of deal records, each with: date, price (USD), discount_pct, title, site, type. "
            "Pass the result directly to analyze_promotions to compute promo dependency score."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Product ASIN to look up"},
                "keyword": {"type": "string", "description": "Optional search keyword override (defaults to product title)"},
                "max_pages": {"type": "integer", "default": 3, "description": "Max pages to scrape per deal site"}
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="analyze_promotions",
        description=(
            "Compute promotion risk metrics from deal history data. "
            "Returns: promo_frequency (deals/month), all_time_low (USD), "
            "median_discount_pct (%), promo_dependency_score (0–1), "
            "risk_level ('Low (Stable Price)'|'Medium (Regular Promotions)'|'High (Price War/Clearance)'), "
            "total_deals_found. "
            "Call get_deal_history first to obtain the deals input."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "current_price": {"type": "number", "description": "Current selling price in USD"},
                "deals": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Deal records from get_deal_history (each with date, price, discount_pct fields)"
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
        description=(
            "[Xiyouzhaoci — NOT Amazon search] Analyze a keyword via Xiyouzhaoci's ABA database. "
            "Returns file_path to a local xlsx containing: keyword, search_volume, ASIN list, "
            "traffic_share per ASIN, ranking trend, click_share, conversion_share. "
            "Do NOT use for live Amazon search — use search_products for that."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Search term to analyze (e.g. 'wireless charger')"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["keyword"],
        },
    ),
    Tool(
        name="xiyou_asin_lookup",
        description=(
            "[Xiyouzhaoci — NOT Amazon product details] Reverse-lookup all ranking keywords for an ASIN. "
            "Returns file_path to a local xlsx containing: keyword, search_frequency_rank (SFR), "
            "ASIN rank for that keyword, click_share, conversion_share, estimated_traffic. "
            "Do NOT use for product attributes — use get_product_details for that."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Amazon ASIN to reverse-lookup"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["asin"],
        },
    ),
    Tool(
        name="xiyou_asin_compare_keywords",
        description=(
            "[Xiyouzhaoci] Compare keyword overlap and performance across up to 20 ASINs. "
            "Returns file_path to a local xlsx with: keyword, each ASIN's rank, "
            "click_share, conversion_share, and competitive gap analysis. "
            "Useful for identifying keywords competitors rank for that you do not."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asins": {"type": "array", "items": {"type": "string"}, "description": "List of ASINs to compare (max 20)"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "period": {"type": "string", "default": "last7days", "description": "Time period: 'last7days' or 'last30days'"},
                "output_dir": {"type": "string", "default": "data", "description": "Local directory for the downloaded xlsx"},
            },
            "required": ["asins"],
        },
    ),
    Tool(
        name="xiyou_get_aba_top_asins",
        description=(
            "[Xiyouzhaoci] Get top-3 ASINs by click/conversion share for given search terms, from Amazon Brand Analytics (ABA). "
            "Returns list of {search_term, rank_1_asin, rank_1_click_share, rank_1_conversion_share, …} for each term. "
            "Use to identify who dominates a keyword and estimate their traffic advantage."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "search_terms": {"type": "array", "items": {"type": "string"}, "description": "Search terms to query (e.g. ['wireless charger', 'usb c hub'])"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
            },
            "required": ["search_terms"],
        },
    ),
    Tool(
        name="xiyou_get_search_terms_ranking",
        description=(
            "[Xiyouzhaoci] Find ranked keyword variations for a root query using ABA data. "
            "Returns paginated list of {search_term, search_frequency_rank (SFR), week/month volume trend}. "
            "Lower SFR = more searched. Use to discover long-tail and adjacent keyword opportunities."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Root query string (e.g. 'iphone 15 case')"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "page": {"type": "integer", "default": 1, "description": "Page number (1-based)"},
                "page_size": {"type": "integer", "default": 100, "description": "Results per page (max 100)"},
                "field": {"type": "string", "default": "week", "description": "Time granularity: 'week' or 'month'"},
                "rank_pattern": {"type": "string", "default": "aba", "description": "Ranking source pattern (default: 'aba')"},
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="xiyou_get_traffic_scores",
        description=(
            "[Xiyouzhaoci] Fetch 7-day rolling traffic metrics for a list of ASINs. "
            "Returns list of {asin, traffic_score, ad_traffic_ratio (ad dependency 0–1), "
            "organic_traffic_ratio, traffic_growth_rate (7d change %). "
            "ad_traffic_ratio > 0.7 signals high ad dependency — organic rank may drop if ads stop."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asins": {"type": "array", "items": {"type": "string"}, "description": "List of ASINs to query"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
            },
            "required": ["asins"],
        },
    ),
    Tool(
        name="xiyou_get_asin_daily_trends",
        description=(
            "[Xiyouzhaoci] Fetch daily historical data for an ASIN over a date range. "
            "Returns list of daily records, each with: date, price, rating (stars), "
            "review_count, bsr_rank, deal_flag (bool). "
            "Earliest available: 2023-02-01. Max continuous range: 25 months."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {"type": "string", "description": "Amazon ASIN"},
                "country": {"type": "string", "default": "US", "description": "Amazon marketplace country code"},
                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD). Earliest: 2023-02-01"},
                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD). Max span: 25 months"},
            },
            "required": ["asin", "start_date", "end_date"],
        },
    ),
    Tool(
        name="xiyou_get_asin_keywords",
        description=(
            "[Xiyouzhaoci] Fetch keywords that drive traffic to an ASIN, with per-keyword "
            "topAsins (top 3 competitor ASINs by click/conversion share). "
            "Returns: list of {searchTerm, weeklySearchVolume, searchFrequencyRank, "
            "ranks[{position, totalRank, page, pageRank}], "
            "trafficRatio.{total, organic, advertising}, "
            "searchTermShare.{clickShare, conversionShare}, "
            "topAsins.list[{asin, clickShare, conversionShare}]}. "
            "Use start_date/end_date to set the comparison window. "
            "API uses monthly cycleFilter internally — set start_date to first day of month "
            "and end_date to last day of month for clean monthly snapshots. "
            "Daily granularity (e.g. start_date='2026-04-01', end_date='2026-04-21') "
            "may also work — test to confirm actual API behaviour. "
            "Ideal for: finding non-brand competitor ASINs, measuring keyword-level ad vs organic split."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin":       {"type": "string", "description": "Target ASIN to reverse-lookup"},
                "country":    {"type": "string", "default": "US", "description": "Marketplace country code"},
                "start_date": {"type": "string", "description": "Start date YYYY-MM-DD (e.g. '2026-01-01')"},
                "end_date":   {"type": "string", "description": "End date YYYY-MM-DD (e.g. '2026-04-21')"},
                "page":       {"type": "integer", "default": 1,  "description": "Page number (1-based)"},
                "page_size":  {"type": "integer", "default": 50, "description": "Keywords per page (max 50)"},
            },
            "required": ["asin", "start_date", "end_date"],
        },
    ),
    Tool(
        name="xiyou_get_search_term_trends",
        description=(
            "[Third-party Xiyouzhaoci tool] Fetch weekly historical ABA search-volume trends "
            "for a single keyword over the past ~52 weeks. Each record contains the keyword's "
            "Search Frequency Rank (SFR) for that week — lower SFR means more searches. "
            "Use this to detect category seasonality directly from demand intent rather than "
            "from BSR proxy data."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "search_term": {"type": "string", "description": "The keyword to query (e.g. 'nebulizer')"},
                "country": {"type": "string", "default": "US", "description": "Marketplace country code"},
                "weeks": {"type": "integer", "description": "Number of recent weeks to return (default: API decides, typically 52)"},
                "week_interval": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional explicit ISO week list to filter, e.g. ['2024-W01','2024-W52']. Pass [] for default range.",
                },
            },
            "required": ["search_term"],
        },
    ),
    Tool(
        name="xiyou_get_asin_search_term_rank_trends",
        description=(
            "[Xiyouzhaoci] Fetch daily organic search rank history for an ASIN × keyword(s) "
            "combination. Returns per-keyword daily series of organic page, pageRank, and "
            "totalRank positions. Supports up to 24 months of history (earliest: 2023-02-01). "
            "Use this to: (1) track how organic rank changes after ad bid/budget adjustments "
            "(halo effect); (2) provide time-series input for ITS/CausalImpact rank analysis; "
            "(3) identify keywords where ads are compensating for poor organic rank. "
            "Response shape: {entities: [{country, asin, searchTerm, "
            "trends: [{localDate, displayPositions: {or: {page, pageRank, totalRank}}}]}]}"
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "asin": {
                    "type": "string",
                    "description": "Amazon ASIN (e.g. 'B0FXFGMD7Z')",
                },
                "country": {
                    "type": "string",
                    "default": "US",
                    "description": "Marketplace country code (e.g. 'US', 'DE', 'JP')",
                },
                "search_terms": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "List of keywords to track (≤5 recommended). "
                        "Use the highest-spend or LP-top-allocation keywords for best signal."
                    ),
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date YYYY-MM-DD.",
                },
                "end_date": {
                    "type": "string",
                    "description": (
                        "End date YYYY-MM-DD. Max span from start_date: 24 calendar months "
                        "(e.g. start 2024-04-21 → end 2026-04-21)."
                    ),
                },
            },
            "required": ["asin", "search_terms", "start_date", "end_date"],
        },
    ),
]

_MARKET_META = {
    "sellersprite_competing_lookup": ("DATA", "paginated BSR competitor list with monthly sales trends"),
    "sellersprite_resolve_node_path": ("DATA", "full nodeIdPath resolved from a bare Amazon BSR node ID via nodeLabelPath search"),
    "sellersprite_category_nodes": ("DATA", "child category nodes for a given BSR nodeIdPath"),
    "sellersprite_market_research": ("DATA", "subcategory list with return_rate_pct (%), avg_return_rate_pct (%), and search_to_buy_ratio_pm (‰)"),
    "xiyou_get_login_qr": ("DATA", "URL for WeChat login QR code"),
    "xiyou_check_login_status": ("DATA", "authentication status of pending QR scan"),
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
    "xiyou_get_traffic_scores": ("DATA", "JSON containing traffic scores, ad ratio, and growth for ASINs"),
    "xiyou_get_asin_daily_trends": ("DATA", "JSON containing daily historical trends for price and ratings"),
    "xiyou_get_search_term_trends": ("DATA", "JSON containing weekly ABA SFR history for a keyword (52-week seasonality signal)"),
    "xiyou_get_asin_keywords": ("DATA", "JSON list of keywords driving traffic to an ASIN, with topAsins per keyword and traffic ratio breakdown"),
}

for tool in market_tools:
    cat, ret = _MARKET_META.get(tool.name, ("DATA", ""))
    tool_registry.register_tool(tool, handle_market_tool, category=cat, returns=ret)
