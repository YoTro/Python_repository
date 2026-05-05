import json
import logging
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry

logger = logging.getLogger("mcp-erp")


async def handle_erp_tool(name: str, arguments: dict) -> list[TextContent]:
    """Route ERP tool calls to the configured provider."""
    from .registry import get_erp_client

    provider = arguments.pop("provider", "lingxing")
    try:
        client = get_erp_client(provider)
    except ValueError as e:
        return [TextContent(type="text", text=str(e))]

    try:
        if name == "erp_inventory":
            result = client.get_inventory(sku=arguments["sku"])
        elif name == "erp_purchase_orders":
            result = client.get_purchase_orders(
                sku=arguments.get("sku"),
                status=arguments.get("status"),
            )
        elif name == "erp_sales_orders":
            result = client.get_sales_orders(
                sku=arguments.get("sku"),
                days=int(arguments.get("days", 30)),
            )
        elif name == "erp_sp_campaign_ad_report":
            asin_raw = arguments.get("asin")
            result = client.get_sp_campaign_ad_report(
                profile_id=arguments["profile_id"],
                report_date=arguments["report_date"],
                asin=asin_raw if isinstance(asin_raw, list) else ([asin_raw] if asin_raw else None),
                search_type=arguments.get("search_type", "campaign_name"),
                date_key=arguments.get("date_key", "day"),
                is_daily=int(arguments.get("is_daily", 1)),
                record_key=arguments.get("record_key", "total"),
                page=int(arguments.get("page", 1)),
                length=int(arguments.get("length", 50)),
                fetch_all=bool(arguments.get("fetch_all", False)),
            )
        else:
            return [TextContent(type="text", text=f"Unknown ERP tool: {name}")]
    except NotImplementedError as e:
        return [TextContent(type="text", text=f"Provider '{provider}' does not support this tool: {e}")]
    except Exception as e:
        logger.error(f"ERP tool '{name}' failed: {e}")
        return [TextContent(type="text", text=f"ERP tool error: {e}")]

    return [TextContent(type="text", text=json.dumps(result, ensure_ascii=False, default=str))]


erp_tools = [
    Tool(
        name="erp_inventory",
        description=(
            "Query real-time inventory for a SKU from the configured ERP system. "
            "Returns: {sku, available_qty, total_qty, pending_orders, warehouse_location, last_updated}. "
            "Supported providers: lingxing (领星ERP)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sku":      {"type": "string", "description": "Seller SKU to query"},
                "provider": {"type": "string", "description": "ERP provider name (default: lingxing)"},
            },
            "required": ["sku"],
        },
    ),
    Tool(
        name="erp_purchase_orders",
        description=(
            "Query inbound purchase orders (replenishment shipments) from the ERP. "
            "Returns list of orders with status, qty, ETA. "
            "Supported providers: lingxing."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sku":      {"type": "string",  "description": "Filter by SKU (optional)"},
                "status":   {"type": "string",  "description": "Filter by order status (optional)"},
                "provider": {"type": "string",  "description": "ERP provider name (default: lingxing)"},
            },
        },
    ),
    Tool(
        name="erp_sales_orders",
        description=(
            "Query recent sales orders for a SKU from the ERP. "
            "Returns list of orders with quantities and dates. "
            "Supported providers: lingxing."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "sku":      {"type": "string",  "description": "Filter by SKU (optional)"},
                "days":     {"type": "integer", "description": "Lookback window in days (default: 30)"},
                "provider": {"type": "string",  "description": "ERP provider name (default: lingxing)"},
            },
        },
    ),
    Tool(
        name="erp_sp_campaign_ad_report",
        description=(
            "Query Sponsored Products campaign-level ad performance report from Lingxing ERP. "
            "Returns aggregate totals plus optional daily breakdown rows. "
            "Key metrics per row: clicks, impressions, orders, spends, sales, acos, roas, ctr, cvr, cpc, cpa. "
            "data[0] is always the aggregate/total row (key=null); data[1:] are daily rows (is_daily=1). "
            "Supported providers: lingxing."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "profile_id":  {"type": "string",  "description": "Amazon Advertising profile ID"},
                "report_date": {
                    "type": "string",
                    "description": "Date range string, e.g. '2025-04-02 - 2025-05-01'. No range-length restriction.",
                },
                "asin": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "string"}},
                    ],
                    "description": "ASIN or list of ASINs to filter by (optional)",
                },
                "search_type": {
                    "type": "string",
                    "description": "Grouping dimension: 'campaign_name' (default), 'ad_group', etc.",
                },
                "date_key": {
                    "type": "string",
                    "description": "Time granularity: 'day' (default) or 'month'",
                },
                "is_daily": {
                    "type": "integer",
                    "description": "1 = include per-day rows (default), 0 = aggregate only",
                },
                "record_key": {
                    "type": "string",
                    "description": "Record scope: 'total' (default)",
                },
                "page":       {"type": "integer", "description": "Page number, 1-based (default: 1)"},
                "length":     {
                    "type": "integer",
                    "description": "Rows per page. Range: 25–500 (default: 50)",
                    "minimum": 25,
                    "maximum": 500,
                },
                "fetch_all":  {"type": "boolean", "description": "Auto-paginate and merge all pages (default: false)"},
                "provider":   {"type": "string",  "description": "ERP provider name (default: lingxing)"},
            },
            "required": ["profile_id", "report_date"],
        },
    ),
]

_TOOL_RETURNS = {
    "erp_inventory":             "ERP real-time inventory levels for a SKU",
    "erp_purchase_orders":       "ERP inbound purchase order list with status and ETA",
    "erp_sales_orders":          "ERP recent sales order list with qty and dates",
    "erp_sp_campaign_ad_report": "SP campaign ad report: clicks, impressions, orders, spends, sales, acos, roas per campaign/day",
}

for tool in erp_tools:
    tool_registry.register_tool(
        tool, handle_erp_tool,
        category="DATA",
        returns=_TOOL_RETURNS.get(tool.name, "ERP data"),
    )
