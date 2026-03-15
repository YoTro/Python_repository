from __future__ import annotations
import json
import logging
import asyncio
from mcp.types import Tool, TextContent
from src.registry.tools import tool_registry
from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
from src.core.utils.cookie_helper import AmazonCookieHelper
from src.core.data_cache import data_cache

logger = logging.getLogger("mcp-amazon")

async def handle_amazon_tool(name: str, arguments: dict) -> list[TextContent]:
    """
    Dispatcher for atomic Amazon tools.
    """
    if name == "get_amazon_bestsellers":
        url = arguments.get("url")
        extractor = BestSellersExtractor()
        products = await extractor.get_bestsellers(url)
        
        # L1 Action: Write to Data Cache
        for p in products:
            if isinstance(p, dict) and "asin" in p:
                data_cache.set("amazon", p["asin"], p)
        
        return [TextContent(type="text", text=json.dumps(products, indent=2, ensure_ascii=False))]

    elif name == "refresh_amazon_cookies":
        headless = arguments.get("headless", False) # Default to headed for this tool to allow login
        wait_for_login = arguments.get("wait_for_login", True)
        
        helper = AmazonCookieHelper(headless=headless)
        # Pass the manual wait flag
        cookies = await asyncio.to_thread(helper.fetch_fresh_cookies, wait_for_manual=wait_for_login)
        
        if cookies and 'session-id' in cookies:
            return [TextContent(type="text", text="✅ Successfully refreshed Amazon cookies. Session captured.")]
        else:
            return [TextContent(type="text", text="❌ Failed to capture Amazon cookies. Please ensure you logged in if requested.")]

    return [TextContent(type="text", text=f"Unknown Amazon tool: {name}")]

# Register Tools
amazon_tools = [
    Tool(
        name="get_amazon_bestsellers",
        description="Scrape Amazon's Best Sellers list from a given URL.",
        inputSchema={
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "The Amazon Best Sellers category URL"}
            },
            "required": ["url"]
        }
    ),
    Tool(
        name="refresh_amazon_cookies",
        description="Launch a browser to refresh Amazon session cookies. Highly recommended to run this in headed mode if blocked or if login is needed.",
        inputSchema={
            "type": "object",
            "properties": {
                "headless": {"type": "boolean", "default": False, "description": "Set to false to see the browser and login manually."},
                "wait_for_login": {"type": "boolean", "default": True, "description": "If true, waits up to 60s for manual login completion."}
            }
        }
    )
]

for tool in amazon_tools:
    tool_registry.register_tool(tool, handle_amazon_tool)
