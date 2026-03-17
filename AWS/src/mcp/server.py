from __future__ import annotations
import asyncio
import logging
import sys
import os
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Adjust sys.path to ensure project-wide imports work
# This is necessary when running the script directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Use absolute imports instead of relative to avoid 'no known parent package' errors
from src.mcp.exceptions import MCPError, ToolNotFoundError, ToolExecutionError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

class AWSHelperServer:
    def __init__(self):
        self.server = Server("aws-market-intelligence")
        self._setup_handlers()

    def _setup_handlers(self):
        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            from src.registry.tools import tool_registry
            return tool_registry.get_all_tools()

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict) -> list[TextContent]:
            from src.registry.tools import tool_registry
            logger.info(f"Incoming tool call: {name}")
            try:
                return await tool_registry.call_tool(name, arguments)
            except ToolNotFoundError as e:
                return [TextContent(type="text", text=f"❌ Error: {e.message}")]
            except ToolExecutionError as e:
                return [TextContent(type="text", text=f"⚠️ Execution failed: {e.message}")]
            except Exception as e:
                logger.exception(f"Unhandled error in {name}")
                return [TextContent(type="text", text=f"🆘 Critical error: {str(e)}")]

        @self.server.list_resources()
        async def list_resources():
            from src.registry.resources import resource_registry
            return resource_registry.get_all_resources()

        @self.server.read_resource()
        async def read_resource(uri: str) -> str:
            from src.registry.resources import resource_registry
            logger.info(f"Reading resource: {uri}")
            return resource_registry.read_resource(uri)

        @self.server.list_prompts()
        async def list_prompts():
            from src.registry.prompts import prompt_registry
            return prompt_registry.get_all_prompts()

    async def run(self):
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options()
            )

if __name__ == "__main__":
    server_instance = AWSHelperServer()
    asyncio.run(server_instance.run())
