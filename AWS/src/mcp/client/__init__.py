from __future__ import annotations
from .base import MCPClient
from .local import LocalMCPClient

# In our current architecture, the Workflow Engine uses a LocalMCPClient
# to call tools registered in the same process.
_default_client = LocalMCPClient()

def get_mcp_client() -> MCPClient:
    """Returns the globally configured MCP Client."""
    return _default_client
