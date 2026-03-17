from __future__ import annotations
class MCPError(Exception):
    """Base class for all MCP related errors."""
    def __init__(self, message: str, hint: str = ""):
        super().__init__(message)
        self.message = message
        self.hint = hint

class ToolNotFoundError(MCPError):
    """Raised when the requested tool does not exist."""
    pass

class ToolExecutionError(MCPError):
    """Raised when a tool is found but fails during execution (e.g., Scraper blocked)."""
    pass

class ValidationError(MCPError):
    """Raised when input arguments do not meet requirements."""
    pass

class ResourceNotFoundError(MCPError):
    """Raised when a requested resource (ASIN, File) is missing."""
    pass
