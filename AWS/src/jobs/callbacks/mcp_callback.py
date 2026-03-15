from __future__ import annotations
"""
MCPCallback — stores results for retrieval by MCP tool handlers.
"""

import logging
from typing import Optional

from src.jobs.callbacks.base import JobCallback

logger = logging.getLogger(__name__)


class MCPCallback(JobCallback):
    """
    Callback for MCP tool calls.
    Stores the result internally for synchronous retrieval.
    """

    def __init__(self):
        self._result = None
        self._error = None

    async def on_progress(
        self, step_index: int, total_steps: int, step_name: str, message: str = ""
    ) -> None:
        logger.info(f"[MCP] [{step_index}/{total_steps}] {step_name} {message}")

    async def on_complete(self, result) -> None:
        self._result = result
        items = result.final_items if hasattr(result, "final_items") else []
        logger.info(f"[MCP] Workflow completed with {len(items)} items")

    async def on_error(self, error: Exception) -> None:
        self._error = error
        logger.error(f"[MCP] Workflow failed: {error}")

    def get_result(self) -> Optional[dict]:
        """Retrieve stored result for MCP response."""
        if self._error:
            return {"success": False, "error": str(self._error)}
        if self._result:
            items = self._result.final_items if hasattr(self._result, "final_items") else []
            return {
                "success": True,
                "items": items,
                "total": len(items),
                "duration_ms": getattr(self._result, "total_duration_ms", 0),
            }
        return None
