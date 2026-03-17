from __future__ import annotations
"""
JobCallback ABC — output strategy interface.

Different entry points use different callbacks:
  FeishuCallback  — progress messages + Bitable output
  CSVCallback     — CSV file output
  MCPCallback     — structured JSON for MCP clients
"""

from abc import ABC, abstractmethod


class JobCallback(ABC):
    """Abstract callback for job lifecycle events."""

    @abstractmethod
    async def on_progress(
        self,
        step_index: int,
        total_steps: int,
        step_name: str,
        message: str = "",
    ) -> None:
        """Called after each step completes."""
        ...

    @abstractmethod
    async def on_complete(self, result) -> None:
        """Called when the workflow finishes successfully."""
        ...

    @abstractmethod
    async def on_error(self, error: Exception) -> None:
        """Called when the workflow fails."""
        ...
