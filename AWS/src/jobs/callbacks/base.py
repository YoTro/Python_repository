from __future__ import annotations
"""
JobCallback ABC — output strategy interface.

Different entry points use different callbacks:
  FeishuCallback  — progress messages + Bitable output
  CSVCallback     — CSV file output
  MCPCallback     — structured JSON for MCP clients
"""

from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Set

class CallbackCapability(Enum):
    """Capabilities that a specific output channel might support."""
    MARKDOWN = auto()
    IMAGE_DISPLAY = auto()
    INTERACTIVE_BUTTONS = auto()
    FORM_INPUT = auto()

class JobCallback(ABC):
    """Abstract callback for job lifecycle events."""

    @property
    def capabilities(self) -> Set[CallbackCapability]:
        """Declare the capabilities of this callback channel. Defaults to Markdown only."""
        return {CallbackCapability.MARKDOWN}

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
    async def on_error(self, error: Exception, job_id: str = None) -> None:
        """
        Called when the workflow fails.

        :param error:  The exception that caused the failure.
        :param job_id: The job identifier — present when a checkpoint exists and
                       the job can be resumed via JobManager.resume_from_checkpoint().
        """
        ...

    async def notify(self, message: str) -> None:
        """
        Send a plain-text notification to the user on this channel.

        Used for system-level messages (e.g. rate-limit rejection, job queued)
        that are not tied to a specific step or error.  Default implementation
        delegates to on_progress so all existing callbacks work without changes.
        Subclasses may override for a lighter-weight send (e.g. a simple chat
        message instead of a full progress card).
        """
        await self.on_progress(step_index=0, total_steps=1, step_name="notify", message=message)
