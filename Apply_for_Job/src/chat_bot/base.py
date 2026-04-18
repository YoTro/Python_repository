"""
base.py - Abstract platform adapter interface

Each recruitment platform implements these 4 methods.
The rest of the chat logic lives in core.py and never touches platform DOM directly.
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class PlatformAdapter(ABC):
    """
    Encapsulates all DOM interactions for one recruitment platform.

    Subclasses must set:
        PLATFORM_NAME : str   — human-readable name ("BOSS直聘", "拉勾", ...)
        CHAT_URL      : str   — URL to navigate to for the chat inbox

    The tab (DrissionPage ChromiumTab) is injected by ChatBotCore.connect().
    """

    PLATFORM_NAME: str = "unknown"
    CHAT_URL: str = ""

    def __init__(self, tab) -> None:
        self._tab = tab

    # ── Required interface ────────────────────────────────────────────

    @abstractmethod
    def list_conversations(self) -> list[dict]:
        """
        Return all sidebar conversation items.

        Each item is a dict with keys:
          index   : int   — position in the sidebar (0-based)
          name    : str   — company or HR name
          job     : str   — job title (may be empty if not shown in sidebar)
          preview : str   — last message preview
          unread  : bool  — True if there are unread messages
        """

    @abstractmethod
    def open_conversation(self, index: int) -> dict:
        """
        Click the nth sidebar item and wait for the chat to load.

        Returns a dict with keys:
          job_title : str  — position title
          company   : str  — company name
          hr_name   : str  — recruiter name
        """

    @abstractmethod
    def read_messages(self) -> list[dict]:
        """
        Read all visible messages in the currently open chat.

        Each message is a dict:
          role : "hr" | "me"
          text : str
        """

    @abstractmethod
    def send_message(self, text: str) -> bool:
        """
        Type and send a message in the current chat.
        Returns True if the message was sent successfully.
        """

    # ── Optional overrideable helpers ─────────────────────────────────

    def message_count(self) -> int:
        """Number of visible messages (used for change detection)."""
        return len(self.read_messages())

    def platform_name(self) -> str:
        return self.PLATFORM_NAME
