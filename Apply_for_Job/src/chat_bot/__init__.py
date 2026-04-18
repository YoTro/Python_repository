"""
src/chat_bot - Universal recruitment platform chat bot

Conducts automated conversations as a job seeker on any supported
platform to collect structured data for analysis.

Supported platforms: zhipin, lagou, liepin, linkedin

Usage:
    from src.chat_bot import run_chat_sessions

    run_chat_sessions(
        platform='zhipin',
        output_path='data/raw/zhipin_chat.csv',
        max_turns=6,
        max_chats=20,
        unread_only=True,
    )
"""
from .core import ChatBotCore
from .base import PlatformAdapter
from .adapters import REGISTRY, SUPPORTED, get_adapter_cls


def run_chat_sessions(
    platform: str,
    output_path: str,
    max_turns: int = 6,
    max_chats: int = 50,
    reply_timeout: int = 180,
    unread_only: bool = False,
) -> list:
    """
    Connect to an existing Chrome session (localhost:9222) and run
    automated Q&A sessions on the specified platform.

    Parameters
    ----------
    platform      : one of 'zhipin' / 'lagou' / 'liepin' / 'linkedin'
    output_path   : CSV file path for results
    max_turns     : max questions per conversation
    max_chats     : stop after this many conversations
    reply_timeout : seconds to wait for HR reply before giving up
    unread_only   : if True, skip conversations with no unread messages
    """
    from src.chat_bot.llm import get_provider

    adapter_cls = get_adapter_cls(platform)
    provider    = get_provider()

    print(
        f"[chat-bot] platform={platform} max_turns={max_turns} "
        f"max_chats={max_chats} reply_timeout={reply_timeout}s "
        f"unread_only={unread_only}"
    )
    print("[chat-bot] Chrome must be running with --remote-debugging-port=9222")

    core    = ChatBotCore(provider=provider, max_turns=max_turns, reply_timeout=reply_timeout)
    adapter = core.connect(adapter_cls)
    return core.run_all(
        adapter=adapter,
        output_path=output_path,
        max_chats=max_chats,
        unread_only=unread_only,
    )


__all__ = [
    "run_chat_sessions",
    "ChatBotCore",
    "PlatformAdapter",
    "REGISTRY",
    "SUPPORTED",
    "get_adapter_cls",
]
