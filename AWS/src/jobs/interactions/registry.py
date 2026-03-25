from __future__ import annotations
import logging
from typing import Callable, Dict, Any, Awaitable

logger = logging.getLogger(__name__)

class InteractionRegistry:
    """
    Central registry for handling asynchronous interactive events (e.g., button clicks).
    Decouples entry points from job resumption logic.
    """
    _handlers: Dict[str, Callable[[Dict[str, Any]], Awaitable[Any]]] = {}

    @classmethod
    def register(cls, action_name: str):
        """Decorator to register a handler for a specific action."""
        def decorator(fn: Callable[[Dict[str, Any]], Awaitable[Any]]):
            cls._handlers[action_name] = fn
            logger.debug(f"Registered interaction handler: {action_name}")
            return fn
        return decorator

    @classmethod
    async def handle(cls, action_name: str, payload: Dict[str, Any]) -> Any:
        """Routes an event to its registered handler."""
        handler = cls._handlers.get(action_name)
        if not handler:
            logger.error(f"No handler found for interaction: {action_name}")
            raise ValueError(f"Unknown interaction: {action_name}")
        
        return await handler(payload)

    @classmethod
    def list_actions(cls) -> list[str]:
        """List all registered actions."""
        return list(cls._handlers.keys())
