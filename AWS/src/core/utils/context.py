from __future__ import annotations

import contextvars
from typing import Any

# Context variable to hold the current execution context (chat_id, user_id, etc.)
# Default is None (not a mutable {}); callers use _execution_context.get({}) as fallback.
_execution_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "_execution_context", default=None
)

_EMPTY: dict[str, Any] = {}


class ContextPropagator:
    """
    Utility for propagating execution context across asynchronous boundaries.
    Used for Task Context Propagation (e.g., automatically resolving chat_id).
    """

    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """Retrieve a value from the current context."""
        return (_execution_context.get() or _EMPTY).get(key, default)

    @staticmethod
    def set(key: str, value: Any) -> None:
        """Set a value in the current context (modifies the current dict)."""
        ctx = (_execution_context.get() or {}).copy()
        ctx[key] = value
        _execution_context.set(ctx)

    @staticmethod
    def set_all(context: dict[str, Any]) -> contextvars.Token:
        """Set the entire context. Returns a token to reset it."""
        return _execution_context.set(context.copy())

    @staticmethod
    def reset(token: contextvars.Token) -> None:
        """Reset the context to the previous state using a token."""
        _execution_context.reset(token)

    @staticmethod
    def get_all() -> dict[str, Any]:
        """Get the entire current context."""
        return (_execution_context.get() or {}).copy()
