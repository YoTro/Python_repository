from __future__ import annotations
import contextvars
from typing import Dict, Any, Optional

# Context variable to hold the current execution context (chat_id, user_id, etc.)
_execution_context: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar("_execution_context", default={})

class ContextPropagator:
    """
    Utility for propagating execution context across asynchronous boundaries.
    Used for Task Context Propagation (e.g., automatically resolving chat_id).
    """
    
    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """Retrieve a value from the current context."""
        return _execution_context.get().get(key, default)

    @staticmethod
    def set(key: str, value: Any) -> None:
        """Set a value in the current context (modifies the current dict)."""
        ctx = _execution_context.get().copy()
        ctx[key] = value
        _execution_context.set(ctx)

    @staticmethod
    def set_all(context: Dict[str, Any]) -> contextvars.Token:
        """Set the entire context. Returns a token to reset it."""
        return _execution_context.set(context.copy())

    @staticmethod
    def reset(token: contextvars.Token) -> None:
        """Reset the context to the previous state using a token."""
        _execution_context.reset(token)

    @staticmethod
    def get_all() -> Dict[str, Any]:
        """Get the entire current context."""
        return _execution_context.get().copy()
