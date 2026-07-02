from . import handlers  # noqa: F401 — registers all @InteractionRegistry.register decorators
from .registry import InteractionRegistry

__all__ = ["InteractionRegistry"]
