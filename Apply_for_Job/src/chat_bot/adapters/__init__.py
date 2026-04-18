"""
Adapter registry — maps platform name strings to adapter classes.

To add a new platform:
  1. Create adapters/<platform>.py with a class that extends PlatformAdapter
  2. Register it here
"""
from .zhipin   import ZhipinAdapter
from .lagou    import LagouAdapter
from .liepin   import LiepinAdapter
from .linkedin import LinkedInAdapter

REGISTRY = {
    "zhipin":   ZhipinAdapter,
    "lagou":    LagouAdapter,
    "liepin":   LiepinAdapter,
    "linkedin": LinkedInAdapter,
}

SUPPORTED = list(REGISTRY.keys())


def get_adapter_cls(platform: str):
    """
    Return the adapter class for the given platform name.
    Raises ValueError for unknown platforms.
    """
    name = platform.lower().strip()
    if name not in REGISTRY:
        raise ValueError(
            f"Unknown platform '{platform}'. "
            f"Supported: {', '.join(SUPPORTED)}"
        )
    return REGISTRY[name]
