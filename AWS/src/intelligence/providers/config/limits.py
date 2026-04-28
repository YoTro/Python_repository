"""
Model limits loader — reads max_output_tokens (and future limits) from
the per-provider pricing JSON files so that hard API ceilings are config-
driven rather than scattered across provider classes.

Usage:
    from src.intelligence.providers.config.limits import get_max_output_tokens

    ceiling = get_max_output_tokens("claude", "claude-sonnet-4-6")
    # → 16384  (longest-prefix match against model_limits section)
"""
from __future__ import annotations
import json
import os
import functools

_CONFIG_DIR = os.path.dirname(__file__)
_FALLBACK_MAX_OUTPUT = 8_192


@functools.lru_cache(maxsize=None)
def _load(provider: str) -> dict:
    path = os.path.join(_CONFIG_DIR, f"{provider}_pricing.json")
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("model_limits", {})
    except Exception:
        return {}


def get_max_output_tokens(provider: str, model: str) -> int:
    """Return the API hard ceiling for output tokens for *model*.

    Lookup order:
      1. Exact key match in model_limits
      2. Longest prefix key that is a prefix of *model*
      3. ``_default`` entry in model_limits
      4. Module-level fallback (8 192)
    """
    limits = _load(provider)

    # 1. Exact match
    entry = limits.get(model)
    if isinstance(entry, dict):
        val = entry.get("max_output_tokens")
        if val is not None:
            return int(val)

    # 2. Longest prefix match (e.g. "claude-opus-4" matches "claude-opus-4-6")
    candidates = [
        (key, ent)
        for key, ent in limits.items()
        if not key.startswith("_") and isinstance(ent, dict) and model.startswith(key)
    ]
    if candidates:
        best_key, best_entry = max(candidates, key=lambda x: len(x[0]))
        val = best_entry.get("max_output_tokens")
        if val is not None:
            return int(val)

    # 3. Default entry
    default = limits.get("_default")
    if isinstance(default, dict):
        val = default.get("max_output_tokens")
        if val is not None:
            return int(val)

    return _FALLBACK_MAX_OUTPUT
