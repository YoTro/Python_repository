from __future__ import annotations
"""
Workflow configuration management.

Two-layer config merge (single-user version):
  1. workflow_defaults.yaml — baseline defaults per workflow
  2. job_override — per-job parameter overrides

Upgrade path to multi-user: add tenant layer between 1 and 2.
"""

import os
import copy
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Default config file path
_DEFAULTS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "config", "workflow_defaults.yaml"
)

_defaults_cache: Optional[dict] = None


def _load_defaults() -> dict:
    """Load workflow defaults from YAML file. Cached after first load."""
    global _defaults_cache
    if _defaults_cache is not None:
        return _defaults_cache

    try:
        import yaml
        if os.path.exists(_DEFAULTS_PATH):
            with open(_DEFAULTS_PATH, "r") as f:
                _defaults_cache = yaml.safe_load(f) or {}
                logger.debug(f"Loaded workflow defaults from {_DEFAULTS_PATH}")
        else:
            logger.warning(f"Workflow defaults file not found: {_DEFAULTS_PATH}")
            _defaults_cache = {}
    except ImportError:
        logger.warning("PyYAML not installed, using empty defaults")
        _defaults_cache = {}
    except Exception as e:
        logger.warning(f"Failed to load workflow defaults: {e}")
        _defaults_cache = {}

    return _defaults_cache


def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge two dicts. Override values win.
    Lists are replaced, not appended.
    """
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def merge_config(workflow_name: str, job_override: dict = None) -> dict:
    """
    Merge config layers for a workflow.

    Resolution order (highest priority wins):
      job_override > workflow_defaults

    Args:
        workflow_name: Name of the workflow to load defaults for.
        job_override: Per-job parameter overrides.

    Returns:
        Merged configuration dict.
    """
    defaults = _load_defaults()
    workflow_defaults = defaults.get(workflow_name, {})

    if job_override:
        return deep_merge(workflow_defaults, job_override)
    return copy.deepcopy(workflow_defaults)


def get_default(workflow_name: str, key: str, fallback: Any = None) -> Any:
    """Get a single default value for a workflow."""
    defaults = _load_defaults()
    workflow_defaults = defaults.get(workflow_name, {})
    return workflow_defaults.get(key, fallback)
