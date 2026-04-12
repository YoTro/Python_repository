from __future__ import annotations
import json
import os
import logging
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = "config/settings.json"


class ConfigHelper:
    """
    Centralised configuration access backed by ``config/settings.json``.

    All static configuration (scraper tuning, rate limit parameters, integration
    settings) lives in one file.  Access is via dot-separated key paths:

        ConfigHelper.get("rate_limits.source_limits.sellersprite.burst", 3)

    Runtime secrets (API keys, passwords) stay in environment variables and are
    never stored here.  Runtime state (counters, tokens) belongs in a StateStore,
    not in config.

    Lazy-loads on first access; call ``reload()`` to pick up file changes at
    runtime (useful in tests or when hot-reloading config without restart).
    """

    _config: Dict[str, Any] = {}
    _is_loaded: bool = False
    _config_path: str = _DEFAULT_CONFIG_PATH

    # ── Loading ───────────────────────────────────────────────────────────────

    @classmethod
    def load_config(cls, config_path: str = _DEFAULT_CONFIG_PATH) -> None:
        """Load (or reload) configuration from *config_path*."""
        cls._config_path = config_path
        if not os.path.exists(config_path):
            logger.warning(f"[ConfigHelper] Config file not found: {config_path}. Using empty defaults.")
            cls._config = {}
            cls._is_loaded = True
            return
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cls._config = json.load(f)
            logger.info(f"[ConfigHelper] Loaded config from {config_path}")
        except Exception as e:
            logger.error(f"[ConfigHelper] Failed to load {config_path}: {e}")
            cls._config = {}
        cls._is_loaded = True

    @classmethod
    def reload(cls) -> None:
        """Re-read the config file from disk (picks up changes without restart)."""
        cls._is_loaded = False
        cls.load_config(cls._config_path)

    @classmethod
    def _ensure_loaded(cls) -> None:
        if not cls._is_loaded:
            cls.load_config()

    # ── Access ────────────────────────────────────────────────────────────────

    @classmethod
    def get(cls, key_path: str, default: Any = None) -> Any:
        """
        Retrieve a value using a dot-separated key path.

            ConfigHelper.get("rate_limits.source_limits.sellersprite.burst", 3)
        """
        cls._ensure_loaded()
        node = cls._config
        for key in key_path.split("."):
            if isinstance(node, dict) and key in node:
                node = node[key]
            else:
                return default
        return node

    @classmethod
    def get_section(cls, section: str) -> dict:
        """Return a top-level config section as a dict (empty dict if absent)."""
        value = cls.get(section, {})
        return value if isinstance(value, dict) else {}

    # ── Feishu helpers ────────────────────────────────────────────────────────

    @classmethod
    def get_feishu_bot(cls, bot_name: str) -> Optional[Dict[str, str]]:
        """
        Load Feishu bot credentials from environment variables.
        Naming convention: FEISHU_{BOT_NAME_UPPER}_{FIELD}
        Returns None if APP_ID is not set.
        """
        prefix = f"FEISHU_{bot_name.upper()}_"
        app_id = os.getenv(f"{prefix}APP_ID", "")
        if not app_id:
            return None
        return {
            "app_id":             app_id,
            "app_secret":         os.getenv(f"{prefix}APP_SECRET", ""),
            "user_access_token":  os.getenv(f"{prefix}USER_ACCESS_TOKEN", ""),
            "webhook_url":        os.getenv(f"{prefix}WEBHOOK_URL", ""),
        }
