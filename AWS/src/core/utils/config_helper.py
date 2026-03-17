from __future__ import annotations
import json
import os
import logging
from typing import Any, Dict, Optional
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class ConfigHelper:
    """
    Utility class to load and access application configuration from config/settings.json.
    """
    _config: Dict[str, Any] = {}
    _is_loaded: bool = False

    @classmethod
    def load_config(cls, config_path: str = "config/settings.json"):
        """
        Loads the configuration file into memory.
        """
        if not os.path.exists(config_path):
            logger.warning(f"Configuration file {config_path} not found. Using empty defaults.")
            cls._config = {}
            cls._is_loaded = True
            return

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                cls._config = json.load(f)
            logger.info(f"Successfully loaded configuration from {config_path}")
            cls._is_loaded = True
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_path}: {e}")
            cls._config = {}
            cls._is_loaded = True

    @classmethod
    def get(cls, key_path: str, default: Any = None) -> Any:
        """
        Retrieves a value from the configuration using a dot-separated key path.
        Example: ConfigHelper.get("scraper.max_retries", 5)
        """
        if not cls._is_loaded:
            cls.load_config()

        keys = key_path.split('.')
        current_dict = cls._config

        for key in keys:
            if isinstance(current_dict, dict) and key in current_dict:
                current_dict = current_dict[key]
            else:
                return default

        return current_dict

    @classmethod
    def get_feishu_bot(cls, bot_name: str) -> Optional[Dict[str, str]]:
        """
        Load Feishu bot credentials from environment variables.
        Env var naming: FEISHU_{BOT_NAME_UPPER}_{FIELD}
        Example for bot_name='amazon_bot': FEISHU_AMAZON_BOT_APP_ID, ...
        Returns None if APP_ID is not set (bot not configured).
        """
        prefix = f"FEISHU_{bot_name.upper()}_"
        app_id = os.getenv(f"{prefix}APP_ID", "")
        if not app_id:
            return None
        return {
            "app_id": app_id,
            "app_secret": os.getenv(f"{prefix}APP_SECRET", ""),
            "user_access_token": os.getenv(f"{prefix}USER_ACCESS_TOKEN", ""),
            "webhook_url": os.getenv(f"{prefix}WEBHOOK_URL", ""),
        }
