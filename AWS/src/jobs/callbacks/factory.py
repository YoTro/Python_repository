from __future__ import annotations
import logging
from typing import Optional

from src.core.models.request import CallbackConfig
from src.jobs.callbacks.base import JobCallback
from src.jobs.callbacks.feishu import FeishuCallback
from src.jobs.callbacks.csv_callback import CSVCallback
from src.jobs.callbacks.mcp_callback import MCPCallback

logger = logging.getLogger(__name__)

class CallbackFactory:
    """
    Factory for instantiating callbacks based on the UnifiedRequest's CallbackConfig.
    """
    @staticmethod
    def create(config: Optional[CallbackConfig]) -> Optional[JobCallback]:
        if not config:
            return None
            
        cb_type = config.type.lower()
        
        if cb_type == "feishu_bitable" or cb_type == "feishu_card":
            output_mode = "card" if cb_type == "feishu_card" else "bitable"
            return FeishuCallback(
                chat_id=config.target,
                output_mode=output_mode,
                **config.options
            )
        elif cb_type == "csv":
            return CSVCallback(output_path=config.target)
        elif cb_type == "mcp":
            return MCPCallback()
        elif cb_type == "composite":
            logger.warning("Composite callback type requested but not fully implemented.")
            return None
        elif cb_type == "json":
            logger.warning("JSON callback type requested but not fully implemented.")
            return None
        else:
            logger.error(f"Unknown callback type: {cb_type}")
            return None
