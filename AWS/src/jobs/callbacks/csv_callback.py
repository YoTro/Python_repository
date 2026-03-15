from __future__ import annotations
"""
CSVCallback — writes workflow results to a CSV file.
"""

import logging
from datetime import datetime

from src.jobs.callbacks.base import JobCallback

logger = logging.getLogger(__name__)


class CSVCallback(JobCallback):
    """Callback that outputs results to a CSV file."""

    def __init__(self, output_path: str = None):
        self.output_path = output_path

    async def on_progress(
        self, step_index: int, total_steps: int, step_name: str, message: str = ""
    ) -> None:
        logger.info(f"[{step_index}/{total_steps}] {step_name} {message}")

    async def on_complete(self, result) -> None:
        items = result.final_items if hasattr(result, "final_items") else []
        workflow_name = result.name if hasattr(result, "name") else "workflow"

        if not self.output_path:
            date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_path = f"output/{workflow_name}_{date_str}.csv"

        try:
            from src.core.utils.csv_helper import CSVHelper
            CSVHelper.save_to_csv(items, self.output_path)
            logger.info(f"Results saved to {self.output_path} ({len(items)} items)")
        except ImportError:
            # Fallback if csv_helper not moved yet
            from src.core.utils.csv_helper import CSVHelper
            CSVHelper.save_to_csv(items, self.output_path)
            logger.info(f"Results saved to {self.output_path} ({len(items)} items)")

    async def on_error(self, error: Exception) -> None:
        logger.error(f"Workflow failed: {error}")
