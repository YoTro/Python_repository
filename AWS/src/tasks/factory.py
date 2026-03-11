from typing import Dict, Type
from .base_task import BaseTask
from .search_tasks import SalesTask, KeywordsRankTask, BestsellersTask
from .asin_tasks import (
    ReviewsTask, DetailsTask, FulfillmentTask, DimensionsTask, 
    RanksTask, ImagesTask, VideosTask, StockTask, ReviewCountTask, PastMonthSalesTask
)
from .seller_tasks import ProductNumTask, FeedbackTask
from .analysis_tasks import AnalyzeSimilarityTask
from .composite_tasks import FullAsinDetailsTask

class TaskFactory:
    _registry: Dict[str, Type[BaseTask]] = {
        "sales": SalesTask,
        "keywords_rank": KeywordsRankTask,
        "bestsellers": BestsellersTask,
        "reviews": ReviewsTask,
        "details": DetailsTask,
        "fulfillment": FulfillmentTask,
        "dimensions": DimensionsTask,
        "ranks": RanksTask,
        "images": ImagesTask,
        "videos": VideosTask,
        "stock": StockTask,
        "review_count": ReviewCountTask,
        "past_month_sales": PastMonthSalesTask,
        "product_num": ProductNumTask,
        "feedback": FeedbackTask,
        "analyze_similarity": AnalyzeSimilarityTask,
        "full_asin_details": FullAsinDetailsTask
    }

    @classmethod
    def get_task(cls, task_name: str) -> BaseTask:
        task_class = cls._registry.get(task_name)
        if not task_class:
            raise ValueError(f"Task '{task_name}' is not recognized.")
        return task_class()

    @classmethod
    def get_available_tasks(cls) -> list:
        return list(cls._registry.keys())
