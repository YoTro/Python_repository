from __future__ import annotations
import logging
from typing import Any, Optional, Dict
from enum import Enum
from ..providers.factory import ProviderFactory
from ..dto import LLMResponse
from ..parsers.markdown_cleaner import OutputParser

from ..fallback import FallbackHandler, FailureType
from google.api_core import exceptions as google_exceptions
from anthropic import APIStatusError

logger = logging.getLogger(__name__)

class TaskCategory(Enum):
    SIMPLE_CLEANING = "simple_cleaning"
    DATA_EXTRACTION = "extraction"
    DEEP_REASONING = "reasoning"
    CREATIVE_WRITING = "creative"
    SIMPLE_CHAT = "simple_chat"

class IntelligenceRouter:
    """
    Model-as-a-Router: Intelligently routes tasks to the most cost-effective provider.
    """
    
    def __init__(self, local_provider: Optional[BaseLLMProvider] = None, cloud_provider: Optional[BaseLLMProvider] = None):
        try:
            self.local = local_provider or ProviderFactory.get_provider("local")
        except Exception as e:
            logger.warning(f"Local provider failed to load: {e}. Some functionalities might be limited.")
            self.local = None
        
        try:
            self.cloud = cloud_provider or ProviderFactory.get_provider("gemini")
        except Exception as e:
            logger.error(f"FATAL: Cloud provider failed to load: {e}. Full functionality will be degraded.")
            self.cloud = None

    async def route_and_execute(self, prompt: str, category: Optional[TaskCategory] = None, **kwargs) -> LLMResponse:
        session_id = kwargs.pop("session_id", "unknown_session") # For fallback context

        if not category:
            category = await self._classify_task(prompt)
            logger.info(f"Task auto-classified as: {category}")

        # --- LOCAL MODEL ROUTE ---
        if self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
            logger.info(f"Routing task to LOCAL model (prompt length: {len(prompt)} chars)")
            response = await self.local.generate_text(prompt, **kwargs)
            logger.info(f"LOCAL model returned response: {len(response.text)} chars")
            response.text = OutputParser.clean_for_feishu(response.text)
            return response
        
        # --- CLOUD MODEL ROUTE (with Fallback) ---
        else:
            if not self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
                logger.warning("Local model not available. Routing to cloud model instead.")
            
            if not self.cloud:
                return await FallbackHandler.handle(FailureType.CLOUD_API_UNAVAILABLE, context={"session_id": session_id, "reason": "Cloud provider not loaded"})

            logger.info("Routing task to CLOUD model")
            schema = kwargs.pop("schema", None)
            
            try:
                if schema:
                    return await self.cloud.generate_structured(prompt, schema=schema, **kwargs)
                return await self.cloud.generate_text(prompt, **kwargs)
            except (google_exceptions.ServiceUnavailable, APIStatusError) as e:
                return await FallbackHandler.handle(
                    FailureType.CLOUD_API_UNAVAILABLE, 
                    context={"session_id": session_id, "error": str(e)}
                )
            except (google_exceptions.PermissionDenied, google_exceptions.Unauthenticated) as e:
                 logger.critical(f"Cloud API Authentication Error: {e}")
                 return LLMResponse(text="FATAL: Cloud API authentication failed. Check credentials.", provider_name="fallback", model_name="auth-error")
            except Exception as e:
                 logger.error(f"Unhandled Cloud API error: {e}")
                 raise e

    async def batch_route_and_execute(self, prompts: list[str], category: Optional[TaskCategory] = None, **kwargs) -> list[Any]:
        if not prompts:
            return []
            
        if not category:
            category = await self._classify_task(prompts[0])
            logger.info(f"Batch task auto-classified based on first item: {category}")

        if self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
            logger.info(f"Routing batch of {len(prompts)} tasks to LOCAL model")
            responses = await self.local.batch_generate_text(prompts, **kwargs)
            for response in responses:
                response.text = OutputParser.clean_for_feishu(response.text)
            return responses
        
        else:
            if not self.local and category in [
                TaskCategory.SIMPLE_CLEANING,
                TaskCategory.DATA_EXTRACTION,
                TaskCategory.SIMPLE_CHAT
            ]:
                logger.warning("Local model not available. Routing batch to cloud model instead.")

            logger.info(f"Routing batch of {len(prompts)} tasks to CLOUD model")
            schema = kwargs.pop("schema", None)
            if schema:
                return await self.cloud.batch_generate_structured(prompts, schema=schema, **kwargs)
            return await self.cloud.batch_generate_text(prompts, **kwargs)

    async def _classify_task(self, prompt: str) -> TaskCategory:
        if not self.local:
            logger.warning("Local model unavailable for classification, defaulting to DEEP_REASONING.")
            return TaskCategory.DEEP_REASONING

        classification_prompt = f"""
        Classify the task: 'simple_cleaning', 'extraction', 'reasoning', 'creative', or 'simple_chat'.
        TASK: {prompt[:300]}
        CATEGORY:
        """
        try:
            result_obj = await self.local.generate_text(classification_prompt)
            result = result_obj.text.lower().strip()
            
            for cat in TaskCategory:
                if cat.value in result:
                    return cat
            return TaskCategory.DEEP_REASONING
        except Exception as e:
            logger.warning(f"Classification failed: {e}. Defaulting to DEEP_REASONING.")
            return TaskCategory.DEEP_REASONING
