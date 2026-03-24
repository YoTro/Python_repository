from __future__ import annotations
import os
import json
import logging
import asyncio
from datetime import datetime
from typing import Any, Optional, Dict, List
from enum import Enum
from ..providers.factory import ProviderFactory
from ..providers.base import BaseLLMProvider
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

class RouterLogger:
    """Handles persistent logging of classification results for future fine-tuning."""
    def __init__(self, log_path: str = None):
        self.log_path = log_path or os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "data", "intelligence", "raw_prompts.jsonl"
        )
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    async def log(self, entry: Dict[str, Any]):
        """Append a single classification entry to the JSONL file asynchronously."""
        try:
            # We use a simple non-blocking-ish write for JSONL append
            # For high-volume, consider a dedicated logging background task
            entry["timestamp"] = datetime.utcnow().isoformat()
            line = json.dumps(entry, ensure_ascii=False) + "\n"
            
            # Use run_in_executor to avoid blocking the event loop with file I/O
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._append_to_file, line)
        except Exception as e:
            logger.error(f"RouterLogger failed to log entry: {e}")

    def _append_to_file(self, line: str):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line)

class IntelligenceRouter:
    """
    Model-as-a-Router: Intelligently routes tasks to the most cost-effective provider.
    Now includes heuristic pre-screening (<1ms) and data collection for future distillation.
    """
    
    def __init__(self, local_provider: Optional[BaseLLMProvider] = None, cloud_provider: Optional[BaseLLMProvider] = None):
        self.router_logger = RouterLogger()
        
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
        loadsion_id = kwargs.get("session_id", "unknown_session")
        tenant_id = kwargs.get("tenant_id", "default")
        user_id = kwargs.get("user_id", "default")

        # 1. Classification (with heuristics and logging)
        if not category:
            category, confidence = await self._classify_task(prompt)
            logger.info(f"Task auto-classified as: {category} (Confidence: {confidence if confidence else 'N/A'})")
            
            # Log the classification for future model distillation/fine-tuning
            await self.router_logger.log({
                "tenant_id": tenant_id,
                "user_id": user_id,
                "session_id": session_id,
                "prompt_preview": prompt[:300],
                "prompt_len": len(prompt),
                "final_category": category.value,
                "confidence": confidence,
                "metadata": kwargs.get("metadata", {})
            })

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

    async def _classify_task(self, prompt: str) -> tuple[TaskCategory, Optional[float]]:
        """
        Classifies the task using zero-cost heuristics first, then falling back to local model.
        Returns: (Category, Confidence Score)
        """
        # Step 1: Heuristic Pre-screening (<1ms)
        heuristic_cat = self._run_heuristics(prompt)
        if heuristic_cat:
            return heuristic_cat, 1.0  # Heuristics are considered high-confidence triggers

        # Step 2: Fallback to Local Model Classification
        if not self.local:
            return TaskCategory.DEEP_REASONING, None

        classification_prompt = f"""
        Classify the task: 'simple_cleaning', 'extraction', 'reasoning', 'creative', or 'simple_chat'.
        Return ONLY the category name.
        TASK: {prompt[:400]}
        CATEGORY:
        """
        try:
            # Note: Current BaseLLMProvider doesn't return confidence directly yet, 
            # but we prepare the architecture to capture it from metadata or logprobs
            result_obj = await self.local.generate_text(classification_prompt)
            result = result_obj.text.lower().strip()
            confidence = result_obj.metadata.get("confidence") # If provider supports logprobs
            
            for cat in TaskCategory:
                if cat.value in result:
                    return cat, confidence
            return TaskCategory.DEEP_REASONING, confidence
        except Exception as e:
            logger.warning(f"Classification failed: {e}. Defaulting to DEEP_REASONING.")
            return TaskCategory.DEEP_REASONING, None

    def _run_heuristics(self, prompt: str) -> Optional[TaskCategory]:
        """
        Fast keyword-based and length-based pre-screening.
        
        PRIORITY ORDER (Top-down):
        1. Complexity/Constraint: Overly long prompts (>4000) are forced to DEEP_REASONING to 
           prevent local model context collapse.
        2. Strong Intent: Specific keywords like 'analyze' or 'strategy' trigger DEEP_REASONING.
        3. Constrained Extraction: 'extract' keywords within safe length (<2000) trigger EXTRACTION.
        4. Specific Cleaning: 'clean'/'format' keywords within small context (<1000) trigger SIMPLE_CLEANING.
        """
        p_lower = prompt.lower()
        p_len = len(prompt)

        # Priority 1: High complexity / Large context
        if p_len > 4000:
            return TaskCategory.DEEP_REASONING

        # Priority 2: High-reasoning intent
        reasoning_keys = ["analyze", "compare", "strategy", "why", "logic", "optimize", "evaluate", "summarize", "分析", "对比", "策略", "评估", "总结"]
        if any(k in p_lower for k in reasoning_keys):
            return TaskCategory.DEEP_REASONING

        # Priority 3: Medium-complexity extraction
        extraction_keys = ["extract", "find", "phone number", "email", "regex", "list all", "parse", "提取", "查找", "抓取"]
        if any(k in p_lower for k in extraction_keys) and p_len < 2000:
            return TaskCategory.DATA_EXTRACTION

        # Priority 4: Low-complexity string manipulation
        cleaning_keys = ["clean", "format", "strip", "lowercase", "uppercase", "json-ify", "remove", "清洗", "格式化", "去重"]
        if any(k in p_lower for k in cleaning_keys) and p_len < 1000:
            return TaskCategory.SIMPLE_CLEANING

        return None

    async def record_feedback(self, session_id: str, ground_truth: TaskCategory, reason: str = None):
        """
        Records manual or cloud-verified feedback to track misclassification rates.
        This allows us to identify which heuristic rules are failing most often.
        """
        await self.router_logger.log({
            "event_type": "router_feedback",
            "session_id": session_id,
            "ground_truth": ground_truth.value,
            "reason": reason,
            "is_correction": True
        })
        logger.info(f"Feedback recorded for session {session_id}: Verified as {ground_truth.value}")

    async def batch_route_and_execute(self, prompts: list[str], category: Optional[TaskCategory] = None, **kwargs) -> list[Any]:
        if not prompts:
            return []
            
        if not category:
            category, _ = await self._classify_task(prompts[0])
            logger.info(f"Batch task auto-classified based on first item: {category}")

        if self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
            logger.info(f"Routing batch of {len(prompts)} tasks to LOCAL model")
            responses = await self.local.batch_generate_text(prompts, **kwargs)
            for response in responses:
                response.text = OutputParser.clean_for_feishu(response.text)
            return responses
        
        else:
            logger.info(f"Routing batch of {len(prompts)} tasks to CLOUD model")
            schema = kwargs.pop("schema", None)
            if schema:
                return await self.cloud.batch_generate_structured(prompts, schema=schema, **kwargs)
            return await self.cloud.batch_generate_text(prompts, **kwargs)
