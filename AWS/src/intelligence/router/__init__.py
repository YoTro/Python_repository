from __future__ import annotations
import os
import json
import logging
import asyncio
import time
from datetime import datetime
from typing import Any, Optional, Dict, List, Union
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
    """Handles persistent logging of classification and execution results."""
    def __init__(self, log_path: str = None):
        self.log_path = log_path or os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "data", "intelligence", "raw_prompts.jsonl"
        )
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)

    async def log_trace(self, entry: Dict[str, Any]):
        """Append a trace entry to the JSONL file asynchronously."""
        try:
            entry["timestamp"] = datetime.utcnow().isoformat()
            line = json.dumps(entry, ensure_ascii=False) + "\n"
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._append_to_file, line)
        except Exception as e:
            logger.error(f"RouterLogger failed to log trace: {e}")

    def _append_to_file(self, line: str):
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line)

class IntelligenceRouter:
    """
    Model-as-a-Router: Intelligently routes tasks to the most cost-effective provider.
    Includes full-lifecycle tracing for model distillation and routing optimization.
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
        session_id = kwargs.get("session_id", "unknown_session")
        tenant_id = kwargs.get("tenant_id", "default")
        user_id = kwargs.get("user_id", "default")
        
        start_time = time.time()
        classification_metadata = {}

        # 1. Classification
        if not category:
            category, confidence = await self._classify_task(prompt)
            classification_metadata = {"confidence": confidence, "auto_classified": True}
            logger.info(f"Task auto-classified as: {category} (Confidence: {confidence if confidence else 'N/A'})")

        # 2. Execution logic
        response: Optional[LLMResponse] = None
        
        # --- LOCAL MODEL ROUTE ---
        if self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
            logger.info(f"Routing task to LOCAL model (prompt length: {len(prompt)} chars)")
            response = await self.local.generate_text(prompt, **kwargs)
            response.text = OutputParser.clean_for_feishu(response.text)
        
        # --- CLOUD MODEL ROUTE (with Fallback) ---
        else:
            if not self.local and category in [TaskCategory.SIMPLE_CLEANING, TaskCategory.DATA_EXTRACTION, TaskCategory.SIMPLE_CHAT]:
                logger.warning("Local model not available. Routing to cloud model instead.")
            
            if not self.cloud:
                response = await FallbackHandler.handle(FailureType.CLOUD_API_UNAVAILABLE, context={"session_id": session_id, "reason": "Cloud provider not loaded"})
            else:
                logger.info("Routing task to CLOUD model")
                schema = kwargs.pop("schema", None)
                try:
                    if schema:
                        response = await self.cloud.generate_structured(prompt, schema=schema, **kwargs)
                    else:
                        response = await self.cloud.generate_text(prompt, **kwargs)
                except (google_exceptions.ServiceUnavailable, APIStatusError) as e:
                    response = await FallbackHandler.handle(
                        FailureType.CLOUD_API_UNAVAILABLE, 
                        context={"session_id": session_id, "error": str(e)}
                    )
                except (google_exceptions.PermissionDenied, google_exceptions.Unauthenticated) as e:
                    logger.critical(f"Cloud API Authentication Error: {e}")
                    response = LLMResponse(text="FATAL: Cloud API authentication failed.", provider_name="fallback", model_name="auth-error")
                except Exception as e:
                    logger.error(f"Unhandled Cloud API error: {e}")
                    raise e

        # 3. Post-Execution Tracing (The strategy discussed)
        latency = time.time() - start_time
        is_local = response.provider_name == "local" or "llama" in response.provider_name.lower()
        
        trace_data = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "category": category.value,
            "provider": response.provider_name,
            "model": response.model_name,
            "latency": latency,
            "token_usage": response.token_usage,
            "classification": classification_metadata,
            # Strategy: Full logs for Local (Google doesn't track), Metadata for Cloud (Google handles 55 days)
            "prompt": prompt if (is_local or kwargs.get("force_full_log")) else prompt[:200] + "...",
            "response": response.text if (is_local or kwargs.get("force_full_log")) else f"Length: {len(response.text)} chars (Refer to Gemini API Logs)"
        }
        
        await self.router_logger.log_trace(trace_data)
        return response

    async def _classify_task(self, prompt: str) -> tuple[TaskCategory, Optional[float]]:
        heuristic_cat = self._run_heuristics(prompt)
        if heuristic_cat:
            return heuristic_cat, 1.0

        if not self.local:
            return TaskCategory.DEEP_REASONING, None

        classification_prompt = f"""
        Classify the task: 'simple_cleaning', 'extraction', 'reasoning', 'creative', or 'simple_chat'.
        Return ONLY the category name.
        TASK: {prompt[:400]}
        CATEGORY:
        """
        try:
            result_obj = await self.local.generate_text(classification_prompt)
            result = result_obj.text.lower().strip()
            confidence = result_obj.metadata.get("confidence")
            
            for cat in TaskCategory:
                if cat.value in result:
                    return cat, confidence
            return TaskCategory.DEEP_REASONING, confidence
        except Exception as e:
            logger.warning(f"Classification failed: {e}. Defaulting to DEEP_REASONING.")
            return TaskCategory.DEEP_REASONING, None

    def _run_heuristics(self, prompt: str) -> Optional[TaskCategory]:
        p_lower = prompt.lower()
        p_len = len(prompt)
        if p_len > 4000: return TaskCategory.DEEP_REASONING
        reasoning_keys = ["analyze", "compare", "strategy", "why", "logic", "optimize", "evaluate", "summarize", "分析", "对比", "策略", "评估", "总结"]
        if any(k in p_lower for k in reasoning_keys): return TaskCategory.DEEP_REASONING
        extraction_keys = ["extract", "find", "phone number", "email", "regex", "list all", "parse", "提取", "查找", "抓取"]
        if any(k in p_lower for k in extraction_keys) and p_len < 2000: return TaskCategory.DATA_EXTRACTION
        cleaning_keys = ["clean", "format", "strip", "lowercase", "uppercase", "json-ify", "remove", "清洗", "格式化", "去重"]
        if any(k in p_lower for k in cleaning_keys) and p_len < 1000: return TaskCategory.SIMPLE_CLEANING
        return None

    async def record_feedback(self, session_id: str, ground_truth: TaskCategory, reason: str = None):
        await self.router_logger.log_trace({
            "event_type": "router_feedback",
            "session_id": session_id,
            "ground_truth": ground_truth.value,
            "reason": reason,
            "is_correction": True
        })
        logger.info(f"Feedback recorded for session {session_id}: Verified as {ground_truth.value}")
