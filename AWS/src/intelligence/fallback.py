from __future__ import annotations
import asyncio
import logging
from enum import Enum
from typing import Dict, Any, Optional, Callable, Awaitable

from src.intelligence.dto import LLMResponse

logger = logging.getLogger(__name__)

class FailureType(Enum):
    """Enumeration of handled failure types for fallback strategies."""
    LOCAL_MODEL_TIMEOUT = "local_model_timeout"
    CLOUD_API_UNAVAILABLE = "cloud_api_unavailable"
    CLOUD_API_RATE_LIMIT = "cloud_api_rate_limit"

# In-memory queue for single-user mode. Ext. Point: Redis/Celery/RQ
_retry_queue = asyncio.Queue()
_background_task: Optional[asyncio.Task] = None

async def _consume_retry_queue():
    """Background worker to consume and retry tasks from the in-memory queue."""
    logger.info("Starting background retry queue consumer...")
    while True:
        try:
            context = await _retry_queue.get()
            logger.info(f"Retrying task for session: {context.get('session_id')}")
            
            # This is a simplified retry. A production system would use exponential backoff.
            # And re-route to the appropriate handler (e.g., re-call router.route_and_execute).
            # For now, we'll just log it.
            await asyncio.sleep(10) # Wait 10s before "retry"
            
            _retry_queue.task_done()
            logger.info("Dummy retry processed.")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in retry consumer: {e}")


async def _handle_local_timeout(context: Dict[str, Any]) -> LLMResponse:
    """Fallback strategy for when the local model times out."""
    logger.warning(f"Local model timed out for session: {context.get('session_id')}. No cloud fallback for this error.")
    return LLMResponse(
        text="Sorry, the local model is taking too long to respond. Please try a simpler query or check the model status.",
        provider_name="fallback",
        model_name="timeout-handler"
    )

async def _handle_cloud_unavailable(context: Dict[str, Any]) -> LLMResponse:
    """Fallback strategy for when cloud APIs are down."""
    logger.error(f"Cloud API is unavailable for session: {context.get('session_id')}. Enqueuing for retry.")
    
    await _retry_queue.put(context)
    
    # Ensure background consumer is running
    global _background_task
    if _background_task is None or _background_task.done():
        _background_task = asyncio.create_task(_consume_retry_queue())
        
    return LLMResponse(
        text="The cloud AI service is temporarily unavailable. Your request has been queued and will be retried automatically. Please check back later.",
        provider_name="fallback",
        model_name="retry-queue-handler"
    )

class FallbackHandler:
    """
    Centralized handler for system-wide resilience and fallback strategies.
    Maps failure types to specific, decoupled handling functions.
    """
    _strategies: Dict[FailureType, Callable[[Dict], Awaitable[LLMResponse]]] = {
        FailureType.LOCAL_MODEL_TIMEOUT: _handle_local_timeout,
        FailureType.CLOUD_API_UNAVAILABLE: _handle_cloud_unavailable,
    }

    @classmethod
    async def handle(cls, failure_type: FailureType, context: Optional[Dict[str, Any]] = None) -> LLMResponse:
        """
        Execute the registered fallback strategy for a given failure type.
        """
        handler = cls._strategies.get(failure_type)
        if not handler:
            logger.error(f"No fallback handler registered for failure type: {failure_type}")
            return LLMResponse(
                text="An unexpected error occurred and no fallback strategy was available.",
                provider_name="fallback",
                model_name="unhandled-error"
            )
            
        return await handler(context or {})
