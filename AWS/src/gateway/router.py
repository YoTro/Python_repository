from __future__ import annotations
import logging
from typing import Dict, Any, Optional

from src.core.models.request import UnifiedRequest, CallbackConfig
from src.jobs.manager import get_job_manager
from src.gateway.auth import AuthMiddleware
from src.gateway.rate_limit import RateLimiter
from src.core.errors.exceptions import AWSBaseError

logger = logging.getLogger(__name__)

class APIGateway:
    """
    Unified entry point router (Gateway):
    Normalizes incoming requests from heterogeneous sources (CLI, Webhook, Feishu)
    into UnifiedRequest objects, and submits them to the multi-user JobManager.
    
    This layer acts as the centralized authority for Auth, Rate Limiting, and Mode Selection.
    """

    @staticmethod
    async def dispatch_cli_workflow(workflow_name: str, params: Dict[str, Any]) -> Any:
        """Handles CLI deterministic workflow requests."""
        identity = AuthMiddleware.authenticate()
        
        if not RateLimiter.check_limit(identity, request_type="workflow"):
            raise AWSBaseError("Rate limit exceeded for workflow execution.")
        
        request = UnifiedRequest(
            tenant_id=identity["tenant_id"],
            user_id=identity["user_id"],
            plan_tier=identity["plan_tier"],
            workflow_name=workflow_name,
            params=params
        )
        job_mgr = get_job_manager()
        return await job_mgr.submit_and_wait(request)

    @staticmethod
    async def dispatch_cli_explore(intent: str) -> Any:
        """Handles CLI exploratory agent requests."""
        identity = AuthMiddleware.authenticate()
        
        if not RateLimiter.check_limit(identity, request_type="explore"):
            raise AWSBaseError("Rate limit exceeded for agent exploration.")
        
        request = UnifiedRequest(
            tenant_id=identity["tenant_id"],
            user_id=identity["user_id"],
            plan_tier=identity["plan_tier"],
            intent=intent
        )
        job_mgr = get_job_manager()
        return await job_mgr.submit_and_wait(request)

    @staticmethod
    def dispatch_feishu_command(
        workflow_name: str, 
        params: Dict[str, Any], 
        chat_id: str,
        bot_name: str = "amazon_bot"
    ) -> str:
        """
        Handles async Feishu Bot commands.
        Immediately returns job_id so bot can reply 'Accepted'.
        """
        identity = AuthMiddleware.authenticate()
        
        if not RateLimiter.check_limit(identity, request_type="workflow"):
            logger.warning("Rate limit exceeded for Feishu command, but proceeding (Stub).")
        
        # Injects the appropriate Callback preset with the dynamic bot_name
        callback = CallbackConfig(
            type="feishu_bitable",
            target=chat_id,
            options={"bot_name": bot_name}
        )
        
        request = UnifiedRequest(
            tenant_id=identity["tenant_id"],
            user_id=identity["user_id"],
            plan_tier=identity["plan_tier"],
            workflow_name=workflow_name,
            params=params,
            callback=callback
        )
        
        job_mgr = get_job_manager()
        return job_mgr.submit(request)

    @staticmethod
    def dispatch_feishu_explore(intent: str, chat_id: str, bot_name: str = "amazon_bot") -> str:
        """
        Handles async Feishu Bot exploration (Agent) commands.
        Immediately returns job_id so bot can reply 'Accepted'.
        """
        identity = AuthMiddleware.authenticate()
        
        if not RateLimiter.check_limit(identity, request_type="explore"):
            logger.warning("Rate limit exceeded for Feishu explore command (Stub).")
            
        callback = CallbackConfig(
            type="feishu_card", 
            target=chat_id,
            options={"bot_name": bot_name, "total_steps": 15} 
        )
        
        request = UnifiedRequest(
            tenant_id=identity["tenant_id"],
            user_id=identity["user_id"],
            plan_tier=identity["plan_tier"],
            intent=intent,
            callback=callback
        )
        
        job_mgr = get_job_manager()
        return job_mgr.submit(request)
