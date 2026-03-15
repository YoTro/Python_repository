from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class CallbackConfig(BaseModel):
    """Configuration for output callback strategies."""
    type: str = Field(..., description="Callback type, e.g., 'feishu_bitable', 'json', 'csv', 'mcp'")
    target: Optional[str] = Field(None, description="Target destination, e.g., chat_id, webhook_url, or file path")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional callback-specific options")

class UnifiedRequest(BaseModel):
    """
    Unified Request DTO used by the API Gateway to normalize all incoming requests
    (CLI, Feishu, Cron, MCP) before handing them off to the Orchestration Layer.
    """
    tenant_id: str = Field("default", description="Tenant ID for multi-user support")
    user_id: str = Field("default", description="User ID issuing the request")
    plan_tier: str = Field("free", description="User subscription tier (free, pro, enterprise)")
    
    workflow_name: Optional[str] = Field(None, description="Name of the deterministic workflow to execute")
    intent: Optional[str] = Field(None, description="Natural language intent for exploratory Agent execution")
    
    params: Dict[str, Any] = Field(default_factory=dict, description="Parameters, including filters_override")
    callback: Optional[CallbackConfig] = Field(None, description="Callback strategy definition")
