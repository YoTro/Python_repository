from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

@dataclass
class LLMResponse:
    """Standardized response object from any LLM provider."""
    text: str
    provider_name: str
    model_name: str
    token_usage: int = 0
    cost: float = 0.0
    currency: str = "USD"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchRequest:
    """A single request within a provider batch job."""
    custom_id: str
    prompt: str
    system_message: Optional[str] = None
    schema: Optional[Any] = None  # Pydantic model for structured output


@dataclass
class BatchJobHandle:
    """
    Opaque handle returned after submitting a provider batch job.
    Stored in the workflow event log to enable resume after process restart.
    """
    job_id: str
    provider: str                  # "gemini" | "claude"
    status: str = "pending"        # pending | in_progress | completed | failed
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
