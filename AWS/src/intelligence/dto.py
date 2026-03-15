from __future__ import annotations
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
    metadata: Dict[str, Any] = field(default_factory=dict)
