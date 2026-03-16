from __future__ import annotations
import os
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class AgentMessage(BaseModel):
    """Represents a single message in an Agent's conversation history."""
    role: str = Field(..., description="Role of the sender (e.g., 'user', 'assistant', 'system', 'tool')")
    content: str = Field(..., description="Content of the message")
    name: Optional[str] = Field(None, description="Optional name (useful for tool calls/results)")

class AgentSession(BaseModel):
    """
    Tracks the complete state, history, and token budget of an Agent execution.
    This replaces the in-memory string 'conversation' and enables multi-turn,
    suspend/resume capabilities (e.g., human-in-the-loop).
    """
    session_id: str
    tenant_id: str = "default"
    user_id: str = "default"
    history: List[AgentMessage] = Field(default_factory=list)
    token_usage: int = 0           # total (cloud + local)
    cloud_token_usage: int = 0      # cloud API tokens only (budget-relevant)
    max_steps: int = 15
    current_step: int = 0
    status: str = "active"  # active, suspended_for_human, completed, failed
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def add_message(self, role: str, content: str, name: Optional[str] = None):
        self.history.append(AgentMessage(role=role, content=content, name=name))
        self.updated_at = datetime.utcnow().isoformat()

    def format_history_as_text(self) -> str:
        """Helper to format the history into a prompt string for LLMs."""
        formatted = []
        for msg in self.history:
            prefix = msg.role.capitalize()
            if msg.name:
                prefix += f" ({msg.name})"
            formatted.append(f"{prefix}: {msg.content}")
        return "\n\n".join(formatted)


class AgentSessionManager:
    """
    Handles persistence of AgentSessions.
    Single-user version: stores to local JSON files in data/sessions/
    Multi-user extension point: Swap to Redis.
    """
    def __init__(self, session_dir: str = None):
        self.session_dir = session_dir or os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "sessions"
        )
        os.makedirs(self.session_dir, exist_ok=True)

    def _path(self, session_id: str) -> str:
        return os.path.join(self.session_dir, f"{session_id}.json")

    def save(self, session: AgentSession) -> None:
        """Persist session state."""
        session.updated_at = datetime.utcnow().isoformat()
        path = self._path(session.session_id)
        try:
            with open(path, "w", encoding="utf-8") as f:
                # Using model_dump (Pydantic V2) or dict() fallback
                data = session.model_dump() if hasattr(session, "model_dump") else session.dict()
                json.dump(data, f, ensure_ascii=False, default=str)
            logger.debug(f"Agent session saved: {session.session_id} (Status: {session.status})")
        except Exception as e:
            logger.error(f"Failed to save agent session {session.session_id}: {e}")

    def load(self, session_id: str) -> Optional[AgentSession]:
        """Load session state from disk."""
        path = self._path(session_id)
        if not os.path.exists(path):
            return None
            
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return AgentSession(**data)
        except Exception as e:
            logger.error(f"Failed to load agent session {session_id}: {e}")
            return None

    def create(self, session_id: str, **kwargs) -> AgentSession:
        """Create and persist a new empty session."""
        session = AgentSession(session_id=session_id, **kwargs)
        self.save(session)
        return session
