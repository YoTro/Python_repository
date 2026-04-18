"""
hr_chat - AI-driven HR conversation module

Workflow:
  1. questioner.py  — builds a question set from job type (Strategy pattern)
  2. agent.py       — drives multi-turn Claude conversation, extracts answers
  3. parser.py      — parses HR free-text replies into structured fields
  4. schemas.py     — DTOs shared across the module

Quick start:
    from src.hr_chat import run_session

    detail = run_session(job_row)   # returns HrChatResult
"""
from .agent import run_session

__all__ = ["run_session"]
