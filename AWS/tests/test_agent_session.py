import os
import tempfile
import pytest
from src.agents.session import AgentSession, AgentMessage, AgentSessionManager

def test_agent_message_creation():
    msg = AgentMessage(role="user", content="Hello", name="John")
    assert msg.role == "user"
    assert msg.content == "Hello"
    assert msg.name == "John"

def test_agent_session_creation():
    session = AgentSession(session_id="test_123")
    assert session.session_id == "test_123"
    assert session.status == "active"
    assert session.current_step == 0
    assert len(session.history) == 0

    session.add_message("user", "What's the BSR?")
    assert len(session.history) == 1
    assert session.history[0].role == "user"

def test_format_history_as_text():
    session = AgentSession(session_id="test_fmt")
    session.add_message("user", "Hello")
    session.add_message("assistant", "Hi there")
    session.add_message("tool", '{"result": "success"}', name="amazon_search")

    text = session.format_history_as_text()
    assert "User: Hello" in text
    assert "Assistant: Hi there" in text
    assert "Tool (amazon_search): {\"result\": \"success\"}" in text

def test_agent_session_manager():
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = AgentSessionManager(session_dir=temp_dir)
        
        # Test Create
        session = manager.create("session_abc", tenant_id="tenant_1")
        assert session.session_id == "session_abc"
        assert session.tenant_id == "tenant_1"
        
        # Test Save & Modify
        session.add_message("user", "Step 1")
        session.current_step = 1
        manager.save(session)
        
        # Test Load
        loaded_session = manager.load("session_abc")
        assert loaded_session is not None
        assert loaded_session.current_step == 1
        assert len(loaded_session.history) == 1
        assert loaded_session.history[0].content == "Step 1"

        # Test Load non-existent
        assert manager.load("missing_session") is None
