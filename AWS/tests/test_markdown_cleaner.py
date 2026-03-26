import pytest
import json
from src.intelligence.parsers.markdown_cleaner import OutputParser

def test_parse_standard_json():
    """Verify standard clean JSON parsing."""
    raw = '{"action": "test", "action_input": {"key": "value"}}'
    result = OutputParser.parse_dirty_json(raw)
    assert result == {"action": "test", "action_input": {"key": "value"}}

def test_parse_markdown_wrapped():
    """Verify extraction from markdown code blocks."""
    raw = 'Here is the tool call:\n```json\n{"action": "test"}\n```\nHope this helps!'
    result = OutputParser.parse_dirty_json(raw)
    assert result == {"action": "test"}

def test_parse_truncated_json():
    """Verify structural repair for truncated JSON (missing braces/quotes)."""
    # Truncated in the middle of a string
    raw = '{"action": "export_md", "action_input": {"content": "Starting text...'
    result = OutputParser.parse_dirty_json(raw)
    assert result["action"] == "export_md"
    assert result["action_input"]["content"] == "Starting text..."

def test_parse_unescaped_control_chars():
    """Verify that newlines and tabs inside strings are escaped by the state machine."""
    raw = '{"text": "Line 1\nLine 2\tTabbed"}'
    result = OutputParser.parse_dirty_json(raw)
    assert result["text"] == "Line 1\nLine 2\tTabbed"

def test_parse_comments():
    """Verify removal of C-style comments."""
    raw = """
    {
        // This is a line comment
        "action": "test", /* This is a 
        block comment */
        "value": 123
    }
    """
    result = OutputParser.parse_dirty_json(raw)
    assert result == {"action": "test", "value": 123}

def test_heuristic_quote_repair():
    """Verify repair of unescaped quotes within property values."""
    # Note: This relies on the heuristic repair regex
    raw = '{"reason": "The user said "Hello world" to me", "status": "ok"}'
    result = OutputParser.parse_dirty_json(raw)
    assert result["reason"] == 'The user said \"Hello world\" to me'
    assert result["status"] == "ok"

def test_trailing_commas():
    """Verify removal of trailing commas."""
    raw = '{"list": [1, 2, 3,], "obj": {"a": 1,},}'
    result = OutputParser.parse_dirty_json(raw)
    assert result == {"list": [1, 2, 3], "obj": {"a": 1}}

def test_fallback_extraction():
    """Verify fallback regex extraction for action/action_input."""
    raw = 'Conversation filler... "action": "search", "action_input": {"q": "test"} ...more filler'
    result = OutputParser.parse_dirty_json(raw)
    assert result == {"action": "search", "action_input": {"q": "test"}}

def test_recursion_limit():
    """Verify that depth limit prevents infinite recursion."""
    # This is a bit contrived but tests the logic
    # We pass a string that triggers fallback and then nested calls
    raw = '"action": "a", "action_input": {"action": "b", "action_input": {"action": "c"}}'
    result = OutputParser.parse_dirty_json(raw)
    # Depth 0 finds "a", Depth 1 finds "b", Depth 2 returns {}
    assert result["action"] == "a"
    assert result["action_input"]["action"] == "b"
    assert result["action_input"]["action_input"] == {}

def test_parse_large_truncated_report():
    """Verify handling of a large, truncated report with unescaped internal quotes."""
    # Simulating the user's case where content is truncated and contains "zevo" in quotes
    raw = '{ "action": "export_md", "action_input": { "content": "## Summary\\nZevo index is \\"53.47\\", indicating \'Medium\'.\\nMore data here... lead to a rapi'
    # No closing quotes or braces
    result = OutputParser.parse_dirty_json(raw)
    
    assert result["action"] == "export_md"
    assert "content" in result["action_input"]
    # Verify unescaped quotes were fixed and truncation was handled
    assert "53.47" in result["action_input"]["content"]
    assert "rapi" in result["action_input"]["content"]

def test_no_warning_on_normal_text(caplog):
    """Verify that plain text doesn't trigger a warning."""
    text = "I can help you with a wide range of tasks using the following tools:\n\n## Data Collection\n- refresh_data"
    result = OutputParser.parse_dirty_json(text)
    assert result == {}
    # Check that no WARNING logs from the parser specifically
    for record in caplog.records:
        if record.levelname == "WARNING" and "markdown_cleaner" in record.name:
            assert "Failed to parse dirty JSON" not in record.message

if __name__ == "__main__":
    pytest.main([__file__])
