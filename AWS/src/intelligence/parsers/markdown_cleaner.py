from __future__ import annotations
import re
import json
import html
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MAX_RECURSION_DEPTH = 2 # Allows depth 0 and depth 1, so 2 parsing levels

class OutputParser:
    """
    Unified parser for LLM outputs. Handles markdown cleaning, 
    dirty JSON parsing, and format-specific sanitization.
    """

    @staticmethod
    def parse_dirty_json(json_str: str, depth: int = 0) -> Dict[str, Any]:
        """
        Attempts to parse JSON that might contain common LLM errors like 
        unescaped newlines or extra text around the block.
        """
        # Bug 2: Check depth immediately and strictly enforce MAX_RECURSION_DEPTH
        if depth >= MAX_RECURSION_DEPTH:
            return {}
            
        if not json_str or not isinstance(json_str, str):
            return {}

        original_input = json_str.strip()
        current_str = original_input

        # 1. Extract JSON block if wrapped in markdown
        json_match = re.search(r"```(?:json|JSON)?\s*(\{.*?\})\s*```", current_str, re.DOTALL | re.IGNORECASE)
        if json_match:
            current_str = json_match.group(1).strip()
        else:
            # Smarter isolation: only isolate if the block seems to be the root object.
            first_brace = current_str.find("{")
            action_pos = current_str.find('"action":')
            
            if first_brace != -1:
                # If no "action" found or it's inside/after the first brace, it's safe to isolate.
                if action_pos == -1 or first_brace < action_pos:
                    end_brace = current_str.rfind("}")
                    if end_brace > first_brace:
                        current_str = current_str[first_brace:end_brace+1].strip()
                    else:
                        # TRUNCATED CASE: Take everything from the first brace to the absolute end
                        current_str = current_str[first_brace:].strip()

        if not current_str:
            return {}

        # 1.1 Integrated State Machine (Handles Structure, Strings, Comments, and Newlines)
        repaired_str = []
        in_string = False
        in_line_comment = False
        in_block_comment = False
        escaped = False
        brace_stack = []
        
        i = 0
        while i < len(current_str):
            c = current_str[i]
            next_c = current_str[i+1] if i+1 < len(current_str) else ""
            
            if escaped:
                if c == '\n':
                    repaired_str.append('\\\\n')
                else:
                    repaired_str.append(c)
                escaped = False
                i += 1
                continue
            
            if in_string:
                if c == '\\':
                    escaped = True
                    repaired_str.append(c)
                elif c == '"':
                    in_string = False
                    repaired_str.append(c)
                elif c == '\n': repaired_str.append('\\n')
                elif c == '\r': repaired_str.append('\\r')
                elif c == '\t': repaired_str.append('\\t')
                else: repaired_str.append(c)
            elif in_line_comment:
                if c == '\n':
                    in_line_comment = False
                    repaired_str.append(c)
            elif in_block_comment:
                if c == '*' and next_c == '/':
                    in_block_comment = False
                    i += 1
            else:
                if c == '"':
                    in_string = True
                    repaired_str.append(c)
                elif c == '/' and next_c == '/':
                    in_line_comment = True
                    i += 1
                elif c == '/' and next_c == '*':
                    in_block_comment = True
                    i += 1
                elif c == '{':
                    brace_stack.append('}')
                    repaired_str.append(c)
                elif c == '[':
                    brace_stack.append(']')
                    repaired_str.append(c)
                elif c == '}':
                    if brace_stack and brace_stack[-1] == '}':
                        brace_stack.pop()
                    repaired_str.append(c)
                elif c == ']':
                    if brace_stack and brace_stack[-1] == ']':
                        brace_stack.pop()
                    repaired_str.append(c)
                else:
                    repaired_str.append(c)
            i += 1

        current_str = "".join(repaired_str)

        # 1.2 Structural Repair for Truncation
        if in_string:
            current_str += '"'
        while brace_stack:
            current_str += brace_stack.pop()

        # 1.3 Final structural cleanup (Trailing Commas)
        current_str = re.sub(r",\s*([\]}])", r"\1", current_str)

        # 2. Try standard parse
        try:
            parsed = json.loads(current_str)
            # Enforce recursion limit on clean JSON to match fallback behavior
            if isinstance(parsed, dict) and depth + 1 >= MAX_RECURSION_DEPTH:
                if "action_input" in parsed:
                    parsed["action_input"] = {}
            return parsed
        except json.JSONDecodeError:
            pass

        # 3. Targeted Heuristic repair (for unescaped quotes inside strings)
        def repair_value(match):
            key_part = match.group(1)
            val_content = match.group(2)
            
            # Bug 1: Robust character loop to escape internal quotes
            repaired_val = []
            esc = False
            for char in val_content:
                if esc:
                    repaired_val.append(char)
                    esc = False
                elif char == '\\':
                    repaired_val.append(char)
                    esc = True
                elif char == '"':
                    repaired_val.append('\\"')
                else:
                    repaired_val.append(char)
            
            return f'{key_part}"{"".join(repaired_val)}"'

        try:
            # Fix: Regex should be greedy for the value part (.*?) to allow internal quotes
            cleaned = re.sub(
                r'("[\w ]+":\s*)"(.*?)"(?=\s*[,}\n]|$)',
                repair_value,
                current_str,
                flags=re.DOTALL
            )
            parsed = json.loads(cleaned)
            # Enforce recursion limit on repaired JSON
            if isinstance(parsed, dict) and depth + 1 >= MAX_RECURSION_DEPTH:
                if "action_input" in parsed:
                    parsed["action_input"] = {}
            return parsed
        except Exception:
            pass

        # 4. Fallback: extract action/action_input with limited recursion
        # Use original_input to ensure we catch fields even if JSON structure is messy
        # Removed 'if depth < 1:' to allow fallback logic at any depth.
        if depth < 1:
            action_match = re.search(r'"action":\s*"([^"]+)"', original_input)
            if action_match:
                action = action_match.group(1)
                action_input = {}
                # Lenient match for action_input: capture from the first '{' to the end of string if necessary
                input_match = re.search(r'"action_input":\s*(\{.*)', original_input, flags=re.DOTALL)
                if input_match:
                    try:
                        # The recursive call will handle balancing the captured segment
                        action_input = OutputParser.parse_dirty_json(input_match.group(1), depth=depth + 1)
                    except Exception:
                        pass
                return {"action": action, "action_input": action_input}
        
        # Only log a warning if the input looks like it was attempting to be JSON or a tool call
        if "{" in original_input and "action" in original_input:
            logger.warning(f"Failed to parse dirty JSON even after aggressive cleanup. String snippet: {original_input[:100]}...")
        else:
            logger.debug(f"Input does not appear to be JSON. String snippet: {original_input[:100]}...")
            
        return {}

    @staticmethod
    def clean_markdown(text: str) -> str:
        """
        Standardizes markdown formatting, fixes unclosed blocks,
        and removes LLM conversational filler.
        """
        if not isinstance(text, str):
            return str(text)

        # 0. If the whole text is a bare action-call JSON, extract content directly.
        # Handles LLM outputs like {"action": "export_md", "action_input": {"content": "..."}}
        stripped = text.strip()
        if stripped.startswith("{") and '"action"' in stripped:
            parsed = OutputParser.parse_dirty_json(stripped)
            action = parsed.get("action", "")
            content = (parsed.get("action_input") or {}).get("content")
            if content and isinstance(content, str):
                text = content

        # 1. Normalize JSON blocks (and fix dirty ones while we're at it)
        def replace_json_block(match):
            raw_content = match.group(1)
            parsed = OutputParser.parse_dirty_json(raw_content)
            if parsed:
                return "```json\n" + json.dumps(parsed, indent=2, ensure_ascii=False) + "\n```"
            return "```json\n" + raw_content + "\n```"

        text = re.sub(r"```(?:json)?\s*([^`]+?)\s*```", replace_json_block, text, flags=re.DOTALL)

        # 2. Normalize whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = text.strip()

        # 3. Handle HTML entities
        text = html.unescape(text)

        # 4. Strip LLM chatter
        prefixes = [
            "Sure", "Okay", "Here is", "Here's", "I can help with that", 
            "As an AI language model", "Here's what I found", "Final Answer"
        ]
        pattern = r'^\s*(?:' + '|'.join(prefixes) + r'):?\s*'
        text = re.sub(pattern, '', text, flags=re.IGNORECASE).lstrip()
        
        return text

    @staticmethod
    def clean_for_feishu(text: Any) -> str:
        """Specific cleaning for Feishu messages (interactive cards)."""
        text = OutputParser.clean_markdown(text)
        # Feishu card markdown requires uploaded image_keys, not URLs.
        # Strip ![alt](url) → alt to avoid ErrCode 11310 "no imagekey is passed in".
        text = re.sub(r'!\[([^\]]*)\]\([^)]*\)', r'\1', text)
        return text

    @staticmethod
    def clean_for_cli(text: Any) -> str:
        """Specific cleaning for CLI output."""
        return OutputParser.clean_markdown(text)
