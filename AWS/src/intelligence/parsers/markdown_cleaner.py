from __future__ import annotations
import re
import json
import html
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class OutputParser:
    """
    Unified parser for LLM outputs. Handles markdown cleaning, 
    dirty JSON parsing, and format-specific sanitization.
    """

    @staticmethod
    def parse_dirty_json(json_str: str) -> Dict[str, Any]:
        if not isinstance(json_str, str) or not json_str.strip():
            return {}

        json_str = json_str.strip()

        # 1. Extract from markdown fence
        if "```json" in json_str:
            json_str = json_str.split("```json")[1].split("```")[0].strip()
        elif "{" in json_str:
            start = json_str.find("{")
            end = json_str.rfind("}")
            if end > start:
                json_str = json_str[start:end + 1]
            else:
                return {}

        if not json_str:
            return {}

        # 2. Standard parse
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass

        # 3. Heuristic repair
        def repair_value(match):
            key_part = match.group(1)
            val_content = match.group(2)
            val_content = val_content.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
            val_content = re.sub(r'(?<!\\)"', r'\\"', val_content)
            return f'{key_part}"{val_content}"'

        try:
            cleaned = re.sub(
                r'("[\w ]+":\s*)"(.*?)"(?=\s*[,}\n])',
                repair_value,
                json_str,
                flags=re.DOTALL
            )
            return json.loads(cleaned)
        except Exception:
            pass

        # 4. Fallback: extract action/action_input
        action_match = re.search(r'"action":\s*"([^"]+)"', json_str)
        if action_match:
            action = action_match.group(1)
            action_input = {}
            input_match = re.search(r'"action_input":\s*(\{.*\})', json_str, flags=re.DOTALL)
            if input_match:
                try:
                    action_input = OutputParser.parse_dirty_json(input_match.group(1))
                except Exception:
                    pass
            return {"action": action, "action_input": action_input}

        logger.warning("Failed to parse dirty JSON even after aggressive cleanup")
        return {}

    @staticmethod
    def clean_markdown(text: str) -> str:
        """
        Standardizes markdown formatting, fixes unclosed blocks, 
        and removes LLM conversational filler.
        """
        if not isinstance(text, str):
            return str(text)

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
        return OutputParser.clean_markdown(text)

    @staticmethod
    def clean_for_cli(text: Any) -> str:
        """Specific cleaning for CLI output."""
        return OutputParser.clean_markdown(text)
