from __future__ import annotations
import re
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class MarkdownCleaner:
    """
    Cleans and standardizes Markdown-like text from LLMs for various outputs (e.g., Feishu).
    Handles common LLM formatting issues like unclosed code blocks, inconsistent backticks, etc.
    """
    @staticmethod
    def clean(text: str) -> str:
        if not isinstance(text, str): # Ensure it's a string
            return str(text)

        def replace_json_block(match):
            json_str = match.group(1)
            try:
                parsed_json = json.loads(json_str)
                # Construct string safely without multi-line f-string inside re.sub
                return "```json\n" + json.dumps(parsed_json, indent=2, ensure_ascii=False) + "\n```"
            except json.JSONDecodeError:
                return "```json\n" + json_str + "\n```"
        
        text = re.sub(r"```(?:json)?\s*([^`]+?)\s*```", replace_json_block, text, flags=re.DOTALL)

        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = text.strip()

        import html
        text = html.unescape(text)

        text = re.sub(r'^\s*(?:Sure|Okay|Here is|Here\'s|I can help with that|As an AI language model|Here\'s what I found):?\s*', '', text, flags=re.IGNORECASE).lstrip()
        
        return text


class OutputParser:
    """
    Aggregates various cleaners and parsers based on target format.
    """
    @staticmethod
    def clean_for_feishu(text: Any) -> str:
        """
        Specific cleaning for Feishu messages (interactive cards).
        """
        return MarkdownCleaner.clean(text)

    @staticmethod
    def clean_for_cli(text: Any) -> str:
        """
        Specific cleaning for CLI output (less strict Markdown, more focus on readability).
        """
        # For CLI, we might want simpler stripping of markdown if it's not rendered.
        # For now, MarkdownCleaner is good enough, or can be customized.
        return MarkdownCleaner.clean(text)

    # FUTURE: @staticmethod
    # def parse_json(text: str) -> Dict[str, Any]: ...
    # def parse_csv(text: str) -> List[Dict[str, Any]]: ...
