from __future__ import annotations
import os
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Centralized manager for LLM prompt templates.
    Loads and renders YAML-based prompts from src/intelligence/prompts/.
    """
    
    def __init__(self, prompts_dir: str = None):
        if prompts_dir is None:
            # Default to the directory of this file
            prompts_dir = os.path.dirname(os.path.abspath(__file__))
        self.prompts_dir = prompts_dir
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get_prompt(self, name: str) -> Dict[str, Any]:
        """Load a prompt by its file name (without .yaml)."""
        if name in self._cache:
            return self._cache[name]
        
        file_path = os.path.join(self.prompts_dir, f"{name}.yaml")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Prompt template {name}.yaml not found in {self.prompts_dir}")
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            self._cache[name] = data
            return data

    def render(self, name: str, variables: Dict[str, Any]) -> tuple[str, str]:
        """
        Renders the system_message and user_prompt with provided variables.
        Returns (system_message, user_prompt).
        """
        template = self.get_prompt(name)
        system_msg = template.get("system_message", "")
        user_prompt = template.get("user_prompt", "")
        
        # Simple string replacement (or use Jinja2 for more complex logic)
        for key, value in variables.items():
            placeholder = f"{{{{ {key} }}}}"
            user_prompt = user_prompt.replace(placeholder, str(value))
            system_msg = system_msg.replace(placeholder, str(value))
            
        return system_msg, user_prompt

# Global instance for easy access
prompt_manager = PromptManager()
