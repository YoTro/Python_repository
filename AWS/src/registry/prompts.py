from __future__ import annotations
from typing import List
from mcp.types import Prompt

class PromptRegistry:
    def __init__(self):
        self._prompts: List[Prompt] = []

    def register_prompt(self, prompt: Prompt):
        self._prompts.append(prompt)

    def get_all_prompts(self) -> List[Prompt]:
        return self._prompts

prompt_registry = PromptRegistry()

# Example registration
# prompt_registry.register_prompt(Prompt(name="analyze-competition", description="SOP to analyze competitors for an ASIN"))
