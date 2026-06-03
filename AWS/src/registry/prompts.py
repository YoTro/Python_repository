from __future__ import annotations

from mcp.types import Prompt


class PromptRegistry:
    def __init__(self):
        self._prompts: list[Prompt] = []

    def register_prompt(self, prompt: Prompt):
        self._prompts.append(prompt)

    def get_all_prompts(self) -> list[Prompt]:
        return self._prompts


prompt_registry = PromptRegistry()

# Example registration
# prompt_registry.register_prompt(Prompt(name="analyze-competition", description="SOP to analyze competitors for an ASIN"))
