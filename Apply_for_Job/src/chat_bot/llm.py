"""
llm.py - Unified LLM provider abstraction for HR Chat

Supported providers (set HRC_PROVIDER in .env or environment):
  anthropic  →  Anthropic Claude  (default)
  openai     →  OpenAI ChatGPT
  gemini     →  Google Gemini

All providers expose the same interface:
    provider.chat(system, messages, max_tokens) -> str

messages format (same as Anthropic / OpenAI):
    [
        {"role": "user",      "content": "..."},
        {"role": "assistant", "content": "..."},
        ...
    ]

Default models:
  anthropic : claude-haiku-4-5-20251001
  openai    : gpt-4o-mini
  gemini    : gemini-1.5-flash
"""
from __future__ import annotations
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_DEFAULTS = {
    "anthropic": "claude-haiku-4-5-20251001",
    "openai":    "gpt-4o-mini",
    "gemini":    "gemini-1.5-flash",
    "deepseek":  "deepseek-chat",
}


# ══════════════════════════════════════════════════════════════════════
# Provider classes
# ══════════════════════════════════════════════════════════════════════

class _AnthropicProvider:
    def __init__(self, model: str):
        try:
            import anthropic
        except ImportError as e:
            raise ImportError("pip install anthropic") from e
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise EnvironmentError("ANTHROPIC_API_KEY not set")
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def chat(self, system: str, messages: list[dict], max_tokens: int = 512,
             temperature: float = 1.0) -> str:
        resp = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=messages,
        )
        return resp.content[0].text.strip()


class _OpenAIProvider:
    def __init__(self, model: str):
        try:
            from openai import OpenAI
        except ImportError as e:
            raise ImportError("pip install openai") from e
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY not set")
        self._client = OpenAI(api_key=api_key)
        self.model = model

    def chat(self, system: str, messages: list[dict], max_tokens: int = 512,
             temperature: float = 1.0) -> str:
        full_messages = [{"role": "system", "content": system}] + messages
        resp = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=full_messages,
        )
        return resp.choices[0].message.content.strip()


class _DeepSeekProvider:
    """DeepSeek — OpenAI-compatible API at https://api.deepseek.com"""
    _BASE_URL = "https://api.deepseek.com"

    def __init__(self, model: str):
        try:
            from openai import OpenAI
        except ImportError as e:
            raise ImportError("pip install openai  # DeepSeek uses the OpenAI client") from e
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise EnvironmentError("DEEPSEEK_API_KEY not set")
        self._client = OpenAI(api_key=api_key, base_url=self._BASE_URL)
        self.model = model

    def chat(self, system: str, messages: list[dict], max_tokens: int = 512,
             temperature: float = 1.0) -> str:
        full_messages = [{"role": "system", "content": system}] + messages
        resp = self._client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
            messages=full_messages,
        )
        return resp.choices[0].message.content.strip()


class _GeminiProvider:
    def __init__(self, model: str):
        try:
            import google.generativeai as genai
        except ImportError as e:
            raise ImportError("pip install google-generativeai") from e
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise EnvironmentError("GEMINI_API_KEY not set")
        genai.configure(api_key=api_key)
        self._genai = genai
        self.model_name = model

    def chat(self, system: str, messages: list[dict], max_tokens: int = 512,
             temperature: float = 1.0) -> str:
        model = self._genai.GenerativeModel(
            model_name=self.model_name,
            system_instruction=system,
        )
        history = []
        for msg in messages[:-1]:
            role = "model" if msg["role"] == "assistant" else "user"
            history.append({"role": role, "parts": [msg["content"]]})

        chat = model.start_chat(history=history)
        last = messages[-1]["content"] if messages else ""
        resp = chat.send_message(
            last,
            generation_config=self._genai.types.GenerationConfig(
                max_output_tokens=max_tokens,
                temperature=temperature,
            ),
        )
        return resp.text.strip()


# ══════════════════════════════════════════════════════════════════════
# Factory
# ══════════════════════════════════════════════════════════════════════

def get_provider(
    provider_name: Optional[str] = None,
    model: Optional[str] = None,
):
    """
    Build and return the appropriate LLM provider.

    Resolution order (highest priority first):
      1. Arguments passed directly
      2. HRC_PROVIDER / HRC_MODEL environment variables
      3. Defaults (anthropic / provider-specific default model)
    """
    _load_dotenv()

    name  = (provider_name or os.environ.get("HRC_PROVIDER", "anthropic")).lower()
    model = model or os.environ.get("HRC_MODEL", "") or _DEFAULTS.get(name)

    if not model:
        raise ValueError(f"Unknown provider '{name}' — choose: anthropic / openai / gemini")

    logger.debug("LLM provider=%s  model=%s", name, model)

    if name == "anthropic":
        return _AnthropicProvider(model)
    if name == "openai":
        return _OpenAIProvider(model)
    if name == "gemini":
        return _GeminiProvider(model)
    if name == "deepseek":
        return _DeepSeekProvider(model)

    raise ValueError(f"Unknown provider '{name}' — choose: anthropic / openai / gemini / deepseek")


def _load_dotenv() -> None:
    """
    Load .env from the project root (two levels up from this file).
    Silent no-op if python-dotenv is not installed or file does not exist.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    env_path = _find_dotenv()
    if env_path:
        load_dotenv(env_path, override=False)  # override=False: env vars take priority
        logger.debug("Loaded .env from %s", env_path)


def _find_dotenv():
    """Walk up from this file to find the nearest .env."""
    from pathlib import Path
    here = Path(__file__).resolve()
    for parent in [here.parent, here.parent.parent, here.parent.parent.parent]:
        candidate = parent / ".env"
        if candidate.exists():
            return candidate
    return None
