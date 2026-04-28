from __future__ import annotations
import os
import json
import logging
import asyncio
from typing import Optional, Any, Dict
from .base import BaseLLMProvider
from src.intelligence.dto import LLMResponse

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "deepseek-v4-flash"

_CONTEXT_WINDOWS = {
    "deepseek-v4-flash":  131_072,
    "deepseek-v4-pro":    131_072,
    # Legacy aliases — resolved to deepseek-v4-flash by PriceManager
    "deepseek-chat":      131_072,
    "deepseek-reasoner":   65_536,
}


class DeepSeekProvider(BaseLLMProvider):
    """
    DeepSeek provider using the OpenAI-compatible REST API.
    Requires `openai` package (pip install openai).

    Supports:
      - deepseek-chat     (DeepSeek-V3, general purpose)
      - deepseek-reasoner (DeepSeek-R1, chain-of-thought reasoning)

    Server-side KV cache is automatic; prompt_cache_hit_tokens in the
    usage response drives the cheaper cache-hit billing rate.
    """

    _MODEL_CONTEXT_WINDOWS = _CONTEXT_WINDOWS

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        base_url: str = "https://api.deepseek.com",
    ):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY missing.")

        resolved = model_name or os.getenv("DEEPSEEK_MODEL", _DEFAULT_MODEL)
        super().__init__("deepseek", resolved)

        try:
            from openai import AsyncOpenAI
            self._client = AsyncOpenAI(api_key=self.api_key, base_url=base_url)
        except ImportError:
            raise ImportError("openai package required: pip install openai")

        from .config.limits import get_max_output_tokens
        _ceiling = get_max_output_tokens("deepseek", self.model_name)
        _user_pref = int(os.getenv("MAX_LLM_OUTPUT_TOKENS", str(_ceiling)))
        self._DEFAULT_MAX_TOKENS = min(_user_pref, _ceiling)

        logger.info(f"DeepSeekProvider initialized: model={self.model_name}, max_output_tokens: {self._DEFAULT_MAX_TOKENS}")

    # ── Token counting ────────────────────────────────────────────────────────

    async def count_tokens(self, prompt: str, system_message: Optional[str] = None) -> int:
        # DeepSeek has no dedicated token-count endpoint; estimate via char count.
        full = (system_message or "") + prompt
        return max(1, len(full) // 4)

    # ── Text generation ───────────────────────────────────────────────────────

    async def generate_text(
        self,
        prompt: str,
        system_message: Optional[str] = None,
        **kwargs,
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        filtered = self._filter_kwargs(kwargs)
        messages = self._build_messages(prompt, system_message)

        try:
            resp = await self._client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                max_tokens=self._DEFAULT_MAX_TOKENS,
                temperature=filtered.pop("temperature", 0.0),
                **filtered,
            )
            if resp.choices and resp.choices[0].finish_reason == "length":
                logger.warning(
                    f"DeepSeek response truncated at max_tokens={self._DEFAULT_MAX_TOKENS}. "
                    f"Set MAX_LLM_OUTPUT_TOKENS env var to increase the limit (max 8192)."
                )
            return self._parse_response(resp, is_batch=False)
        except Exception as e:
            logger.error(f"DeepSeek generate_text failed: {e}")
            raise

    # ── Structured generation (JSON mode) ────────────────────────────────────

    async def generate_structured(
        self,
        prompt: str,
        schema: Any,
        system_message: Optional[str] = None,
        **kwargs,
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        filtered = self._filter_kwargs(kwargs)

        schema_hint = ""
        try:
            schema_hint = "\n\nRespond with valid JSON matching this schema:\n" + json.dumps(
                schema.model_json_schema(), indent=2
            )
        except Exception:
            pass

        messages = self._build_messages(prompt + schema_hint, system_message)

        try:
            resp = await self._client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                max_tokens=self._DEFAULT_MAX_TOKENS,
                temperature=filtered.pop("temperature", 0.0),
                response_format={"type": "json_object"},
                **filtered,
            )
            if resp.choices and resp.choices[0].finish_reason == "length":
                logger.warning(
                    f"DeepSeek structured response truncated at max_tokens={self._DEFAULT_MAX_TOKENS}. "
                    f"Set MAX_LLM_OUTPUT_TOKENS env var to increase the limit (max 8192)."
                )
            return self._parse_response(resp, is_batch=False)
        except Exception as e:
            logger.error(f"DeepSeek generate_structured failed: {e}")
            raise

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _build_messages(prompt: str, system_message: Optional[str]) -> list:
        msgs = []
        if system_message:
            msgs.append({"role": "system", "content": system_message})
        msgs.append({"role": "user", "content": prompt})
        return msgs

    def _parse_response(self, resp, *, is_batch: bool) -> LLMResponse:
        choice  = resp.choices[0]
        text    = choice.message.content or ""
        usage   = resp.usage

        input_tokens      = getattr(usage, "prompt_tokens", 0) or 0
        output_tokens     = getattr(usage, "completion_tokens", 0) or 0

        # Reasoning tokens (deepseek-reasoner only) are included in completion_tokens.
        # Extract them for transparency but do NOT double-count in cost — the API
        # already rolls them into completion_tokens for billing.
        completion_detail = getattr(usage, "completion_tokens_details", None)
        reasoning_tokens  = getattr(completion_detail, "reasoning_tokens", 0) or 0

        # Cache hit tokens from server-side KV cache
        prompt_detail     = getattr(usage, "prompt_tokens_details", None)
        cached_tokens     = getattr(prompt_detail, "cached_tokens", 0) or 0

        return self.create_response(
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            reasoning_tokens=reasoning_tokens,
            is_batch=is_batch,
        )
