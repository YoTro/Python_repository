from __future__ import annotations

import json
import logging
import os
from typing import Any

from src.intelligence.dto import LLMResponse

from .base import BaseLLMProvider

try:
    from deepseek_tokenizer import ds_token as _ds_token
except ImportError:
    _ds_token = None

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "deepseek-v4-flash"

# DeepSeek recommended temperatures (API default: 1.0).
# Source: https://api-docs.deepseek.com/quick_start/token_usage
#
#   Coding / Math                → 0.0
#   Data Cleaning / Analysis     → 1.0
#   General Conversation         → 1.3
#   Translation                  → 1.3
#   Creative Writing / Poetry    → 1.5
TEMPERATURE_PRESETS = {
    "coding": 0.0,
    "math": 0.0,
    "data_cleaning": 1.0,
    "data_analysis": 1.0,
    "conversation": 1.3,
    "translation": 1.3,
    "creative": 1.5,
}

_CONTEXT_WINDOWS = {
    "deepseek-v4-flash": 131_072,
    "deepseek-v4-pro": 131_072,
    # Legacy aliases — deprecated 2026-07-24, resolved to deepseek-v4-flash
    "deepseek-chat": 131_072,
    "deepseek-reasoner": 131_072,
}


class DeepSeekProvider(BaseLLMProvider):
    """
    DeepSeek provider using the OpenAI-compatible REST API.
    Requires `openai` package (pip install openai).

    Current models:
      - deepseek-v4-flash  (general purpose, formerly deepseek-chat / deepseek-v3)
      - deepseek-v4-pro    (reasoning, formerly deepseek-reasoner / deepseek-r1)

    Deprecated aliases (removed 2026-07-24): deepseek-chat, deepseek-reasoner.

    Server-side KV cache is automatic; prompt_cache_hit_tokens in the
    usage response drives the cheaper cache-hit billing rate.
    """

    _MODEL_CONTEXT_WINDOWS = _CONTEXT_WINDOWS

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str | None = None,
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
            raise ImportError("openai package required: pip install openai") from None

        from .config.limits import get_max_output_tokens

        _ceiling = get_max_output_tokens("deepseek", self.model_name)
        _env = os.getenv("MAX_LLM_OUTPUT_TOKENS", "").strip()
        self._DEFAULT_MAX_TOKENS = min(int(_env) if _env else _ceiling, _ceiling)

        self._resolved_model: str | None = None  # set on first call after live validation

        logger.info(
            f"DeepSeekProvider initialized: model={self.model_name}, max_output_tokens: {self._DEFAULT_MAX_TOKENS}"
        )

    # ── Token counting ────────────────────────────────────────────────────────

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Character-ratio fallback per DeepSeek docs: CJK ≈ 0.6 tok/char, other ≈ 0.3 tok/char."""
        total = 0.0
        for ch in text:
            cp = ord(ch)
            if (0x4E00 <= cp <= 0x9FFF    # CJK Unified Ideographs
                    or 0x3400 <= cp <= 0x4DBF    # CJK Extension A
                    or 0xF900 <= cp <= 0xFAFF):  # CJK Compatibility Ideographs
                total += 0.6
            else:
                total += 0.3
        return max(1, int(total))

    async def count_tokens(self, prompt: str, system_message: str | None = None) -> int:
        full = (system_message or "") + prompt
        if _ds_token is not None:
            return max(1, len(_ds_token.encode(full)))
        return self._estimate_tokens(full)

    # ── Model listing & validation ────────────────────────────────────────────

    async def list_models(self) -> list[dict]:
        """Return all models available on the DeepSeek platform via GET /models."""
        response = await self._client.models.list()
        return [{"id": m.id, "owned_by": getattr(m, "owned_by", None)} for m in response.data]

    async def _active_model(self) -> str:
        """Return the validated model name, falling back to the first available if needed.

        Result is cached after the first successful /models call so subsequent
        inference calls pay no extra latency.
        """
        if self._resolved_model is not None:
            return self._resolved_model

        try:
            available = [m["id"] for m in await self.list_models()]
        except Exception as e:
            logger.warning(f"DeepSeek /models lookup failed ({e}); using configured model as-is")
            self._resolved_model = self.model_name
            return self._resolved_model

        if self.model_name in available:
            self._resolved_model = self.model_name
        else:
            fallback = available[0] if available else _DEFAULT_MODEL
            logger.warning(
                f"Model '{self.model_name}' not in live model list {available}; "
                f"falling back to '{fallback}'"
            )
            self._resolved_model = fallback

        return self._resolved_model

    # ── Text generation ───────────────────────────────────────────────────────

    async def generate_text(
        self,
        prompt: str,
        system_message: str | None = None,
        **kwargs,
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        filtered = self._filter_kwargs(kwargs)
        messages = self._build_messages(prompt, system_message)

        try:
            resp = await self._client.chat.completions.create(
                model=await self._active_model(),
                messages=messages,
                max_tokens=self._DEFAULT_MAX_TOKENS,
                # Caller should pass temperature via kwargs; 0.0 suits coding/math tasks.
                # See TEMPERATURE_PRESETS for DeepSeek's per-use-case recommendations.
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
            self._raise_mapped_error(e)

    # ── Structured generation (JSON mode) ────────────────────────────────────

    async def generate_structured(
        self,
        prompt: str,
        schema: Any,
        system_message: str | None = None,
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
                model=await self._active_model(),
                messages=messages,
                max_tokens=self._DEFAULT_MAX_TOKENS,
                # Caller should pass temperature via kwargs; 0.0 suits coding/math tasks.
                # See TEMPERATURE_PRESETS for DeepSeek's per-use-case recommendations.
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
            self._raise_mapped_error(e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _build_messages(prompt: str, system_message: str | None) -> list:
        msgs = []
        if system_message:
            msgs.append({"role": "system", "content": system_message})
        msgs.append({"role": "user", "content": prompt})
        return msgs

    def _parse_response(self, resp, *, is_batch: bool) -> LLMResponse:
        choice = resp.choices[0]
        text = choice.message.content or ""
        usage = resp.usage

        input_tokens = getattr(usage, "prompt_tokens", 0) or 0
        output_tokens = getattr(usage, "completion_tokens", 0) or 0

        # Reasoning tokens (deepseek-reasoner only) are included in completion_tokens.
        # Extract them for transparency but do NOT double-count in cost — the API
        # already rolls them into completion_tokens for billing.
        completion_detail = getattr(usage, "completion_tokens_details", None)
        reasoning_tokens = getattr(completion_detail, "reasoning_tokens", 0) or 0

        # Cache hit tokens from server-side KV cache
        prompt_detail = getattr(usage, "prompt_tokens_details", None)
        cached_tokens = getattr(prompt_detail, "cached_tokens", 0) or 0

        return self.create_response(
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_tokens=cached_tokens,
            reasoning_tokens=reasoning_tokens,
            is_batch=is_batch,
        )
