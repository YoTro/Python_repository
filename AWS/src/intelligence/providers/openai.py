from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from src.intelligence.dto import LLMResponse

from .base import BaseLLMProvider

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "gpt-5.5"


class OpenAIProvider(BaseLLMProvider):
    """
    OpenAI (GPT) provider built on the official ``openai`` async SDK.
    Requires the ``openai`` package (pip install openai).

    Notes specific to the GPT-5.x / reasoning generation:
      - These models take ``max_completion_tokens`` rather than the deprecated
        ``max_tokens`` and reject a non-default ``temperature`` (only the
        default is accepted), so temperature is forwarded only when the caller
        explicitly provides it.
      - Automatic prompt caching is server-side; ``prompt_tokens_details.cached_tokens``
        in the usage response drives the cheaper cache-hit rate in PriceManager.
      - Context windows are intentionally not hard-coded (the base context-limit
        guard stays disabled): the API returns a clean error if a request truly
        exceeds the model window, which is preferable to a fabricated ceiling that
        could reject valid requests.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model_name: str | None = None,
        base_url: str | None = None,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY missing.")

        resolved = model_name or os.getenv("OPENAI_MODEL") or _DEFAULT_MODEL
        super().__init__("openai", resolved)

        try:
            from openai import AsyncOpenAI

            # base_url defaults to the OpenAI endpoint; override for Azure/proxies
            # via the OPENAI_BASE_URL env var or the constructor argument.
            self._client = AsyncOpenAI(
                api_key=self.api_key,
                base_url=base_url or os.getenv("OPENAI_BASE_URL") or None,
            )
        except ImportError:
            raise ImportError("openai package required: pip install openai") from None

        from .config.limits import get_max_output_tokens

        _ceiling = get_max_output_tokens("openai", self.model_name)
        _env = os.getenv("MAX_LLM_OUTPUT_TOKENS", "").strip()
        self._DEFAULT_MAX_TOKENS = min(int(_env) if _env else _ceiling, _ceiling)

        self._resolved_model: str | None = None

        logger.info(
            f"OpenAIProvider initialized: model={self.model_name}, "
            f"max_output_tokens: {self._DEFAULT_MAX_TOKENS}"
        )

    # ── Token counting ────────────────────────────────────────────────────────

    def _tiktoken_encoding(self) -> str:
        """Return the tiktoken encoding name for the current model.

        o200k_base: GPT-5+ and o-series (o1, o3, o4…)
        cl100k_base: GPT-4 and older
        """
        m = self.model_name.lower()
        if re.search(r"\bo\d", m):  # o1, o3, o4-mini, …
            return "o200k_base"
        match = re.search(r"gpt-(\d+)", m)
        if match and int(match.group(1)) >= 5:
            return "o200k_base"
        return "cl100k_base"

    async def list_models(self) -> list[dict]:
        """Return all models available via GET /models."""
        try:
            response = await self._client.models.list()
            return [{"id": m.id, "owned_by": getattr(m, "owned_by", None)} for m in response.data]
        except Exception as e:
            logger.warning(f"OpenAI /models lookup failed: {e}")
            return [{"id": _DEFAULT_MODEL, "owned_by": None}]

    async def _active_model(self) -> str:
        """Return the validated model name, falling back to the first available if needed.

        Result is cached after the first successful /models call so subsequent
        inference calls pay no extra latency.
        """
        if self._resolved_model is not None:
            return self._resolved_model

        try:
            # Filter to chat-completion-capable models only; /v1/models also returns
            # dall-e, whisper, text-embedding, etc. which would crash at inference.
            available = [
                m["id"]
                for m in await self.list_models()
                if m["id"].startswith("gpt-") or re.match(r"o\d", m["id"])
            ]
        except Exception as e:
            logger.warning(f"OpenAI model resolution failed ({e}); using configured model as-is")
            self._resolved_model = self.model_name
            return self._resolved_model

        if self.model_name in available:
            self._resolved_model = self.model_name
        else:
            fallback = available[0] if available else _DEFAULT_MODEL
            logger.warning(
                f"Model '{self.model_name}' not in live model list; falling back to '{fallback}'"
            )
            self._resolved_model = fallback

        return self._resolved_model

    async def count_tokens(self, prompt: str, system_message: str | None = None) -> int:
        full = (system_message or "") + prompt
        try:
            import tiktoken

            try:
                enc = tiktoken.encoding_for_model(self.model_name)
            except KeyError:
                enc = tiktoken.get_encoding(self._tiktoken_encoding())
            return len(enc.encode(full))
        except ImportError:
            logger.debug("tiktoken not installed; falling back to char estimate")
            return max(1, len(full) // 4)

    # ── Text generation ───────────────────────────────────────────────────────

    async def generate_text(
        self,
        prompt: str,
        system_message: str | None = None,
        **kwargs,
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)
        params = self._build_params(prompt, system_message, kwargs, await self._active_model())

        try:
            resp = await self._client.chat.completions.create(**params)
            self._warn_if_truncated(resp)
            return self._parse_response(resp, is_batch=False)
        except Exception as e:
            logger.error(f"OpenAI generate_text failed: {e}")
            self._raise_mapped_error(e)

    # ── Structured generation (JSON mode) ─────────────────────────────────────

    async def generate_structured(
        self,
        prompt: str,
        schema: Any,
        system_message: str | None = None,
        **kwargs,
    ) -> LLMResponse:
        await self._check_context_limit(prompt, system_message)

        schema_hint = ""
        try:
            schema_hint = "\n\nRespond with valid JSON matching this schema:\n" + json.dumps(
                schema.model_json_schema(), indent=2
            )
        except Exception:
            pass

        params = self._build_params(
            prompt + schema_hint, system_message, kwargs, await self._active_model()
        )
        params["response_format"] = {"type": "json_object"}

        try:
            resp = await self._client.chat.completions.create(**params)
            self._warn_if_truncated(resp)
            return self._parse_response(resp, is_batch=False)
        except Exception as e:
            logger.error(f"OpenAI generate_structured failed: {e}")
            self._raise_mapped_error(e)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_params(
        self, prompt: str, system_message: str | None, kwargs: dict, model: str | None = None
    ) -> dict[str, Any]:
        """Assemble chat.completions.create() params, honoring GPT-5/reasoning rules."""
        filtered = self._filter_kwargs(kwargs)
        params: dict[str, Any] = {
            "model": model or self.model_name,
            "messages": self._build_messages(prompt, system_message),
            # GPT-5 / o-series require max_completion_tokens; max_tokens is rejected.
            "max_completion_tokens": self._DEFAULT_MAX_TOKENS,
        }
        # Reasoning models accept only the default temperature, so forward it only
        # when the caller explicitly set one.
        temperature = filtered.pop("temperature", None)
        if temperature is not None:
            params["temperature"] = temperature
        params.update(filtered)
        return params

    @staticmethod
    def _build_messages(prompt: str, system_message: str | None) -> list:
        msgs = []
        if system_message:
            msgs.append({"role": "system", "content": system_message})
        msgs.append({"role": "user", "content": prompt})
        return msgs

    def _warn_if_truncated(self, resp) -> None:
        if resp.choices and resp.choices[0].finish_reason == "length":
            logger.warning(
                f"OpenAI response truncated at max_completion_tokens={self._DEFAULT_MAX_TOKENS}. "
                "Set MAX_LLM_OUTPUT_TOKENS env var to increase the limit."
            )

    def _parse_response(self, resp, *, is_batch: bool) -> LLMResponse:
        choice = resp.choices[0]
        text = choice.message.content or ""
        usage = resp.usage

        input_tokens = getattr(usage, "prompt_tokens", 0) or 0
        output_tokens = getattr(usage, "completion_tokens", 0) or 0

        # Reasoning tokens (o-series / gpt-5 reasoning) are already rolled into
        # completion_tokens for billing — surface them but do not double-count.
        completion_detail = getattr(usage, "completion_tokens_details", None)
        reasoning_tokens = getattr(completion_detail, "reasoning_tokens", 0) or 0

        # Cached input tokens from automatic prompt caching → cheaper cache-hit rate.
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
