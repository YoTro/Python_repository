"""Unit tests for OpenAIProvider — fully mocked, no network calls.

Covers the OpenAI-specific behaviors that differ from the DeepSeek template:
  - factory wiring for the "openai" / "gpt" provider types
  - max_completion_tokens (not the deprecated max_tokens)
  - temperature omitted by default, forwarded only when explicitly given
  - internal metadata kwargs stripped before reaching the SDK
  - usage parsing + cost via the PriceManager "openai" branch (cached tokens
    billed at the cheaper cache-hit rate)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("openai")  # provider requires the openai package

from src.intelligence.providers.factory import ProviderFactory  # noqa: E402
from src.intelligence.providers.openai import OpenAIProvider  # noqa: E402


def _fake_completion(
    *,
    text: str = "hello",
    finish_reason: str = "stop",
    prompt_tokens: int = 1_000_000,
    completion_tokens: int = 1_000_000,
    cached_tokens: int = 0,
    reasoning_tokens: int = 0,
):
    """Build an object shaped like an OpenAI ChatCompletion response."""
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=text),
                finish_reason=finish_reason,
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            prompt_tokens_details=SimpleNamespace(cached_tokens=cached_tokens),
            completion_tokens_details=SimpleNamespace(reasoning_tokens=reasoning_tokens),
        ),
    )


@pytest.fixture
def provider(monkeypatch):
    """A real OpenAIProvider with its network client swapped for an AsyncMock."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    monkeypatch.delenv("MAX_LLM_OUTPUT_TOKENS", raising=False)

    p = OpenAIProvider(model_name="gpt-5.5")
    p._client.chat.completions.create = AsyncMock(return_value=_fake_completion())
    return p


# ── Factory wiring ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("ptype", ["openai", "gpt"])
def test_factory_builds_openai_provider(monkeypatch, ptype):
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    p = ProviderFactory.get_provider(ptype)
    assert isinstance(p, OpenAIProvider)
    assert p.provider_name == "openai"
    assert p.model_name == "gpt-5.5"  # _DEFAULT_MODEL


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        OpenAIProvider()


# ── Request shaping ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_uses_max_completion_tokens_and_omits_temperature(provider):
    await provider.generate_text("hi", system_message="sys")

    kwargs = provider._client.chat.completions.create.call_args.kwargs
    # GPT-5 / o-series require max_completion_tokens; max_tokens must not be sent.
    assert "max_completion_tokens" in kwargs
    assert "max_tokens" not in kwargs
    # Temperature omitted unless explicitly requested (reasoning models reject it).
    assert "temperature" not in kwargs
    # System + user messages assembled in order.
    assert kwargs["messages"] == [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "hi"},
    ]


@pytest.mark.asyncio
async def test_temperature_forwarded_when_given_and_internal_keys_stripped(provider):
    await provider.generate_text(
        "hi",
        temperature=0.7,
        session_id="s1",
        tenant_id="t1",
        cache_system_prompt=True,
    )

    kwargs = provider._client.chat.completions.create.call_args.kwargs
    assert kwargs["temperature"] == 0.7
    for internal in ("session_id", "tenant_id", "cache_system_prompt"):
        assert internal not in kwargs


@pytest.mark.asyncio
async def test_structured_requests_json_object(provider):
    class Schema:
        @staticmethod
        def model_json_schema():
            return {"type": "object", "properties": {"x": {"type": "integer"}}}

    await provider.generate_structured("extract", Schema)

    kwargs = provider._client.chat.completions.create.call_args.kwargs
    assert kwargs["response_format"] == {"type": "json_object"}


# ── Response parsing + cost ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_parse_usage_and_cost_with_cached_tokens(provider):
    provider._client.chat.completions.create = AsyncMock(
        return_value=_fake_completion(
            text="result",
            prompt_tokens=1_000_000,
            completion_tokens=1_000_000,
            cached_tokens=500_000,
            reasoning_tokens=200_000,
        )
    )

    resp = await provider.generate_text("hi")

    assert resp.text == "result"
    assert resp.provider_name == "openai"
    assert resp.currency == "USD"
    # gpt-5.5: 0.5M input@$5 + 0.5M cached@$0.5 + 1M output@$30
    #        = 2.5 + 0.25 + 30 = 32.75  (per 1M-token rates)
    assert resp.cost == pytest.approx(32.75)
    # token_usage = input + output + thought (reasoning is already inside output)
    assert resp.token_usage == 2_000_000
    assert resp.metadata["cached_tokens"] == 500_000


@pytest.mark.asyncio
async def test_batch_flag_halves_cost():
    """is_batch routes to the batch tier (uniform 50% discount)."""
    import os

    os.environ["OPENAI_API_KEY"] = "test-key"
    p = OpenAIProvider(model_name="gpt-5.5")

    std = p._parse_response(
        _fake_completion(prompt_tokens=1_000_000, completion_tokens=1_000_000),
        is_batch=False,
    )
    batch = p._parse_response(
        _fake_completion(prompt_tokens=1_000_000, completion_tokens=1_000_000),
        is_batch=True,
    )
    # standard 5 + 30 = 35 ; batch 2.5 + 15 = 17.5
    assert std.cost == pytest.approx(35.0)
    assert batch.cost == pytest.approx(17.5)
