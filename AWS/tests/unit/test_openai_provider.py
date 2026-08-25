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
async def test_internal_keys_stripped_and_temperature_blocked_for_reasoning_model(
    provider, monkeypatch
):
    # provider uses gpt-5.5 which is a reasoning model → temperature must NOT be forwarded
    # (only default=1 accepted; sending any other value causes a 400).
    await provider.generate_text(
        "hi",
        temperature=0.7,
        session_id="s1",
        tenant_id="t1",
        cache_system_prompt=True,
    )

    kwargs = provider._client.chat.completions.create.call_args.kwargs
    # Internal tracking keys must be stripped before reaching the SDK.
    for internal in ("session_id", "tenant_id", "cache_system_prompt"):
        assert internal not in kwargs
    # Reasoning model: temperature is silently dropped to avoid a 400.
    assert "temperature" not in kwargs


@pytest.mark.asyncio
async def test_temperature_forwarded_for_non_reasoning_model(monkeypatch):
    """Temperature IS forwarded when the model is not an o-series / gpt-5+ reasoning model."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    monkeypatch.delenv("MAX_LLM_OUTPUT_TOKENS", raising=False)

    p = OpenAIProvider(model_name="gpt-4o")
    p._client.chat.completions.create = AsyncMock(return_value=_fake_completion())
    p._resolved_model = "gpt-4o"  # skip the live /models check in _active_model()

    await p.generate_text("hi", temperature=0.7)

    kwargs = p._client.chat.completions.create.call_args.kwargs
    assert kwargs["temperature"] == 0.7


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
    # Token counts stay under the 272,000 long-context threshold so standard-tier
    # rates apply (1M tokens would trip gpt-5.5's long_context_standard tier).
    provider._client.chat.completions.create = AsyncMock(
        return_value=_fake_completion(
            text="result",
            prompt_tokens=200_000,
            completion_tokens=200_000,
            cached_tokens=100_000,
            reasoning_tokens=40_000,
        )
    )

    resp = await provider.generate_text("hi")

    assert resp.text == "result"
    assert resp.provider_name == "openai"
    assert resp.currency == "USD"
    # gpt-5.5 standard: 100K input@$5 + 100K cached@$0.5 + 200K output@$30
    #        = 0.5 + 0.05 + 6.0 = 6.55  (rates are per 1M tokens)
    assert resp.cost == pytest.approx(6.55)
    # token_usage = input + output (reasoning is already inside output)
    assert resp.token_usage == 400_000
    assert resp.metadata["cached_tokens"] == 100_000


@pytest.mark.asyncio
async def test_batch_flag_halves_cost():
    """is_batch routes to the batch tier (uniform 50% discount)."""
    import os

    os.environ["OPENAI_API_KEY"] = "test-key"
    p = OpenAIProvider(model_name="gpt-5.5")

    # Under the 272,000 long-context threshold → standard-tier rates.
    std = p._parse_response(
        _fake_completion(prompt_tokens=200_000, completion_tokens=200_000),
        is_batch=False,
    )
    batch = p._parse_response(
        _fake_completion(prompt_tokens=200_000, completion_tokens=200_000),
        is_batch=True,
    )
    # standard 200K in@$5 + 200K out@$30 = 1.0 + 6.0 = 7.0
    # batch (50% off)  200K in@$2.5 + 200K out@$15 = 0.5 + 3.0 = 3.5
    assert std.cost == pytest.approx(7.0)
    assert batch.cost == pytest.approx(3.5)


@pytest.mark.asyncio
async def test_long_context_tier_above_272k_threshold():
    """>272,000 prompt tokens switches gpt-5.5 to its long-context tier.

    Standard rates ($5/$0.5/$30) give way to long_context_standard ($10/$1/$45),
    and the batch discount stacks on top (long_context_batch = $5/$0.5/$22.5).
    """
    import os

    os.environ["OPENAI_API_KEY"] = "test-key"
    p = OpenAIProvider(model_name="gpt-5.5")

    # 300K prompt tokens > 272K threshold; 100K of them are a cache hit.
    completion = _fake_completion(
        prompt_tokens=300_000,
        completion_tokens=100_000,
        cached_tokens=100_000,
    )

    std = p._parse_response(completion, is_batch=False)
    batch = p._parse_response(completion, is_batch=True)

    # long_context_standard: 200K in@$10 + 100K cached@$1 + 100K out@$45
    #                      = 2.0 + 0.1 + 4.5 = 6.6
    assert std.cost == pytest.approx(6.6)
    # long_context_batch: 200K in@$5 + 100K cached@$0.5 + 100K out@$22.5
    #                   = 1.0 + 0.05 + 2.25 = 3.3
    assert batch.cost == pytest.approx(3.3)
