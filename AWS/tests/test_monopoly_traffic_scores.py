from types import SimpleNamespace

import pytest

from src.workflows.definitions import category_monopoly_analysis as cma


class _MCP:
    def __init__(self, response):
        self.response = response

    async def call_tool_json(self, _name, _arguments):
        return self.response


@pytest.mark.asyncio
async def test_enrich_batch_traffic_scores_reads_decoded_tool_json(monkeypatch):
    """call_tool_json() returns the parsed payload directly (a dict here) —
    the real Xiyouzhaoci trafficScore API responds with {"entities": [...]}
    and string-typed ratio fields.
    """
    monkeypatch.setattr(cma, "_l2_get", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cma, "_l2_set", lambda *_args, **_kwargs: None)
    ctx = SimpleNamespace(
        cache={},
        config={"store_id": "US"},
        mcp=_MCP(
            {
                "entities": [
                    {"asin": "B000000001", "advertisingTrafficScoreRatio": "0.20"},
                    {"asin": "B000000002", "advertisingTrafficScoreRatio": "0.40"},
                ]
            }
        ),
    )

    await cma._enrich_batch_traffic_scores(
        [{"ASIN": "B000000001"}, {"ASIN": "B000000002"}], ctx
    )

    assert ctx.cache["actual_bsr_ad_ratio"] == pytest.approx(0.30)
