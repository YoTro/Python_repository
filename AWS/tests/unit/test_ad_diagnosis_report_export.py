from unittest.mock import AsyncMock, MagicMock

import pytest

from src.core.errors.exceptions import FatalError
from src.intelligence.dto import LLMResponse
from src.workflows.definitions.ad_diagnosis import _export_report
from src.workflows.steps.base import ComputeTarget, WorkflowContext
from src.workflows.steps.process import ProcessStep


@pytest.mark.asyncio
async def test_process_step_realtime_llm_maps_result_without_tuple_unpack_error():
    ctx = WorkflowContext(job_id="job-1")
    ctx.router = MagicMock()
    ctx.router.cloud = None
    ctx.router.route_and_execute = AsyncMock(
        return_value=LLMResponse(text="report", provider_name="mock", model_name="mock")
    )
    step = ProcessStep(
        name="ad_diagnosis_llm",
        prompt_template="Diagnose {asin}",
        compute_target=ComputeTarget.CLOUD_LLM,
    )

    result = await step.run([{"asin": "B0TESTASIN"}], ctx)

    assert result.items[0]["ad_diagnosis_llm"].text == "report"


@pytest.mark.asyncio
async def test_process_step_prompt_format_error_fails_fast():
    ctx = WorkflowContext(job_id="job-1")
    ctx.router = MagicMock()
    step = ProcessStep(
        name="ad_diagnosis_llm",
        prompt_template="Diagnose {missing_field}",
        compute_target=ComputeTarget.CLOUD_LLM,
    )

    with pytest.raises(FatalError, match="prompt formatting failed"):
        await step.run([{"asin": "B0TESTASIN"}], ctx)


def test_export_report_raises_clear_error_when_llm_text_missing():
    ctx = WorkflowContext(job_id="job-1", config={})
    item = {"asin": "B0TESTASIN", "chart_urls": {}}

    with pytest.raises(FatalError, match="missing LLM report text"):
        _export_report([item], ctx)

    assert "未生成报告正文" in item["response"]


def test_export_report_accepts_llm_response_object(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    ctx = WorkflowContext(job_id="job-1", config={})
    item = {
        "asin": "B0TESTASIN",
        "ad_diagnosis_llm": LLMResponse(
            text="# Diagnosis\n\nBody",
            provider_name="mock",
            model_name="mock",
        ),
        "chart_urls": {},
    }

    result = _export_report([item], ctx)

    assert result[0]["report_file_path"].endswith(".md")
    assert "完整报告已保存" in result[0]["response"]
