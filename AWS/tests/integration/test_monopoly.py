from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.intelligence.dto import LLMResponse
from src.intelligence.processors.monopoly_analyzer import CategoryMonopolyAnalyzer
from src.workflows.definitions import category_monopoly_analysis as cma
from src.workflows.definitions.category_monopoly_analysis import _run_monopoly_analysis
from src.workflows.steps.base import ComputeTarget, WorkflowContext
from src.workflows.steps.process import ProcessStep


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def analyzer():
    return CategoryMonopolyAnalyzer()


# ---------------------------------------------------------------------------
# CategoryMonopolyAnalyzer — unit tests
# ---------------------------------------------------------------------------


class TestMonopolyAnalyzer:
    def test_analyze_empty(self, analyzer):
        result = analyzer.analyze([])
        assert "error" in result

    def test_sales_distribution(self, analyzer):
        products = [
            {"rank": 1, "sales": 10000},
            {"rank": 2, "sales": 8000},
            {"rank": 3, "sales": 5000},
            {"rank": 4, "sales": 1000},
            {"rank": 5, "sales": 500},
        ]
        for i in range(6, 60):
            products.append({"rank": i, "sales": 100})

        scores = analyzer._analyze_sales_distribution(products)
        assert scores["top3_concentration"] == 100
        assert scores["cr3"] > 0.70

    def test_brand_concentration(self, analyzer):
        diverse = [{"rank": i, "brand": f"Brand_{i}"} for i in range(10)]
        monopoly = [{"rank": i, "brand": f"Brand_{i % 2}"} for i in range(10)]

        score_diverse = analyzer._analyze_brand_concentration(diverse)
        score_monopoly = analyzer._analyze_brand_concentration(monopoly)

        assert score_monopoly > score_diverse

    def test_review_barrier(self, analyzer):
        high = [
            {"rank": i, "review_count": 10000 if i <= 10 else 100, "rating": 4.6 if i <= 10 else 4.0}
            for i in range(1, 60)
        ]
        low = [
            {"rank": i, "review_count": 1000 if i <= 10 else 800, "rating": 4.1}
            for i in range(1, 60)
        ]

        assert analyzer._analyze_review_barrier(high) > analyzer._analyze_review_barrier(low)

    def test_price_convergence(self, analyzer):
        converged = [{"rank": i, "price": 20.0 + (i % 3)} for i in range(20)]
        diverged = [{"rank": i, "price": (i * 10) + 10} for i in range(20)]

        assert analyzer._analyze_price_convergence(converged) > analyzer._analyze_price_convergence(
            diverged
        )

    def test_full_analysis_integration(self, analyzer):
        products = []
        for i in range(1, 101):
            products.append(
                {
                    "rank": i,
                    "sales": max(10, 1000 - i * 10),
                    "price": 25.0 + (i % 10),
                    "brand": f"Brand_{i % 30}",
                    "seller_type": "FBA" if i % 5 != 0 else "Amazon",
                    "feedback_count": 5000 if i > 10 else 20000,
                    "review_count": 2000 if i <= 10 else 500,
                    "rating": 4.5 if i <= 10 else 4.2,
                }
            )

        keyword_data = {
            "top_asins": [{"clickShare": 0.20}, {"clickShare": 0.15}, {"clickShare": 0.10}]
        }
        ad_data = {"ad_ratio": 0.25}

        result = analyzer.analyze(products, keyword_data=keyword_data, ad_data=ad_data)

        assert "overall_score" in result
        assert "status" in result
        assert "dimension_details" in result
        assert "cr3" in result["summary_metrics"]
        assert 30 < result["overall_score"] < 80


# ---------------------------------------------------------------------------
# _run_monopoly_analysis — data flattening & prompt integration
# ---------------------------------------------------------------------------


class TestRunMonopolyAnalysis:
    @pytest.mark.asyncio
    async def test_data_flattening_and_prompt_formatting(self):
        mock_items = [
            {"Rank": i, "Price": "$50.00", "Reviews": "1000", "Rating": "4.5", "sales": 500}
            for i in range(1, 101)
        ]

        ctx = MagicMock(spec=WorkflowContext)
        ctx.cache = {"main_keyword": "test espresso machine", "keyword_data": {}, "ad_ratio": 0.2}
        ctx.config = {"category_node_id": "123456"}
        ctx.job_id = "test_job"

        with (
            patch(
                "src.intelligence.processors.monopoly_analyzer.CategoryMonopolyAnalyzer.analyze"
            ) as mock_analyze,
            patch(
                "src.workflows.definitions.category_monopoly_analysis.SalesEstimator"
            ) as mock_est_cls,
        ):
            mock_analyze.return_value = {"overall_score": 75.5, "status": "High Monopoly"}

            mock_estimator = mock_est_cls.return_value
            mock_estimator.category_params = {
                "123456": {"r_squared": 0.99, "market_logic": {"typical_cr3": 0.45}}
            }

            results = await _run_monopoly_analysis(mock_items, ctx)
            item = results[0]

            assert "recommended_capital" in item
            assert "niche_median_price" in item
            assert "industry_typical_cr3" in item
            assert item["recommended_capital"].startswith("$")
            assert item["industry_typical_cr3"] == "45.0%"
            assert item["data_confidence_r2"] == 0.99

            # Verify ProcessStep prompt formatting
            step = ProcessStep(
                name="test_step",
                prompt_template=(
                    "Advising on a {recommended_capital} investment for "
                    "{main_keyword} (R2: {data_confidence_r2})."
                ),
                compute_target=ComputeTarget.CLOUD_LLM,
            )

            ctx.router = MagicMock()
            mock_response = LLMResponse(text="Success", provider_name="mock", model_name="mock")
            ctx.router.route_and_execute = AsyncMock(return_value=mock_response)

            await step.run(results, ctx)

            formatted_prompt = ctx.router.route_and_execute.call_args[0][0]
            capital = item["recommended_capital"]
            assert capital.startswith("$")
            assert capital in formatted_prompt
            assert "test espresso machine" in formatted_prompt
            assert "0.99" in formatted_prompt


# ---------------------------------------------------------------------------
# _enrich_batch_traffic_scores
# ---------------------------------------------------------------------------


class _MCP:
    def __init__(self, response):
        self.response = response

    async def call_tool_json(self, _name, _arguments):
        return self.response


class TestEnrichBatchTrafficScores:
    @pytest.mark.asyncio
    async def test_reads_decoded_tool_json(self, monkeypatch):
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
