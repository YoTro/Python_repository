import pytest
import asyncio
from unittest.mock import MagicMock, patch
from src.workflows.definitions.category_monopoly_analysis import _run_monopoly_analysis
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import WorkflowContext, ComputeTarget

@pytest.mark.asyncio
async def test_data_flattening_logic():
    """
    Verify that _run_monopoly_analysis returns flattened keys
    and ProcessStep can use them in a prompt template.
    """
    # 1. Setup Mock Items (Top 100 sample)
    mock_items = [
        {"Rank": i, "Price": "$50.00", "Reviews": "1000", "Rating": "4.5", "sales": 500}
        for i in range(1, 101)
    ]

    # 2. Setup Mock Context
    ctx = MagicMock(spec=WorkflowContext)
    ctx.cache = {"main_keyword": "test espresso machine", "keyword_data": {}, "ad_ratio": 0.2}
    ctx.config = {"category_node_id": "123456"}
    ctx.job_id = "test_job"

    # 3. Mock the Analyzer and Estimator to avoid real calculations
    with patch("src.intelligence.processors.monopoly_analyzer.CategoryMonopolyAnalyzer.analyze") as mock_analyze, \
         patch("src.intelligence.processors.sales_estimator.SalesEstimator") as mock_est_cls:
        
        # Mock Analyzer output
        mock_analyze.return_value = {"overall_score": 75.5, "status": "High Monopoly"}
        
        # Mock Estimator and its params
        mock_estimator = mock_est_cls.return_value
        mock_estimator.category_params = {
            "123456": {
                "r_squared": 0.99,
                "market_logic": {"typical_cr3": 0.45}
            }
        }

        # 4. Execute the function under test
        results = await _run_monopoly_analysis(mock_items, ctx)

        # 5. Assert Flattening
        item = results[0]
        print(f"\nFlattened Item Keys: {item.keys()}")
        
        assert "recommended_capital" in item
        assert "niche_median_price" in item
        assert "industry_typical_cr3" in item
        assert item["recommended_capital"].startswith("$")
        assert item["industry_typical_cr3"] == "45.0%"
        assert item["data_confidence_r2"] == 0.99

        # 6. Test actual formatting in ProcessStep
        step = ProcessStep(
            name="test_step",
            prompt_template="Advising on a {recommended_capital} investment for {main_keyword} (R2: {data_confidence_r2}).",
            compute_target=ComputeTarget.CLOUD_LLM
        )
        
        # Mock IntelligenceRouter to avoid LLM call
        from src.intelligence.dto import LLMResponse
        from unittest.mock import AsyncMock
        
        ctx.router = MagicMock()
        mock_response = LLMResponse(text="Success", provider_name="mock", model_name="mock")
        ctx.router.route_and_execute = AsyncMock(return_value=mock_response)
        
        # This will trigger _run_llm -> prompt.format(...)
        await step.run(results, ctx)
        
        # Check if formatting succeeded
        formatted_prompt = ctx.router.route_and_execute.call_args[0][0]
        print(f"Formatted Prompt: {formatted_prompt}")
        
        assert "$125,000" in formatted_prompt # 50 * 2500 = 125000
        assert "test espresso machine" in formatted_prompt
        assert "0.99" in formatted_prompt

if __name__ == "__main__":
    asyncio.run(test_data_flattening_logic())
