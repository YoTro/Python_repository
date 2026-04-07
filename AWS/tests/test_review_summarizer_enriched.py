from __future__ import annotations
import pytest
import json
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timedelta
from src.core.models.review import Review, ReviewSummary
from src.intelligence.processors.review_summarizer import ReviewSummarizer

@pytest.fixture
def mock_provider():
    provider = MagicMock()
    provider.generate_structured = AsyncMock()
    return provider

def test_metrics_calculation():
    """验证追评速率和时间屏障的纯算逻辑。"""
    summarizer = ReviewSummarizer(provider=MagicMock())
    
    # 模拟 30 天内的 10 条评论
    now = datetime.now()
    reviews = []
    for i in range(10):
        # 均匀分布在过去 30 天
        date_str = (now - timedelta(days=i*3)).strftime("%B %d, %Y")
        reviews.append(Review(
            asin="B0TEST",
            rating=5 if i % 2 == 0 else 4,
            date=f"Reviewed in the United States on {date_str}",
            is_verified=True
        ))
    
    # 计算指标，设定竞争水位为 100 条
    stats = summarizer._calculate_metrics(reviews, benchmark=100)
    
    # 10 条评论 / 27 天 span * 30.44 = 约 11.27
    assert stats["velocity"] > 10.0
    assert stats["distribution"][5] == 5
    assert stats["distribution"][4] == 5
    # (100 - 10) / velocity = 约 8-9 个月
    assert stats["barrier_months"] > 0
    assert stats["barrier_months"] < 10

@pytest.mark.asyncio
async def test_summarize_integration(mock_provider):
    """验证 summarize 方法能否正确调用 Prompt 渲染并返回富化后的模型。"""
    summarizer = ReviewSummarizer(provider=mock_provider)
    
    # 准备测试数据
    reviews = [
        Review(asin="B0TEST", rating=5, date="January 1, 2024", content="Great!", is_verified=True),
        Review(asin="B0TEST", rating=1, date="February 1, 2024", content="Bad!", is_verified=True)
    ]
    
    # 模拟 LLM 返回的基础总结
    mock_summary = ReviewSummary(
        pros=["Quality"],
        cons=["Price"],
        sentiment_score=0.5,
        top_complaints=["Broken"],
        buyer_persona="Everyone"
    )
    mock_provider.generate_structured.return_value = mock_summary
    
    # 执行
    result = await summarizer.summarize(reviews, competitive_benchmark=100)
    
    # 验证
    assert result.review_velocity > 0
    assert result.rating_distribution[5] == 1
    assert result.rating_distribution[1] == 1
    assert result.competitive_barrier_months is not None
    
    # 验证 Prompt 渲染是否包含了量化数据 (检查 mock 调用参数)
    args, kwargs = mock_provider.generate_structured.call_args
    user_prompt = kwargs['prompt']
    assert "Monthly Review Velocity" in user_prompt
    assert "Rating Distribution" in user_prompt
