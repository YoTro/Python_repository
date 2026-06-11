import asyncio
import logging
import os
import sys

import pytest

project_root = os.path.dirname(os.path.abspath(__name__))
sys.path.insert(0, project_root)

from src.mcp.servers.amazon.extractors.comments import CommentsExtractor


@pytest.mark.live
@pytest.mark.asyncio
async def test_fetch():
    logging.basicConfig(level=logging.INFO)
    extractor = CommentsExtractor()
    asin = "B0CPJ37XZH"

    print(f"正在尝试抓取 ASIN: {asin} 的评论...")

    # 尝试抓取第 1 页评论
    try:
        # 使用 AJAX 逻辑
        reviews = await extractor.get_all_comments(asin, max_pages=1)

        if reviews:
            print(f"✅ 成功获取到 {len(reviews)} 条评论！")
            for i, r in enumerate(reviews[:2]):
                print(f"--- 评论 {i + 1} ---")
                print(f"作者: {r.author}")
                print(f"评分: {r.rating}")
                print(f"标题: {r.title}")
        else:
            print("❌ 未获取到评论。可能是触发了验证码或 Token 获取失败。")

    except Exception as e:
        print(f"💥 抓取过程中出现异常: {e}")


if __name__ == "__main__":
    asyncio.run(test_fetch())
