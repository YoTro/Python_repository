from __future__ import annotations

import asyncio
import logging
import os
import sys

import pytest

# Add project root for imports when run directly
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.mcp.servers.amazon.extractors.comments import CommentsExtractor


@pytest.mark.live
@pytest.mark.asyncio
async def test_fetch_comments():
    """Live integration: fetch page 1 of reviews via AJAX for a known ASIN."""
    logging.basicConfig(level=logging.INFO)
    extractor = CommentsExtractor()
    asin = "B0CPJ37XZH"

    try:
        reviews = await extractor.get_all_comments(asin, max_pages=1)
        if reviews:
            print(f"Fetched {len(reviews)} reviews")
            for i, r in enumerate(reviews[:2]):
                print(f"--- Review {i + 1} ---")
                print(f"Author: {r.author}")
                print(f"Rating: {r.rating}")
                print(f"Title: {r.title}")
        else:
            print("No reviews returned — possible captcha or token issue")
    except Exception as e:
        print(f"Fetch failed: {e}")
