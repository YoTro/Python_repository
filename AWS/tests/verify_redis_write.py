import pytest
import json
import os
from src.core.data_cache import DataCache, _RedisBackend
from src.mcp.servers.amazon.tools import handle_amazon_tool
from src.mcp.servers.finance.tools import handle_finance_tool
from unittest.mock import patch, AsyncMock
import src.core.data_cache

# Force Redis URL for this verification
os.environ["REDIS_URL"] = "redis://localhost:6379"

async def verify_redis_write():
    # 1. Setup Redis cache
    backend = _RedisBackend("redis://localhost:6379")
    cache = DataCache(backend=backend)
    
    # 2. L1 Action
    mock_products = [{"asin": "B0TEST", "price": 100.0, "title": "Test Product"}]
    
    with patch("src.mcp.servers.amazon.tools.BestSellersExtractor") as mock_ext_class:
        mock_ext = AsyncMock()
        mock_ext.get_bestsellers.return_value = mock_products
        mock_ext_class.return_value = mock_ext
        
        with patch("src.mcp.servers.amazon.tools.data_cache", cache):
            await handle_amazon_tool("get_amazon_bestsellers", {"url": "http://test"})
    
    # 3. Verify L1 wrote to Redis
    exists = cache.exists("amazon", "B0TEST")
    print(f"Key exists in Redis: {exists}")
    
    if exists:
        cached_data = cache.get("amazon", "B0TEST")
        print(f"Cached data price: {cached_data['price']}")
    
    return exists

if __name__ == "__main__":
    import asyncio
    asyncio.run(verify_redis_write())
