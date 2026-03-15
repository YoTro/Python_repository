from __future__ import annotations
import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCache:
    """
    Core Data Cache for L1/L2 orchestration.
    
    L1 Servers (Amazon, Market, Social) write raw scraped data here.
    L2 Servers (Finance, Compliance) read from here to perform calculations.
    
    Implementation:
    - Single-user: Local JSON file-based key-value store.
    - Multi-user extension: Redis.
    """
    
    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "cache"
        )
        os.makedirs(self.cache_dir, exist_ok=True)
        self._memory_cache: Dict[str, Dict[str, Any]] = {}

    def _get_domain_path(self, domain: str) -> str:
        return os.path.join(self.cache_dir, f"{domain}.json")

    def _load_domain(self, domain: str):
        if domain in self._memory_cache:
            return
            
        path = self._get_domain_path(domain)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self._memory_cache[domain] = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load cache domain {domain}: {e}")
                self._memory_cache[domain] = {}
        else:
            self._memory_cache[domain] = {}

    def set(self, domain: str, key: str, value: Any):
        """Store data in a specific domain (e.g., 'amazon', 'market')."""
        self._load_domain(domain)
        
        # Ensure value is serializable or convert it
        # If it's a Pydantic model, it should be converted to dict before storage
        if hasattr(value, "model_dump"):
            value = value.model_dump()
        elif hasattr(value, "dict"):
            value = value.dict()
            
        self._memory_cache[domain][key] = {
            "data": value,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Persistence (Sync for now, easy to make async/Redis later)
        try:
            with open(self._get_domain_path(domain), "w", encoding="utf-8") as f:
                json.dump(self._memory_cache[domain], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Failed to persist cache domain {domain}: {e}")

    def get(self, domain: str, key: str) -> Optional[Any]:
        """Retrieve data from a specific domain."""
        self._load_domain(domain)
        entry = self._memory_cache[domain].get(key)
        if entry:
            return entry["data"]
        return None

    def exists(self, domain: str, key: str) -> bool:
        self._load_domain(domain)
        return key in self._memory_cache[domain]

# Global singleton
data_cache = DataCache()
