from __future__ import annotations
import os
import json
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

class PriceManager:
    """
    Universal LLM pricing manager.
    Supports provider-specific lookup keys and tiered pricing.
    """

    def __init__(self, provider: str = "gemini"):
        self.provider = provider.lower()
        # Map provider to its specific config file
        config_filename = f"{self.provider}_pricing.json"
        self.config_path = os.path.join(os.path.dirname(__file__), "config", config_filename)
        
        self.data = self._load_config()
        self.lookup = self.data.get("lookup", {})
        self.model_index = self.data.get("model_string_index", {})
        self.currency = self.data.get("metadata", {}).get("currency", "USD")

    def _load_config(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.config_path):
                logger.warning(f"Pricing config for {self.provider} not found at {self.config_path}")
                return {}
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {self.provider} pricing config: {e}")
            return {}

    def normalize_model_name(self, model_name: str) -> str:
        """Strip prefixes and map to canonical ID."""
        clean_name = model_name.strip().lower()
        
        # Provider specific prefix stripping
        if self.provider == "gemini":
            clean_name = clean_name.removeprefix("models/")
        
        # Check index
        index_entry = self.model_index.get(clean_name)
        if isinstance(index_entry, dict):
            return index_entry.get("canonical_id", clean_name)
        elif isinstance(index_entry, str):
            return index_entry
            
        return clean_name

    def calculate_cost(
        self, 
        model_name: str, 
        input_tokens: int, 
        output_tokens: int, 
        tier: str = "standard",
        **kwargs
    ) -> float:
        """
        Calculates cost based on provider-specific rules.
        """
        canonical_model = self.normalize_model_name(model_name)
        
        if self.provider == "gemini":
            # 1. Extract detailed token counts from kwargs (flexible naming)
            cached_tokens = kwargs.get("cached_content_token_count") or kwargs.get("cached_tokens") or 0
            thoughts_tokens = kwargs.get("thought_token_count") or kwargs.get("thoughts_token_count") or kwargs.get("thoughts_tokens") or 0
            
            # 2. Prepare billing parameters
            gemini_tier = tier if "_" in tier else f"{tier}_paid"
            # Context tier is typically determined by total input (prompt) tokens
            context_tier = "gt_200k" if input_tokens > 200000 else "lte_200k"
            
            # 3. Construct lookup keys
            in_key = f"{canonical_model}#{gemini_tier}#input#text#{context_tier}"
            cache_key = f"{canonical_model}#{gemini_tier}#cache_read#text#{context_tier}"
            out_key = f"{canonical_model}#{gemini_tier}#output#text#{context_tier}"
            
            # Fallback for models without tiered pricing or missing keys
            if in_key not in self.lookup: in_key = f"{canonical_model}#{gemini_tier}#input#text"
            if out_key not in self.lookup: out_key = f"{canonical_model}#{gemini_tier}#output#text"
            if cache_key not in self.lookup: cache_key = f"{canonical_model}#{gemini_tier}#cache_read#text"

            # 4. Extract prices (per 1M tokens)
            in_price = self.lookup.get(in_key, {}).get("price", 0.0)
            out_price = self.lookup.get(out_key, {}).get("price", 0.0)
            cache_price = self.lookup.get(cache_key, {}).get("price", 0.0)

            # 5. Precise calculation:
            # Input = (non-cached part * input price) + (cached part * cache read price)
            input_cost = (max(0, input_tokens - cached_tokens) * in_price) + (cached_tokens * cache_price)
            # Output = (regular output + thoughts tokens) * output price
            output_cost = (output_tokens + thoughts_tokens) * out_price
            
            return round((input_cost + output_cost) / 1000000.0, 10)

        elif self.provider == "claude":
            # Claude Pattern: {model}#{tier}#{dimension}
            in_key = f"{canonical_model}#{tier}#input"
            out_key = f"{canonical_model}#{tier}#output"
            
            # Support for long context tier switch if tokens > 200k
            if kwargs.get("total_tokens", input_tokens) > 200000:
                long_in = f"{canonical_model}#long_context_gt200k#input"
                if long_in in self.lookup:
                    in_key = long_in
                    out_key = f"{canonical_model}#long_context_gt200k#output"
        else:
            return 0.0

        # Extract prices
        in_price = self.lookup.get(in_key, {}).get("price", 0.0)
        out_price = self.lookup.get(out_key, {}).get("price", 0.0)

        # Calculation (Price is per 1M tokens)
        total_cost = (input_tokens * in_price / 1000000.0) + (output_tokens * out_price / 1000000.0)
        
        return round(total_cost, 10)
