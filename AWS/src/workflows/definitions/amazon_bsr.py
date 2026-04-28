"""
Amazon BSR Workflow Definition

Extracts Best Sellers Rank (BSR) products from a given category URL.
"""
from __future__ import annotations
import hashlib as _hl
import logging
from src.workflows.registry import WorkflowRegistry
from src.workflows.engine import Workflow
from src.workflows.steps.process import ProcessStep
from src.workflows.steps.base import WorkflowContext, ComputeTarget
from src.core.data_cache import data_cache as _data_cache

logger = logging.getLogger(__name__)

_L2_DOMAIN = "amazon_bsr"
_TTL_BSR   = 1800   # 30 min — Amazon BSR refreshes approximately every hour


def _l2_key(ctx: WorkflowContext, *parts) -> str:
    tid = ctx.tenant_id or "default"
    return ":".join(str(p) for p in (tid,) + parts)


def _l2_get(ctx: WorkflowContext, ttl: int, *parts):
    return _data_cache.get(_L2_DOMAIN, _l2_key(ctx, *parts), ttl_seconds=ttl)


def _l2_set(ctx: WorkflowContext, value, *parts) -> None:
    _data_cache.set(_L2_DOMAIN, _l2_key(ctx, *parts), value)


@WorkflowRegistry.register("amazon_bsr")
def build_amazon_bsr(config: dict) -> Workflow:

    async def _extract_bsr(items: list, ctx: WorkflowContext) -> list:
        url = config.get("amazon_url")
        if not url:
            raise ValueError("amazon_url is required for amazon_bsr workflow")

        url_hash = _hl.md5(url.encode()).hexdigest()[:12]
        cached = _l2_get(ctx, _TTL_BSR, "bsr", url_hash)
        if cached is not None:
            logger.info(f"[amazon_bsr] L2 cache hit for url_hash={url_hash}")
            return cached

        if ctx.mcp:
            results = await ctx.mcp.call_tool_json("get_amazon_bestsellers", {"url": url})
        else:
            from src.mcp.servers.amazon.extractors.bestsellers import BestSellersExtractor
            extractor = BestSellersExtractor()
            results = await extractor.get_bestsellers(url)

        _l2_set(ctx, results, "bsr", url_hash)
        logger.info(f"[amazon_bsr] Fetched {len(results) if isinstance(results, list) else '?'} items, cached url_hash={url_hash}")
        return results

    steps = [
        ProcessStep(
            name="extract_bestsellers",
            fn=_extract_bsr,
            compute_target=ComputeTarget.PURE_PYTHON
        )
    ]

    category = config.get("category", "Unknown")
    return Workflow(name=f"amazon_bsr_{category}", steps=steps)
