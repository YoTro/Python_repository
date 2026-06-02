"""
Import integrity tests.

Verifies that every domain package can be imported without circular imports,
missing __init__ exports, or broken intra-package dependencies.  These tests
must pass before any PR is merged (see docs/PR_GUIDELINES.md §5).

Run:
    PYTHONPATH=. pytest tests/test_imports.py -v
"""

import importlib
import pytest


# ---------------------------------------------------------------------------
# Module list — every importable module in src/
# Add new modules here when they are created.
# ---------------------------------------------------------------------------

_CORE = [
    "src.core.errors",
    "src.core.errors.codes",
    "src.core.errors.exceptions",
    "src.core.models",
    "src.core.models.product",
    "src.core.models.review",
    "src.core.models.market",
    "src.core.models.request",
    "src.core.data_cache",
    "src.core.scraper",
    "src.core.storage",
    "src.core.storage.base",
    "src.core.storage.s3_compatible",
    "src.core.storage.local_http",
    "src.core.telemetry",
    "src.core.utils",
    "src.core.utils.config_helper",
    "src.core.utils.context",
    "src.core.utils.cookie_helper",
    "src.core.utils.csv_helper",
    "src.core.utils.decorators",
    "src.core.utils.parser_helper",
    "src.core.utils.proxy",
    "src.core.utils.charts",
]

_GATEWAY = [
    "src.gateway",
    "src.gateway.auth",
    "src.gateway.rate_limit",
    "src.gateway.router",
]

_INTELLIGENCE = [
    "src.intelligence",
    "src.intelligence.dto",
    "src.intelligence.fallback",
    "src.intelligence.parsers.markdown_cleaner",
    "src.intelligence.processors.auto_mining",
    "src.intelligence.processors.causal_analysis",
    "src.intelligence.processors.listing_quality_scorer",
    "src.intelligence.processors.lp_calibration",
    "src.intelligence.processors.monopoly_analyzer",
    "src.intelligence.processors.optimizer_ad_budget",
    "src.intelligence.processors.optimizer_pricing",
    "src.intelligence.processors.product_similarity",
    "src.intelligence.processors.promo_analyzer",
    "src.intelligence.processors.review_summarizer",
    "src.intelligence.processors.sales_estimator",
    "src.intelligence.processors.shipment_lead_time",
    "src.intelligence.processors.social_virality",
    "src.intelligence.prompts.manager",
    "src.intelligence.providers.base",
    "src.intelligence.providers.factory",
    "src.intelligence.providers.price_manager",
    "src.intelligence.providers.config.limits",
    "src.intelligence.router",
]

_WORKFLOWS = [
    "src.workflows",
    "src.workflows.registry",
    "src.workflows.engine.activity_runner",
    "src.workflows.steps.base",
    "src.workflows.steps.enrich",
    "src.workflows.steps.filter",
    "src.workflows.steps.process",
    "src.workflows.definitions",
    "src.workflows.definitions.ad_diagnosis",
    "src.workflows.definitions.amazon_bsr",
    "src.workflows.definitions.category_monopoly_analysis",
    "src.workflows.definitions.lp_validation",
    "src.workflows.definitions.product_screening",
]

_AGENTS = [
    "src.agents",
    "src.agents.base_agent",
    "src.agents.mcp_agent",
    "src.agents.session",
    "src.agents.prompts.prompt_builder",
    "src.agents.prompts.tool_catalog_formatter",
]

_JOBS = [
    "src.jobs",
    "src.jobs.batch_poller",
    "src.jobs.signals",
    "src.jobs.callbacks.base",
    "src.jobs.callbacks.factory",
    "src.jobs.callbacks.feishu",
    "src.jobs.callbacks.mcp_callback",
    "src.jobs.callbacks.csv_callback",
    "src.jobs.interactions.handlers",
    "src.jobs.interactions.registry",
]

_MCP = [
    "src.mcp",
    # Amazon
    "src.mcp.servers.amazon",
    "src.mcp.servers.amazon.tools",
    "src.mcp.servers.amazon.extractors.bsr_category_extractor",
    "src.mcp.servers.amazon.extractors.cart_stock",
    "src.mcp.servers.amazon.extractors.comments",
    "src.mcp.servers.amazon.extractors.dimensions",
    "src.mcp.servers.amazon.extractors.feedback",
    "src.mcp.servers.amazon.extractors.fulfillment",
    "src.mcp.servers.amazon.extractors.images",
    "src.mcp.servers.amazon.extractors.keywords_rank",
    "src.mcp.servers.amazon.extractors.past_month_sales",
    "src.mcp.servers.amazon.extractors.product_details",
    "src.mcp.servers.amazon.extractors.products_num",
    "src.mcp.servers.amazon.extractors.profitability_search",
    "src.mcp.servers.amazon.extractors.ranks",
    "src.mcp.servers.amazon.extractors.review_count",
    "src.mcp.servers.amazon.extractors.search",
    "src.mcp.servers.amazon.extractors.search_result_asins",
    "src.mcp.servers.amazon.extractors.videos",
    "src.mcp.servers.amazon.sp_api.auth",
    "src.mcp.servers.amazon.sp_api.client",
    # Compliance
    "src.mcp.servers.compliance.tools",
    "src.mcp.servers.compliance.cpsc_recalls",
    "src.mcp.servers.compliance.epa_client",
    "src.mcp.servers.compliance.fda_client",
    # ERP
    "src.mcp.servers.erp",
    "src.mcp.servers.erp.base",
    "src.mcp.servers.erp.registry",
    "src.mcp.servers.erp.tools",
    "src.mcp.servers.erp.lingxing",
    "src.mcp.servers.erp.lingxing.auth",
    "src.mcp.servers.erp.lingxing.client",
    # Finance
    "src.mcp.servers.finance.tools",
    # Market
    "src.mcp.servers.market",
    "src.mcp.servers.market.tools",
    "src.mcp.servers.market.deals.client",
    "src.mcp.servers.market.sellersprite.auth",
    "src.mcp.servers.market.sellersprite.client",
    "src.mcp.servers.market.xiyouzhaoci.auth",
    "src.mcp.servers.market.xiyouzhaoci.client",
    # Output
    "src.mcp.servers.output",
    "src.mcp.servers.output.tools",
    "src.mcp.servers.output.tools.create_doc",
    "src.mcp.servers.output.tools.export_csv",
    "src.mcp.servers.output.tools.export_html",
    "src.mcp.servers.output.tools.export_json",
    "src.mcp.servers.output.tools.export_md",
    "src.mcp.servers.output.tools.send_card",
    "src.mcp.servers.output.tools.write_bitable",
    # Social
    "src.mcp.servers.social.tools",
    "src.mcp.servers.social.tiktok.auth",
    "src.mcp.servers.social.tiktok.client",
]

_REGISTRY = [
    "src.registry.tools",
    "src.registry.resources",
    "src.registry.prompts",
]

ALL_MODULES = (
    _CORE
    + _GATEWAY
    + _INTELLIGENCE
    + _WORKFLOWS
    + _AGENTS
    + _JOBS
    + _MCP
    + _REGISTRY
)


# ---------------------------------------------------------------------------
# Parametrized import test
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("module", ALL_MODULES)
def test_module_importable(module):
    """Each module must import without raising any exception."""
    importlib.import_module(module)


# ---------------------------------------------------------------------------
# Public API surface tests — spot-check __init__ exports
# ---------------------------------------------------------------------------

def test_errors_public_api():
    from src.core.errors import (
        AWSBaseError, ScraperError, ExtractorError, ConfigError,
        WorkflowError, StepError, RetryableError, FatalError,
        CheckpointError, BatchPendingError, JobSuspendedError,
        ErrorCode, classify_http, classify_api_code,
        classify_response_message, is_retryable, is_auth_error,
        default_retry_after,
    )
    assert issubclass(RetryableError, AWSBaseError)
    assert issubclass(BatchPendingError, AWSBaseError)
    assert issubclass(JobSuspendedError, AWSBaseError)
    assert ErrorCode.RATE_LIMITED.value == "rate.limited"
    assert is_retryable(ErrorCode.RATE_LIMITED)
    assert not is_retryable(ErrorCode.AUTH_FAILED)


def test_classify_http_provider_override():
    from src.core.errors import classify_http, ErrorCode
    # Standard 406 → INVALID_HEADER
    assert classify_http(406) == ErrorCode.INVALID_HEADER
    # Gemini Exchange override: 406 → BILLING_INSUFFICIENT
    assert classify_http(406, provider="gemini_exchange") == ErrorCode.BILLING_INSUFFICIENT
    # Gemini Exchange override: 403 → AUTH_FAILED (not AUTH_IP_BLOCKED)
    assert classify_http(403, provider="gemini_exchange") == ErrorCode.AUTH_FAILED
    # Unknown 5xx falls back to SERVER_ERROR
    assert classify_http(599) == ErrorCode.SERVER_ERROR


def test_classify_api_code():
    from src.core.errors import classify_api_code, ErrorCode
    assert classify_api_code(-1, "lingxing") == ErrorCode.AUTH_TOKEN_EXPIRED
    assert classify_api_code(234042, "feishu") == ErrorCode.STORAGE_FULL
    assert classify_api_code("ERR_GLOBAL_403", "sellersprite") == ErrorCode.AUTH_IP_BLOCKED
    assert classify_api_code("425", "amazon_ads") == ErrorCode.DUPLICATE_REQUEST
    assert classify_api_code(999, "unknown_provider") == ErrorCode.UNKNOWN


def test_classify_response_message():
    from src.core.errors import classify_response_message, ErrorCode
    assert classify_response_message(
        "Scope header is missing", "amazon_ads"
    ) == ErrorCode.AUTH_SCOPE_MISSING
    assert classify_response_message(
        "authentication fails. wrong api key provided", "deepseek"
    ) == ErrorCode.AUTH_FAILED
    assert classify_response_message(
        "market not open", "gemini_exchange"
    ) == ErrorCode.SERVER_ERROR
    assert classify_response_message(
        "no match", "amazon_ads"
    ) == ErrorCode.UNKNOWN


def test_workflow_registry_not_empty():
    from src.workflows import registry as reg_mod
    # Importing definitions triggers @WorkflowRegistry.register decorators
    import src.workflows.definitions  # noqa: F401
    from src.workflows.registry import WorkflowRegistry
    names = WorkflowRegistry.list_workflows()
    assert len(names) > 0, "WorkflowRegistry must contain at least one workflow"
