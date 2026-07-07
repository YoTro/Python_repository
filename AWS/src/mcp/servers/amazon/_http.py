"""Utility for deriving an SP-API operation name from a URL path."""

from __future__ import annotations

import re
from typing import NamedTuple


class _Rule(NamedTuple):
    pattern: re.Pattern
    operation: str


# Ordered from most-specific to least-specific so the first match wins.
_RULES: list[_Rule] = [
    _Rule(re.compile(r"/reports/[^/]+/reports/[^/]+/document"), "getReportDocument"),
    _Rule(re.compile(r"/reports/[^/]+/reports/[^/]+"), "getReport"),
    _Rule(re.compile(r"/reports/[^/]+/reports"), "createReport"),
    _Rule(re.compile(r"/fba/inventory/v1/summaries"), "getInventorySummaries"),
    _Rule(re.compile(r"/sales/v1/orderMetrics"), "getOrderMetrics"),
    _Rule(re.compile(r"/fba/inbound/v0/shipments"), "getShipments"),
    _Rule(re.compile(r"/catalog/[^/]+/items/[^/]+"), "getCatalogItem"),
    _Rule(re.compile(r"/inbound/fba/[^/]+/inboundPlans"), "listInboundPlans"),
    _Rule(re.compile(r"/inbound/fba/[^/]+/inboundPlan"), "listInboundPlans"),
]


def sp_api_operation(path: str) -> str:
    """Return the SP-API operation name for a given URL path, or 'default'."""
    for rule in _RULES:
        if rule.pattern.search(path):
            return rule.operation
    return "default"
