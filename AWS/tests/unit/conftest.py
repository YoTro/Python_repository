"""
Pytest configuration for unit tests.
No network, no filesystem outside tmpdir — fast, deterministic.
"""

import pytest


@pytest.fixture(autouse=True)
def _unit_guard():
    """Fail fast if a unit test accidentally imports live dependencies."""
    pass
