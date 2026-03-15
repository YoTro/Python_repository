import importlib
import pkgutil
import pytest
import os
import sys

# Ensure src is in path for dynamic imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import src

def test_import_integrity():
    """Dynamically import all core and workflow modules to ensure no circular imports."""
    # We will just test importing the main packages to avoid side effects of executing scripts
    packages_to_test = [
        "src.core.models.product",
        "src.core.models.review",
        "src.core.models.market",
        "src.core.utils.csv_helper",
        "src.core.utils.parser_helper",
        "src.core.utils.config_helper",
        "src.core.telemetry.tracker",
        "src.mcp.client.local",
        "src.registry.tools"
    ]
    
    for modname in packages_to_test:
        try:
            importlib.import_module(modname)
        except Exception as e:
            pytest.fail(f"Failed to import {modname}: {e}")
