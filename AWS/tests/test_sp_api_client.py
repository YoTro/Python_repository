"""
Unit tests for SP-API auth and client (no real credentials needed).
Tests parse helpers and auth logic with mocked HTTP.
"""
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestSPAPIAuth(unittest.TestCase):

    def _make_auth(self, store="US"):
        env = {
            "AMAZON_SP_API_CLIENT_ID": "test_client_id",
            "AMAZON_SP_API_CLIENT_SECRET": "test_client_secret",
            f"AMAZON_SP_API_REFRESH_TOKEN_{store}": "test_refresh_token",
        }
        with patch.dict(os.environ, env):
            from src.mcp.servers.amazon.sp_api.auth import SPAPIAuth
            return SPAPIAuth(store_id=store)

    def test_us_marketplace_id(self):
        auth = self._make_auth("US")
        self.assertEqual(auth.marketplace_id, "ATVPDKIKX0DER")

    def test_us_endpoint_is_na(self):
        auth = self._make_auth("US")
        self.assertIn("sellingpartnerapi-na", auth.endpoint)

    def test_eu_store_uses_eu_endpoint(self):
        env = {
            "AMAZON_SP_API_CLIENT_ID": "c",
            "AMAZON_SP_API_CLIENT_SECRET": "s",
            "AMAZON_SP_API_REFRESH_TOKEN_DE": "r",
        }
        with patch.dict(os.environ, env):
            from src.mcp.servers.amazon.sp_api.auth import SPAPIAuth
            auth = SPAPIAuth(store_id="DE")
        self.assertIn("sellingpartnerapi-eu", auth.endpoint)

    def test_missing_credentials_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            from src.mcp.servers.amazon.sp_api.auth import SPAPIAuth
            with self.assertRaises(ValueError):
                SPAPIAuth(store_id="US")

    def test_token_refresh_and_cache(self):
        auth = self._make_auth("US")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"access_token": "tok123", "expires_in": 3600}
        mock_resp.raise_for_status = MagicMock()

        with patch("requests.post", return_value=mock_resp) as mock_post:
            token1 = auth.get_access_token()
            token2 = auth.get_access_token()

        self.assertEqual(token1, "tok123")
        self.assertEqual(token2, "tok123")
        mock_post.assert_called_once()  # second call hits cache


class TestSPAPIClientParsers(unittest.TestCase):
    """Test parse helpers without network calls."""

    def setUp(self):
        # Import parsers directly from client module
        sys.modules.pop("src.mcp.servers.amazon.sp_api.client", None)

    def _parsers(self):
        env = {
            "AMAZON_SP_API_CLIENT_ID": "c",
            "AMAZON_SP_API_CLIENT_SECRET": "s",
            "AMAZON_SP_API_REFRESH_TOKEN_US": "r",
        }
        with patch.dict(os.environ, env):
            import importlib
            mod = importlib.import_module("src.mcp.servers.amazon.sp_api.client")
            return mod

    def test_parse_inventory_summary(self):
        mod = self._parsers()
        raw = {
            "sellerSku": "SKU-001",
            "asin": "B00TEST123",
            "fnSku": "X001",
            "condition": "NewItem",
            "totalQuantity": 200,
            "inventoryDetails": {
                "fulfillableQuantity": 150,
                "reservedQuantity": {"totalReservedQuantity": 30},
                "inboundReceivingQuantity": 10,
                "inboundShippedQuantity": 5,
                "inboundWorkingQuantity": 5,
            },
        }
        result = mod._parse_inventory_summary(raw)
        self.assertEqual(result["sku"], "SKU-001")
        self.assertEqual(result["available_quantity"], 150)
        self.assertEqual(result["reserved_quantity"], 30)
        self.assertEqual(result["inbound_quantity"], 20)  # 10+5+5

    def test_parse_catalog_item(self):
        mod = self._parsers()
        raw = {
            "summaries": [{"itemName": "Test Widget", "brand": "Acme", "productType": "WIDGET"}],
            "attributes": {
                "color": [{"value": "Blue"}],
                "bullet_point": [{"value": "Point 1"}, {"value": "Point 2"}],
            },
        }
        result = mod._parse_catalog_item("B00TEST123", raw)
        self.assertEqual(result["title"], "Test Widget")
        self.assertEqual(result["color"], "Blue")
        self.assertEqual(result["bullet_point_count"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
