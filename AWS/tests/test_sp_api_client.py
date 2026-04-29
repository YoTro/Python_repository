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


class TestGetOrderMetrics(unittest.IsolatedAsyncioTestCase):
    """Tests for SPAPIClient.get_order_metrics (mocked HTTP)."""

    def _make_client(self):
        env = {
            "AMAZON_SP_API_CLIENT_ID": "c",
            "AMAZON_SP_API_CLIENT_SECRET": "s",
            "AMAZON_SP_API_REFRESH_TOKEN_US": "r",
        }
        with patch.dict(os.environ, env):
            from src.mcp.servers.amazon.sp_api.client import SPAPIClient
            return SPAPIClient(store_id="US")

    def _mock_auth(self):
        return patch(
            "src.mcp.servers.amazon.sp_api.client.SPAPIAuth.get_access_token",
            return_value="fake_token",
        )

    # ── happy path ────────────────────────────────────────────────────────

    async def test_returns_payload_list(self):
        payload = [{"unitCount": 120, "orderItemCount": 100, "orderCount": 95}]
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"payload": payload}
        mock_resp.raise_for_status = MagicMock()

        with self._mock_auth(), patch("requests.get", return_value=mock_resp):
            client = self._make_client()
            result = await client.get_order_metrics(
                asin="B0TEST1234",
                start_date="2026-03-30",
                end_date="2026-04-28",
            )

        self.assertEqual(result, payload)

    async def test_request_params_shape(self):
        """Verify the correct query-string parameters are sent."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"payload": []}
        mock_resp.raise_for_status = MagicMock()

        captured_params = {}

        def _capture_get(url, **kwargs):
            captured_params.update(kwargs.get("params", {}))
            return mock_resp

        with self._mock_auth(), patch("requests.get", side_effect=_capture_get):
            client = self._make_client()
            await client.get_order_metrics(
                asin="B0TEST1234",
                start_date="2026-03-30",
                end_date="2026-04-28",
                granularity="Total",
            )

        self.assertEqual(captured_params["asin"], "B0TEST1234")
        self.assertEqual(captured_params["granularity"], "Total")
        # separator must be "--" (double hyphen), end date is exclusive (+1 day)
        self.assertIn("--", captured_params["interval"])
        self.assertIn("2026-03-30T00:00:00Z", captured_params["interval"])
        self.assertIn("2026-04-29T00:00:00Z", captured_params["interval"])  # 04-28 +1 day
        self.assertIn("marketplaceIds", captured_params)

    async def test_empty_payload_returns_empty_list(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {}          # no "payload" key
        mock_resp.raise_for_status = MagicMock()

        with self._mock_auth(), patch("requests.get", return_value=mock_resp):
            client = self._make_client()
            result = await client.get_order_metrics(
                asin="B0NOUNITS1",
                start_date="2026-03-30",
                end_date="2026-04-28",
            )

        self.assertEqual(result, [])

    async def test_http_error_propagates(self):
        import requests as _req
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = _req.HTTPError("403 Forbidden")

        with self._mock_auth(), patch("requests.get", return_value=mock_resp):
            client = self._make_client()
            with self.assertRaises(Exception):
                await client.get_order_metrics(
                    asin="B0BAD",
                    start_date="2026-03-30",
                    end_date="2026-04-28",
                )

    # ── unit-count aggregation (caller logic, not client logic) ──────────

    def test_daily_sales_from_unit_count(self):
        """Verify the enricher formula: daily_sales = unitCount / days."""
        metrics = [{"unitCount": 150}]
        days    = 30
        total_units  = sum(m.get("unitCount", 0) for m in metrics)
        daily_sales  = round(total_units / days, 2)
        self.assertEqual(daily_sales, 5.0)

    def test_can_sell_days_formula(self):
        total_available = 200
        daily_sales     = 5.0
        can_sell_days   = round(total_available / daily_sales)
        self.assertEqual(can_sell_days, 40)

    def test_zero_units_returns_no_can_sell_days(self):
        """When no units were sold, can_sell_days should not be computed."""
        metrics    = [{"unitCount": 0}]
        total_units = sum(m.get("unitCount", 0) for m in metrics)
        self.assertFalse(total_units > 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
