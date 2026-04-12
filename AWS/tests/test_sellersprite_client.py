from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from src.mcp.servers.market.sellersprite.client import SellerspriteAPI


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def api():
    """Return a SellerspriteAPI instance with mocked session and auth (no I/O)."""
    with patch("src.mcp.servers.market.sellersprite.client.requests.Session") as mock_session_cls:
        mock_request = MagicMock()
        mock_session_cls.return_value.request = mock_request
        with patch.object(SellerspriteAPI, "_load_token", return_value="test-token"):
            client = SellerspriteAPI()
        client.session = mock_session_cls.return_value
        yield client, mock_request


def _ok(body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = body
    return resp


def _err(status: int, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.headers = {}
    return resp


# ---------------------------------------------------------------------------
# get_competing_lookup
# ---------------------------------------------------------------------------

class TestGetCompetingLookup:

    def test_happy_path_returns_items(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({
            "code": 0,
            "data": {
                "items": [
                    {
                        "asin": "B08N5WRWNW",
                        "title": "Sample Product",
                        "price": 19.99,
                        "rating": 4.5,
                        "reviewCount": 1234,
                        "bsrRank": 42,
                        "trends": [
                            {"dk": "202508", "sales": 800},
                            {"dk": "202509", "sales": 950},
                        ],
                    }
                ],
                "total": 1,
                "page": 1,
                "size": 100,
            },
        })

        result = client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["2972638011:553844:3737901"],
        )

        assert len(result["items"]) == 1
        assert result["items"][0]["asin"] == "B08N5WRWNW"
        assert result["total"] == 1
        assert len(result["items"][0]["trends"]) == 2

    def test_request_payload_structure(self, api):
        """Verify the JSON body sent to the API matches the documented contract."""
        client, mock_request = api
        mock_request.return_value = _ok({"data": {"items": [], "total": 0, "page": 1, "size": 50}})

        client.get_competing_lookup(
            market="DE",
            month_name="bsr_sales_monthly_202508",
            node_id_paths=["123:456"],
            page=2,
            size=50,
            order={"field": "sales", "desc": True},
            symbol_flag=False,
            low_price="Y",
        )

        _, kwargs = mock_request.call_args
        payload = kwargs["json"]
        assert payload["market"] == "DE"
        assert payload["monthName"] == "bsr_sales_monthly_202508"
        assert payload["nodeIdPaths"] == ["123:456"]
        assert payload["page"] == 2
        assert payload["size"] == 50
        assert payload["order"] == {"field": "sales", "desc": True}
        assert payload["symbolFlag"] is False
        assert payload["lowPrice"] == "Y"
        assert payload["asins"] == []

    def test_default_order_is_bsr_rank_asc(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"data": {"items": [], "total": 0, "page": 1, "size": 100}})

        client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["999"],
        )

        _, kwargs = mock_request.call_args
        assert kwargs["json"]["order"] == {"field": "bsr_rank", "desc": False}

    def test_auth_token_sent_in_header(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"data": {"items": [], "total": 0, "page": 1, "size": 100}})

        client.auth_token = "my-secret-token"
        client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["1"],
        )

        _, kwargs = mock_request.call_args
        assert kwargs["headers"]["Auth-Token"] == "my-secret-token"

    def test_non_200_returns_empty_result(self, api):
        client, mock_request = api
        mock_request.return_value = _err(500, "Internal Server Error")

        result = client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["1"],
        )

        assert result == {"items": [], "total": 0, "page": 1, "size": 100}

    def test_null_data_field_returns_empty_result(self, api):
        """API may return 200 with data=null on empty category."""
        client, mock_request = api
        mock_request.return_value = _ok({"code": 0, "data": None})

        result = client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["1"],
        )

        assert result["items"] == []
        assert result["total"] == 0

    def test_429_triggers_retry_and_raises(self, api):
        """Three consecutive 429s should raise RetryableError."""
        from src.core.errors.exceptions import RetryableError

        client, mock_request = api
        rate_resp = _err(429)
        rate_resp.headers = {"Retry-After": "1"}
        mock_request.return_value = rate_resp

        with patch("src.mcp.servers.market.sellersprite.client.time.sleep"):
            with pytest.raises(RetryableError):
                client.get_competing_lookup(
                    market="US",
                    month_name="bsr_sales_monthly_202509",
                    node_id_paths=["1"],
                )

        assert mock_request.call_count == 3

    def test_multiple_node_paths(self, api):
        """Supports passing multiple nodeIdPaths in one call."""
        client, mock_request = api
        mock_request.return_value = _ok({
            "data": {
                "items": [{"asin": "AAA"}, {"asin": "BBB"}],
                "total": 2,
                "page": 1,
                "size": 100,
            }
        })

        result = client.get_competing_lookup(
            market="US",
            month_name="bsr_sales_monthly_202509",
            node_id_paths=["111:222", "333:444"],
        )

        _, kwargs = mock_request.call_args
        assert kwargs["json"]["nodeIdPaths"] == ["111:222", "333:444"]
        assert result["total"] == 2


# ---------------------------------------------------------------------------
# get_category_nodes
# ---------------------------------------------------------------------------

class TestGetCategoryNodes:

    _NODES_RESPONSE = [
        {
            "nodeId": 553844,
            "nodeName": "Power Tools",
            "nodeIdPath": "2972638011:553844",
            "hasChildren": True,
        },
        {
            "nodeId": 3737901,
            "nodeName": "Drills & Drivers",
            "nodeIdPath": "2972638011:553844:3737901",
            "hasChildren": False,
        },
    ]

    def test_happy_path_returns_node_list(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"code": 0, "data": self._NODES_RESPONSE})

        nodes = client.get_category_nodes(
            market_id=1,
            table="bsr_sales_monthly_202509",
            node_id_path="2972638011",
        )

        assert len(nodes) == 2
        assert nodes[0]["nodeId"] == 553844
        assert nodes[1]["hasChildren"] is False

    def test_query_params_are_correct(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"data": []})

        client.get_category_nodes(
            market_id=6,
            table="bsr_sales_monthly_202508",
            node_id_path="12345:67890",
        )

        args, kwargs = mock_request.call_args
        assert args[0] == "GET"
        assert "nodes" in args[1]
        params = kwargs["params"]
        assert params["marketId"] == 6
        assert params["table"] == "bsr_sales_monthly_202508"
        assert params["nodeIdPath"] == "12345:67890"

    def test_auth_token_sent_in_header(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"data": []})

        client.auth_token = "secret"
        client.get_category_nodes(
            market_id=1,
            table="bsr_sales_monthly_202509",
            node_id_path="1",
        )

        _, kwargs = mock_request.call_args
        assert kwargs["headers"]["Auth-Token"] == "secret"

    def test_non_200_returns_empty_list(self, api):
        client, mock_request = api
        mock_request.return_value = _err(403, "Forbidden")

        nodes = client.get_category_nodes(
            market_id=1,
            table="bsr_sales_monthly_202509",
            node_id_path="1",
        )

        assert nodes == []

    def test_null_data_field_returns_empty_list(self, api):
        client, mock_request = api
        mock_request.return_value = _ok({"code": 0, "data": None})

        nodes = client.get_category_nodes(
            market_id=1,
            table="bsr_sales_monthly_202509",
            node_id_path="1",
        )

        assert nodes == []

    def test_429_triggers_retry_and_raises(self, api):
        from src.core.errors.exceptions import RetryableError

        client, mock_request = api
        rate_resp = _err(429)
        rate_resp.headers = {"Retry-After": "1"}
        mock_request.return_value = rate_resp

        with patch("src.mcp.servers.market.sellersprite.client.time.sleep"):
            with pytest.raises(RetryableError):
                client.get_category_nodes(
                    market_id=1,
                    table="bsr_sales_monthly_202509",
                    node_id_path="1",
                )

        assert mock_request.call_count == 3

    def test_leaf_node_returns_empty_children(self, api):
        """A leaf node path should return an empty list (no children)."""
        client, mock_request = api
        mock_request.return_value = _ok({"data": []})

        nodes = client.get_category_nodes(
            market_id=1,
            table="bsr_sales_monthly_202509",
            node_id_path="2972638011:553844:3737901",
        )

        assert nodes == []
