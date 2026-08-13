from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from mcp.types import TextContent

from src.mcp.servers.market.tools import handle_market_tool, market_tools
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_api():
    """XiyouZhaociAPI with a mocked requests.Session."""
    with patch("src.mcp.servers.market.xiyouzhaoci.client.requests.Session") as mock_session:
        mock_request = MagicMock()
        mock_session.return_value.request = mock_request

        api = XiyouZhaociAPI()
        api.auth_token = "fake-token"
        api.session = mock_session.return_value

        yield api, mock_request


@pytest.fixture
def mock_api_authed():
    """XiyouZhaociAPI with pre-loaded token (no real auth call)."""
    with (
        patch("src.mcp.servers.market.xiyouzhaoci.client.requests.Session") as mock_session,
        patch(
            "src.mcp.servers.market.xiyouzhaoci.client.XiyouZhaociAPI._load_token",
            return_value="fake-token",
        ),
    ):
        mock_request = MagicMock()
        mock_session.return_value.request = mock_request

        api = XiyouZhaociAPI()
        api.session = mock_session.return_value

        yield api, mock_request


# ---------------------------------------------------------------------------
# Client — export_compare_data & 401 retry
# ---------------------------------------------------------------------------


class TestExportCompareData:
    def test_full_flow(self, mock_api, tmpdir):
        api, mock_request = mock_api

        mock_response_init = MagicMock()
        mock_response_init.status_code = 200
        mock_response_init.json.return_value = {"resourceId": "123456789"}
        mock_response_init.raise_for_status.return_value = None

        mock_response_pending = MagicMock()
        mock_response_pending.status_code = 200
        mock_response_pending.json.return_value = {"status": "Pending"}
        mock_response_pending.raise_for_status.return_value = None

        mock_response_done = MagicMock()
        mock_response_done.status_code = 200
        mock_response_done.json.return_value = {
            "status": "Done",
            "resourceUrl": "https://fake-oss-url.com/report.xlsx",
        }
        mock_response_done.raise_for_status.return_value = None

        mock_request.side_effect = [
            mock_response_init,
            mock_response_pending,
            mock_response_done,
        ]

        with patch.object(api, "_download_file", return_value=True) as mock_download:
            asins = ["ASIN1", "ASIN2"]
            output_dir = str(tmpdir)
            result_path = api.export_compare_data(
                country="US", asins=asins, period="last30days", output_dir=output_dir
            )

            assert result_path.endswith("US_compare_ASIN1_123456789.xlsx")
            assert mock_request.call_count == 3

            init_call = mock_request.call_args_list[0]
            assert init_call.args[1] == "https://api.xydc.com/v4/asins/compare/list/resource"
            assert init_call.kwargs["json"]["cycleFilter"]["period"] == "last30days"
            assert init_call.kwargs["json"]["asins"] == asins

            status_call_done = mock_request.call_args_list[2]
            assert status_call_done.args[1] == "https://api.xydc.com/v4/resource/status"
            assert status_call_done.kwargs["json"]["resourceId"] == "123456789"

            mock_download.assert_called_once_with(
                "https://fake-oss-url.com/report.xlsx", result_path
            )

    def test_401_unauthorized_retry(self, mock_api):
        api, mock_request = mock_api

        mock_response_401 = MagicMock()
        mock_response_401.status_code = 401

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"resourceId": "retry-success"}
        mock_response_200.raise_for_status.return_value = None

        mock_request.side_effect = [mock_response_401, mock_response_200]

        new_token = "new-fake-token"
        with patch.object(api, "_load_token", return_value=new_token):
            result = api.lookup_asin("US", "B0BSYD2VV6")

            assert result == {"resourceId": "retry-success"}
            assert api.auth_token == new_token
            assert api.common_headers["authorization"] == new_token
            assert mock_request.call_count == 2

            second_call = mock_request.call_args_list[1]
            assert second_call.kwargs["headers"]["authorization"] == new_token


# ---------------------------------------------------------------------------
# Client — get_traffic_scores
# ---------------------------------------------------------------------------


class TestGetTrafficScores:
    def test_success(self, mock_api_authed):
        api, mock_request = mock_api_authed

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_data = {
            "success": True,
            "data": [
                {
                    "asin": "B07T869RNY",
                    "advertisingTrafficScoreRatio": 0.45,
                    "totalTrafficScoreGrowthRate": 0.12,
                    "trafficScore": 85,
                },
                {
                    "asin": "B0CKY689WQ",
                    "advertisingTrafficScoreRatio": 0.30,
                    "totalTrafficScoreGrowthRate": -0.05,
                    "trafficScore": 72,
                },
            ],
        }
        mock_response.json.return_value = mock_data
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        asins = ["B07T869RNY", "B0CKY689WQ"]
        result = api.get_traffic_scores("US", asins)

        assert result == mock_data
        assert mock_request.call_count == 1

        call_args = mock_request.call_args
        method, url = call_args[0]
        assert method == "POST"
        assert url == "https://api.xydc.com/v4/asins/trafficScore"
        assert call_args[1]["json"] == {"asins": asins, "country": "US"}

    def test_empty_asins(self, mock_api_authed):
        api, mock_request = mock_api_authed

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True, "data": []}
        mock_response.raise_for_status.return_value = None
        mock_request.return_value = mock_response

        result = api.get_traffic_scores("US", [])

        assert result["success"] is True
        assert mock_request.call_count == 1
        assert (
            mock_request.call_args[1]["headers"]["request-url"] == "/detail/asin/look_up/US/unknown"
        )

    def test_error(self, mock_api_authed):
        api, mock_request = mock_api_authed

        mock_request.side_effect = Exception("Network error")

        result = api.get_traffic_scores("US", ["B07T869RNY"])

        assert result == {}
        assert mock_request.call_count == 1


# ---------------------------------------------------------------------------
# MCP tool registration & dispatch
# ---------------------------------------------------------------------------


class TestXiyouToolDispatch:
    @pytest.mark.asyncio
    async def test_tool_registration(self):
        tool = next((t for t in market_tools if t.name == "xiyou_get_traffic_scores"), None)
        assert tool is not None
        assert tool.description.startswith("[Xiyouzhaoci]")
        assert "asins" in tool.input_schema["properties"]

    @pytest.mark.asyncio
    async def test_handle_traffic_scores(self):
        asins = ["B07T869RNY"]
        mock_response = {
            "success": True,
            "data": [{"asin": "B07T869RNY", "advertisingTrafficScoreRatio": 0.5}],
        }

        with patch("src.mcp.servers.market.tools._get_xiyou_api") as mock_get_api:
            mock_api_instance = MagicMock()
            mock_api_instance.get_traffic_scores.return_value = mock_response
            mock_get_api.return_value = mock_api_instance

            result = await handle_market_tool(
                "xiyou_get_traffic_scores", {"asins": asins, "country": "US"}
            )

            assert len(result) == 1
            assert isinstance(result[0], TextContent)

            data = json.loads(result[0].text)
            assert data == mock_response

            mock_api_instance.get_traffic_scores.assert_called_once_with(country="US", asins=asins)
