from unittest.mock import MagicMock, patch

import pytest

from src.mcp.servers.social.tiktok.client import TikTokClient


@pytest.fixture
def tiktok_client():
    return TikTokClient()


def test_tiktok_client_initialization(tiktok_client):
    assert tiktok_client.user_agent is not None
    assert tiktok_client.session is not None


def test_ms_token_generation_length(tiktok_client):
    token = tiktok_client._generate_ms_token(107)
    assert len(token) == 107


@patch("src.mcp.servers.social.tiktok.client.requests.Session")
def test_seed_ms_token_success(mock_session_class):
    # Mock the session instance
    mock_session = MagicMock()
    mock_session_class.return_value = mock_session

    # Mock the cookies
    mock_cookies = MagicMock()
    mock_cookies.get_dict.return_value = {"msToken": "test_token"}
    mock_session.cookies = mock_cookies

    # Initialize client with the mocked session
    tiktok_client = TikTokClient()

    # Ensure the client uses the mocked session
    token = tiktok_client._seed_ms_token()
    assert token == "test_token"


@patch("src.mcp.servers.social.tiktok.client.TikTokClient._get_ttwid_webid")
@patch("src.mcp.servers.social.tiktok.client.TikTokSigner")
@patch("src.mcp.servers.social.tiktok.client.requests.Session.get")
def test_get_tag_info_failure(mock_get, mock_signer, mock_get_cookies, tiktok_client):
    # Mocking failure to get ttwid
    mock_get_cookies.return_value = ("", "id", "id")

    # Mocking API request response
    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_get.return_value = mock_response

    # get_tag_info should handle the request failure and not crash
    result = tiktok_client.get_tag_info("testtag")
    assert result["view_count"] == 0
    assert result["video_count"] == 0
