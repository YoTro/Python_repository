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


# ---------------------------------------------------------------------------
# get_video_comments — orchestration (signed-API primary, browser fallback)
# ---------------------------------------------------------------------------


def test_get_video_comments_returns_signed_result_without_fallback(tiktok_client):
    """When the signed-API path yields comments, the browser path is never invoked."""
    signed = [{"text": "a"}, {"text": "b"}]
    with (
        patch.object(
            tiktok_client, "_get_video_comments_signed", return_value=signed
        ) as mock_signed,
        patch.object(tiktok_client, "_get_comments_via_browser") as mock_browser,
    ):
        result = tiktok_client.get_video_comments("123", count=20, author_id="tiktok")

    assert result == signed
    mock_signed.assert_called_once_with("123", 20, "tiktok")
    mock_browser.assert_not_called()


def test_get_video_comments_falls_back_to_browser_when_signed_empty(tiktok_client):
    """Empty signed result triggers the DrissionPage fallback and returns its output."""
    browser = [{"text": "from-browser"}]
    with (
        patch.object(tiktok_client, "_get_video_comments_signed", return_value=[]),
        patch.object(
            tiktok_client, "_get_comments_via_browser", return_value=browser
        ) as mock_browser,
    ):
        result = tiktok_client.get_video_comments("123", count=10, author_id=None)

    assert result == browser
    mock_browser.assert_called_once_with("123", None, 10)


def test_get_video_comments_returns_empty_when_browser_returns_none(tiktok_client):
    """Both paths failing yields an empty list, never None."""
    with (
        patch.object(tiktok_client, "_get_video_comments_signed", return_value=[]),
        patch.object(tiktok_client, "_get_comments_via_browser", return_value=None),
    ):
        result = tiktok_client.get_video_comments("123")

    assert result == []


# ---------------------------------------------------------------------------
# _get_video_comments_signed — the core signed-API engine
# ---------------------------------------------------------------------------


def _make_response(status_code=200, json_data=None, text="{}", headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


@pytest.fixture
def signed_env(tiktok_client):
    """Patch out network, signing, seeding and rate-limiting for signed-path tests."""
    with (
        patch.object(tiktok_client, "_get_ttwid_webid", return_value=("ttwid123", "odin1", "dev1")),
        patch.object(tiktok_client, "_seed_ms_token", return_value="mstok"),
        patch("src.mcp.servers.social.tiktok.client.TikTokSigner") as mock_signer,
        patch("src.mcp.servers.social.tiktok.client.RateLimiter") as mock_limiter,
    ):
        mock_signer.generate_x_gnarly.return_value = "GNARLY/SIG"
        mock_signer.generate_x_bogus.return_value = "BOGUS"
        mock_limiter.return_value.acquire_source.return_value = True
        yield tiktok_client


def test_signed_single_page_no_more(signed_env):
    """A single page with has_more=0 returns its comments and stops."""
    resp = _make_response(
        json_data={"comments": [{"text": "c1"}, {"text": "c2"}], "has_more": 0},
        text='{"comments": []}',
    )
    with patch.object(signed_env.session, "get", return_value=resp) as mock_get:
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == [{"text": "c1"}, {"text": "c2"}]
    mock_get.assert_called_once()


def test_signed_pagination_until_count(signed_env):
    """Pagination follows the cursor across pages and trims to the requested count."""
    page1 = _make_response(
        json_data={"comments": [{"text": "1"}, {"text": "2"}], "has_more": 1, "cursor": 2},
        text="x",
    )
    page2 = _make_response(
        json_data={"comments": [{"text": "3"}, {"text": "4"}], "has_more": 1, "cursor": 4},
        text="x",
    )
    with (
        patch.object(signed_env.session, "get", side_effect=[page1, page2]) as mock_get,
        patch("src.mcp.servers.social.tiktok.client.time.sleep"),
    ):
        result = signed_env._get_video_comments_signed("vid", count=3)

    assert [c["text"] for c in result] == ["1", "2", "3"]
    assert mock_get.call_count == 2


def test_signed_stops_when_no_comments_in_batch(signed_env):
    """An empty comments array breaks the loop gracefully."""
    resp = _make_response(json_data={"comments": [], "has_more": 1}, text="x")
    with patch.object(signed_env.session, "get", return_value=resp):
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == []


def test_signed_empty_body_breaks(signed_env):
    """A 200 with an empty body short-circuits (signature/msToken failure signal)."""
    resp = _make_response(status_code=200, text="   ")
    with patch.object(signed_env.session, "get", return_value=resp):
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == []


def test_signed_429_then_success(signed_env):
    """A 429 triggers a backoff retry, then the subsequent 200 is consumed."""
    throttled = _make_response(status_code=429, headers={"Retry-After": "0"})
    ok = _make_response(json_data={"comments": [{"text": "ok"}], "has_more": 0}, text="x")
    with (
        patch.object(signed_env.session, "get", side_effect=[throttled, ok]) as mock_get,
        patch("src.mcp.servers.social.tiktok.client.time.sleep"),
        patch("src.mcp.servers.social.tiktok.client.random.uniform", return_value=0),
    ):
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == [{"text": "ok"}]
    assert mock_get.call_count == 2


def test_signed_non_200_breaks(signed_env):
    """A non-200, non-429 status returns whatever was collected so far (empty)."""
    resp = _make_response(status_code=403)
    with patch.object(signed_env.session, "get", return_value=resp):
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == []


def test_signed_connection_error_breaks(signed_env):
    """A request exception is swallowed and the collected comments are returned."""
    with patch.object(signed_env.session, "get", side_effect=Exception("boom")):
        result = signed_env._get_video_comments_signed("vid", count=20)

    assert result == []


def test_signed_rate_limit_timeout_raises(signed_env):
    """If the source rate limiter never grants a slot, a RetryableError is raised."""
    from src.core.errors.exceptions import RetryableError

    with patch("src.mcp.servers.social.tiktok.client.RateLimiter") as mock_limiter:
        mock_limiter.return_value.acquire_source.return_value = False
        with patch.object(signed_env.session, "get"):
            with pytest.raises(RetryableError):
                signed_env._get_video_comments_signed("vid", count=20)


def test_signed_author_id_normalized_in_referer(signed_env):
    """A bare author handle is prefixed with '@' when building the referer URL."""
    resp = _make_response(json_data={"comments": [{"text": "c"}], "has_more": 0}, text="x")
    with patch.object(signed_env.session, "get", return_value=resp) as mock_get:
        signed_env._get_video_comments_signed("vid", count=20, author_id="someuser")

    referer = mock_get.call_args.kwargs["headers"]["referer"]
    assert referer == "https://www.tiktok.com/@someuser/video/vid"


def test_signed_count_zero_fetches_all_available(signed_env):
    """count=0 means 'all': it keeps paging until has_more is falsey."""
    page1 = _make_response(
        json_data={"comments": [{"text": "1"}], "has_more": 1, "cursor": 1}, text="x"
    )
    page2 = _make_response(
        json_data={"comments": [{"text": "2"}], "has_more": 0, "cursor": 2}, text="x"
    )
    with (
        patch.object(signed_env.session, "get", side_effect=[page1, page2]),
        patch("src.mcp.servers.social.tiktok.client.time.sleep"),
    ):
        result = signed_env._get_video_comments_signed("vid", count=0)

    assert [c["text"] for c in result] == ["1", "2"]
