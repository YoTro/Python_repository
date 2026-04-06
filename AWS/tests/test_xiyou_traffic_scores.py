from __future__ import annotations
import pytest
import json
from unittest.mock import patch, MagicMock
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

@pytest.fixture
def mock_api():
    """Fixture to create a XiyouZhaociAPI instance with a mocked session."""
    with patch('src.mcp.servers.market.xiyouzhaoci.client.requests.Session') as mock_session, \
         patch('src.mcp.servers.market.xiyouzhaoci.client.XiyouZhaociAPI._load_token', return_value="fake-token"):
        # Mock the session object and its request method
        mock_request = MagicMock()
        mock_session.return_value.request = mock_request
        
        # Instantiate the API client
        api = XiyouZhaociAPI()
        api.session = mock_session.return_value
        
        yield api, mock_request

def test_get_traffic_scores_success(mock_api):
    """
    Test the get_traffic_scores method for successful data retrieval.
    """
    api, mock_request = mock_api
    
    # 1. Mock API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_data = {
        "success": True,
        "data": [
            {
                "asin": "B07T869RNY",
                "advertisingTrafficScoreRatio": 0.45,
                "totalTrafficScoreGrowthRate": 0.12,
                "trafficScore": 85
            },
            {
                "asin": "B0CKY689WQ",
                "advertisingTrafficScoreRatio": 0.30,
                "totalTrafficScoreGrowthRate": -0.05,
                "trafficScore": 72
            }
        ]
    }
    mock_response.json.return_value = mock_data
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response
    
    # 2. Execute the function
    asins = ["B07T869RNY", "B0CKY689WQ"]
    country = "US"
    result = api.get_traffic_scores(country, asins)
    
    # 3. Assertions
    assert result == mock_data
    assert mock_request.call_count == 1
    
    # Verify request details
    call_args = mock_request.call_args
    method, url = call_args[0]
    kwargs = call_args[1]
    
    assert method == "POST"
    assert url == "https://api.xiyouzhaoci.com/v4/asins/trafficScore"
    assert kwargs["json"] == {"asins": asins, "country": country}
    assert kwargs["headers"]["request-url"] == "/detail/asin/look_up/US/B07T869RNY"
    assert "authorization" in kwargs["headers"]
    assert kwargs["headers"]["authorization"] == "fake-token"

def test_get_traffic_scores_empty_asins(mock_api):
    """
    Test the get_traffic_scores method with an empty ASIN list.
    """
    api, mock_request = mock_api
    
    # 1. Mock API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True, "data": []}
    mock_response.raise_for_status.return_value = None
    mock_request.return_value = mock_response
    
    # 2. Execute the function
    result = api.get_traffic_scores("US", [])
    
    # 3. Assertions
    assert result["success"] is True
    assert mock_request.call_count == 1
    assert mock_request.call_args[1]["headers"]["request-url"] == "/detail/asin/look_up/US/unknown"

def test_get_traffic_scores_error(mock_api):
    """
    Test the get_traffic_scores method handling an API error.
    """
    api, mock_request = mock_api
    
    # 1. Mock API failure
    mock_request.side_effect = Exception("Network error")
    
    # 2. Execute the function
    result = api.get_traffic_scores("US", ["B07T869RNY"])
    
    # 3. Assertions
    assert result == {}
    assert mock_request.call_count == 1
