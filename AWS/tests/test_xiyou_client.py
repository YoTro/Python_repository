from __future__ import annotations
import pytest
import os
import json
from unittest.mock import patch, MagicMock, call
from src.mcp.servers.market.xiyouzhaoci.client import XiyouZhaociAPI

@pytest.fixture
def mock_api():
    """Fixture to create a XiyouZhaociAPI instance with a mocked session."""
    with patch('src.mcp.servers.market.xiyouzhaoci.client.requests.Session') as mock_session:
        # Mock the session object and its request method
        mock_request = MagicMock()
        mock_session.return_value.request = mock_request
        
        # Instantiate the API client
        api = XiyouZhaociAPI()
        api.auth_token = "fake-token" # Assume authenticated
        api.session = mock_session.return_value
        
        yield api, mock_request

def test_export_compare_data_flow(mock_api, tmpdir):
    """
    Test the full multi-ASIN comparison flow, from initiating the request to downloading the file.
    """
    api, mock_request = mock_api
    
    # 1. Mock API responses
    # Mock for initial request -> returns resourceId
    mock_response_init = MagicMock()
    mock_response_init.status_code = 200
    mock_response_init.json.return_value = {"resourceId": "123456789"}
    mock_response_init.raise_for_status.return_value = None

    # Mock for status poll -> first pending, then done
    mock_response_pending = MagicMock()
    mock_response_pending.status_code = 200
    mock_response_pending.json.return_value = {"status": "Pending"}
    mock_response_pending.raise_for_status.return_value = None
    
    mock_response_done = MagicMock()
    mock_response_done.status_code = 200
    mock_response_done.json.return_value = {
        "status": "Done",
        "resourceUrl": "https://fake-oss-url.com/report.xlsx"
    }
    mock_response_done.raise_for_status.return_value = None

    mock_request.side_effect = [
        mock_response_init,       # For compare_asins
        mock_response_pending,    # For _poll_and_download (1st poll)
        mock_response_done        # For _poll_and_download (2nd poll)
    ]
    
    # 2. Mock the file download
    with patch.object(api, '_download_file', return_value=True) as mock_download:
        
        # 3. Execute the function
        asins = ["ASIN1", "ASIN2"]
        output_dir = str(tmpdir)
        result_path = api.export_compare_data(
            country="US", 
            asins=asins, 
            period="last30days",
            output_dir=output_dir
        )

        # 4. Assertions
        assert result_path.endswith("US_compare_ASIN1_123456789.xlsx")
        
        # Check that the correct API calls were made
        assert mock_request.call_count == 3
        
        # Call 1: Initiate comparison
        init_call = mock_request.call_args_list[0]
        assert init_call.args[1] == "https://api.xiyouzhaoci.com/v4/asins/compare/list/resource"
        assert init_call.kwargs['json']['cycleFilter']['period'] == "last30days"
        assert init_call.kwargs['json']['asins'] == asins

        # Call 3: Final status check
        status_call_done = mock_request.call_args_list[2]
        assert status_call_done.args[1] == "https://api.xiyouzhaoci.com/v4/resource/status"
        assert status_call_done.kwargs['json']['resourceId'] == "123456789"

        # Check that download was called
        mock_download.assert_called_once_with("https://fake-oss-url.com/report.xlsx", result_path)

def test_401_unauthorized_retry(mock_api):
    """Test that the client reloads the token and retries on 401 error."""
    api, mock_request = mock_api
    
    # 1. Mock first response as 401, second as 200
    mock_response_401 = MagicMock()
    mock_response_401.status_code = 401
    
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"resourceId": "retry-success"}
    mock_response_200.raise_for_status.return_value = None
    
    mock_request.side_effect = [mock_response_401, mock_response_200]
    
    # 2. Mock _load_token to return a NEW token
    new_token = "new-fake-token"
    with patch.object(api, '_load_token', return_value=new_token):
        # 3. Call an API method
        result = api.lookup_asin("US", "B0BSYD2VV6")
        
        # 4. Assertions
        assert result == {"resourceId": "retry-success"}
        assert api.auth_token == new_token
        assert api.common_headers["authorization"] == new_token
        
        # Should have called request twice
        assert mock_request.call_count == 2
        
        # Second call should have the new token
        second_call = mock_request.call_args_list[1]
        assert second_call.kwargs['headers']['authorization'] == new_token
