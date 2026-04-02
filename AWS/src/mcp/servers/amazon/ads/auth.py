import os
import requests
import logging
from typing import Optional, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class AmazonAdsAuth:
    """
    Handles Login with Amazon (LWA) Authentication for multiple stores.
    Supports token refreshing and caching.
    """
    
    _token_cache: Dict[str, Dict] = {} # In-memory cache: { "US": {"token": "...", "expiry": datetime} }

    def __init__(self, store_id: Optional[str] = None):
        """
        :param store_id: The identifier for the store (e.g., 'US', 'UK'). 
                         Defaults to AMAZON_ADS_DEFAULT_STORE.
        """
        self.store_id = store_id or os.getenv("AMAZON_ADS_DEFAULT_STORE", "US").upper()
        self.client_id = os.getenv("AMAZON_ADS_CLIENT_ID")
        self.client_secret = os.getenv("AMAZON_ADS_CLIENT_SECRET")
        
        # Dynamic env lookup for refresh token
        refresh_token_env = f"AMAZON_ADS_REFRESH_TOKEN_{self.store_id}"
        self.refresh_token = os.getenv(refresh_token_env)

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError(f"Missing Amazon Ads credentials for store '{self.store_id}'. "
                             f"Check your .env file for CLIENT_ID, CLIENT_SECRET and {refresh_token_env}")

    def get_access_token(self) -> str:
        """
        Returns a valid access token. Refreshes if expired or not in cache.
        """
        cache = self._token_cache.get(self.store_id)
        
        if cache and cache["expiry"] > datetime.now():
            return cache["token"]

        return self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        """
        Exchange refresh_token for a new access_token.
        """
        logger.info(f"Refreshing Amazon Ads access token for store: {self.store_id}")
        
        url = "https://api.amazon.com/auth/o2/token"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            data = response.json()
            
            access_token = data["access_token"]
            expires_in = data.get("expires_in", 3600)
            
            # Cache the token with 5 min buffer
            self._token_cache[self.store_id] = {
                "token": access_token,
                "expiry": datetime.now() + timedelta(seconds=expires_in - 300)
            }
            
            return access_token
            
        except Exception as e:
            logger.error(f"Failed to refresh Amazon Ads token: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise

    def get_profile_id(self) -> str:
        """
        Returns the Profile ID for the current store.
        """
        profile_env = f"AMAZON_ADS_PROFILE_ID_{self.store_id}"
        profile_id = os.getenv(profile_env)
        if not profile_id:
            raise ValueError(f"Missing Profile ID for store '{self.store_id}'. Check {profile_env} in .env")
        return profile_id
