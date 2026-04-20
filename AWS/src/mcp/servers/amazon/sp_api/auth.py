from __future__ import annotations
import os
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)

MARKETPLACE_IDS: Dict[str, str] = {
    "US": "ATVPDKIKX0DER",
    "CA": "A2EUQ1WTGCTBG2",
    "UK": "A1F83G8C2ARO7P",
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "JP": "A1VC38T7YXB528",
}

SP_API_ENDPOINTS: Dict[str, str] = {
    "NA": "https://sellingpartnerapi-na.amazon.com",
    "EU": "https://sellingpartnerapi-eu.amazon.com",
    "FE": "https://sellingpartnerapi-fe.amazon.com",
}

STORE_TO_REGION: Dict[str, str] = {
    "US": "NA", "CA": "NA",
    "UK": "EU", "DE": "EU", "FR": "EU", "IT": "EU", "ES": "EU",
    "JP": "FE",
}


class SPAPIAuth:
    """
    LWA OAuth2 auth for Amazon Selling Partner API.

    Env vars (per store, e.g. store_id="US"):
      AMAZON_SP_API_CLIENT_ID
      AMAZON_SP_API_CLIENT_SECRET
      AMAZON_SP_API_REFRESH_TOKEN_US
      AMAZON_SP_MARKETPLACE_ID_US   (optional override; defaults from MARKETPLACE_IDS)
    """

    _token_cache: Dict[str, Dict] = {}

    def __init__(self, store_id: Optional[str] = None):
        self.store_id = (store_id or os.getenv("AMAZON_SP_API_DEFAULT_STORE", "US")).upper()
        # LWA shared creds take priority; SP-API-specific creds as fallback
        self.client_id = (
            os.getenv("AMAZON_LWA_CLIENT_ID")
            or os.getenv("AMAZON_SP_API_CLIENT_ID")
        )
        self.client_secret = (
            os.getenv("AMAZON_LWA_CLIENT_SECRET")
            or os.getenv("AMAZON_SP_API_CLIENT_SECRET")
            or ""
        )

        refresh_env = f"AMAZON_SP_API_REFRESH_TOKEN_{self.store_id}"
        self.refresh_token = os.getenv(refresh_env) or os.getenv("AMAZON_SP_API_REFRESH_TOKEN")

        if not self.client_id or not self.refresh_token:
            raise ValueError(
                f"Missing SP-API credentials for store '{self.store_id}'. "
                f"Set AMAZON_LWA_CLIENT_ID and {refresh_env} (or AMAZON_SP_API_REFRESH_TOKEN)."
            )

        region = STORE_TO_REGION.get(self.store_id, "NA")
        self.endpoint = SP_API_ENDPOINTS[region]

        marketplace_env = f"AMAZON_SP_MARKETPLACE_ID_{self.store_id}"
        self.marketplace_id = os.getenv(marketplace_env) or MARKETPLACE_IDS.get(self.store_id)
        if not self.marketplace_id:
            raise ValueError(f"Unknown marketplace for store '{self.store_id}'. Set {marketplace_env}.")

    def get_access_token(self) -> str:
        cache = self._token_cache.get(self.store_id)
        if cache and cache["expiry"] > datetime.now():
            return cache["token"]
        return self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        logger.info(f"Refreshing SP-API access token for store: {self.store_id}")
        try:
            resp = requests.post(
                "https://api.amazon.com/auth/o2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            expires_in = data.get("expires_in", 3600)
            self._token_cache[self.store_id] = {
                "token": token,
                "expiry": datetime.now() + timedelta(seconds=expires_in - 300),
            }
            return token
        except Exception as e:
            logger.error(f"SP-API token refresh failed: {e}")
            raise
