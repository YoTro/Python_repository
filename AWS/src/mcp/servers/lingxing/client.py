from __future__ import annotations
import logging
import uuid
from curl_cffi import requests
from .auth import LingxingAuth

logger = logging.getLogger(__name__)


class LingxingClient:
    """
    API client for Lingxing ERP (领星ERP).
    Handles authenticated requests to the Lingxing gateway.
    """

    BASE_URL = "https://gw.lingxingerp.com"

    def __init__(self, account: str = None, password: str = None):
        self.auth = LingxingAuth()
        self.session = requests.Session(impersonate="chrome")
        self.token = self.auth.load_token()

        if not self.token:
            self.token = self.auth.login(account, password)

        if not self.token:
            logger.warning("LingxingClient initialized without a valid token. "
                           "Call client.auth.login() to authenticate.")

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """
        Send an authenticated request to the Lingxing API.

        :param method: HTTP method (GET, POST, etc.)
        :param path: API path (e.g., '/newadmin/api/some/endpoint')
        :param kwargs: Additional arguments passed to session.request (json, params, etc.)
        :return: Parsed JSON response dict, or empty dict on failure.
        """
        url = f"{self.BASE_URL}{path}"
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "ak-origin": "https://erp.lingxing.com",
            "x-ak-request-source": "erp",
            "x-ak-version": "AKVERSIONNUM",
            "x-ak-request-id": str(uuid.uuid4()),
            "auth-token": self.token or "",
        }
        headers.update(kwargs.pop("headers", {}))

        try:
            resp = self.session.request(method, url, headers=headers, **kwargs)
            data = resp.json()

            if data.get("code") in (401, "401", -1) or "token" in str(data.get("msg", "")).lower():
                logger.warning("Token expired, re-authenticating...")
                self.token = self.auth.login()
                if self.token:
                    headers["auth-token"] = self.token
                    resp = self.session.request(method, url, headers=headers, **kwargs)
                    data = resp.json()

            return data
        except Exception as e:
            logger.error(f"Lingxing API request failed [{method} {path}]: {e}")
            return {}
