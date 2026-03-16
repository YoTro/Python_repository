from __future__ import annotations
import logging
import os
import json
import uuid
import base64
from curl_cffi import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

logger = logging.getLogger(__name__)


class LingxingAuth:
    """
    Authentication handler for Lingxing ERP (领星ERP).
    Flow: getLoginSecretKey → AES-ECB encrypt password → login → auth-token.
    """

    BASE_URL = "https://gw.lingxingerp.com/newadmin/api/passport"

    # Default: <project_root>/config/lingxing_token.json
    _DEFAULT_TOKEN_FILE = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "config", "lingxing_token.json"
    )

    def __init__(self, token_file: str = None):
        token_file = token_file or os.path.abspath(self._DEFAULT_TOKEN_FILE)
        self.session = requests.Session(impersonate="chrome")
        self.token_file = token_file
        self.common_headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "content-type": "application/json;charset=UTF-8",
            "ak-origin": "https://erp.lingxing.com",
            "x-ak-request-source": "erp",
            "x-ak-version": "AKVERSIONNUM",
            "auth-token": "",
            "x-ak-zid": "",
        }

    def get_secret_key(self) -> tuple:
        """
        Request a one-time secret key pair for password encryption.
        :return: (secret_id, secret_key) or (None, None) on failure.
        """
        url = f"{self.BASE_URL}/getLoginSecretKey"
        headers = {
            **self.common_headers,
            "x-ak-request-id": str(uuid.uuid4()),
        }

        try:
            resp = self.session.post(url, headers=headers)
            data = resp.json()
            if "data" in data and data["data"].get("secretKey"):
                secret_id = data["data"]["secretId"]
                secret_key = data["data"]["secretKey"]
                logger.info(f"Obtained secret key: {secret_id}")
                return secret_id, secret_key
            logger.error(f"getLoginSecretKey failed: {data}")
        except Exception as e:
            logger.error(f"getLoginSecretKey request error: {e}")
        return None, None

    @staticmethod
    def _encrypt_password(password: str, secret_key: str) -> str:
        """
        Encrypt password using AES-128-ECB with PKCS7 padding, return base64 string.
        """
        key_bytes = secret_key.encode("utf-8")[:16]
        padder = padding.PKCS7(128).padder()
        padded = padder.update(password.encode("utf-8")) + padder.finalize()

        cipher = Cipher(algorithms.AES(key_bytes), modes.ECB())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode("utf-8")

    def login(self, account: str = None, password: str = None) -> str:
        """
        Full login flow: get secret key → encrypt password → POST login.
        Returns auth-token on success, None on failure.
        Credentials read from params or env vars LINGXING_ACCOUNT / LINGXING_PASSWORD.
        """
        account = account or os.getenv("LINGXING_ACCOUNT", "")
        password = password or os.getenv("LINGXING_PASSWORD", "")
        if not account or not password:
            logger.error("Lingxing account or password not provided. "
                         "Set LINGXING_ACCOUNT and LINGXING_PASSWORD in .env")
            return None

        # Step 1: Get secret key
        secret_id, secret_key = self.get_secret_key()
        if not secret_id or not secret_key:
            return None

        # Step 2: Encrypt password
        encrypted_pwd = self._encrypt_password(password, secret_key)

        # Step 3: Login
        url = f"{self.BASE_URL}/login"
        payload = {
            "account": account,
            "pwd": encrypted_pwd,
            "verify_code": "",
            "uuid": str(uuid.uuid4()),
            "auto_login": 1,
            "secretId": secret_id,
            "doubleCheckLoginReq": {
                "doubleCheckType": 1,
                "mobileLoginCode": "",
                "loginTick": "",
            },
        }
        headers = {
            **self.common_headers,
            "x-ak-request-id": str(uuid.uuid4()),
        }

        try:
            logger.info(f"Logging in to Lingxing ERP as '{account}'")
            resp = self.session.post(url, headers=headers, json=payload)
            data = resp.json()

            if "data" in data and data["data"]:
                token = data["data"].get("auth-token") or data["data"].get("authToken")
                if token:
                    self._save_token(token)
                    logger.info("Lingxing login successful")
                    return token
                logger.error(f"Token not found in response: {list(data['data'].keys())}")
            else:
                msg = data.get("msg") or data.get("message", "Unknown error")
                logger.error(f"Lingxing login failed: {msg} (code={data.get('code')})")
        except Exception as e:
            logger.error(f"Lingxing login request error: {e}")
        return None

    def _save_token(self, token: str):
        try:
            os.makedirs(os.path.dirname(self.token_file), exist_ok=True)
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump({"auth_token": token}, f, indent=4)
            logger.info(f"Token saved to {self.token_file}")
        except Exception as e:
            logger.error(f"Failed to save token: {e}")

    def load_token(self) -> str:
        """Load previously saved auth-token from disk."""
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get("auth_token")
        except (FileNotFoundError, json.JSONDecodeError):
            return None
