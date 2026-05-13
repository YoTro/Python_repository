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

    _DEFAULT_TOKEN_FILE = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "..", "config", "auth", "lingxing_token.json"
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
        url = f"{self.BASE_URL}/getLoginSecretKey"
        headers = {**self.common_headers, "x-ak-request-id": str(uuid.uuid4())}
        try:
            resp = self.session.post(url, headers=headers)
            data = resp.json()
            if "data" in data and data["data"].get("secretKey"):
                return data["data"]["secretId"], data["data"]["secretKey"]
            logger.error(f"getLoginSecretKey failed: {data}")
        except Exception as e:
            logger.error(f"getLoginSecretKey request error: {e}")
        return None, None

    @staticmethod
    def _encrypt_password(password: str, secret_key: str) -> str:
        key_bytes = secret_key.encode("utf-8")[:16]
        padder = padding.PKCS7(128).padder()
        padded = padder.update(password.encode("utf-8")) + padder.finalize()
        cipher = Cipher(algorithms.AES(key_bytes), modes.ECB())
        encryptor = cipher.encryptor()
        return base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode("utf-8")

    def login(self, account: str = None, password: str = None) -> str:
        account  = account  or os.getenv("LINGXING_ACCOUNT", "")
        password = password or os.getenv("LINGXING_PASSWORD", "")
        if not account or not password:
            logger.error("Lingxing credentials not provided. Set LINGXING_ACCOUNT / LINGXING_PASSWORD.")
            return None

        secret_id, secret_key = self.get_secret_key()
        if not secret_id:
            return None

        encrypted_pwd = self._encrypt_password(password, secret_key)
        url = f"{self.BASE_URL}/login"
        payload = {
            "account": account,
            "pwd": encrypted_pwd,
            "verify_code": "",
            "uuid": str(uuid.uuid4()),
            "auto_login": 1,
            "secretId": secret_id,
            "doubleCheckLoginReq": {"doubleCheckType": 1, "mobileLoginCode": "", "loginTick": ""},
        }
        headers = {**self.common_headers, "x-ak-request-id": str(uuid.uuid4())}
        try:
            logger.info(f"Logging in to Lingxing ERP as '{account}'")
            resp = self.session.post(url, headers=headers, json=payload)
            data = resp.json()
            # Response is flat: code=1 means success; token at root level.
            if data.get("code") == 1:
                token = data.get("token")
                if token:
                    self._save_token(token, meta={
                        "uid":        data.get("uid", ""),
                        "zid":        str(data.get("zid", "")),
                        "env_key":    data.get("envKey", ""),
                        "company_id": data.get("companyId", ""),
                    })
                    logger.info("Lingxing login successful")
                    return token
                logger.error(f"Token not found in login response: {list(data.keys())}")
            else:
                logger.error(f"Lingxing login failed: {data.get('msg')} (code={data.get('code')})")
        except Exception as e:
            logger.error(f"Lingxing login error: {e}")
        return None

    def _save_token(self, token: str, meta: dict = None):
        try:
            os.makedirs(os.path.dirname(self.token_file), exist_ok=True)
            payload = {"auth_token": token}
            if meta:
                payload.update(meta)
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=4)
            logger.info(f"Token saved to {self.token_file}")
        except Exception as e:
            logger.error(f"Failed to save token: {e}")

    def load_token(self) -> str:
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                return json.load(f).get("auth_token")
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def load_meta(self) -> dict:
        """Return saved identity fields (uid, zid, env_key, company_id)."""
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            return {k: d.get(k, "") for k in ("uid", "zid", "env_key", "company_id")}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
