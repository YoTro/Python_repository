from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import platform
import uuid

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from curl_cffi import requests

logger = logging.getLogger(__name__)


class LingxingAuth:
    """
    Authentication handler for Lingxing ERP (领星ERP).
    Flow: getLoginSecretKey → AES-ECB encrypt password → login → auth-token.
    """

    BASE_URL = "https://gw.lingxingerp.com/newadmin/api/passport"

    _DEFAULT_TOKEN_FILE = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "..",
        "config",
        "auth",
        "lingxing_token.json",
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
        account = account or os.getenv("LINGXING_ACCOUNT", "")
        password = password or os.getenv("LINGXING_PASSWORD", "")
        if not account or not password:
            logger.error(
                "Lingxing credentials not provided. Set LINGXING_ACCOUNT / LINGXING_PASSWORD."
            )
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
            # A stable device fingerprint lets Lingxing recognise this client as a
            # trusted device and skip the double-check (2FA) challenge that otherwise
            # withholds the token. Without these fields the server treats every login
            # as a new device and returns doubleCheckConfigRes instead of a token.
            "device": self._USER_AGENT,
            "fingerprint": self._fingerprint(account),
            "sensorsAnonymousId": self._sensors_id(account),
            "secretId": secret_id,
            "doubleCheckLoginReq": {"doubleCheckType": 1, "mobileLoginCode": "", "loginTick": ""},
        }
        headers = {**self.common_headers, "x-ak-request-id": str(uuid.uuid4())}
        try:
            logger.info(f"Logging in to Lingxing ERP as '{account}'")
            resp = self.session.post(url, headers=headers, json=payload)
            data = resp.json()
            # code=1 means the request was accepted. On a fully successful login the
            # token sits at the body root, alongside uid/zid/envKey/companyId. When the
            # device is not trusted, code is still 1 but the body carries
            # doubleCheckConfigRes and no token — a 2FA challenge, not a real success.
            if data.get("code") == 1:
                token = data.get("token")
                if token:
                    self._save_token(
                        token,
                        meta={
                            "uid": str(data.get("uid", "")),
                            "zid": str(data.get("zid", "")),
                            "env_key": data.get("envKey", ""),
                            "company_id": str(data.get("companyId", "")),
                        },
                    )
                    logger.info("Lingxing login successful")
                    return token
                if data.get("doubleCheckConfigRes"):
                    logger.error(
                        "Lingxing login requires double-check (2FA): the device was not "
                        "recognised, so no token was issued. Complete the mobile verification "
                        "in a browser once from this environment, then set LINGXING_FINGERPRINT "
                        "to that trusted browser's fingerprint value and retry."
                    )
                else:
                    logger.error(
                        f"Lingxing login returned code=1 but no token: {list(data.keys())}"
                    )
            else:
                logger.error(f"Lingxing login failed: {data.get('msg')} (code={data.get('code')})")
        except Exception as e:
            logger.error(f"Lingxing login error: {e}")
        return None

    # Chrome UA matching the curl_cffi impersonation profile; sent as the login `device`.
    _USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
    )

    @staticmethod
    def _fingerprint(account: str) -> str:
        """Stable per-install device fingerprint so a trusted device can skip 2FA.

        Prefers LINGXING_FINGERPRINT (recommended: copy the value a trusted browser
        session sends); otherwise derives a value that is at least stable across runs
        on this machine, so a device stays recognised once 2FA has been completed once.
        """
        fp = os.getenv("LINGXING_FINGERPRINT")
        if fp:
            return fp
        seed = f"{account}:{platform.node()}:lingxing-erp".encode()
        return hashlib.md5(seed, usedforsecurity=False).hexdigest()

    @staticmethod
    def _sensors_id(account: str) -> str:
        """Stable analytics id sent with login; env override or a per-install derivation."""
        sid = os.getenv("LINGXING_SENSORS_ID")
        if sid:
            return sid
        seed = f"sensors:{account}:{platform.node()}".encode()
        return hashlib.md5(seed, usedforsecurity=False).hexdigest()

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
            with open(self.token_file, encoding="utf-8") as f:
                return json.load(f).get("auth_token")
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def load_meta(self) -> dict:
        """Return saved identity fields (uid, zid, env_key, company_id)."""
        try:
            with open(self.token_file, encoding="utf-8") as f:
                d = json.load(f)
            return {k: d.get(k, "") for k in ("uid", "zid", "env_key", "company_id")}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
