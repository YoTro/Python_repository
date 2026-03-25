from __future__ import annotations
import logging
from curl_cffi import requests
import json
import os
import time

logger = logging.getLogger(__name__)

class XiyouZhaociAuth:
    """
    Authentication handler for Xiyou Zhaoci (西柚找词).
    Handles SMS code, WeChat QR login, and session persistence.
    """

    def __init__(self, tenant_id: str = "default", token_file: str = None):
        self.tenant_id = tenant_id
        
        # Calculate file paths based on tenant_id for multi-user isolation
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "config", "auth"))
        os.makedirs(config_dir, exist_ok=True)
        
        self.token_file = token_file or os.path.join(config_dir, f"xiyou_{tenant_id}_token.json")
        self.state_file = os.path.join(config_dir, f"xiyou_{tenant_id}_state.json")
        
        self.session = requests.Session(impersonate="chrome")
        self.base_url = "https://api.xiyouzhaoci.com"
        
        self.common_headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "content-type": "application/json",
            "krs-ver": "1.0.0",
            "select-lang": "zh-cn",
            "web-version": "4.0",
            "priority": "u=1, i",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "referrer": "https://www.xiyouzhaoci.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        }

    def _save_token(self, token: str):
        try:
            os.makedirs(os.path.dirname(self.token_file), exist_ok=True)
            with open(self.token_file, 'w', encoding='utf-8') as f:
                json.dump({"token": token}, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save token to {self.token_file}: {e}")

    def _save_auth_state(self, ticket: str):
        try:
            state = {
                "ticket": ticket,
                "created_at": time.time(),
                "tenant_id": self.tenant_id
            }
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save auth state to {self.state_file}: {e}")

    def get_wechat_qr(self) -> dict:
        """
        Step 1: Request WeChat login QR and ticket.
        """
        url = f"{self.base_url}/v2/system/login/wechatChannel"
        logger.info(f"[{self.tenant_id}] Requesting WeChat login QR")
        
        # Ensure we use the common headers which include the necessary browser fingerprints
        headers = self.common_headers.copy()
        headers["authorization"] = "" # Explicitly clear auth for login request
        
        try:
            response = self.session.post(url, headers=headers)
            data = response.json()
            
            if data.get("ticket"):
                self._save_auth_state(data["ticket"])
                return {
                    "ticket": data["ticket"],
                    "url": data["url"],
                    "expires_in": 120,
                    "msg": "Please scan the QR code within 120 seconds and confirm on your phone."
                }
            else:
                logger.error(f"Failed to get ticket: {data}")
                return {"error": data.get("msg", "Unknown error")}
        except Exception as e:
            logger.error(f"Error fetching WeChat QR: {e}")
            return {"error": str(e)}

    def check_wechat_login(self) -> dict:
        """
        Step 2: Check scan status using the persisted ticket.
        """
        if not os.path.exists(self.state_file):
            return {"status": "ERROR", "msg": "No pending login task found for this tenant."}

        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except Exception as e:
            return {"status": "ERROR", "msg": f"Failed to read auth state: {e}"}

        # Check 120s expiry (with 5s buffer)
        elapsed = time.time() - state.get("created_at", 0)
        if elapsed > 115:
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
            return {"status": "EXPIRED", "msg": "QR code has expired. Please request a new one."}

        url = f"{self.base_url}/v2/system/login/wechatChannel/scanStatus"
        payload = {"ticket": state["ticket"]}
        
        logger.info(f"[{self.tenant_id}] Checking scan status for ticket: {state['ticket'][:10]}...")
        try:
            response = self.session.post(url, headers=self.common_headers, json=payload)
            data = response.json()
            
            # The API returns the token directly or in a 'data' field upon success
            token = data.get("token") or data.get("data", {}).get("token")
            
            if token:
                logger.info(f"[{self.tenant_id}] WeChat login successful")
                self._save_token(token)
                if os.path.exists(self.state_file):
                    os.remove(self.state_file)
                return {"status": "SUCCESS", "token": token, "msg": "Login successful."}
            
            # Still waiting
            return {"status": "WAITING", "msg": "Still waiting for scan or confirmation...", "elapsed": int(elapsed)}
            
        except Exception as e:
            logger.error(f"Error checking scan status: {e}")
            return {"status": "ERROR", "msg": str(e)}

    def send_sms_code(self, phone_num: str = None) -> bool:
        """
        Request an SMS verification code for the given phone number.
        """
        if not phone_num:
            phone_num = os.getenv("XIYOUZHAOCI_PHONE", "")
            
        if not phone_num:
            logger.error("Xiyouzhaoci phone number not provided.")
            return False

        url = f"{self.base_url}/v2/system/login/phoneNumChannel/smsCode"
        payload = {"phoneNum": phone_num}
        
        logger.info(f"Requesting SMS code for {phone_num}")
        try:
            response = self.session.post(url, headers=self.common_headers, json=payload)
            
            if response.status_code == 200 and not response.text.strip():
                logger.info("SMS code sent successfully (empty response)")
                return True

            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON. Status: {response.status_code}, Raw response: {response.text}")
                return False

            if data.get("code") == 200 or data.get("code") == "success" or response.status_code == 200:
                logger.info("SMS code sent successfully")
                return True
            else:
                logger.error(f"Failed to send SMS code: {data.get('msg')} ({data.get('code')})")
                return False
        except Exception as e:
            logger.error(f"Error requesting SMS code: {e}")
            if 'response' in locals():
                logger.error(f"Response status: {response.status_code}, text: {response.text}")
            return False

    def login_with_sms(self, sms_code: str, phone_num: str = None) -> bool:
        """
        Login using phone number and SMS verification code.
        """
        if not phone_num:
            phone_num = os.getenv("XIYOUZHAOCI_PHONE", "")

        url = f"{self.base_url}/v2/system/login/phoneNumChannel"
        payload = {
            "phoneNum": phone_num,
            "smsCode": sms_code,
            "chan": "",
            "registerChannel": "",
            "sourceHistory": []
        }
        
        logger.info(f"Attempting login for {phone_num}")
        try:
            headers = self.common_headers.copy()
            headers["authorization"] = ""
            
            response = self.session.post(url, headers=headers, json=payload)
            
            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to parse JSON. Status: {response.status_code}, Raw response: {response.text}")
                return False
            
            if data.get("code") == 200 or data.get("code") == "success" or response.status_code == 200:
                logger.info("Login successful")
                token = data.get("token") or data.get("data", {}).get("token")
                
                if token:
                    self._save_token(token)
                    return True
                else:
                    logger.error(f"Token not found in response. Full response: {data}")
                    return False
            else:
                logger.error(f"Login failed: {data.get('msg')} ({data.get('code')})")
                return False
        except Exception as e:
            logger.error(f"Error during login: {e}")
            return False

if __name__ == "__main__":
    auth = XiyouZhaociAuth()
    phone = input("Enter phone number (leave blank to use config): ")
    if auth.send_sms_code(phone if phone else None):
        code = input("Enter the SMS code received: ")
        if auth.login_with_sms(code, phone if phone else None):
            print("Login successful and token saved.")
