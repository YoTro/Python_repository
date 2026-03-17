from __future__ import annotations
import logging
from curl_cffi import requests
import json
import os

logger = logging.getLogger(__name__)

class XiyouZhaociAuth:
    """
    Authentication handler for Xiyou Zhaoci (西柚找词).
    Handles SMS code and login to get the token.
    """

    # Default: <project_root>/config/xiyouzhaoci_token.json
    _DEFAULT_TOKEN_FILE = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "..", "config", "xiyouzhaoci_token.json"
    )

    def __init__(self, token_file: str = None):
        token_file = token_file or os.path.abspath(self._DEFAULT_TOKEN_FILE)
        self.session = requests.Session(impersonate="chrome")
        self.base_url = "https://api.xiyouzhaoci.com"
        self.token_file = token_file
        
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
