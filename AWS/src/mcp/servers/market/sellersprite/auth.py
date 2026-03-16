from __future__ import annotations
import logging
import hashlib
import os
import requests

logger = logging.getLogger(__name__)

class SellerspriteAuth:
    """
    Authentication handler for Sellersprite (卖家精灵).
    """
    def __init__(self):
        self.session = requests.Session()
        self.GOOGLE_TKK_DEFAULT = "446379.1364508470"
        self.EXT_VERSION = "3.4.2".replace(".", "00", 1).replace(".", "0") + ".1364508470"

    def _cal(self, e: str, t: str) -> str:
        """
        Internal JS-like token generation algorithm translated to Python.
        """
        def n(e, t):
            for i in range(0, len(t) - 2, 3):
                r = t[i + 2]
                r = ord(r) - 87 if r >= "a" else int(r)
                r = e >> r if t[i + 1] == "+" else e << r
                e = (e + r) & 4294967295 if t[i] == "+" else e ^ r
            return e

        r = t.split(".")
        t_val = int(r[0]) if r[0] else 0
        s = []
        a = 0
        for i in range(len(e)):
            o = ord(e[i])
            if o < 128:
                s.append(o)
                a += 1
            else:
                if o >= 2048:
                    if 64512 == (64512 & o) and i + 1 < len(e) and 56320 == (64512 & ord(e[i + 1])):
                        o = 65536 + ((1023 & o) << 10) + (1023 & ord(e[i + 1]))
                        s.append(o >> 18 | 240)
                        s.append(o >> 12 & 63 | 128)
                        i += 1
                    else:
                        s.append(o >> 12 | 224)
                        s.append(o >> 6 & 63 | 128)
                else:
                    s.append(o >> 6 | 192)
                    s.append(63 & o | 128)
                a += 2
        e_val = t_val
        for i in range(len(s)):
            e_val = n(e_val + s[i], "+-a^+6")
        e_val = n(e_val, "+-3^+b+-f")
        e_val ^= int(r[1]) if r[1] else 0
        if e_val < 0:
            e_val = 2147483648 + (2147483647 & e_val)
        res = e_val % 1000000
        return str(res) + "." + str(res ^ t_val)

    def generate_tk(self, email: str, identifier: str) -> str:
        """
        Generates the 'tk' token required by the Sellersprite API.
        """
        s = []
        a = [email, identifier]
        for item in a:
            if item and len(str(item)) > 0:
                s.append(str(item))
        return "" if len(s) < 1 else self._cal("".join(s), self.EXT_VERSION)

    def salt_password(self, email: str, password: str) -> tuple:
        """
        Hash the password along with the email salt.
        """
        password_hash = hashlib.md5(password.encode()).hexdigest()
        email_password_hash = email + password_hash
        salt = hashlib.md5(email_password_hash.encode()).hexdigest()
        return password_hash, salt

    def login_extension(self, email: str = None, password: str = None) -> str:
        """
        Authenticate via the extension API endpoint.
        Returns the Auth-Token if successful.
        """
        if not email or not password:
            email = os.getenv("SELLERSPRITE_EMAIL", email or "")
            password = os.getenv("SELLERSPRITE_PASSWORD", password or "")
            
        if not email or not password:
            logger.error("Sellersprite email or password not provided in config or params.")
            return None

        password_hash, _ = self.salt_password(email, password)
        tk = self.generate_tk(email, password_hash)
        url = f"https://www.sellersprite.com/v2/extension/signin?email={email}&password={password_hash}&tk={tk}&version=3.4.2&language=zh_CN&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome"
        
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json",
            "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Authenticating Sellersprite for {email}")
        res = self.session.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()
            if 'data' in data and 'token' in data['data']:
                return data['data']['token']
        logger.error(f"Failed to authenticate: {res.text}")
        return None
