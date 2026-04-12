from __future__ import annotations
import json
import logging
import hashlib
import os
import requests

logger = logging.getLogger(__name__)

_CONFIG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..", "config", "auth")
)

# Cookies set by the Sellersprite web login that the v3 API requires.
# The extension signin endpoint alone is insufficient for v3 — these cookies
# must be present in the session alongside the Auth-Token header.
_SESSION_COOKIE_KEYS = ("rank-login-user", "Sprite-X-Token", "rank-login-user-info")


class SellerspriteAuth:
    """
    Authentication handler for Sellersprite (卖家精灵).

    Auth state is persisted at ``config/auth/sellersprite_{tenant_id}_token.json``.
    The file stores the plain Auth-Token AND the three session cookies required
    by the v3 API (rank-login-user, Sprite-X-Token, rank-login-user-info).
    Without the cookies, the v3/api/competing-lookup endpoint returns
    ERR_GLOBAL_SESSION_EXPIRED even with a valid Auth-Token header.

    Cookie lifecycle:
      - On ``load_token()``: cookies are restored onto ``self.session`` so all
        subsequent requests carry them automatically.
      - On ``login_extension()``: cookies set by the signin response are captured
        from ``self.session`` and saved alongside the token.
      - On ``save_cookies()``: manually inject browser-captured cookies (used when
        the extension signin endpoint is blocked by datacenter IP detection).

    Credential resolution order for ``login_extension``:
      1. ``SELLERSPRITE_EMAIL_{TENANT_ID}`` / ``SELLERSPRITE_PASSWORD_{TENANT_ID}``
      2. ``SELLERSPRITE_EMAIL`` / ``SELLERSPRITE_PASSWORD``  (shared fallback)
    """

    def __init__(self, tenant_id: str = "default", token_file: str = None):
        self.tenant_id = tenant_id
        os.makedirs(_CONFIG_DIR, exist_ok=True)
        self.token_file = token_file or os.path.join(
            _CONFIG_DIR, f"sellersprite_{tenant_id}_token.json"
        )
        self.session = requests.Session()
        self.VERSION = "5.0.2"
        self.GOOGLE_TKK_DEFAULT = "346379.1364508470"

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save_token(self, token: str) -> None:
        """Save token + current session cookies to the token file."""
        cookies = {k: self.session.cookies.get(k) for k in _SESSION_COOKIE_KEYS
                   if self.session.cookies.get(k)}
        try:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump({"token": token, "cookies": cookies}, f, indent=4)
        except Exception as e:
            logger.error(f"[sellersprite:{self.tenant_id}] Failed to save token: {e}")

    def load_token(self) -> str:
        """Load token from file and restore session cookies. Returns token or ''."""
        if not os.path.exists(self.token_file):
            return ""
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            token = data.get("token", "")
            for name, value in (data.get("cookies") or {}).items():
                if value:
                    self.session.cookies.set(name, value, domain="sellersprite.com")
            if data.get("cookies"):
                logger.debug(f"[sellersprite:{self.tenant_id}] Restored {len(data['cookies'])} session cookies.")
            return token
        except Exception as e:
            logger.error(f"[sellersprite:{self.tenant_id}] Failed to load token: {e}")
        return ""

    def save_cookies(self, token: str, cookies: dict) -> None:
        """
        Manually persist a browser-captured token + cookies.
        Use this when the extension signin endpoint is unavailable (datacenter IP block):

            auth.save_cookies(
                token="<rank-login-user cookie value>",
                cookies={
                    "rank-login-user":      "...",
                    "Sprite-X-Token":       "eyJhbG...",
                    "rank-login-user-info": "eyJ...",
                },
            )
        """
        for name, value in cookies.items():
            if value:
                self.session.cookies.set(name, value, domain="sellersprite.com")
        self._save_token(token)
        logger.info(f"[sellersprite:{self.tenant_id}] Browser cookies saved successfully.")

    # ── Token generation ──────────────────────────────────────────────────────

    def _cal(self, e: str, t: str) -> str:
        """Internal JS-like token generation algorithm translated to Python."""
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
        """Generates the 'tk' token required by the Sellersprite API."""
        s = [str(item) for item in [email, identifier] if item and len(str(item)) > 0]
        return "" if not s else self._cal("".join(s), self.GOOGLE_TKK_DEFAULT)

    def salt_password(self, email: str, password: str) -> tuple:
        """Hash the password along with the email salt."""
        password_hash = hashlib.md5(password.encode()).hexdigest()
        email_password_hash = email + password_hash
        salt = hashlib.md5(email_password_hash.encode()).hexdigest()
        return password_hash, salt

    # ── Login ─────────────────────────────────────────────────────────────────

    def login_extension(self, email: str = None, password: str = None) -> str:
        """
        Authenticate and obtain the Auth-Token + session cookies.

        Strategy (tries in order, stops at first success):
          1. Web form login  POST /w/user/signin  — works from any IP (incl. datacenter),
             returns a 302 redirect and sets all three session cookies via Set-Cookie.
          2. Extension API   GET  /v2/extension/signin — fallback; blocked on datacenter IPs
             (returns ERR_GLOBAL_403) but works on residential / office IPs.

        Returns the Auth-Token string on success, None on failure.
        Falls back to ``save_cookies()`` for manual injection when both methods fail.
        """
        if not email or not password:
            suffix = self.tenant_id.upper()
            email = (
                os.getenv(f"SELLERSPRITE_EMAIL_{suffix}")
                or os.getenv("SELLERSPRITE_EMAIL", "")
            )
            password = (
                os.getenv(f"SELLERSPRITE_PASSWORD_{suffix}")
                or os.getenv("SELLERSPRITE_PASSWORD", "")
            )

        if not email or not password:
            logger.error(f"[sellersprite:{self.tenant_id}] Email or password not configured.")
            return None

        password_hash, salt = self.salt_password(email, password)
        logger.info(f"[sellersprite:{self.tenant_id}] Authenticating for {email}")

        # ── Method 1: web form login (works from all IPs) ─────────────────────
        token = self._login_web(email, password_hash, salt)
        if token:
            return token

        # ── Method 2: extension API (residential / office IPs only) ──────────
        token = self._login_extension_api(email, password_hash)
        if token:
            return token

        logger.error(
            f"[sellersprite:{self.tenant_id}] Both login methods failed. "
            f"Use save_cookies() to inject a browser-captured token manually."
        )
        return None

    def _login_web(self, email: str, password_hash: str, salt: str) -> str | None:
        """POST /w/user/signin — sets all three session cookies via 302 redirect."""
        url = "https://www.sellersprite.com/w/user/signin"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.sellersprite.com",
            "referer": "https://www.sellersprite.com/en/w/user/login",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        }
        data = {
            "callback":     "",
            "password":     password_hash,
            "email":        email,
            "password_otn": "xxxxxxxxxx",
            "salt":         salt,
        }
        try:
            res = self.session.post(url, headers=headers, data=data, allow_redirects=False)
        except Exception as e:
            logger.warning(f"[sellersprite:{self.tenant_id}] Web login request failed: {e}")
            return None

        if res.status_code not in (302, 200):
            logger.warning(f"[sellersprite:{self.tenant_id}] Web login returned {res.status_code}")
            return None

        token = self.session.cookies.get("rank-login-user")
        if not token:
            logger.warning(f"[sellersprite:{self.tenant_id}] Web login did not set rank-login-user cookie")
            return None

        self._save_token(token)
        logger.info(f"[sellersprite:{self.tenant_id}] Web login successful, token + cookies saved.")
        return token

    def _login_extension_api(self, email: str, password_hash: str) -> str | None:
        """GET /v2/extension/signin — fallback; blocked on datacenter IPs."""
        tk = self.generate_tk(email, password_hash)
        url = (
            f"https://www.sellersprite.com/v2/extension/signin"
            f"?email={email}&password={password_hash}&tk={tk}"
            f"&version={self.VERSION}&language=zh_CN"
            f"&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome"
        )
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json",
            "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
        }
        try:
            res = self.session.get(url, headers=headers)
        except Exception as e:
            logger.warning(f"[sellersprite:{self.tenant_id}] Extension signin request failed: {e}")
            return None

        if res.status_code != 200:
            logger.warning(f"[sellersprite:{self.tenant_id}] Extension signin returned {res.status_code}")
            return None

        body = res.json()
        code = body.get("code")
        if code == "ERR_ACCOUNT_LOCKED":
            logger.error(
                f"[sellersprite:{self.tenant_id}] Account locked — {body.get('message')}. "
                f"Unlock via main account before retrying."
            )
            return None
        if code == "ERR_ROBOT_CHECK":
            logger.error(
                f"[sellersprite:{self.tenant_id}] Robot/CAPTCHA check — {body.get('message')}. "
                f"Complete verification at sellersprite.com or wait ~30 min."
            )
            return None
        if code == "ERR_GLOBAL_403":
            logger.warning(
                f"[sellersprite:{self.tenant_id}] Extension signin blocked (datacenter IP) — "
                f"web login should have been tried first."
            )
            return None
        data = body.get("data") or {}
        if isinstance(data, dict) and "token" in data:
            token = data["token"]
            self._save_token(token)
            logger.info(f"[sellersprite:{self.tenant_id}] Extension signin successful, token + cookies saved.")
            return token
        logger.error(f"[sellersprite:{self.tenant_id}] Extension signin failed: {res.text[:300]}")
        return None
