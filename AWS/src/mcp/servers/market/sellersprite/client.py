from __future__ import annotations
import logging
import random
import threading
import time
import requests
from .auth import SellerspriteAuth
from src.gateway.rate_limit import RateLimiter
from src.core.errors.exceptions import RetryableError

logger = logging.getLogger(__name__)

# Process-level lock: only one thread may call login_extension() at a time.
# Cooldown duration is read from config/settings.json → rate_limits.source_limits.sellersprite.login_cooldown_seconds.
_LOGIN_LOCK = threading.Lock()
_LOGIN_LAST_ATTEMPT: dict[str, float] = {}   # tenant_id → monotonic seconds

class SellerspriteAPI:
    """
    API client for Sellersprite (卖家精灵).

    Auth is entirely cookie-based (rank-login-user, Sprite-X-Token,
    rank-login-user-info).  No Auth-Token header is sent — the browser
    doesn't send one either, and the server ignores it for v2/v3 endpoints.

    Auth lifecycle:
      1. On init, loads cookies from
         ``config/auth/sellersprite_{tenant_id}_token.json`` via
         ``SellerspriteAuth.load_token()``, which restores them onto
         ``self.session``.
      2. If the file is absent, calls ``login_extension()`` (reads
         ``SELLERSPRITE_EMAIL_{TENANT_ID}`` or ``SELLERSPRITE_EMAIL`` from env)
         and saves the new cookies to the same file.
      3. On soft-401 (HTTP 200 with data="/user/login?…"), re-logins once;
         the new session cookies are applied to ``self.session`` automatically.
    """

    def __init__(self, tenant_id: str = "default"):
        self.tenant_id = tenant_id
        self.auth = SellerspriteAuth(tenant_id=tenant_id)
        # Share the auth session so login cookies are present in all API calls.
        # login_extension() sets cookies on auth.session; using the same object
        # means those cookies are automatically sent on every subsequent request.
        self.session = self.auth.session
        self.VERSION = self.auth.VERSION
        self._ensure_cookies()

    @property
    def auth_token(self) -> str:
        """The rank-login-user cookie value (empty string when not authenticated)."""
        return self.session.cookies.get("rank-login-user", "")

    def _ensure_cookies(self) -> None:
        """Load saved cookies; auto-login if none found."""
        token = self.auth.load_token()
        if not token:
            logger.info("[sellersprite] No saved cookies — attempting auto-login")
            self.auth.login_extension()

    def _safe_relogin(self) -> bool:
        """
        Rate-limited, mutex-protected re-login.
        Returns True if new cookies were obtained. At most one attempt per
        login_cooldown_seconds per tenant to avoid account lockout.
        """
        with _LOGIN_LOCK:
            cooldown = RateLimiter().get_source_config("sellersprite").get("login_cooldown_seconds", 60)
            now = time.monotonic()
            last = _LOGIN_LAST_ATTEMPT.get(self.tenant_id, 0.0)
            if now - last < cooldown:
                wait = cooldown - (now - last)
                logger.warning(
                    f"[sellersprite:{self.tenant_id}] re-login cooldown — "
                    f"skipping (next attempt in {wait:.0f}s)"
                )
                return False
            _LOGIN_LAST_ATTEMPT[self.tenant_id] = now
            token = self.auth.login_extension() or ""
            return bool(token)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """HTTP request with Layer 3 token-bucket and 429 backoff.

        Auth is carried by session cookies — no Auth-Token header needed.
        Soft-401 (login redirect in response body) is handled per-method.
        """
        limiter = RateLimiter()
        for attempt in range(3):
            if not limiter.acquire_source("sellersprite"):
                raise RetryableError("sellersprite source rate limit timeout", retry_after_seconds=60)

            response = self.session.request(method, url, **kwargs)

            if response.status_code == 429:
                wait = int(response.headers.get("Retry-After", 2 ** (attempt + 1))) + random.uniform(0, 1)
                logger.warning(f"[sellersprite] 429 rate limited — waiting {wait:.1f}s (attempt {attempt + 1}/3)")
                time.sleep(wait)
                continue

            if response.status_code == 401:
                logger.warning("[sellersprite] 401 Unauthorized — re-logging in and retrying")
                self._safe_relogin()
                response = self.session.request(method, url, **kwargs)

            return response

        raise RetryableError("sellersprite still rate limited after 3 retries", retry_after_seconds=120)

    def get_keepa_data(self, asin: str) -> dict:
        """
        Fetch Keepa ranking data for an ASIN.
        """
        tk = self.auth.generate_tk("", asin)
        url = (
            f"https://www.sellersprite.com/v2/extension/keepa"
            f"?station=US&asin={asin}&tk={tk}&version={self.VERSION}"
            f"&language=zh_CN&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome"
        )
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json",
            "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        }

        logger.info(f"Fetching Keepa data for {asin}")
        res = self._request("GET", url, headers=headers)
        response_data = {'times': [], 'bsr': [], 'subRanks': []}

        if res.status_code == 200:
            data = res.json()
            if 'data' in data and 'keepa' in data['data']:
                keepa = data['data']['keepa']
                response_data['bsr'] = keepa.get('bsr', [])
                response_data['times'] = data['data'].get('times', [])
                sub_ranks = keepa.get('subRanks', {})
                if sub_ranks:
                    response_data['subRanks'] = list(sub_ranks.values())[0]
        else:
            logger.error(f"Failed to fetch Keepa data: {res.text}")

        return response_data

    def get_competing_lookup(
        self,
        market: str,
        month_name: str,
        node_id_paths: list[str],
        page: int = 1,
        size: int = 100,
        order: dict | None = None,
        symbol_flag: bool = True,
        low_price: str = "N",
    ) -> dict:
        """
        Fetch BSR-ranked competitor product list for a category node (monthly snapshot).

        POST /v3/api/competing-lookup

        Args:
            market: Marketplace code, e.g. "US".
            month_name: BSR snapshot table name, e.g. "bsr_sales_monthly_202509".
            node_id_paths: List of colon-joined category node paths,
                           e.g. ["2972638011:553844:3737901"].
            page: Page number (1-based).
            size: Page size, max 100.
            order: Sort spec dict, e.g. {"field": "bsr_rank", "desc": False}.
                   Defaults to ascending BSR rank when None.
            symbol_flag: Include brand symbol filtering (default True).
            low_price: Low-price filter flag ("Y"/"N").

        Returns:
            Parsed response dict with keys:
              - items (list): Product entries with ASIN, price, rating, reviews,
                              monthly sales trends (``trends`` list of {dk, sales}).
              - total (int): Total matching products.
              - page / size: Pagination echo.
        """
        if order is None:
            order = {"field": "bsr_rank", "desc": False}

        url = "https://www.sellersprite.com/v3/api/competing-lookup"
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        }
        payload = {
            "market": market,
            "monthName": month_name,
            "asins": [],
            "page": page,
            "nodeIdPaths": node_id_paths,
            "symbolFlag": symbol_flag,
            "size": size,
            "order": order,
            "lowPrice": low_price,
        }

        logger.info(
            f"[sellersprite] competing-lookup market={market} month={month_name} "
            f"nodes={node_id_paths} page={page}"
        )

        for attempt in range(2):
            res = self._request("POST", url, json=payload, headers=headers)

            if res.status_code != 200:
                logger.error(f"[sellersprite] competing-lookup failed {res.status_code}: {res.text}")
                return {"items": [], "total": 0, "page": page, "size": size}

            body = res.json()
            if not isinstance(body, dict):
                logger.error(f"[sellersprite] competing-lookup unexpected body type={type(body).__name__}: {body!r:.200}")
                return {"items": [], "total": 0, "page": page, "size": size}

            data = body.get("data") or {}
            if isinstance(data, dict):
                return {
                    "items": data.get("items") or [],
                    "total": data.get("total") or 0,
                    "page": data.get("page") or page,
                    "size": data.get("size") or size,
                }

            # Soft-401: server returned 200 but data is a login redirect URL
            if isinstance(data, str) and "/user/login" in data:
                if attempt == 0:
                    logger.warning(
                        "[sellersprite] competing-lookup soft-401 (login redirect) — "
                        "re-logging in and retrying"
                    )
                    self._safe_relogin()
                    continue

            logger.error(
                f"[sellersprite] competing-lookup data field is not a dict "
                f"(type={type(data).__name__}): {data!r:.200}"
            )
            return {"items": [], "total": 0, "page": page, "size": size}

        return {"items": [], "total": 0, "page": page, "size": size}

    def resolve_node_path(
        self,
        market_id: int,
        table: str,
        query: str,
    ) -> list[dict]:
        """
        Search BSR category nodes by label using the ``nodeLabelPath`` parameter.

        ``query`` can be:
          - A bare numeric node ID (e.g. ``"8297518011"`` from an Amazon BSR URL)
            → typically returns a single exact match.
          - A category name keyword (e.g. ``"Traps"``)
            → returns all nodes whose label contains the keyword, ordered by
            product count.  The caller should present the list to the user for
            selection.

        Each item in the returned list contains:
          ``id``                  full colon-joined nodeIdPath (input for competing_lookup)
          ``label``               full English breadcrumb path
          ``nodeLabelLocale``     Chinese label of the leaf node
          ``nodeLabelPathLocale`` full Chinese breadcrumb path
          ``products``            number of ranked products in this node

        GET /v2/competitor-lookup/nodes?marketId=&table=&nodeLabelPath=
        """
        url = "https://www.sellersprite.com/v2/competitor-lookup/nodes"
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        }
        params = {"marketId": market_id, "table": table, "nodeLabelPath": query}

        logger.info(f"[sellersprite] resolve-node marketId={market_id} nodeLabelPath={query!r} table={table}")

        for attempt in range(2):
            res = self._request("GET", url, params=params, headers=headers)

            if res.status_code != 200:
                logger.error(f"[sellersprite] resolve-node failed {res.status_code}: {res.text}")
                return []

            body = res.json()
            if not isinstance(body, dict):
                logger.error(f"[sellersprite] resolve-node unexpected body type={type(body).__name__}: {body!r:.200}")
                return []

            # Soft-401: data field contains login redirect URL
            data_field = body.get("data")
            if isinstance(data_field, str) and "/user/login" in data_field:
                if attempt == 0:
                    logger.warning("[sellersprite] resolve-node soft-401 — re-logging in and retrying")
                    self._safe_relogin()
                    continue
                logger.error("[sellersprite] resolve-node still getting soft-401 after re-login")
                return []

            items = body.get("items") or []
            if not items:
                logger.warning(f"[sellersprite] resolve_node_path: no match for query={query!r} in table={table}")
            return items

        return []

    def get_category_nodes(
        self,
        market_id: int,
        table: str,
        node_id_path: str,
    ) -> list[dict]:
        """
        Fetch child category nodes for a given node path.

        GET /v2/competitor-lookup/nodes

        Args:
            market_id: Numeric market identifier (e.g. 1 for US).
            table: BSR snapshot table name, e.g. "bsr_sales_monthly_202509".
            node_id_path: Colon-joined ancestor path of the target node,
                          e.g. "2972638011".

        Returns:
            List of node dicts, each with at least ``nodeId``, ``nodeName``,
            ``nodeIdPath``, and ``hasChildren`` fields.
        """
        url = "https://www.sellersprite.com/v2/competitor-lookup/nodes"
        headers = {
            "Host": "www.sellersprite.com",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        }
        params = {
            "marketId": market_id,
            "table": table,
            "nodeIdPath": node_id_path,
        }

        logger.info(
            f"[sellersprite] category-nodes marketId={market_id} table={table} "
            f"nodeIdPath={node_id_path}"
        )
        for attempt in range(2):
            res = self._request("GET", url, params=params, headers=headers)

            if res.status_code != 200:
                logger.error(f"[sellersprite] category-nodes failed {res.status_code}: {res.text}")
                return []

            body = res.json()
            if not isinstance(body, dict):
                logger.error(f"[sellersprite] category-nodes unexpected body type={type(body).__name__}: {body!r:.200}")
                return []

            # Soft-401: data field contains login redirect URL
            data_field = body.get("data")
            if isinstance(data_field, str) and "/user/login" in data_field:
                if attempt == 0:
                    logger.warning("[sellersprite] category-nodes soft-401 — re-logging in and retrying")
                    self._safe_relogin()
                    continue
                logger.error("[sellersprite] category-nodes still getting soft-401 after re-login")
                return []

            return body.get("items") or data_field or []

        return []
