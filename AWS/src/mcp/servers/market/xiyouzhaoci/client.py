from __future__ import annotations
import logging
from typing import List
from curl_cffi import requests
from datetime import datetime
from urllib.parse import quote
import json
import os
import time
from .auth import XiyouZhaociAuth

logger = logging.getLogger(__name__)


class XiyouAuthRequiredError(Exception):
    """Exception raised when Xiyouzhaoci token is missing or expired, and SMS auth is needed."""
    def __init__(self, message="Xiyouzhaoci token expired or invalid. Re-authentication required."):
        self.message = message
        super().__init__(self.message)

class XiyouZhaociAPI:
    """
    API client for Xiyou Zhaoci (西柚找词).
    Supports ASIN reverse-lookup and keyword (search term) analysis.
    """

    # Calculate project root relative to this file
    _PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
    
    # Default paths based on project root
    _DEFAULT_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")

    def __init__(self, tenant_id: str = "default", token_file: str = None):
        self.tenant_id = tenant_id
        
        # Identity-based token isolation
        config_dir = os.path.join(self._PROJECT_ROOT, "config", "auth")
        os.makedirs(config_dir, exist_ok=True)
        
        self.token_file = token_file or os.path.join(config_dir, f"xiyou_{tenant_id}_token.json")
        self.session = requests.Session(impersonate="chrome")
        self.base_url = "https://api.xiyouzhaoci.com"
        self.auth_token = self._load_token()

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
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        }
        if self.auth_token:
            self.common_headers["authorization"] = self.auth_token

        self.auth = XiyouZhaociAuth(tenant_id=tenant_id, token_file=self.token_file)

    def _load_token(self) -> str:
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("token", "")
            except Exception as e:
                logger.error(f"Failed to load token from {self.token_file}: {e}")
        return ""

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Make an HTTP request and handle 401 Unauthorized by reloading the token.
        """
        response = self.session.request(method, url, **kwargs)
        
        if response.status_code == 401:
            logger.warning("Received 401 Unauthorized. Attempting to reload token...")
            new_token = self._load_token()
            if new_token and new_token != self.auth_token:
                self.auth_token = new_token
                self.common_headers["authorization"] = self.auth_token
                
                # Update the headers for the retry
                if "headers" in kwargs:
                    kwargs["headers"] = kwargs["headers"].copy()
                    kwargs["headers"]["authorization"] = self.auth_token
                
                logger.info("Retrying request with reloaded token.")
                response = self.session.request(method, url, **kwargs)
            else:
                logger.error("401 Unauthorized: Token is missing or invalid. Please re-authenticate.")
                raise XiyouAuthRequiredError("Xiyouzhaoci token expired or invalid. Re-authentication required.")
        
        return response

    @property
    def needs_auth(self) -> bool:
        return not self.auth_token

    def request_sms_code(self, phone_num: str = None) -> bool:
        """Send SMS verification code. Returns True if sent."""
        return self.auth.send_sms_code(phone_num)

    def verify_sms_code(self, sms_code: str, phone_num: str = None) -> bool:
        """Verify SMS code and save token. Returns True on success."""
        if self.auth.login_with_sms(sms_code, phone_num):
            self.auth_token = self._load_token()
            if self.auth_token:
                self.common_headers["authorization"] = self.auth_token
            return True
        return False

    def get_login_qr(self) -> dict:
        """Initiate WeChat QR code login."""
        return self.auth.get_wechat_qr()

    def check_qr_login_status(self) -> dict:
        """Check the status of a pending QR code login."""
        res = self.auth.check_wechat_login()
        # If login is successful, refresh the client's own token
        if res.get("status") == "SUCCESS":
            self.auth_token = self._load_token()
            if self.auth_token:
                self.common_headers["authorization"] = self.auth_token
        return res


    @staticmethod
    def _krs_ver() -> str:
        """Generate a dynamic krs-ver timestamp header value."""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Shared: poll + download ──────────────────────────────────────────

    def _poll_and_download(
        self,
        resource_id: str,
        status_url: str,
        status_payload: dict,
        request_url_header: str,
        output_path: str,
        max_retries: int = 30,
        poll_interval: int = 2,
    ) -> str:
        """
        Poll resource status until Done, then download the xlsx.
        Returns the local file path on success, empty string on failure.
        """
        headers = self.common_headers.copy()
        headers["request-url"] = request_url_header
        headers["krs-ver"] = self._krs_ver()

        for i in range(max_retries):
            try:
                response = self._request("POST", status_url, headers=headers, json=status_payload)
                response.raise_for_status()
                status_res = response.json()
            except Exception as e:
                logger.error(f"Error polling resource status: {e}")
                time.sleep(poll_interval)
                continue

            data = status_res.get("data", status_res)
            status = data.get("status")

            if status == "Done":
                resource_url = data.get("resourceUrl")
                if resource_url:
                    if self._download_file(resource_url, output_path):
                        return output_path
                logger.error(f"Status is Done but no resourceUrl found: {status_res}")
                return ""
            elif status == "Failed":
                logger.error(f"Resource generation failed: {status_res}")
                return ""

            logger.info(f"Status: {status}, waiting {poll_interval}s... ({i + 1}/{max_retries})")
            time.sleep(poll_interval)

        logger.error(f"Timeout waiting for resource {resource_id}")
        return ""

    def _download_file(self, resource_url: str, output_path: str) -> bool:
        """Download a file from OSS to local disk."""
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "upgrade-insecure-requests": "1",
        }
        logger.info(f"Downloading {resource_url[:80]}... → {output_path}")
        try:
            # Download doesn't need _request with 401 handling usually
            response = self.session.get(resource_url, headers=headers)
            response.raise_for_status()
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return False

    # ── ASIN reverse-lookup ──────────────────────────────────────────────

    def lookup_asin(self, country: str, asin: str) -> dict:
        """Look up keyword data for a specific ASIN using the v3 research API."""
        url = f"{self.base_url}/v3/asins/research/list/resource"
        payload = {
            "resource": {"country": country, "asin": asin},
            "biz": {
                "asin": asin,
                "country": country,
                "page": 1,
                "pageSize": 50,
                "query": "",
                "orders": [{"field": "follow", "order": "desc"}],
                "filters": [{"field": "asinResearchType", "filter": ["all"]}],
                "rangeFilters": [],
                "cycleFilter": {
                    "cycle": "daily",
                    "period": "last7days",
                    "startCycle": {"startDate": "", "endDate": ""},
                    "endCycle": {"startDate": "", "endDate": ""},
                },
                "tableType": "asinResearchTotalList",
            },
        }
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/asin/look_up/{country}/{asin}"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Looking up ASIN {asin} in {country}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error looking up ASIN {asin}: {e}")
            return {}

    def export_asin_data(self, country: str, asin: str, output_dir: str = None) -> str:
        """
        Full ASIN export flow: lookup → poll → download xlsx.
        Returns path to downloaded file, or empty string on failure.
        """
        if output_dir is None:
            output_dir = self._DEFAULT_DATA_DIR
        elif not os.path.isabs(output_dir):
            output_dir = os.path.join(self._DEFAULT_DATA_DIR, output_dir)
        
        output_dir = os.path.normpath(output_dir)

        lookup_result = self.lookup_asin(country, asin)
        if not lookup_result:
            return ""

        resource_id = (
            lookup_result.get("data", {}).get("resourceId")
            or lookup_result.get("resourceId")
        )
        if not resource_id:
            logger.error(f"No resourceId in response for ASIN {asin}: {lookup_result}")
            return ""

        logger.info(f"Got resourceId {resource_id} for ASIN {asin}, polling...")

        return self._poll_and_download(
            resource_id=resource_id,
            status_url=f"{self.base_url}/v4/resource/status",
            status_payload={
                "resource": {"country": country, "asin": asin},
                "resourceId": str(resource_id),
            },
            request_url_header=f"/detail/asin/look_up/{country}/{asin}",
            output_path=os.path.join(output_dir, f"{country}_{asin}_{resource_id}.xlsx"),
        )

    # ── Keyword (search term) analysis ───────────────────────────────────

    def analyze_keyword(self, country: str, keyword: str) -> dict:
        """
        Request keyword analysis resource.
        Returns the raw API response containing a resourceId.
        """
        url = f"{self.base_url}/v3/searchTerms/analysis/list/resource"
        encoded_keyword = quote(keyword, safe="")
        payload = {
            "resource": {"country": country, "searchTerm": keyword},
            "biz": {
                "query": "",
                "keyword": keyword,
                "searchTerm": keyword,
                "country": country,
                "page": 1,
                "pageSize": 50,
                "orders": [{"field": "traffic", "order": "desc"}],
                "filters": [],
                "rangeFilters": [],
                "cycleFilter": {
                    "cycle": "daily",
                    "period": "last7days",
                    "startCycle": {"startDate": "", "endDate": ""},
                    "endCycle": {"startDate": "", "endDate": ""},
                },
            },
        }
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/search_term/look_up/{country}/{encoded_keyword}"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Analyzing keyword '{keyword}' in {country}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error analyzing keyword '{keyword}': {e}")
            return {}

    def check_keyword_status(self, country: str, keyword: str, resource_id: str) -> dict:
        """Poll the status of a keyword analysis resource."""
        url = f"{self.base_url}/v3/resource/status"
        encoded_keyword = quote(keyword, safe="")
        payload = {
            "resource": {"country": country, "searchTerm": keyword},
            "biz": {"resourceId": str(resource_id)},
        }
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/search_term/look_up/{country}/{encoded_keyword}"
        headers["krs-ver"] = self._krs_ver()

        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error checking keyword status for '{keyword}': {e}")
            return {}

    def export_keyword_data(self, country: str, keyword: str, output_dir: str = None) -> str:
        """
        Full keyword analysis flow: analyze → poll → download xlsx.
        Returns path to downloaded file, or empty string on failure.
        """
        if output_dir is None:
            output_dir = self._DEFAULT_DATA_DIR
        elif not os.path.isabs(output_dir):
            output_dir = os.path.join(self._DEFAULT_DATA_DIR, output_dir)
        
        output_dir = os.path.normpath(output_dir)

        analyze_result = self.analyze_keyword(country, keyword)
        if not analyze_result:
            return ""

        resource_id = (
            analyze_result.get("data", {}).get("resourceId")
            or analyze_result.get("resourceId")
        )
        if not resource_id:
            logger.error(f"No resourceId for keyword '{keyword}': {analyze_result}")
            return ""

        logger.info(f"Got resourceId {resource_id} for keyword '{keyword}', polling...")

        encoded_keyword = quote(keyword, safe="")
        return self._poll_and_download(
            resource_id=resource_id,
            status_url=f"{self.base_url}/v3/resource/status",
            status_payload={
                "resource": {"country": country, "searchTerm": keyword},
                "biz": {"resourceId": str(resource_id)},
            },
            request_url_header=f"/detail/search_term/look_up/{country}/{encoded_keyword}",
            output_path=os.path.join(
                output_dir, f"{country}_{keyword.replace(' ', '_')}_{resource_id}.xlsx"
            ),
        )

    # ── Multi-ASIN Comparison (v4) ──────────────────────────────────────

    def compare_asins(self, country: str, asins: List[str], period: str = "last7days") -> dict:
        """
        Compare multiple ASINs (max 20) for common keywords.
        Period options: 'last7days', 'last30days', etc.
        """
        if len(asins) > 20:
            logger.warning(f"Comparing {len(asins)} ASINs, but max supported is 20. Truncating.")
            asins = asins[:20]

        url = f"{self.base_url}/v4/asins/compare/list/resource"
        asins_str = ",".join(asins)
        
        payload = {
            "resource": {"country": country, "asins": asins},
            "asins": asins,
            "country": country,
            "query": "",
            "page": 1,
            "pageSize": 50,
            "orders": [{"field": "follow", "order": "desc", "value": ""}],
            "filters": [],
            "rangeFilters": [],
            "cycleFilter": {
                "cycle": "daily",
                "period": period,
                "startCycle": {"startDate": "", "endDate": ""},
                "endCycle": {"startDate": "", "endDate": ""}
            },
            "tableType": "multiAsinsComparisonList"
        }
        
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/asin_compare/look_up/{country}/{asins_str}"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Comparing {len(asins)} ASINs in {country} for period {period}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error initiating ASIN comparison: {e}")
            return {}

    def export_compare_data(self, country: str, asins: List[str], period: str = "last7days", output_dir: str = None) -> str:
        """
        Full Multi-ASIN comparison export flow: compare → poll → download xlsx.
        Returns path to downloaded file, or empty string on failure.
        """
        if output_dir is None:
            output_dir = self._DEFAULT_DATA_DIR
        elif not os.path.isabs(output_dir):
            output_dir = os.path.join(self._DEFAULT_DATA_DIR, output_dir)
        
        output_dir = os.path.normpath(output_dir)

        compare_result = self.compare_asins(country, asins, period)
        if not compare_result:
            return ""

        resource_id = (
            compare_result.get("data", {}).get("resourceId")
            or compare_result.get("resourceId")
        )
        if not resource_id:
            logger.error(f"No resourceId in comparison response: {compare_result}")
            return ""

        logger.info(f"Got resourceId {resource_id} for ASIN comparison, polling...")

        asins_str = ",".join(asins)
        return self._poll_and_download(
            resource_id=resource_id,
            status_url=f"{self.base_url}/v4/resource/status",
            status_payload={
                "resource": {"country": country, "asins": asins},
                "resourceId": str(resource_id),
            },
            request_url_header=f"/detail/asin_compare/look_up/{country}/{asins_str}",
            output_path=os.path.join(output_dir, f"{country}_compare_{asins[0]}_{resource_id}.xlsx"),
        )

    # ── ABA Ranking Data ────────────────────────────────────────────────

    def get_aba_top_asins(self, country: str, search_terms: List[str]) -> dict:
        """
        Query top ASINs for the given search terms based on ABA ranking data.
        """
        url = f"{self.base_url}/v2/searchTerms/topAsins"
        
        terms_payload = [{"country": country, "searchTerm": term} for term in search_terms]
        
        payload = {
            "biz": {
                "searchTerms": terms_payload
            }
        }
        
        headers = self.common_headers.copy()
        headers["request-url"] = "/detail/ranking_list"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Querying ABA top ASINs for {len(search_terms)} search terms in {country}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except XiyouAuthRequiredError:
            raise
        except Exception as e:
            logger.error(f"Error querying ABA top ASINs: {e}")
            return {}

    def get_search_terms_ranking(self, country: str, query: str, page: int = 1, page_size: int = 100, field: str = "week", rank_pattern: str = "aba") -> dict:
        """
        Query ranking list for search terms based on a query string.
        """
        url = f"{self.base_url}/v3/rankingList/searchTerms"
        
        payload = {
            "biz": {
                "country": country,
                "filed": field,  # Note: The API misspells 'field' as 'filed'
                "page": page,
                "pageSize": page_size,
                "rankPattern": rank_pattern,
                "query": query
            }
        }
        
        headers = self.common_headers.copy()
        headers["request-url"] = "/detail/ranking_list"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Querying search terms ranking for query '{query}' in {country}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error querying search terms ranking for '{query}': {e}")
            return {}

    def get_traffic_scores(self, country: str, asins: List[str]) -> dict:
        """
        Fetch 7-day traffic scores for a list of ASINs.
        Useful fields: advertisingTrafficScoreRatio (Ad dependency), totalTrafficScoreGrowthRate (Growth).
        """
        url = f"{self.base_url}/v4/asins/trafficScore"
        
        payload = {
            "asins": asins,
            "country": country
        }
        
        headers = self.common_headers.copy()
        # Use the first ASIN for the request-url header to match Xiyou's behavior
        first_asin = asins[0] if asins else "unknown"
        headers["request-url"] = f"/detail/asin/look_up/{country}/{first_asin}"
        headers["krs-ver"] = self._krs_ver()

        logger.info(f"Querying traffic scores for {len(asins)} ASINs in {country}")
        try:
            response = self._request("POST", url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error querying traffic scores: {e}")
            return {}


if __name__ == "__main__":
    api = XiyouZhaociAPI()
    if not api.auth_token:
        print("No token found. Run auth.py to authenticate first.")
        raise SystemExit(1)

    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "keyword"

    if mode == "asin":
        path = api.export_asin_data("US", sys.argv[2] if len(sys.argv) > 2 else "B0BSYD2VV6")
    elif mode == "compare":
        asins = sys.argv[2].split(",") if len(sys.argv) > 2 else ["B08X4615SC", "B07BJN11KV"]
        path = api.export_compare_data("US", asins)
    elif mode == "traffic":
        asins = sys.argv[2].split(",") if len(sys.argv) > 2 else ["B07T869RNY", "B0CKY689WQ"]
        res = api.get_traffic_scores("US", asins)
        print(json.dumps(res, indent=2, ensure_ascii=False))
        sys.exit(0)
    else:
        path = api.export_keyword_data("US", sys.argv[2] if len(sys.argv) > 2 else "iphone case")

    if path:
        print(f"Export successful: {path}")
    else:
        print("Export failed.")
