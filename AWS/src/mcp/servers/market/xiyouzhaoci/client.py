from __future__ import annotations
import logging
from curl_cffi import requests
from datetime import datetime
from urllib.parse import quote
import json
import os
import time
from .auth import XiyouZhaociAuth

logger = logging.getLogger(__name__)


class XiyouZhaociAPI:
    """
    API client for Xiyou Zhaoci (西柚找词).
    Supports ASIN reverse-lookup and keyword (search term) analysis.
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

        self.auth = XiyouZhaociAuth(token_file=token_file)

    def _load_token(self) -> str:
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data.get("token", "")
            except Exception as e:
                logger.error(f"Failed to load token from {self.token_file}: {e}")
        return ""

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
                response = self.session.post(status_url, headers=headers, json=status_payload)
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
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error looking up ASIN {asin}: {e}")
            return {}

    def export_asin_data(self, country: str, asin: str, output_dir: str = "data") -> str:
        """
        Full ASIN export flow: lookup → poll → download xlsx.
        Returns path to downloaded file, or empty string on failure.
        """
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
            response = self.session.post(url, headers=headers, json=payload)
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
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error checking keyword status for '{keyword}': {e}")
            return {}

    def export_keyword_data(self, country: str, keyword: str, output_dir: str = "data") -> str:
        """
        Full keyword analysis flow: analyze → poll → download xlsx.
        Returns path to downloaded file, or empty string on failure.
        """
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


if __name__ == "__main__":
    api = XiyouZhaociAPI()
    if not api.auth_token:
        print("No token found. Run auth.py to authenticate first.")
        raise SystemExit(1)

    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "keyword"

    if mode == "asin":
        path = api.export_asin_data("US", sys.argv[2] if len(sys.argv) > 2 else "B0BSYD2VV6")
    else:
        path = api.export_keyword_data("US", sys.argv[2] if len(sys.argv) > 2 else "iphone case")

    if path:
        print(f"Export successful: {path}")
    else:
        print("Export failed.")
