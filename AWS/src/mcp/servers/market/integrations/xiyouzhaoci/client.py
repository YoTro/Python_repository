from __future__ import annotations
import logging
from curl_cffi import requests
import json
import os
import time
from .auth import XiyouZhaociAuth

logger = logging.getLogger(__name__)

class XiyouZhaociAPI:
    """
    API client for Xiyou Zhaoci (西柚找词).
    Handles ASIN lookup.
    """

    def __init__(self, token_file: str = "config/xiyouzhaoci_token.json"):
        # Use impersonate="chrome" for better TLS fingerprinting
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
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"
        }
        if self.auth_token:
            self.common_headers["authorization"] = self.auth_token

    def _load_token(self) -> str:
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("token", "")
            except Exception as e:
                logger.error(f"Failed to load token from {self.token_file}: {e}")
        return ""

    def lookup_asin(self, country: str, asin: str) -> dict:
        """
        Look up keyword data for a specific ASIN using the v3 research API.
        """
        url = f"{self.base_url}/v3/asins/research/list/resource"
        
        # Construct the complex payload based on the observed fetch request
        payload = {
            "resource": {
                "country": country,
                "asin": asin
            },
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
                "tableType": "asinResearchTotalList"
            }
        }
        
        # Update headers with specific v3 requirements
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/asin/look_up/{country}/{asin}"
        # Note: krs-ver seems to be a dynamic timestamp or version, 
        # using a static one from the fetch for now as it's likely required.
        headers["krs-ver"] = "2026-03-10 15:13:52" 
        
        logger.info(f"Looking up ASIN {asin} in {country} via research API")
        try:
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error looking up ASIN {asin}: {e}")
            if 'response' in locals():
                logger.error(f"Response status: {response.status_code}, text: {response.text}")
            return {}

    def check_export_status(self, country: str, asin: str, resource_id: str) -> dict:
        """
        Poll the status of the requested resource.
        """
        url = f"{self.base_url}/v4/resource/status"
        payload = {
            "resource": {
                "country": country,
                "asin": asin
            },
            "resourceId": str(resource_id)
        }
        headers = self.common_headers.copy()
        headers["request-url"] = f"/detail/asin/look_up/{country}/{asin}"
        headers["krs-ver"] = "2026-03-10 15:13:52"
        
        try:
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error checking export status for {asin}: {e}")
            if 'response' in locals():
                logger.error(f"Response status: {response.status_code}, text: {response.text}")
            return {}

    def download_excel(self, resource_url: str, output_path: str) -> bool:
        """
        Download the exported excel file from OSS.
        """
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "cross-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1"
        }
        
        logger.info(f"Downloading file from {resource_url[:50]}... to {output_path}")
        try:
            # We use a standard get without special headers since it's an OSS URL
            response = self.session.get(resource_url, headers=headers)
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Successfully downloaded file to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            return False

    def export_asin_data(self, country: str, asin: str, output_dir: str = "data") -> str:
        """
        Lookup ASIN, wait for export generation, and download the resulting excel file.
        Returns the path to the downloaded file, or empty string if failed.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        lookup_result = self.lookup_asin(country, asin)
        if not lookup_result:
            return ""
            
        # Parse the resourceId
        resource_id = lookup_result.get("data", {}).get("resourceId") or lookup_result.get("resourceId")
        if not resource_id:
            logger.error(f"Could not find resourceId in lookup response for {asin}: {lookup_result}")
            return ""
            
        logger.info(f"Obtained resourceId {resource_id} for ASIN {asin}. Polling for completion...")
        
        max_retries = 30
        poll_interval = 2 # seconds
        
        for i in range(max_retries):
            status_res = self.check_export_status(country, asin, resource_id)
            if not status_res:
                time.sleep(poll_interval)
                continue
                
            # The status response might have data wrapped or not
            data = status_res.get("data", status_res)
            status = data.get("status")
            
            if status == "Done":
                resource_url = data.get("resourceUrl")
                if resource_url:
                    output_path = os.path.join(output_dir, f"{country}_{asin}_{resource_id}.xlsx")
                    if self.download_excel(resource_url, output_path):
                        return output_path
                else:
                    logger.error(f"Status is Done but no resourceUrl found: {status_res}")
                return ""
            elif status == "Failed":
                logger.error(f"Export failed for {asin}: {status_res}")
                return ""
            
            logger.info(f"Status is {status}, waiting {poll_interval}s... ({i+1}/{max_retries})")
            time.sleep(poll_interval)
            
        logger.error(f"Timeout waiting for export of ASIN {asin}")
        return ""

if __name__ == "__main__":
    api = XiyouZhaociAPI()
    if api.auth_token:
        # Instead of just lookup, try the full export flow
        file_path = api.export_asin_data("US", "B0BSYD2VV6")
        if file_path:
            print(f"Export successful. File saved at: {file_path}")
        else:
            print("Export failed.")
    else:
        print("No token found. Please run src/integrations/xiyouzhaoci/auth.py to authenticate first.")
