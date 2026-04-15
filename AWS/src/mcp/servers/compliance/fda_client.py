import requests
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("fda-client")

class FDAClient:
    """
    Client for OpenFDA API.
    Reference: https://open.fda.gov/apis/
    """
    BASE_URL = "https://api.fda.gov"

    def search_device(self, keyword: str, limit: int = 5) -> Dict[str, Any]:
        """Search Medical Device Registration and Listing."""
        # Search in device name or manufacturer name
        query = f'device_name:"{keyword}"+registration.name:"{keyword}"'
        url = f"{self.BASE_URL}/device/registrationlisting.json?search={query}&limit={limit}"
        return self._get(url)

    def search_drug(self, keyword: str, limit: int = 5) -> Dict[str, Any]:
        """Search Drug NDC (National Drug Code) Directory."""
        # Search in brand name or generic name
        query = f'brand_name:"{keyword}"+generic_name:"{keyword}"'
        url = f"{self.BASE_URL}/drug/ndc.json?search={query}&limit={limit}"
        return self._get(url)

    def search_food_recall(self, keyword: str, limit: int = 5) -> Dict[str, Any]:
        """Search Food Enforcement Reports (Recalls)."""
        query = f'product_description:"{keyword}"'
        url = f"{self.BASE_URL}/food/enforcement.json?search={query}&limit={limit}"
        return self._get(url)

    def _get(self, url: str) -> Dict[str, Any]:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"results": [], "message": "No results found"}
            else:
                logger.error(f"FDA API error {response.status_code}: {response.text}")
                return {"error": f"API returned status {response.status_code}"}
        except Exception as e:
            logger.exception("Failed to connect to OpenFDA")
            return {"error": str(e)}
