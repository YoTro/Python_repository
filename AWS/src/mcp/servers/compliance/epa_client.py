import requests
import logging
import urllib.parse
from typing import Dict, Any, List

logger = logging.getLogger("epa-client")

class EPAClient:
    """
    Client for EPA Pesticide Product Label System (PPLS) API.
    Reference: https://www.epa.gov/pesticide-labels/pesticide-product-label-system-ppls-application-program-interface-api
    """
    # Active examples in documentation use the 'cswu' path
    BASE_URL = "https://ordspub.epa.gov/ords/pesticides/cswu"

    def search_by_name(self, name: str, partial: bool = True) -> Dict[str, Any]:
        """Search pesticide products by name."""
        encoded_name = urllib.parse.quote(name)
        if partial:
            # Partial Match (v2)
            url = f"{self.BASE_URL}/ProductSearch/partialprodsearch/v2/riname/{encoded_name}"
        else:
            # Exact Match
            url = f"{self.BASE_URL}/pplstxt/{encoded_name}"
        return self._get(url)

    def search_by_registration_number(self, reg_num: str, partial: bool = True) -> Dict[str, Any]:
        """Search pesticide products by EPA Registration Number."""
        encoded_num = urllib.parse.quote(reg_num)
        if partial:
            # Partial Match (v2)
            url = f"{self.BASE_URL}/ProductSearch/partialprodsearch/v2/regnum/{encoded_num}"
        else:
            # Exact Match (format: Company Number-Product Number)
            url = f"{self.BASE_URL}/ppls/{encoded_num}"
        return self._get(url)

    def _get(self, url: str) -> Dict[str, Any]:
        try:
            logger.info(f"EPA PPLS API Request: {url}")
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"items": [], "message": "No results found"}
            else:
                logger.error(f"EPA API error {response.status_code}: {response.text}")
                return {"error": f"API returned status {response.status_code}"}
        except Exception as e:
            logger.exception("Failed to connect to EPA PPLS API")
            return {"error": str(e)}
