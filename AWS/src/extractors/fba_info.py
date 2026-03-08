import logging
import json
import time
import random
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class FBAInfoExtractor(AmazonBaseScraper):
    """
    Extractor to fetch detailed FBA (Fulfillment by Amazon) info like weight and dimensions
    from the Amazon Seller Central public profitability calculator API.
    """

    def get_fba_dimensions_and_weight(self, asin: str) -> dict:
        """
        Fetch FBA specific product info (weight, length, width, height) using the public API.
        
        :param asin: The product ASIN.
        :return: A dictionary with dimensions and weight data.
        """
        # Amazon Seller Central public profitability calculator endpoint
        url = f"https://sellercentral.amazon.com/rcpublic/getadditionalpronductinfo?countryCode=US&asin={asin}&fnsku=&searchType=GENERAL&locale=en-US"
        
        headers = {
            "authority": "sellercentral.amazon.com",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://sellercentral.amazon.com/fba/profitabilitycalculator/index?lang=en_US",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self._get_random_ua()
        }

        logger.info(f"Fetching FBA info for ASIN: {asin}")
        
        # We don't use the standard fetch here because we need to handle specific JSON structure 
        # and different headers/logic.
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                if not data or 'data' not in data:
                    logger.warning(f"No data returned for ASIN {asin} from FBA API.")
                    return {}
                
                prod_info = data['data']
                return {
                    "ASIN": asin,
                    "Weight": prod_info.get('weight', 0),
                    "WeightUnit": prod_info.get('weightUnit', 'pounds'),
                    "Length": prod_info.get('length', 0),
                    "Width": prod_info.get('width', 0),
                    "Height": prod_info.get('height', 0),
                    "DimensionUnit": prod_info.get('dimensionUnit', 'inches')
                }
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to fetch FBA info for {asin}: {e}")
                time.sleep(random.uniform(2, 5))
                
        return {}
