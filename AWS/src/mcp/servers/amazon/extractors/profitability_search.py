from __future__ import annotations

import logging
from typing import Any

from src.core.errors.codes import ErrorCode, classify_http, classify_response_message
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

_PROVIDER = "amazon_profitability"


class ProfitabilitySearchExtractor(AmazonBaseScraper):
    """
    Extractor using Amazon's FBA Profitability Calculator public API.
    This API is designed for sellers to search products for fee estimation.
    It works without cookies or CSRF tokens if headers are properly set.
    """

    async def _get_gl_product_group(self, asin: str) -> str:
        """
        Fetch the GL product group name for an ASIN via the productmatch endpoint.
        The browser always resolves this before calling getfees; it's needed for accurate
        category-specific fee calculation.
        """
        url = (
            f"https://sellercentral.amazon.com/rcpublic/productmatch"
            f"?searchKey={asin}&countryCode=US&locale=en-US"
        )
        headers = {
            "Referer": "https://sellercentral.amazon.com/revcalpublic?lang=en_US",
            "Accept": "application/json",
        }
        try:
            resp = await self.session.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                products = data.get("data", {}).get("otherProducts", {}).get(
                    "products", []
                ) or data.get("data", {}).get("myProducts", {}).get("products", [])
                if products:
                    return products[0].get("gl", "")
        except Exception:
            pass
        return ""

    async def get_fees(self, asin: str, price: float) -> dict[str, Any]:
        """
        Fetch FBA fulfillment fees for a product via Amazon's public fee calculator API.

        Mirrors the browser flow: resolves ``glProductGroupName`` via ``productmatch``
        first, then posts the complete payload to ``getfees``.  The ``Core#0`` program
        result contains a ``fees`` list and a ``totalFee`` field.  Returns the raw
        ``data`` dict, or ``{}`` on any failure.

        :param asin: Product ASIN to calculate fees for.
        :param price: Selling price used for the fee estimate.
        :return: Raw ``data`` payload from the API, or ``{}`` on failure.
        """
        url = "https://sellercentral.amazon.com/rcpublic/getfees?countryCode=US&locale=en-US"

        gl = await self._get_gl_product_group(asin)
        price_str = f"{price:.2f}"

        csrf = self.session.cookies.get("anti-csrftoken-a2z", "")
        headers = {
            "sec-ch-ua-platform": '"macOS"',
            "Referer": "https://sellercentral.amazon.com/revcalpublic?lang=en_US",
            "Accept": "application/json",
            "content-type": "application/json; charset=UTF-8",
            "anti-csrftoken-a2z": csrf,
        }

        payload = {
            "countryCode": "US",
            "itemInfo": {
                "asin": asin,
                "glProductGroupName": gl,
                "packageLength": "0",
                "packageWidth": "0",
                "packageHeight": "0",
                "dimensionUnit": "",
                "packageWeight": "0",
                "weightUnit": "",
                "afnPriceStr": price_str,
                "mfnPriceStr": price_str,
                "mfnShippingPriceStr": "0",
                "currency": "USD",
                "isNewDefined": False,
            },
            "programIdList": ["Core#0", "MFN#1"],
            "programParamMap": {
                "Core#0": {
                    "inboundingFeeParam": {
                        "regionLocationsMap": {"West": 1},
                        "serviceOption": "Premium",
                        "unitToInbound": 1,
                    }
                }
            },
        }

        try:
            logger.info(
                f"Fetching FBA fees for ASIN {asin} (gl={gl or 'unknown'}) at ${price:.2f}..."
            )
            response = await self.session.post(url, json=payload, headers=headers, timeout=20)

            if response.status_code != 200:
                code = classify_http(response.status_code)
                logger.warning(f"FBA fee API [{code}] status {response.status_code} for {asin}")
                return {}

            try:
                data = response.json()
            except Exception:
                logger.error(f"FBA fee API [{ErrorCode.PARSE_ERROR}] malformed JSON for {asin}")
                return {}

            if not data.get("succeed"):
                error_msg = str(data.get("error") or "")
                code = classify_response_message(error_msg, _PROVIDER)
                if code == ErrorCode.UNKNOWN:
                    code = ErrorCode.SERVER_ERROR
                logger.warning(f"FBA fee API [{code}] reported failure for {asin}: {error_msg}")
                return {}

            return data.get("data", {})

        except Exception as e:
            logger.error(f"FBA fee API [{ErrorCode.TIMEOUT}] request failed for {asin}: {e}")
            return {}

    async def search_products(self, keywords: str, page_offset: int = 1) -> list[dict[str, Any]]:
        """
        Search for products and return their full metadata dictionaries.

        The returned dictionaries typically contain the following rich metadata:
        - asin: The product ASIN
        - title: Full product title
        - brandName: The brand of the product
        - price & currency: Current price
        - weight & weightUnit: e.g., 0.2910 pounds
        - length, width, height & dimensionUnit: Physical dimensions
        - salesRank & salesRankContextName: e.g., Rank 1 in "Computer Mice"
        - customerReviewsCount & customerReviewsRating: e.g., 41039 reviews, 4.5 rating
        - imageUrl & thumbStringUrl: Product images
        - feeCategoryString: e.g., "Electronic Accessories"

        :param keywords: Search query keywords.
        :param page_offset: Page offset (1-indexed).
        :return: A list of dictionaries containing detailed product data.
        """
        url = "https://sellercentral.amazon.com/rcpublic/searchproduct?countryCode=US&locale=en-US"

        headers = {
            "sec-ch-ua-platform": '"macOS"',
            "Referer": "https://sellercentral.amazon.com/hz/fba/profitabilitycalculator/index?lang=en_US",
            "Accept": "application/json",
            "content-type": "application/json; charset=UTF-8",
            "anti-csrftoken-a2z": "",  # Explicitly empty as discovered
        }

        payload = {
            "keywords": keywords,
            "countryCode": "US",
            "searchType": "GENERAL",
            "pageOffset": page_offset,
        }

        try:
            logger.info(f"Searching profitability API for '{keywords}' (page {page_offset})...")
            response = await self.session.post(url, json=payload, headers=headers, timeout=20)

            if response.status_code != 200:
                code = classify_http(response.status_code)
                logger.warning(f"Profitability search API [{code}] status {response.status_code}")
                return []

            try:
                data = response.json()
            except Exception:
                logger.error(f"Profitability search API [{ErrorCode.PARSE_ERROR}] malformed JSON")
                return []

            if not data.get("succeed"):
                error_msg = str(data.get("error") or "")
                code = classify_response_message(error_msg, _PROVIDER)
                if code == ErrorCode.UNKNOWN:
                    code = ErrorCode.SERVER_ERROR
                logger.warning(f"Profitability search API [{code}] reported failure: {error_msg}")
                return []

            products = data.get("data", {}).get("products", [])
            logger.info(f"Profitability API found {len(products)} products on page {page_offset}.")
            return products

        except Exception as e:
            logger.error(f"Profitability search API [{ErrorCode.TIMEOUT}] request failed: {e}")
            return []
