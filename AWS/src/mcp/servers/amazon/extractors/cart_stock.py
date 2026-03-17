from __future__ import annotations
import logging
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import urlencode

from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)


class CartStockExtractor(AmazonBaseScraper):
    """
    Extractor to estimate the remaining stock of an Amazon product by using the 999 Add-to-Cart method.
    Uses API requests (curl_cffi) with dynamic payload extraction to bypass WAF.
    """

    async def get_stock(self, asin: str, host: str = "https://www.amazon.com") -> dict:
        """
        Full workflow to add item to cart, attempt to update quantity to 999,
        read available stock, and remove item from cart.

        :return: A dictionary: {"Stock": int, "StockStatus": str}
        """
        logger.info(f"Starting API 999 cart method for ASIN: {asin}")
        url = f"{host}/dp/{asin}"

        result = {"Stock": -1, "StockStatus": "Unknown"}

        # 1. Fetch product page to get ALL form parameters dynamically
        html = await self.fetch(url)
        if not html:
            return result

        payload = self._get_dynamic_form_parameters(html, asin)
        if not payload.get("offerListingID"):
            logger.warning(f"Product not available for sale or could not find add-to-cart form for {asin}.")
            result["Stock"] = 0
            result["StockStatus"] = "OutOfStock"
            return result

        # 2. Add to cart
        if not await self._add_to_cart(asin, payload, host):
            return result

        # 3. View cart to get tokens for quantity update
        cart_data = await self._get_cart_view(host)
        if not cart_data.get("actionItemID") or not cart_data.get("token"):
            logger.warning(f"Failed to extract cart tokens for {asin}.")
            return result

        # 4. Update quantity to 999 to get actual stock
        update_result = await self._update_quantity(asin, cart_data, host)
        result["Stock"] = update_result["value"]
        result["StockStatus"] = update_result["status"]

        # 5. Cleanup: Delete from cart
        await self._delete_from_cart(asin, result["Stock"], cart_data, host)

        return result

    def _get_dynamic_form_parameters(self, html: str, asin: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        payload = {}

        form = soup.find("form", id="addToCart")
        if form:
            for inp in form.find_all("input"):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    payload[name] = value

        logger.debug(
            f"Extracted payload tokens for {asin}: {[k for k in payload.keys() if 'token' in k or 'session' in k]}"
        )

        # Fallback and common overrides
        if not payload.get("offerListingID"):
            match = re.search(r'name="offerListingID" value="(.*?)"', html)
            if match:
                payload["offerListingID"] = match.group(1)

        if "session-id" not in payload:
            match = re.search(r"session-id=(.*?);", html)
            if match:
                payload["session-id"] = match.group(1)

        payload["rsid"] = payload.get("session-id", "")
        payload["ASIN"] = asin
        payload["quantity"] = "1"
        payload["items[0.base][quantity]"] = "1"
        payload["submit.add-to-cart"] = "Add to Cart"
        payload["pipelineType"] = "Chewbacca"
        payload["referrer"] = "detail"

        return payload

    async def _add_to_cart(self, asin: str, payload: dict, host: str) -> bool:
        url = f"{host}/cart/add-to-cart/ref=dp_start-bbf_1_glance"
        headers = self._get_default_headers()
        headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": host,
                "Referer": f"{host}/dp/{asin}",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        try:
            logger.info(f"Adding {asin} to cart via API...")
            encoded_data = urlencode(payload)
            res = await self.session.post(url, data=encoded_data, headers=headers, timeout=15)
            # Amazon often redirects (302) on success
            if res.status_code in [200, 302]:
                return True
            logger.warning(f"Add to cart returned status: {res.status_code}")
            return False
        except Exception as e:
            logger.warning(f"Failed to add to cart: {e}")
            return False

    async def _get_cart_view(self, host: str) -> dict:
        url = f"{host}/gp/cart/view.html?ref_=sw_gtc"
        headers = self._get_default_headers()

        try:
            logger.info("Viewing cart...")
            res = await self.session.get(url, headers=headers, timeout=15)
            html = res.text

            data = {
                "price": self._regex_first(r'data-price="(.*?)"', html)
                or self._regex_first(r"sc-product-price.*?>(.*?)</span>", html),
                "token": self._regex_first(r'name="anti-csrftoken-a2z" value="(.*?)"', html),
                "actionItemID": self._regex_first(r'data-itemid="(.*?)"', html),
                "encodedOffering": self._regex_first(r'data-encoded-offering="(.*?)"', html),
            }
            if data["price"]:
                data["price"] = re.sub(r"[^\d.]", "", data["price"])
            return data
        except Exception as e:
            logger.warning(f"Failed to view cart: {e}")
            return {}

    async def _update_quantity(self, asin: str, cart_data: dict, host: str) -> dict:
        """
        :return: {"value": int, "status": str}
        """
        action_id = cart_data["actionItemID"]
        url = f"{host}/cart/ref=ox_sc_update_quantity_1|1|999"

        price = cart_data.get("price") or "0"
        active_items = [
            {
                "itemId": f"sc-active-{action_id}",
                "giftable": 1,
                "giftWrapped": 0,
                "quantity": 1,
                "price": float(price) if price else 0,
                "incentivizedCartMessage": "",
                "nestedItemsQuantity": 0,
                "installments": {},
                "showLineLevelRecommender": 0,
            }
        ]

        payload = {
            f"quantity.{action_id}": "999",
            "pageAction": "update-quantity-increment",
            f"submit.update-quantity.{action_id}": "1",
            "displayedSavedItemNum": "0",
            "actionItemID": action_id,
            "actionType": "update-quantity",
            "asin": asin,
            "encodedOffering": cart_data.get("encodedOffering", ""),
            "hasMoreItems": "false",
            "addressId": "",
            "addressZip": "",
            "activeItems": json.dumps(active_items),
            "savedItems": "[]",
        }

        headers = self._get_default_headers()
        headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8;",
                "Origin": host,
                "Referer": f"{host}/cart/ref=ord_cart_shr?app-nav-type=none&dc=df",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Dest": "empty",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-AUI-View": "Desktop",
                "anti-csrftoken-a2z": cart_data.get("token", ""),
            }
        )

        try:
            logger.info(f"Updating quantity to 999 for {asin}...")
            encoded_data = urlencode(payload)
            res = await self.session.post(url, data=encoded_data, headers=headers, timeout=15)

            if res.status_code == 200:
                try:
                    data = res.json()
                    json_str = json.dumps(data)

                    # 1. Limit detection (Priority)
                    if "limit of" in json_str.lower():
                        match = re.search(r"limit of.*?(\d+)", json_str, re.IGNORECASE)
                        if match:
                            stock = int(match.group(1))
                            logger.info(f"Detected Purchase Limit: {stock}")
                            return {"value": stock, "status": "Limit"}

                    # 2. Actual stock detection
                    if "only have" in json_str.lower() or "available" in json_str.lower():
                        match = re.search(r"(?:only have|available).*?(\d+)", json_str, re.IGNORECASE)
                        if match:
                            stock = int(match.group(1))
                            logger.info(f"Detected Actual Stock: {stock}")
                            return {"value": stock, "status": "Actual"}

                    # 3. Fallback to nav-cart quantity
                    if "features" in data and "nav-cart" in data["features"]:
                        qty = int(data["features"]["nav-cart"].get("cartQty", 0))
                        logger.info(f"Cart Qty from features: {qty}")
                        return {"value": qty, "status": "Actual"}
                except Exception as e:
                    logger.warning(f"Error parsing update quantity response: {e}")
                return {"value": 999, "status": "Actual"}
            else:
                logger.warning(f"Update quantity failed with status {res.status_code}")
                return {"value": -1, "status": "Failed"}
        except Exception as e:
            logger.warning(f"Failed to update quantity: {e}")
            return {"value": -1, "status": "Failed"}

    async def _delete_from_cart(self, asin: str, stock: int, cart_data: dict, host: str):
        url = f"{host}/cart/ref=ox_sc_cart_actions_1"
        action_id = cart_data["actionItemID"]
        price = cart_data.get("price") or "0"

        action_payload = [
            {
                "type": "DELETE_START",
                "payload": {
                    "itemId": action_id,
                    "list": "activeItems",
                    "relatedItemIds": [],
                    "isPrimeAsin": "false",
                },
            }
        ]

        active_items = [
            {
                "itemId": f"sc-active-{action_id}",
                "giftable": 1,
                "giftWrapped": 0,
                "quantity": max(stock, 1),
                "price": float(price) if price else 0,
                "incentivizedCartMessage": "",
                "installments": {},
                "isSelected": 1,
            }
        ]

        payload = {
            "submit.cart-actions": "1",
            "pageAction": "cart-actions",
            "actionPayload": json.dumps(action_payload),
            "hasMoreItems": "false",
            "activeItems": json.dumps(active_items),
            "savedItems": "[]",
            "anti-csrftoken-a2z": cart_data.get("token", ""),
        }

        headers = self._get_default_headers()
        headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": host,
                "Referer": f"{host}/cart/ref=ord_cart_shr?app-nav-type=none&dc=df",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }
        )

        try:
            logger.info(f"Deleting {asin} from cart...")
            encoded_data = urlencode(payload)
            await self.session.post(url, data=encoded_data, headers=headers, timeout=15)
        except Exception as e:
            logger.warning(f"Failed to delete from cart: {e}")

    def _regex_first(self, pattern: str, text: str) -> str:
        match = re.search(pattern, text)
        return match.group(1) if match else ""
