from __future__ import annotations
import asyncio
import logging
from typing import Any, Dict, List, Optional

import requests

from .auth import SPAPIAuth

logger = logging.getLogger(__name__)


class SPAPIClient:
    """
    Amazon Selling Partner API client.

    Covers two domains needed for ad diagnostics:
      - FBA Inventory (available, reserved, inbound quantities)
      - Catalog (basic item metadata fallback)
    """

    def __init__(self, store_id: Optional[str] = None):
        self.auth = SPAPIAuth(store_id)

    # ── internal ───────────────────────────────────────────────────────────

    def _headers(self) -> Dict[str, str]:
        return {
            "x-amz-access-token": self.auth.get_access_token(),
            "x-amz-date": _utc_now(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self.auth.endpoint}{path}"
        try:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.error(f"SP-API GET {path} → HTTP {e.response.status_code}: {e.response.text[:400]}")
            raise
        except Exception as e:
            logger.error(f"SP-API GET {path} failed: {e}")
            raise

    # ── Inventory ──────────────────────────────────────────────────────────

    async def get_inventory(
        self,
        seller_skus: Optional[List[str]] = None,
        include_details: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        FBA Inventory Summaries (v1).

        Returns per-SKU dict with:
          sku, asin, fn_sku, condition, total_quantity, available_quantity,
          reserved_quantity, inbound_quantity, last_updated
        """
        params: Dict[str, Any] = {
            "details": str(include_details).lower(),
            "granularityType": "Marketplace",
            "granularityId": self.auth.marketplace_id,
            "marketplaceIds": self.auth.marketplace_id,
        }
        if seller_skus:
            params["sellerSkus"] = ",".join(seller_skus)

        data = await asyncio.to_thread(
            self._get, "/fba/inventory/v1/summaries", params
        )
        summaries = data.get("payload", {}).get("inventorySummaries", [])
        return [_parse_inventory_summary(s) for s in summaries]

    # ── Catalog ────────────────────────────────────────────────────────────

    async def get_catalog_item(self, asin: str) -> Dict[str, Any]:
        """
        Catalog Items API 2022-04-01.

        Returns: asin, title, brand, product_type, color, size, bullet_point_count
        """
        params = {
            "marketplaceIds": self.auth.marketplace_id,
            "includedData": "attributes,summaries,identifiers",
        }
        data = await asyncio.to_thread(
            self._get, f"/catalog/2022-04-01/items/{asin}", params
        )
        return _parse_catalog_item(asin, data)


# ── parsers ────────────────────────────────────────────────────────────────

def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_inventory_summary(s: Dict) -> Dict[str, Any]:
    inv_details = s.get("inventoryDetails", {})
    fulfillable = inv_details.get("fulfillableQuantity", 0)
    reserved = inv_details.get("reservedQuantity", {})
    inbound_receiving = inv_details.get("inboundReceivingQuantity", 0)
    inbound_shipped = inv_details.get("inboundShippedQuantity", 0)
    inbound_working = inv_details.get("inboundWorkingQuantity", 0)
    return {
        "sku": s.get("sellerSku"),
        "asin": s.get("asin"),
        "fn_sku": s.get("fnSku"),
        "condition": s.get("condition"),
        "total_quantity": s.get("totalQuantity", 0),
        "available_quantity": fulfillable,
        "reserved_quantity": reserved.get("totalReservedQuantity", 0) if isinstance(reserved, dict) else reserved,
        "inbound_quantity": inbound_receiving + inbound_shipped + inbound_working,
        "last_updated": s.get("lastUpdatedTime"),
    }


def _parse_catalog_item(asin: str, data: Dict) -> Dict[str, Any]:
    summaries = data.get("summaries", [{}])
    summary = summaries[0] if summaries else {}
    attributes = data.get("attributes", {})
    return {
        "asin": asin,
        "title": summary.get("itemName"),
        "brand": summary.get("brand"),
        "product_type": summary.get("productType"),
        "color": _first_attr(attributes, "color"),
        "size": _first_attr(attributes, "size"),
        "bullet_point_count": len(attributes.get("bullet_point", [])),
    }


def _first_attr(attributes: Dict, key: str) -> Optional[str]:
    vals = attributes.get(key, [])
    if vals and isinstance(vals, list):
        return vals[0].get("value")
    return None
