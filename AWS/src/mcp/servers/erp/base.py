from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ERPClient(ABC):
    """
    Abstract interface for ERP system clients.

    Concrete implementations (e.g. LingxingClient) live in provider-specific
    sub-packages under erp/<provider>/ and are registered via erp.registry.

    All methods return plain dicts so callers are decoupled from provider schemas.
    """

    @abstractmethod
    def get_inventory(self, sku: str) -> dict[str, Any]:
        """
        Return inventory snapshot for a single SKU.

        Expected keys (provider fills what it has, omits the rest):
          sku, available_qty, total_qty, pending_orders,
          warehouse_location, last_updated
        """

    def get_purchase_orders(
        self,
        sku: str | None = None,
        status: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Purchase orders (inbound shipments). Override if supported."""
        raise NotImplementedError(f"{type(self).__name__} does not implement get_purchase_orders")

    def get_sales_orders(
        self,
        sku: str | None = None,
        days: int = 30,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Sales orders for a SKU over the past N days. Override if supported."""
        raise NotImplementedError(f"{type(self).__name__} does not implement get_sales_orders")

    def get_fba_shipment_tracking(
        self,
        sku: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        transport_type: str | None = None,
        shipment_status: list[str] | None = None,
        search_field: str = "shipment_id",
        search_value: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Historical FBA shipment records with stage timestamps.

        Each record should contain (provider fills what it has):
          shipment_id, shipment_name, sku, quantity, transport_type,
          domestic_ship_date      — origin dispatch date
          overseas_arrival_date   — overseas warehouse arrival date
          overseas_ship_date      — overseas warehouse departure date
          fba_received_date       — FBA receive-complete date
          status

        search_field valid values (Lingxing):
          shipment_id, destination_fulfillment_center_id,
          product_sku, fnsku, asin, parent_asin, product_name
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement get_fba_shipment_tracking"
        )
