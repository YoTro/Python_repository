from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ERPClient(ABC):
    """
    Abstract interface for ERP system clients.

    Concrete implementations (e.g. LingxingClient) live in provider-specific
    sub-packages under erp/<provider>/ and are registered via erp.registry.

    All methods return plain dicts so callers are decoupled from provider schemas.
    """

    @abstractmethod
    def get_inventory(self, sku: str) -> Dict[str, Any]:
        """
        Return inventory snapshot for a single SKU.

        Expected keys (provider fills what it has, omits the rest):
          sku, available_qty, total_qty, pending_orders,
          warehouse_location, last_updated
        """

    def get_purchase_orders(
        self,
        sku: Optional[str] = None,
        status: Optional[str] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Purchase orders (inbound shipments). Override if supported."""
        raise NotImplementedError(f"{type(self).__name__} does not implement get_purchase_orders")

    def get_sales_orders(
        self,
        sku: Optional[str] = None,
        days: int = 30,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Sales orders for a SKU over the past N days. Override if supported."""
        raise NotImplementedError(f"{type(self).__name__} does not implement get_sales_orders")
