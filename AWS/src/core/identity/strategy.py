"""Abstract base for per-domain identity strategies used by IdentityPool."""

from __future__ import annotations

from abc import ABC, abstractmethod


class BaseIdentityStrategy(ABC):
    """
    Policy interface for one web domain's identity requirements.

    IdentityPool is domain-agnostic; it delegates all domain-specific
    decisions to a strategy instance injected at init time.

    Implementations MUST NOT import browser or HTTP-client types — those
    belong in the pool mechanism layer, not here.
    """

    @abstractmethod
    def warmup_url(self) -> str:
        """URL to load on browser init to seed WAF / session cookies."""

    @abstractmethod
    def cookie_domain(self) -> str:
        """Domain string passed when injecting cookies into the browser."""

    @abstractmethod
    def user_agent(self) -> str:
        """Default User-Agent for slots that don't specify one in their entry dict."""

    @abstractmethod
    def is_hard_block(self, html: str) -> bool:
        """Return True if *html* indicates an unrecoverable identity block."""
