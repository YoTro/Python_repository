"""AmazonIdentityStrategy — domain policy for Amazon.com scraping identities."""

from __future__ import annotations

from src.core.identity.strategy import BaseIdentityStrategy
from src.core.utils.cookie_helper import AMAZON_UA

# Markers present in Amazon CAPTCHA / auth challenge pages.
_HARD_BLOCK_MARKERS = (
    "validateCaptcha",
    "auth-page-heading",
    "Type the characters you see in this image",
    "Enter the characters you see below",
)


class AmazonIdentityStrategy(BaseIdentityStrategy):
    """
    Policy for Amazon.com browser identity slots.

    warmup_url   — amazon.com homepage (seeds CloudFront WAF cookies)
    cookie_domain — ``.amazon.com`` (applies cookies to all sub-domains)
    user_agent   — Chrome 130 UA matching the curl_cffi impersonation profile
    is_hard_block — detects CAPTCHA / identity challenge pages
    """

    def warmup_url(self) -> str:
        return "https://www.amazon.com/"

    def cookie_domain(self) -> str:
        return ".amazon.com"

    def user_agent(self) -> str:
        return AMAZON_UA

    def is_hard_block(self, html: str) -> bool:
        return any(marker in html for marker in _HARD_BLOCK_MARKERS)
