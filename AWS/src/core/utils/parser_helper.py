from __future__ import annotations
import re
from typing import Optional

def parse_price(price_text: Optional[str]) -> Optional[float]:
    """Clean and parse price string to float."""
    if not price_text:
        return None
    # Remove currency symbols, commas, etc.
    match = re.search(r'([\d,]+\.?\d*)', price_text.replace(',', ''))
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def parse_rating(rating_text: Optional[str]) -> Optional[float]:
    """Clean and parse rating string (e.g., '4.5 out of 5 stars') to float."""
    if not rating_text:
        return None
    match = re.search(r'([\d.]+)', rating_text)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def parse_integer(text: Optional[str]) -> Optional[int]:
    """Extract integer from text (e.g., '1,234 ratings' -> 1234)."""
    if not text:
        return None
    match = re.search(r'([\d,]+)', text.replace(',', ''))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None
