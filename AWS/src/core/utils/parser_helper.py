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
    """
    Extract integer from text. 
    Handles basic digits ('1,234'), suffixes ('1K+', '10k', '1.5M'), 
    and plus signs ('500+').
    """
    if not text:
        return None
    
    # Remove commas and plus signs, then trim and uppercase
    clean_text = text.replace(',', '').replace('+', '').strip().upper()
    
    # Try to find a number part (including decimal) followed by optional K/M
    match = re.search(r'([\d.]+)\s*([KM]?)', clean_text)
    if not match:
        return None
    
    try:
        num_val = float(match.group(1))
        suffix = match.group(2)
        
        if suffix == 'K':
            return int(num_val * 1000)
        if suffix == 'M':
            return int(num_val * 1000000)
            
        return int(num_val)
    except (ValueError, TypeError):
        return None
