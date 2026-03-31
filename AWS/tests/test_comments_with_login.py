
import asyncio
import logging
import sys
import os
import re
from curl_cffi import requests

# Add src to path
sys.path.append(os.getcwd())

from src.core.utils.cookie_helper import AmazonCookieHelper
from src.mcp.servers.amazon.extractors.comments import CommentsExtractor

async def test_tokens_with_login():
    logging.basicConfig(level=logging.INFO)
    cookie_helper = AmazonCookieHelper()
    
    # Force manual login to capture cookies
    print("🔑 TRIGER MANUAL LOGIN FLOW...")
    print("A browser window should open. Please log in to Amazon.")
    
    # fetch_fresh_cookies(wait_for_manual=True) triggers the login detection loop
    cookies_dict = cookie_helper.fetch_fresh_cookies(wait_for_manual=True)
    
    if not cookies_dict or 'session-id' not in cookies_dict:
        print("❌ Login failed or timed out. Could not capture cookies.")
        return

    print(f"✅ Successfully captured {len(cookies_dict)} cookies.")
    
    # Initialize the extractor which will use the newly saved cookies.json
    extractor = CommentsExtractor()
    asin = "B0DLN8GNXH"
    
    print(f"\n=== Testing CommentsExtractor (LOGGED IN) for ASIN: {asin} ===\n")
    
    # Test token acquisition using the extractor's logic
    csrf, next_token = await extractor._acquire_tokens(asin)
    
    print(f"Result: CSRF Acquired = {csrf is not None}")
    if csrf:
        print(f"CSRF Token: {csrf[:20]}...")
    print(f"Result: Initial NextPageToken = {next_token}")
    if next_token:
        print(f"NextPageToken: {next_token[:20]}...")
    
    # Test first page AJAX fetch
    if csrf:
        print("\nAttempting AJAX Fetch for Page 1...")
        reviews, new_token = await extractor._fetch_reviews_via_ajax(asin, 1, csrf, next_token)
        if reviews is not None:
            print(f"Success: Fetched {len(reviews)} reviews via AJAX.")
            print(f"Next Page Token for P2: {new_token}")
        else:
            print("❌ AJAX Fetch failed even with logged-in session.")
    else:
        print("❌ Token acquisition failed. AJAX Fetch skipped.")

if __name__ == "__main__":
    asyncio.run(test_tokens_with_login())
