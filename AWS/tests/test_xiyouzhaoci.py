import sys
import os
import json
import logging

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.integrations.xiyouzhaoci.auth import XiyouZhaociAuth
from src.integrations.xiyouzhaoci.client import XiyouZhaociAPI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_login_flow():
    """
    Test the manual login flow. 
    Note: Requires manual input of the SMS code.
    """
    print("\n--- Testing Login Flow ---")
    auth = XiyouZhaociAuth()
    phone = "15669012019" # Or pull from config
    
    if auth.send_sms_code(phone):
        code = input(f"SMS code sent to {phone}. Enter it here: ")
        if auth.login_with_sms(code, phone):
            print("Successfully logged in and saved token.")
            return True
    return False

def test_asin_lookup():
    """
    Test fetching data for an ASIN using the saved token.
    """
    print("\n--- Testing ASIN Lookup & Export ---")
    api = XiyouZhaociAPI()
    
    if not api.auth_token:
        print("No auth token found in config/xiyouzhaoci_token.json.")
        print("Please run the login flow test first.")
        return False
    
    country = "US"
    asin = "B0FXFGMD7Z"
    
    result = api.lookup_asin(country, asin)
    if result:
        print(f"Successfully fetched data for {asin}:")
        print(json.dumps(result, indent=2, ensure_ascii=False)[:500] + "\n...[truncated]...")
        
        print(f"\nTesting export flow for {asin}...")
        file_path = api.export_asin_data(country, asin)
        if file_path:
            print(f"Successfully exported data to {file_path}")
            return True
        else:
            print("Failed to export data.")
            return False
    else:
        print(f"Failed to fetch data for {asin}. Token might be expired.")
        return False

if __name__ == "__main__":
    api = XiyouZhaociAPI()
    
    needs_login = True
    if api.auth_token:
        print("Existing token found. Attempting lookup & export...")
        success = test_asin_lookup()
        if success:
            needs_login = False
        else:
            print("Lookup/Export failed. Token might be expired.")
    
    if needs_login:
        print("\nStarting Login Flow...")
        if test_login_flow():
            print("Login successful. Retrying lookup...")
            test_asin_lookup()
        else:
            print("Login flow failed.")

