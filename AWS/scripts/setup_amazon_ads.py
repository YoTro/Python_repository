import os
import sys
import urllib.parse
import requests
import json
from dotenv import load_dotenv

# Ensure project root is in path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

load_dotenv(os.path.join(project_root, ".env"))

def setup_ads_auth():
    """
    Assistant script to set up Amazon Advertising API credentials.
    Includes Security warnings and Profile ID fetching.
    """
    client_id = os.getenv("AMAZON_ADS_CLIENT_ID")
    client_secret = os.getenv("AMAZON_ADS_CLIENT_SECRET")
    store_id = os.getenv("AMAZON_ADS_DEFAULT_STORE", "US").upper()
    
    if not client_id or not client_secret:
        print("❌ Error: Please set AMAZON_ADS_CLIENT_ID and AMAZON_ADS_CLIENT_SECRET in your .env first.")
        return

    # Security Warning
    print("\n" + "!"*60)
    print("SECURITY WARNING: ENVIRONMENT ISOLATION")
    print("!"*60)
    print("To prevent account linkage (linkage risk)):")
    print("1. DO NOT open the following URL in your local development browser.")
    print("2. ALWAYS open the URL within your isolated Ziniu (紫鸟) browser environment.")
    print("3. Ensure you are logged into the correct Amazon Seller account in that session.")
    print("!"*60)

    # 1. Construct Authorization URL
    redirect_uri = "http://localhost:3000/callback" 
    scope = "advertising::campaign_management"
    
    params = {
        "client_id": client_id,
        "scope": scope,
        "response_type": "code",
        "redirect_uri": redirect_uri
    }
    
    auth_url = f"https://www.amazon.com/ap/oa?{urllib.parse.urlencode(params)}"

    print("\n" + "="*60)
    print("STEP 1: Get Authorization Code")
    print("="*60)
    print(f"COPY and OPEN this URL in your ZINIU BROWSER:\n")
    print(auth_url)
    print("\n" + "="*60)
    print("STEP 2: Extract Code from Callback")
    print("="*60)
    print("1. After authorization, you will be redirected to a localhost page (it will fail to load).")
    print("2. Look at the browser's ADDRESS BAR.")
    print("3. Find the string after 'code='. It looks like a long random string.")
    print("   Example: http://localhost:3000/callback?code=ANxx...&scope=...")
    print("\nEnter the 'code' value from the address bar:")
    
    auth_code = input("> ").strip()
    
    if not auth_code:
        print("❌ No code entered. Aborting.")
        return

    # 2. Exchange code for Refresh Token
    print("\nExchanging code for Refresh Token...")
    
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()
        token_data = response.json()
        
        refresh_token = token_data.get("refresh_token")
        access_token = token_data.get("access_token")
        
        print("\n" + "✅"*5 + " Refresh Token Acquired! " + "✅"*5)
        print(f"AMAZON_ADS_REFRESH_TOKEN_{store_id}={refresh_token}")

        # 3. Fetch Profile IDs
        print("\n" + "="*60)
        print("STEP 3: Fetching Advertising Profile IDs")
        print("="*60)
        print("Wait while we fetch available profiles for this account...")

        # Determine region based on store_id (Simple mapping)
        region_map = {
            "US": "https://advertising-api.amazon.com",
            "UK": "https://advertising-api-eu.amazon.com",
            "DE": "https://advertising-api-eu.amazon.com",
            "JP": "https://advertising-api-fe.amazon.com"
        }
        base_url = region_map.get(store_id, "https://advertising-api.amazon.com")
        
        profiles_url = f"{base_url}/v2/profiles"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Amazon-Advertising-API-ClientId": client_id,
            "Content-Type": "application/json"
        }

        prof_resp = requests.get(profiles_url, headers=headers)
        prof_resp.raise_for_status()
        profiles = prof_resp.json()

        if not profiles:
            print("\n⚠️ No profiles found. Ensure this account has an active Amazon Ads account.")
        else:
            print("\nAvailable Profiles (Pick the one matching your store):")
            for p in profiles:
                print(f"- Name: {p.get('accountInfo', {}).get('name')} | ID: {p.get('profileId')} | Country: {p.get('countryCode')}")
            
            # Suggest the first one
            first_id = profiles[0].get('profileId')
            print(f"\nRecommended for .env:")
            print(f"AMAZON_ADS_PROFILE_ID_{store_id}={first_id}")

        print("\n" + "="*60)
        print("SETUP COMPLETE")
        print("="*60)
        print("Please update your .env file with the values above.")
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response details: {e.response.text}")
        
        print("\nTROUBLESHOOTING:")
        print("1. 'unknown scope': You must apply for Advertising API access at https://advertising.amazon.com/api-solutions")
        print("2. 'redirect_uri_mismatch': Ensure 'http://localhost:3000/callback' is in your LWA Allowed Return URLs.")
        print("3. 'invalid_grant': The code might have expired. Codes are only valid for a few minutes.")

if __name__ == "__main__":
    setup_ads_auth()
