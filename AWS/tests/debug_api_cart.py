import logging
import sys
import os
from bs4 import BeautifulSoup
import re

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.scraper import AmazonBaseScraper

def debug_api_cart(asin="B0C99F534D"):
    logging.basicConfig(level=logging.DEBUG)
    scraper = AmazonBaseScraper()
    
    url = f"https://www.amazon.com/dp/{asin}"
    print(f"Fetching: {url}")
    html = scraper.fetch(url)
    
    if not html:
        print("Failed to fetch product page.")
        return
        
    soup = BeautifulSoup(html, 'html.parser')
    form = soup.find('form', id='addToCart')
    if not form:
        print("Could not find <form id='addToCart'>")
        # Let's try to find any input with name offerListingID
        inputs = soup.find_all('input', attrs={'name': 'offerListingID'})
        if inputs:
            print("Found offerListingID inputs, but no form. The HTML structure might be different.")
            for inp in inputs:
                print(f"  {inp}")
        else:
            print("No offerListingID found at all.")
        return
        
    print("\n--- Form Inputs ---")
    payload = {}
    for inp in form.find_all('input'):
        name = inp.get('name')
        value = inp.get('value', '')
        if name:
            payload[name] = value
            print(f"{name}: {value}")
            
    # Add our required additions
    payload['quantity'] = '1'
    payload['submit.add-to-cart'] = 'Add to Cart'
    
    # Let's try to post
    post_url = "https://www.amazon.com/cart/add-to-cart/ref=dp_start-bbf_1_glance"
    headers = scraper._get_default_headers()
    headers.update({
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.amazon.com",
        "Referer": url,
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty"
    })
    
    print(f"\nPosting to: {post_url}")
    print(f"Headers: {headers}")
    
    try:
        res = scraper.session.post(post_url, data=payload, headers=headers, timeout=15)
        print(f"Status Code: {res.status_code}")
        print(f"Response Headers: {res.headers}")
        
        if res.status_code == 200:
            print("Successfully added to cart via API!")
        else:
            print(f"Failed. Output:\n{res.text[:500]}")
    except Exception as e:
        print(f"Request Error: {e}")

if __name__ == "__main__":
    import sys
    asin_to_test = sys.argv[1] if len(sys.argv) > 1 else "B0C99F534D"
    debug_api_cart(asin_to_test)
