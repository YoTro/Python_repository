import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.scraper import AmazonBaseScraper

def test_scraper():
    logging.basicConfig(level=logging.INFO)
    scraper = AmazonBaseScraper()
    
    # Test with a common Amazon product or search page
    test_url = "https://www.amazon.com/s?k=laptop"
    print(f"Fetching: {test_url}")
    
    html = scraper.fetch(test_url)
    
    if html:
        print(f"Successfully fetched page. Length: {len(html)}")
        if "s-result-item" in html:
            print("Found search results in HTML!")
        else:
            print("Warning: Page fetched but search results not found. Might be a different layout.")
            
        if "_TTD_" in html:
            print("FAILED: TTD block still present in HTML.")
        else:
            print("SUCCESS: No TTD block detected.")
    else:
        print("FAILED: Could not fetch page.")

if __name__ == "__main__":
    test_scraper()
