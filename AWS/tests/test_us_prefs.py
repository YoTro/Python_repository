import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.scraper import AmazonBaseScraper

def test_scraper_us_prefs():
    logging.basicConfig(level=logging.INFO)
    scraper = AmazonBaseScraper()
    
    # Force US preferences in current session cookies
    scraper.session.cookies.set("i18n-prefs", "USD", domain=".amazon.com")
    scraper.session.cookies.set("lc-main", "en_US", domain=".amazon.com")
    
    # Test with a common Amazon product or search page
    test_url = "https://www.amazon.com/s?k=laptop"
    print(f"Fetching: {test_url}")
    
    html = scraper.fetch(test_url)
    
    if html:
        print(f"Successfully fetched page. Length: {len(html)}")
        if "$" in html:
            print("SUCCESS: Found US Dollar symbol '$' in HTML.")
        elif "£" in html:
            print("FAILED: Found British Pound symbol '£' in HTML.")
        else:
            print("Warning: Neither $ nor £ found.")
    else:
        print("FAILED: Could not fetch page.")

if __name__ == "__main__":
    test_scraper_us_prefs()
