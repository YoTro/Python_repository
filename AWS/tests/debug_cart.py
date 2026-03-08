import logging
from DrissionPage import ChromiumPage, ChromiumOptions
import time

def debug_add_to_cart(asin):
    print(f"Starting DrissionPage to debug Add to Cart for {asin}...")
    co = ChromiumOptions()
    co.headless(False) # We want to see the page
    co.incognito()

    page = ChromiumPage(co)
    
    try:
        # Start listening to network requests before navigating
        page.listen.start('amazon.com')
        
        url = f"https://www.amazon.com/dp/{asin}"
        print(f"Navigating to {url}")
        page.get(url)
        
        print("\n*** ACTION REQUIRED IN BROWSER ***")
        print("1. Handle 'Continue shopping' / 'Dismiss' / Captchas.")
        print("2. Click 'Add to Cart'.")
        print("3. Go to your Cart (Click the Cart icon).")
        print("4. Change quantity to '10+', type '999', and click 'Update'.")
        print("5. Wait for the page to show the stock limit or updated qty.")
        input("\nAfter completing ALL steps above, press Enter here in the terminal...")
        
        print("\n--- Network Requests Captured ---")
        output_file = "tests/cart_debug_output.txt"
        with open(output_file, "w") as f:
            for packet in page.listen.steps():
                req = packet.request
                # Capture both Add to Cart and Update Quantity
                if req.method == 'POST' and ('cart' in req.url or 'buy' in req.url or 'handle' in req.url):
                    msg = f"\n[POST] URL: {req.url}\nHeaders: {req.headers}\nPost Data: {req.postData}\n"
                    print(f"Captured POST to: {req.url}")
                    f.write(msg)
        print(f"\n✅ All network requests have been saved to {output_file}. The agent will read this file automatically.")
            
    except Exception as e:
        print(f"Error during debugging: {e}")
    finally:
        page.listen.stop()
        # Keep browser open for a few seconds to inspect visually if needed
        time.sleep(5)
        page.quit()

if __name__ == "__main__":
    import sys
    asin = sys.argv[1] if len(sys.argv) > 1 else "B0C99F534D"
    debug_add_to_cart(asin)
