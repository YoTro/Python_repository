from DrissionPage import ChromiumPage, ChromiumOptions
import time

def test_amazon_cookies():
    print("Starting DrissionPage...")
    co = ChromiumOptions()
    # Run visibly to see what's happening
    co.headless(False) 
    co.incognito()

    page = ChromiumPage(co)
    try:
        print("Navigating to https://www.amazon.com/")
        page.get('https://www.amazon.com/')
        
        # Wait a bit for page to load and any popups
        time.sleep(3)
        
        # Check for "Continue shopping" button
        continue_btn = page.ele('text:Continue shopping', timeout=2)
        if continue_btn:
            print("Found 'Continue shopping' button. Clicking...")
            continue_btn.click()
            time.sleep(3)
        else:
            print("No 'Continue shopping' button found.")
            
        # Check for Captcha
        captcha_text = page.ele('text:Type the characters you see in this image', timeout=1)
        if captcha_text:
            print("WARNING: Captcha detected on the page!")

        # Get cookies - older versions might return list of dicts directly
        raw_cookies = page.cookies()
        print(f"\nObtained {len(raw_cookies)} cookies:")
        
        cookies_dict = {}
        for cookie in raw_cookies:
            name = cookie.get('name')
            value = cookie.get('value')
            cookies_dict[name] = value
            print(f"- {name}: {value[:30]}...")
            
        if 'session-id' in cookies_dict:
            print("\nSUCCESS: Found 'session-id'. Cookie is likely valid.")
        else:
            print("\nWARNING: 'session-id' not found. Cookie might be incomplete.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        page.quit()

if __name__ == "__main__":
    test_amazon_cookies()
