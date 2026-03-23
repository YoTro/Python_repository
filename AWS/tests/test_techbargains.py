from __future__ import annotations
import pytest
import asyncio
import re
import urllib.parse
from bs4 import BeautifulSoup
from curl_cffi import requests
from typing import List, Dict, Any

# Helper functions for parsing - duplicated from client for testing isolation
def _extract_price(text: str) -> float:
    match = re.search(r'\$([\d,]+(?:\.\d+)?)', text.replace(',', ''))
    return float(match.group(1)) if match else 0.0

def _parse_techbargains_test(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    deals = []
    for card in soup.select('.deal-item-row-new'): 
        try:
            title_el = card.select_one('strong a')
            price_el = card.select_one('.price')
            date_el = card.select_one('.tb-date')

            title = title_el.get_text(strip=True) if title_el else "Unknown Deal"
            price = _extract_price(price_el.get_text(strip=True)) if price_el else 0.0
            date = date_el.get_text(strip=True) if date_el else "N/A"

            # TechBargains doesn't always show explicit discount % directly in list view
            # For now, we set it to 0.0 or can try to derive from original/sale price if available.
            discount_pct = 0.0 

            if price > 0 or title != "Unknown Deal":
                deals.append({
                    "date": date,
                    "price": price,
                    "discount_pct": discount_pct,
                    "title": title,
                    "site": "techbargains.com",
                    "type": "Search Result"
                })
        except Exception as e:
            # logger.warning(f"Error parsing TechBargains card: {e}")
            continue
    return deals

@pytest.mark.asyncio
async def test_fetch_techbargains_direct():
    url = "https://techbargains.com/search?search=dji"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
    }
    
    session = requests.Session(impersonate="chrome110") # Use curl_cffi's impersonation
    response = None
    try:
        # Attempt to fetch without pagination first, just one page
        response = await asyncio.to_thread(session.get, url, headers=headers, timeout=20)
        
        assert response.status_code == 200, f"Expected 200 OK, got {response.status_code}"
        deals = _parse_techbargains_test(response.text)
        assert len(deals) > 0, "Expected to find some deals on TechBargains"
        # Assert some basic structure of a deal
        if deals:
            assert "title" in deals[0]
            assert "price" in deals[0]
            assert "site" in deals[0]
            assert deals[0]["site"] == "techbargains.com"
            print(f"Successfully fetched and parsed {len(deals)} deals from TechBargains.")

    except Exception as e:
        pytest.fail(f"Failed to fetch or parse TechBargains: {e} | Response Status: {response.status_code if response else 'N/A'}")
    finally:
        session.close()


@pytest.mark.asyncio
async def test_fetch_techbargains_pagination():
    url_base = "https://techbargains.com/search?search={search_term}&page={page}"
    search_term = "apple"
    max_pages = 2 # Test with 2 pages
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36'
    }
    
    session = requests.Session(impersonate="chrome110")
    all_deals = []
    try:
        for page in range(1, max_pages + 1):
            url = url_base.format(search_term=urllib.parse.quote(search_term), page=page)
            response = await asyncio.to_thread(session.get, url, headers=headers, timeout=20)
            assert response.status_code == 200, f"Pagination: Expected 200 OK on page {page}, got {response.status_code}"
            
            page_deals = _parse_techbargains_test(response.text)
            if not page_deals and page == 1:
                pytest.fail("No deals found on first page for pagination test.")
            if not page_deals:
                print(f"No deals on page {page}. Stopping pagination test.")
                break
            
            all_deals.extend(page_deals)
            await asyncio.sleep(1.0) # Politeness

        assert len(all_deals) > 0, "Expected to collect deals across multiple pages."
        print(f"Successfully collected {len(all_deals)} deals from TechBargains across {max_pages} pages.")
        
    except Exception as e:
        pytest.fail(f"Pagination test failed: {e}")
    finally:
        session.close()
