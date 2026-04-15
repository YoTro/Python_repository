import re
import logging
import json
from typing import List, Dict, Any, Optional
from curl_cffi import requests

logger = logging.getLogger("mcp-compliance-cpsc")

class CPSCRecallProvider:
    """
    Provider for CPSC (U.S. Consumer Product Safety Commission) recalls.
    Uses the official REST API (saferproducts.gov) as primary source.
    """
    API_URL = "http://www.saferproducts.gov/RestWebServices/Recall"
    BASE_URL = "https://www.cpsc.gov"
    
    def __init__(self, lang: str = "en"):
        self.lang = lang
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        }

    async def search_recalls(self, keyword: str) -> List[Dict[str, Any]]:
        """
        Search for recalls using the REST API.
        """
        if self.lang == "zh":
            # Fallback to scraping for Chinese results as the API is EN-centric
            return await self._scrape_search_zh(keyword)
            
        params = {
            "format": "json",
            "RecallTitle": keyword
        }
        
        async with requests.AsyncSession() as s:
            try:
                resp = await s.get(self.API_URL, params=params, headers=self.headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                return self._parse_api_results(data)
            except Exception as e:
                logger.error(f"Error calling CPSC API for '{keyword}': {e}")
                # Fallback to scraping if API fails
                return await self._scrape_search_en(keyword)

    def _parse_api_results(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for item in data:
            try:
                # Extracting requested fields
                # date, title, img, image_URL, url, Hazard, Remedy, Units
                # Incidents/Injuries, Sold At, Importer(s), Manufactured In
                
                res = {
                    "date": item.get("RecallDate", "").split("T")[0],
                    "title": item.get("Title", ""),
                    "url": item.get("URL", ""),
                    "link": item.get("URL", ""), # For consistency
                    "description": item.get("Description", ""),
                    "hazard": item.get("Hazards", [{}])[0].get("Name", ""),
                    "remedy": item.get("Remedies", [{}])[0].get("Name", ""),
                    "incidents": item.get("Injuries", [{}])[0].get("Name", ""),
                    "sold_at": item.get("Retailers", [{}])[0].get("Name", ""),
                    "importer": item.get("Importers", [{}])[0].get("Name", ""),
                    "manufactured_in": item.get("ManufacturerCountries", [{}])[0].get("Country", ""),
                }
                
                # Units
                products = item.get("Products", [])
                if products:
                    res["units"] = products[0].get("NumberOfUnits", "")
                
                # Images
                images = item.get("Images", [])
                if images:
                    res["img"] = images[0].get("URL", "")
                    res["image_url"] = images[0].get("URL", "")
                
                results.append(res)
            except Exception as e:
                logger.warning(f"Error parsing API item: {e}")
                continue
        return results

    # --- Scraping Fallbacks (kept for zh and as backup) ---

    async def _scrape_search_zh(self, keyword: str) -> List[Dict[str, Any]]:
        url = "https://www.cpsc.gov/zh-CN/Recalls"
        params = {"search_combined_fields": keyword}
        async with requests.AsyncSession(impersonate="chrome110") as s:
            try:
                resp = await s.get(url, params=params, headers=self.headers, timeout=30)
                return self._parse_list_zh(resp.text)
            except Exception as e:
                logger.error(f"Scraping search zh failed: {e}")
                return []

    async def _scrape_search_en(self, keyword: str) -> List[Dict[str, Any]]:
        url = "https://www.cpsc.gov/Recalls"
        params = {"search_combined_fields": keyword}
        async with requests.AsyncSession(impersonate="chrome110") as s:
            try:
                resp = await s.get(url, params=params, headers=self.headers, timeout=30)
                return self._parse_list_en(resp.text)
            except Exception as e:
                logger.error(f"Scraping search en failed: {e}")
                return []

    def _parse_list_html(self, html: str) -> List[Dict[str, Any]]:
        """Parse CPSC recall list page (both en and zh). Extracts date, title, link, img, image_url."""
        results = []
        items = re.split(r'<div class="recall-list">', html)[1:]
        for item in items:
            date_match = re.search(r'<div class="recall-list__date">\s*(.*?)\s*</div>', item, re.S)
            title_link_match = re.search(
                r'<div class="recall-list__title">.*?<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                item, re.S
            )
            if not title_link_match:
                continue
            raw_href = title_link_match.group(1)
            link = self.BASE_URL + raw_href if not raw_href.startswith("http") else raw_href

            # Image: <img ... src="..." ... alt="...">
            img_match = re.search(r'<img\s[^>]*src="([^"]+)"', item, re.S)
            img_url = img_match.group(1) if img_match else ""

            results.append({
                "date": date_match.group(1).strip() if date_match else "",
                "title": re.sub(r'<[^>]+>', '', title_link_match.group(2)).strip(),
                "link": link,
                "img": img_url,
                "image_url": img_url,
                "is_scrape": True,
            })
        return results

    def _parse_list_zh(self, html: str) -> List[Dict[str, Any]]:
        return self._parse_list_html(html)

    def _parse_list_en(self, html: str) -> List[Dict[str, Any]]:
        return self._parse_list_html(html)

    async def get_recall_detail(self, url: str) -> Dict[str, Any]:
        """
        Fetch detail via scraping (API already provides detail, so this is mostly for scraped results).
        """
        async with requests.AsyncSession(impersonate="chrome110") as s:
            try:
                resp = await s.get(url, headers=self.headers, timeout=30)
                return self._parse_detail(resp.text)
            except Exception as e:
                logger.error(f"Detail fetch failed for {url}: {e}")
                return {}

    def _parse_detail(self, html: str) -> Dict[str, Any]:
        detail = {}
        def extract_field(label_regex: str) -> str:
            pattern1 = rf'<div class="recall-product__field-title"[^>]*>\s*{label_regex}\s*</div>\s*<div class="padding-y-1\s*">(.*?)</div>'
            match1 = re.search(pattern1, html, re.S)
            if match1:
                return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', match1.group(1))).strip()
            pattern2 = rf'<div class="recall-product__field-title"[^>]*>\s*{label_regex}\s*</div>\s*(.*?)\s*(?:</div>|(?=<div class="recall-product__field-title"))'
            match2 = re.search(pattern2, html, re.S)
            if match2:
                return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', match2.group(1))).strip()
            return ""

        detail["hazard"]                = extract_field("危险:") or extract_field("Hazard:")
        detail["remedy"]                = extract_field("补救:") or extract_field("Remedy:")
        detail["units"]                 = extract_field("单位:") or extract_field("Units:")
        detail["description"]           = extract_field("说明:") or extract_field("Description:")
        detail["incidents"]             = extract_field("事故/伤亡:") or extract_field("Incidents/Injuries:")
        detail["sold_exclusively_online"] = extract_field("仅在网上销售:") or extract_field("Sold Exclusively Online:")
        detail["sold_at"]               = extract_field("零售商:") or extract_field("Sold At:")
        detail["manufacturer"]          = extract_field("制造商:") or extract_field("Manufacturer:")
        detail["retailer"]              = extract_field("经销商:") or extract_field("Retailer:")
        detail["importer"]              = extract_field("进口商:") or extract_field("Importer\\(s\\):")
        detail["manufactured_in"]       = extract_field("产地:") or extract_field("Manufactured In:")
        return detail

# For backward compatibility with tools.py
CPSCRecallScraper = CPSCRecallProvider
