from __future__ import annotations
import re
import logging
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from src.core.scraper import AmazonBaseScraper

logger = logging.getLogger(__name__)

class BSRCategoryExtractor:
    """
    Extracts category and subcategory information from Amazon BSR (Best Sellers Rank) pages.
    """
    def __init__(self):
        self.scraper = AmazonBaseScraper()
        self.node_id_pattern = re.compile(r"/zgbs/[^/]+/(\d+)")

    async def get_categories_from_page(self, url: str) -> List[Dict[str, Any]]:
        """
        Visits a BSR page and extracts all visible category links in the sidebar.
        Returns a list of categories with name, url, and node_id.
        """
        html = await self.scraper.fetch(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, "html.parser")
        sidebar = soup.find("div", {"role": "tree"}) or soup.find("div", {"id": "zg_left_col1"})
        
        if not sidebar:
            # Fallback to broad search if specific sidebar ID is missing
            sidebar = soup.find("div", {"id": "zg-left-col"}) or soup
            
        links = sidebar.find_all("a", href=re.compile(r"/zgbs/"))
        
        results = []
        seen_urls = set()
        
        for link in links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if not text or "Any Department" in text:
                continue
                
            full_url = f"https://www.amazon.com{href}" if href.startswith("/") else href
            # Clean tracking parameters
            clean_url = re.sub(r"/ref=.*$", "/", full_url)
            if not clean_url.endswith("/"):
                clean_url += "/"
                
            if clean_url in seen_urls:
                continue
            seen_urls.add(clean_url)
            
            # Extract Node ID
            node_id = None
            m = self.node_id_pattern.search(clean_url)
            if m:
                node_id = m.group(1)
                
            results.append({
                "name": text,
                "url": clean_url,
                "node_id": node_id
            })
            
        return results

    def extract_current_node_id(self, html: str) -> Optional[str]:
        """Attempts to find the browseNodeId for the current page from HTML source."""
        patterns = [
            r'"browseNodeId"\s*:\s*"(\d+)"',
            r'"currentBrowseNodeId"\s*:\s*"(\d+)"',
            r'data-node-id="(\d+)"'
        ]
        for p in patterns:
            m = re.search(p, html)
            if m:
                return m.group(1)
        return None
