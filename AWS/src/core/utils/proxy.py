from __future__ import annotations
import logging
import requests
import random
import os

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Manages loading, verifying, and providing proxies.
    Replaces the legacy freeproxies.py logic, simplifying the workflow 
    by allowing proxies to be loaded from a simple text or CSV file.
    """
    
    def __init__(self, proxy_file: str = "config/proxies.txt"):
        self.proxy_file = proxy_file
        self.proxies = {"http": [], "https": []}
        self.load_proxies()

    def load_proxies(self):
        """
        Loads proxies from a local text file if available.
        Expected format: ip:port per line.
        """
        if not os.path.exists(self.proxy_file):
            logger.warning(f"Proxy file {self.proxy_file} not found. Operating without proxies.")
            return

        with open(self.proxy_file, "r") as f:
            for line in f:
                proxy = line.strip()
                if proxy:
                    # If the line already starts with http:// or https://, use it as is
                    if proxy.startswith("http://") or proxy.startswith("https://"):
                        full_proxy = proxy
                    else:
                        full_proxy = f"http://{proxy}"
                        
                    self.proxies["http"].append(full_proxy)
                    self.proxies["https"].append(full_proxy)
        
        logger.info(f"Loaded {len(self.proxies['http'])} HTTP/HTTPS proxies from {self.proxy_file}")

    def verify_proxy(self, proxy_url: str, test_url: str = "https://geo.brdtest.com/welcome.txt?product=isp&method=native", timeout: int = 5) -> bool:
        """
        Tests if a given proxy is working by attempting to fetch a test URL.
        """
        proxies = {"http": proxy_url, "https": proxy_url}
        try:
            logger.info(f"Verifying proxy: {proxy_url}...")
            response = requests.get(test_url, proxies=proxies, timeout=timeout)
            if response.status_code == 200:
                logger.info(f"Proxy {proxy_url} is WORKING.")
                return True
        except Exception as e:
            logger.debug(f"Proxy {proxy_url} FAILED: {e}")
            
        return False

    def get_verified_proxies(self, test_url: str = "https://geo.brdtest.com/welcome.txt?product=isp&method=native", timeout: int = 5) -> list:
        """
        Filters the internal proxy list and returns only the ones that pass verification.
        """
        working_proxies = []
        for proxy in self.proxies["http"]:
            if self.verify_proxy(proxy, test_url, timeout):
                working_proxies.append(proxy)
        
        logger.info(f"Verified {len(working_proxies)} working proxies out of {len(self.proxies['http'])}.")
        return working_proxies

    def get_random_proxy(self) -> dict:
        """
        Returns a randomly selected proxy dictionary suitable for requests.
        """
        if not self.proxies["http"]:
            return {}
            
        proxy = random.choice(self.proxies["http"])
        return {
            "http": proxy,
            "https": proxy
        }
