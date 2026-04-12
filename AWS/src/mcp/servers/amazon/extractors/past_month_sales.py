from __future__ import annotations
import logging
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from src.core.scraper import AmazonBaseScraper
from src.core.utils.parser_helper import parse_integer

logger = logging.getLogger(__name__)

_BATCH_SIZE = 20  # ASINs per search request; >20 causes Amazon to truncate results (hit rate drops)


class PastMonthSalesExtractor(AmazonBaseScraper):
    """
    Extractor for the "X bought in past month" data point.

    Preferred path  : get_batch_past_month_sales() — one search request per 10 ASINs.
    Fallback / single: get_past_month_sales() — individual /dp/ page fetch.
    """

    # ── Batch (primary) ──────────────────────────────────────────────────────

    async def get_batch_past_month_sales(
        self,
        asins: List[str],
        host: str = "https://www.amazon.com",
    ) -> Dict[str, Optional[int]]:
        """
        Fetch 'bought in past month' for multiple ASINs via Amazon search.

        URL pattern: /s/?k=ASIN1%7C+ASIN2%7C+ASIN3&ref=nb_sb_noss
        ASINs are chunked into groups of _BATCH_SIZE to stay within URL limits.

        Returns {ASIN_UPPER: int_or_None}.  None means the badge was absent
        (low-volume listing, new product, or not shown in search results).
        """
        host = host.rstrip("/")
        if not host.startswith("http"):
            host = "https://" + host

        results: Dict[str, Optional[int]] = {a.upper(): None for a in asins}

        for i in range(0, len(asins), _BATCH_SIZE):
            chunk = [a.upper() for a in asins[i : i + _BATCH_SIZE]]
            query = "%7C+".join(chunk)
            url = f"{host}/s/?k={query}&ref=nb_sb_noss"
            logger.info(f"[PastMonthSales] batch url={url}")

            html = await self.fetch(url)
            if not html:
                logger.warning(f"[PastMonthSales] no HTML for chunk starting at index {i}")
                continue

            soup = BeautifulSoup(html, "html.parser")
            _parse_search_page(soup, results)

        hit = sum(1 for v in results.values() if v is not None)
        logger.info(f"[PastMonthSales] batch done: {hit}/{len(results)} hits")
        return results

    # ── Single-product fallback ──────────────────────────────────────────────

    async def get_past_month_sales(self, url_or_asin: str) -> dict:
        """
        Single-product lookup via /dp/ page.

        :param url_or_asin: ASIN string or full Amazon product URL.
        :return: {"ASIN": str, "PastMonthSales": int | None}
        """
        if "http" not in url_or_asin:
            asin = url_or_asin.strip().upper()
            url = f"https://www.amazon.com/dp/{asin}"
        else:
            url = url_or_asin
            m = re.search(r"/dp/([A-Z0-9]{10})", url)
            asin = m.group(1) if m else "UNKNOWN"

        logger.info(f"[PastMonthSales] single fetch asin={asin}")
        html = await self.fetch(url)
        if not html:
            return {"ASIN": asin, "PastMonthSales": None}

        # Method 1: exact span id
        m = re.search(
            r'<span id="social-proofing-faceout-title-tk_bought"[^>]*>(.*?)</span>',
            html, re.DOTALL | re.IGNORECASE,
        )
        if m:
            val = parse_integer(m.group(1).strip())
            logger.debug(f"[PastMonthSales] primary match asin={asin} raw={m.group(1).strip()!r} -> {val}")
            return {"ASIN": asin, "PastMonthSales": val}

        # Method 2: loose text match
        m = re.search(r"([0-9KkMm+.]+)\s+bought\s+in\s+past\s+month", html, re.IGNORECASE)
        if m:
            val = parse_integer(m.group(1).strip())
            logger.debug(f"[PastMonthSales] fallback match asin={asin} raw={m.group(1).strip()!r} -> {val}")
            return {"ASIN": asin, "PastMonthSales": val}

        logger.info(f"[PastMonthSales] no badge found asin={asin}")
        return {"ASIN": asin, "PastMonthSales": None}


# ---------------------------------------------------------------------------
# Parser helper (module-level, testable independently)
# ---------------------------------------------------------------------------

def _parse_search_page(
    soup: BeautifulSoup,
    results: Dict[str, Optional[int]],
) -> None:
    """
    Walk every element with a data-asin attribute and extract
    'X bought in past month' text.  Mutates *results* in-place.
    Skips ASINs not in results or already filled.
    """
    for card in soup.find_all(attrs={"data-asin": True}):
        asin = card.get("data-asin", "").strip().upper()
        if not asin or asin not in results or results[asin] is not None:
            continue
        text = card.get_text(" ", strip=True)
        m = re.search(r"([0-9KkMm+.]+)\s+bought\s+in\s+past\s+month", text, re.IGNORECASE)
        if m:
            results[asin] = parse_integer(m.group(1))
