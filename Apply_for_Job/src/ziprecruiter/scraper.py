"""
scraper.py - ZipRecruiter job scraper via DrissionPage

Actual page architecture (discovered via live testing):
  - Search results page is Next.js SSR HTML (~270KB).
    Job data is NOT in a separate XHR JSON API.
    Job URLs are embedded in a JSON-LD <script type="application/ld+json">
    ItemList block inside the SSR HTML.
  - Each detail page (/jobs/<company>/<slug>?lvk=...) renders the full
    job in a <section> / <main> element with the structure:
      Title\nCompany\nLocation\nEmployment type\nPosted date\nJob description\n<full JD>
    Salary (when present) appears as a span matching "$NNK - $NNK/yr" or "$NN/hr".

Scrape strategy:
  1. Intercept the SSR HTML response of the search page.
  2. Extract job list from JSON-LD ItemList → (name, url) pairs.
  3. For each job URL, navigate the browser and parse the <section> DOM.
  4. Save to CSV.

No login is required for basic job browsing.

Prerequisites:
  Chrome running with remote debugging:
    /Applications/Google Chrome.app/Contents/MacOS/Google Chrome \\
        --remote-debugging-port=9222 \\
        --user-data-dir=/tmp/chrome-debug-profile

Usage:
    python3 main.py ziprecruiter "amazon operations manager" "Remote" 3
"""
from __future__ import annotations
import re
import json
import time
import random
import logging
import os
from urllib.parse import quote_plus
from typing import Optional

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.ziprecruiter.com/jobs-search"


def _build_search_url(query: str, location: str, page: int) -> str:
    return (
        f"{_BASE_URL}"
        f"?search={quote_plus(query)}"
        f"&location={quote_plus(location)}"
        f"&page={page}"
    )


# ══════════════════════════════════════════════════════════════════════
# Step 1 — Extract job URLs from search page SSR HTML
# ══════════════════════════════════════════════════════════════════════

def _extract_jobs_from_search_html(html: str) -> list[dict]:
    """
    Parse the SSR HTML of a search results page.
    Returns a list of {'title': ..., 'url': ...} dicts from the JSON-LD
    ItemList block. Returns [] if none found.
    """
    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL
    )
    for blk in ld_blocks:
        try:
            data = json.loads(blk)
        except json.JSONDecodeError:
            continue
        if data.get("@type") == "ItemList":
            items = data.get("itemListElement", [])
            return [
                {"title": item.get("name", ""), "url": item.get("url", "")}
                for item in items
                if item.get("url")
            ]
    return []


def _fetch_search_page_html(tab, url: str, timeout: int = 25) -> str:
    """
    Navigate to a search URL and return its HTML.

    Strategy:
      1. Try to intercept the SSR HTML response via network listener
         (fastest path, gives raw pre-render HTML with JSON-LD intact).
      2. If the intercepted body is missing or non-string (e.g. redirect
         or gzip bytes), fall back to tab.html after a render wait.
      3. If JSON-LD is still absent from the network body, also try
         tab.html as a final fallback.
    """
    html_from_network = ""
    try:
        tab.listen.start(targets=["jobs-search"])
        tab.get(url)
        pkt = tab.listen.wait(timeout=timeout)
        tab.listen.stop()
        if pkt and pkt.response and isinstance(pkt.response.body, str):
            html_from_network = pkt.response.body
    except Exception as exc:
        logger.debug("Network interception failed: %s", exc)
        try:
            tab.listen.stop()
        except Exception:
            pass

    # If network HTML looks good (has JSON-LD), use it directly
    if html_from_network and "application/ld+json" in html_from_network:
        return html_from_network

    # Otherwise wait for the JS-rendered DOM and use that
    logger.debug("Network HTML missing JSON-LD — waiting for DOM render")
    time.sleep(6)
    return tab.html


# ══════════════════════════════════════════════════════════════════════
# Step 2 — Extract full job detail from detail page DOM
# ══════════════════════════════════════════════════════════════════════

def _parse_detail_page(tab, url: str) -> dict:
    """
    Navigate to a job detail URL and parse the <section>/<main> DOM element.

    Detail page text structure (newline-separated):
      Line 0 : Job title
      Line 1 : Company name
      Line 2 : Location  (e.g. "Columbia, SC • Remote")
      Line 3 : Employment type  (e.g. "Full-time")
      Line 4 : Posted date  (e.g. "Posted 27 days ago")
      Line 5 : "Job description"  (literal header)
      Line 6+: Full JD text

    Salary (optional) appears as a separate span: "$74K - $84K/yr" or "$25/hr".
    """
    tab.get(url)
    time.sleep(random.uniform(2.0, 3.5))

    # Try section first, then main
    container = None
    for sel in ("css:section", "css:main"):
        try:
            el = tab.ele(sel, timeout=5)
            if el and el.text and len(el.text) > 50:
                container = el
                break
        except Exception:
            pass

    title = company = location = employment_type = posted_time = description = ""
    salary_raw = ""
    is_remote = False
    final_url = tab.url  # may have redirected

    _SALARY_RE = re.compile(
        r'\$[\d,]+[Kk]?(?:\s*[-–]\s*\$[\d,]+[Kk]?)?'
        r'(?:\s*/\s*(?:hr|yr|hour|year|an hour|a year))?',
        re.IGNORECASE,
    )
    _EMPLOYMENT_TYPES = {
        "full-time", "part-time", "contract", "contractor",
        "temporary", "internship", "per diem", "other",
    }
    _POSTED_RE = re.compile(r'posted|just posted|\d+\s*(day|hour|week|month)', re.IGNORECASE)

    if container:
        lines = [l.strip() for l in container.text.split("\n") if l.strip()]
        skip = {"job description", "apply now", "similar jobs", "report this job",
                "share this job", "save job", "view job details"}
        content_lines = [l for l in lines if l.lower() not in skip]

        # Fixed fields (positions 0-2 are always title / company / location)
        if len(content_lines) >= 1:
            title = content_lines[0]
        if len(content_lines) >= 2:
            company = content_lines[1]
        if len(content_lines) >= 3:
            loc_raw = content_lines[2]
            if "remote" in loc_raw.lower():
                is_remote = True
            location = loc_raw.replace(" •", ",").strip()

        # Variable fields (lines 3+): salary may or may not be present
        for line in content_lines[3:]:
            ll = line.lower().strip()
            if _SALARY_RE.search(line) and not salary_raw:
                salary_raw = line.strip()
            elif ll in _EMPLOYMENT_TYPES and not employment_type:
                employment_type = line.strip()
            elif _POSTED_RE.search(line) and not posted_time:
                posted_time = line.strip()

        # Full description starts after "Job description" marker
        try:
            jd_idx = next(
                i for i, l in enumerate(lines)
                if l.lower() == "job description"
            )
            description = "\n".join(lines[jd_idx + 1:])
        except StopIteration:
            description = "\n".join(content_lines[5:]) if len(content_lines) > 5 else ""

    return {
        "title":           title,
        "company":         company,
        "location":        location,
        "salary_raw":      salary_raw,
        "description":     description,
        "employment_type": employment_type,
        "is_remote":       is_remote,
        "posted_time":     posted_time,
        "url":             final_url,
    }


# ══════════════════════════════════════════════════════════════════════
# Main public function
# ══════════════════════════════════════════════════════════════════════

def scrape_ziprecruiter(
    query: str,
    location: str,
    output_filename: str,
    max_pages: int = 5,
    proxy_url: Optional[str] = None,
    fetch_descriptions: bool = True,
    desc_limit: int = 50,
) -> None:
    """
    Scrape ZipRecruiter job listings and save to CSV.

    Parameters
    ----------
    query              : Job search keyword, e.g. "amazon operations manager"
    location           : Location string, e.g. "Remote", "New York, NY"
    output_filename    : Path to output CSV file
    max_pages          : Number of search result pages (default 5, ~7 jobs/page)
    proxy_url          : HTTP proxy — must be set at Chrome launch, not here
    fetch_descriptions : If True (default), visit each detail page for full JD
    desc_limit         : Max detail pages to fetch per run (guards against long runs)
    """
    os.environ["no_proxy"] = "127.0.0.1,localhost"

    if proxy_url:
        print(
            "[WARN] ZipRecruiter uses an existing Chrome session on port 9222.\n"
            f"[WARN] Proxy ({proxy_url}) must be set when launching Chrome."
        )

    try:
        from DrissionPage import ChromiumPage
    except ImportError as exc:
        raise ImportError("DrissionPage required: pip install DrissionPage") from exc

    chrome = ChromiumPage(addr_or_opts="localhost:9222")
    tab = chrome.new_tab()
    if not tab:
        print("[ERROR] Could not open a new Chrome tab.")
        return

    all_jobs: list[dict] = []

    try:
        # ── Phase 1: collect job URLs from all search pages ────────────
        for page_num in range(1, max_pages + 1):
            search_url = _build_search_url(query, location, page_num)
            print(f"\n[ZipRecruiter] Search page {page_num}/{max_pages}")

            html = _fetch_search_page_html(tab, search_url)
            page_jobs = _extract_jobs_from_search_html(html)

            print(f"  Found {len(page_jobs)} job listings")
            all_jobs.extend(page_jobs)

            if not page_jobs:
                print("  No more results, stopping early.")
                break

            if page_num < max_pages:
                time.sleep(random.uniform(1.5, 3.0))

        print(f"\n[ZipRecruiter] Total listings collected: {len(all_jobs)}")

        # ── Phase 2: fetch detail pages for full data ──────────────────
        if fetch_descriptions and all_jobs:
            to_fetch = all_jobs[:desc_limit]
            print(f"[ZipRecruiter] Fetching details for {len(to_fetch)} jobs...")
            for job in tqdm(to_fetch, desc="Detail pages"):
                if not job.get("url"):
                    continue
                try:
                    detail = _parse_detail_page(tab, job["url"])
                    # Merge detail into job dict (detail wins on all fields)
                    job.update(detail)
                except Exception as exc:
                    logger.warning("Detail fetch failed for %s: %s", job.get("url"), exc)

    finally:
        print("\n[ZipRecruiter] Done. Browser tab left open.")

    # ── Save CSV ───────────────────────────────────────────────────────
    if not all_jobs:
        print("[ZipRecruiter] No jobs collected — nothing saved.")
        return

    os.makedirs(os.path.dirname(output_filename) or ".", exist_ok=True)
    df = pd.DataFrame(all_jobs)
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"])

    df.to_csv(output_filename, index=False, encoding="utf-8-sig")
    print(f"[ZipRecruiter] Saved {len(df)} jobs → {output_filename}")
