"""
scraper.py - Indeed job scraper via DrissionPage

Page architecture (discovered via live testing):
  - Search results page embeds all job data in a <script> block as:
        window.mosaic.providerData["mosaic-provider-jobcards"] = {...};
    Path inside that JSON:
        .metaData.mosaicProviderJobCardsModel.results[]
    Each result contains: jobkey, displayTitle, company.name,
    formattedLocation, salarySnippet.text (optional), snippet (JD preview).
  - Full JD lives in the detail page (viewjob?jk=…):
        <div id="jobDescriptionText">   ← full description
        Salary / employment-type appear in header attribute spans.
  - Pagination: start=0, 10, 20 … (10 results per page by default)

Scrape strategy:
  1. Navigate to each search page; wait for render.
  2. Execute JS to pull structured data from window.mosaic.providerData.
  3. If JS data is empty (bot-block / layout change) fall back to scraping
     data-jk attributes from the DOM to collect job keys/URLs.
  4. For each job URL, navigate and parse #jobDescriptionText.
  5. Save to CSV.

No login is required for basic job browsing.

Prerequisites:
  Chrome running with remote debugging:
    /Applications/Google Chrome.app/Contents/MacOS/Google Chrome \\
        --remote-debugging-port=9222 \\
        --user-data-dir=/tmp/chrome-debug-profile

Usage:
    python3 main.py indeed "amazon operations manager" "Remote" 3
"""
from __future__ import annotations
import re
import time
import random
import logging
import os
from urllib.parse import quote_plus
from typing import Optional

import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.indeed.com/jobs"
_DETAIL_BASE = "https://www.indeed.com/viewjob"

_SALARY_RE = re.compile(
    r'\$[\d,]+[Kk]?(?:\s*[-–]\s*\$[\d,]+[Kk]?)?'
    r'(?:\s*/\s*(?:hr|yr|hour|year|an hour|a year|per hour|per year))?',
    re.IGNORECASE,
)
_EMPLOYMENT_TYPES = {
    "full-time", "part-time", "contract", "contractor",
    "temporary", "internship", "per diem", "other",
}
_POSTED_RE = re.compile(
    r'posted|just posted|today|yesterday|\d+\s*(day|hour|week|month)',
    re.IGNORECASE,
)


def _build_search_url(query: str, location: str, page: int) -> str:
    start = (page - 1) * 10
    return (
        f"{_BASE_URL}"
        f"?q={quote_plus(query)}"
        f"&l={quote_plus(location)}"
        f"&start={start}"
    )


# ══════════════════════════════════════════════════════════════════════
# Step 1 — Extract job listings from the search page
# ══════════════════════════════════════════════════════════════════════

_JS_EXTRACT = """
var d = window && window.mosaic && window.mosaic.providerData
        && window.mosaic.providerData["mosaic-provider-jobcards"];
if (!d) return [];
var results = (d.metaData
               && d.metaData.mosaicProviderJobCardsModel
               && d.metaData.mosaicProviderJobCardsModel.results) || [];
return results.map(function(r) {
    var jk   = r.jobkey || "";
    var link = r.link || ("/viewjob?jk=" + jk);
    var sal  = (r.salarySnippet && r.salarySnippet.text) ? r.salarySnippet.text : "";
    var co   = "";
    if (r.company) {
        co = (typeof r.company === "string") ? r.company : (r.company.name || "");
    }
    return {
        title:      r.displayTitle || r.title || "",
        jobkey:     jk,
        company:    co,
        location:   r.formattedLocation || "",
        salary_raw: sal,
        snippet:    r.snippet || "",
        link:       link
    };
});
"""


def _extract_jobs_via_js(tab) -> list[dict]:
    """
    Pull job listings from the currently loaded search page by executing
    JavaScript to read window.mosaic.providerData.
    """
    try:
        results = tab.run_js(_JS_EXTRACT)
        if not results or not isinstance(results, list):
            return []
        jobs = []
        for item in results:
            jk   = item.get("jobkey", "")
            link = item.get("link", "") or f"/viewjob?jk={jk}"
            # Sponsored listings use /pagead/clk?... which redirects to
            # the employer's site — always prefer the canonical viewjob URL.
            if jk and ("/pagead/" in link or not link.startswith("/viewjob")):
                link = f"/viewjob?jk={jk}"
            url  = (
                f"https://www.indeed.com{link}"
                if link.startswith("/")
                else link
            )
            if not url:
                continue
            jobs.append({
                "title":      item.get("title", ""),
                "company":    item.get("company", ""),
                "location":   item.get("location", ""),
                "salary_raw": item.get("salary_raw", ""),
                "description": item.get("snippet", ""),
                "jobkey":     jk,
                "url":        url,
            })
        return jobs
    except Exception as exc:
        logger.debug("JS job extraction failed: %s", exc)
        return []


def _extract_jobs_via_dom(tab) -> list[dict]:
    """
    DOM fallback: collect jobs from data-jk attributes on job cards.
    Returns minimal {url, jobkey} dicts — detail pages fill the rest.
    """
    jobs = []
    try:
        cards = tab.eles("css:[data-jk]", timeout=5)
        seen = set()
        for card in cards:
            jk = card.attr("data-jk") or ""
            if not jk or jk in seen:
                continue
            seen.add(jk)

            title   = ""
            company = ""
            for sel in ("css:h2 a span[title]", "css:h2 a span", "css:h2"):
                try:
                    el = card.ele(sel, timeout=1)
                    if el and el.text:
                        title = el.text.strip()
                        break
                except Exception:
                    pass

            for sel in ("css:.companyName", "css:[data-testid='company-name']"):
                try:
                    el = card.ele(sel, timeout=1)
                    if el and el.text:
                        company = el.text.strip()
                        break
                except Exception:
                    pass

            jobs.append({
                "title":      title,
                "company":    company,
                "location":   "",
                "salary_raw": "",
                "description": "",
                "jobkey":     jk,
                "url":        f"https://www.indeed.com/viewjob?jk={jk}",
            })
    except Exception as exc:
        logger.debug("DOM fallback failed: %s", exc)
    return jobs


def _fetch_search_page_jobs(tab, url: str) -> list[dict]:
    """
    Navigate to a search page and extract job listings.
    Tries JS extraction first; falls back to DOM scraping.
    """
    tab.get(url)
    # Wait for job cards to appear (up to ~12s)
    for _ in range(4):
        time.sleep(3)
        jobs = _extract_jobs_via_js(tab)
        if jobs:
            logger.debug("JS extraction returned %d jobs", len(jobs))
            return jobs
        # Check if cards are rendered yet
        try:
            if tab.ele("css:[data-jk]", timeout=1):
                break
        except Exception:
            pass

    # Try JS once more after a full render wait
    jobs = _extract_jobs_via_js(tab)
    if jobs:
        return jobs

    # DOM fallback
    logger.debug("JS data empty — using DOM fallback")
    return _extract_jobs_via_dom(tab)


# ══════════════════════════════════════════════════════════════════════
# Step 2 — Parse full job detail from detail page
# ══════════════════════════════════════════════════════════════════════

def _parse_detail_page(tab, url: str) -> dict:
    """
    Navigate to a job detail page and extract full JD and metadata.

    Stable selectors (verified against live Indeed pages):
      #jobDescriptionText                              ← full JD
      [data-testid="jobsearch-JobInfoHeader-title"]   ← title
      [data-testid="inlineHeader-companyName"]        ← company
      [data-testid="inlineHeader-companyLocation"]    ← location
      [data-testid="jobsearch-OtherJobDetailsContainer"] ← "$NNK - $NNK a year - Full-time"

    NOTE: DrissionPage's run_js() returns None for IIFE-style JS
    ((function(){...})()).  Must use direct `return` style.
    """
    tab.get(url)
    time.sleep(random.uniform(2.5, 4.0))

    title = company = location = description = salary_raw = ""
    employment_type = posted_time = ""
    is_remote = False
    final_url = tab.url

    # ── Full description ──────────────────────────────────────────────
    for sel in (
        "css:#jobDescriptionText",
        "css:[id*='jobDescription']",
        "css:.jobsearch-jobDescriptionText",
    ):
        try:
            el = tab.ele(sel, timeout=6)
            if el and el.text and len(el.text) > 50:
                description = el.text.strip()
                break
        except Exception:
            pass

    # ── Structured metadata via JS (direct-return style, NOT IIFE) ────
    try:
        meta = tab.run_js("""
function txt(sel) {
    var el = document.querySelector(sel);
    return el ? el.innerText.trim() : "";
}
return {
    title:     txt('[data-testid="jobsearch-JobInfoHeader-title"]')
             || txt('h1'),
    company:   txt('[data-testid="inlineHeader-companyName"]'),
    location:  txt('[data-testid="inlineHeader-companyLocation"]'),
    otherInfo: txt('[data-testid="jobsearch-OtherJobDetailsContainer"]'),
    vjDetails: txt('[data-testid="vjJobDetails-test"]')
};
""")
        if meta and isinstance(meta, dict):
            title    = meta.get("title", "")    or title
            company  = meta.get("company", "")  or company
            location = meta.get("location", "") or location

            # Parse salary + employment type.
            #
            # vjDetails has the clearest structure (newline-separated):
            #   "Job details\nPay\n$73,300 - $128,300 a year\nJob type\nFull-time\n..."
            # otherInfo is a compact fallback: "$73,300 - $128,300 a year - Full-time"
            vj = meta.get("vjDetails", "") or ""
            oi = meta.get("otherInfo", "") or ""

            # ── salary from vjDetails (line after "Pay") ──────────────
            if not salary_raw and vj:
                vj_lines = [l.strip() for l in vj.split("\n") if l.strip()]
                for idx, ln in enumerate(vj_lines):
                    if ln.lower() == "pay" and idx + 1 < len(vj_lines):
                        candidate = vj_lines[idx + 1]
                        if _SALARY_RE.search(candidate):
                            salary_raw = candidate
                        break

            # ── employment type from vjDetails (line after "Job type") ─
            if not employment_type and vj:
                vj_lines = [l.strip() for l in vj.split("\n") if l.strip()]
                for idx, ln in enumerate(vj_lines):
                    if ln.lower() == "job type" and idx + 1 < len(vj_lines):
                        candidate = vj_lines[idx + 1]
                        if candidate.lower() in _EMPLOYMENT_TYPES:
                            employment_type = candidate
                        break

            # ── salary fallback: scan otherInfo with regex ─────────────
            if not salary_raw and oi:
                m = _SALARY_RE.search(oi)
                if m:
                    salary_raw = m.group(0)

            # ── employment type fallback: word-search in otherInfo ─────
            if not employment_type and oi:
                for et in _EMPLOYMENT_TYPES:
                    if re.search(r'\b' + re.escape(et) + r'\b', oi, re.IGNORECASE):
                        employment_type = et.title()
                        break
    except Exception as exc:
        logger.debug("Metadata JS failed: %s", exc)

    # ── Remote flag ───────────────────────────────────────────────────
    if "remote" in (location + description).lower():
        is_remote = True

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

def scrape_indeed(
    query: str,
    location: str,
    output_filename: str,
    max_pages: int = 5,
    proxy_url: Optional[str] = None,
    fetch_descriptions: bool = True,
    desc_limit: int = 50,
) -> None:
    """
    Scrape Indeed job listings and save to CSV.

    Parameters
    ----------
    query              : Job search keyword, e.g. "amazon operations manager"
    location           : Location string, e.g. "Remote", "New York, NY"
    output_filename    : Path to output CSV file
    max_pages          : Number of search result pages (default 5, ~10 jobs/page)
    proxy_url          : HTTP proxy — must be set at Chrome launch, not here
    fetch_descriptions : If True (default), visit each detail page for full JD
    desc_limit         : Max detail pages to fetch per run
    """
    os.environ["no_proxy"] = "127.0.0.1,localhost"

    if proxy_url:
        print(
            "[WARN] Indeed uses an existing Chrome session on port 9222.\n"
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
        # ── Phase 1: collect job listings from all search pages ────────
        for page_num in range(1, max_pages + 1):
            search_url = _build_search_url(query, location, page_num)
            print(f"\n[Indeed] Search page {page_num}/{max_pages}")

            page_jobs = _fetch_search_page_jobs(tab, search_url)
            print(f"  Found {len(page_jobs)} job listings")
            all_jobs.extend(page_jobs)

            if not page_jobs:
                print("  No more results, stopping early.")
                break

            if page_num < max_pages:
                time.sleep(random.uniform(2.0, 4.0))

        # De-duplicate by jobkey (then by url)
        seen_keys: set[str] = set()
        unique_jobs = []
        for j in all_jobs:
            key = j.get("jobkey") or j.get("url", "")
            if key and key not in seen_keys:
                seen_keys.add(key)
                unique_jobs.append(j)
        all_jobs = unique_jobs
        print(f"\n[Indeed] Total unique listings collected: {len(all_jobs)}")

        # ── Phase 2: fetch detail pages for full descriptions ──────────
        if fetch_descriptions and all_jobs:
            to_fetch = all_jobs[:desc_limit]
            print(f"[Indeed] Fetching details for {len(to_fetch)} jobs...")
            for job in tqdm(to_fetch, desc="Detail pages"):
                if not job.get("url"):
                    continue
                try:
                    detail = _parse_detail_page(tab, job["url"])
                    # Merge: detail wins on all fields except title/company
                    # already known from search page (keep non-empty values)
                    for k, v in detail.items():
                        if v or k not in job or not job[k]:
                            job[k] = v
                except Exception as exc:
                    logger.warning("Detail fetch failed for %s: %s",
                                   job.get("url"), exc)

    finally:
        print("\n[Indeed] Done. Browser tab left open.")

    # ── Save CSV ───────────────────────────────────────────────────────
    if not all_jobs:
        print("[Indeed] No jobs collected — nothing saved.")
        return

    os.makedirs(os.path.dirname(output_filename) or ".", exist_ok=True)
    df = pd.DataFrame(all_jobs)
    df = df.drop(columns=["jobkey"], errors="ignore")
    if "url" in df.columns:
        df = df.drop_duplicates(subset=["url"])

    df.to_csv(output_filename, index=False, encoding="utf-8-sig")
    print(f"[Indeed] Saved {len(df)} jobs → {output_filename}")
