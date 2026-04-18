"""
ziprecruiter - ZipRecruiter job scraper module

Uses DrissionPage to connect to a locally running Chrome instance (port 9222),
intercepts XHR job-search API responses, and falls back to HTML parsing.

Quick start:
    from src.ziprecruiter import scraper
    scraper.scrape_ziprecruiter(
        query="amazon operations",
        location="Remote",
        output_filename="data/raw/ziprecruiter_jobs.csv",
        max_pages=3,
    )
"""
