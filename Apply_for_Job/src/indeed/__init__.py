"""
indeed - Indeed job scraper module

Uses DrissionPage to connect to a locally running Chrome instance (port 9222).
Job data is extracted from the window.mosaic.providerData JS object embedded
in the search page, with DOM parsing as fallback.

Quick start:
    from src.indeed import scraper
    scraper.scrape_indeed(
        query="amazon operations manager",
        location="Remote",
        output_filename="data/raw/indeed_jobs.csv",
        max_pages=3,
    )
"""
