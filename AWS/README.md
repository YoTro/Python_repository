# Amazon Web Scraper (AWS) V2

A modern, modular, and self-healing Python 3 toolkit for scraping Amazon data with high success rates.

## Core Features
- **Anti-Bot Self-Healing**: Automatically initializes and maintains Amazon cookies using `DrissionPage` (stealth mode). If a "TTD" (bot detection) page is encountered, the scraper automatically refreshes cookies and rotates User-Agents to resume the task.
- **US Station Preference**: Forces US localization (Currency: USD, Language: en_US) regardless of your proxy or host IP location.
- **Advanced 999 Stock Method**: Accurate real-time stock estimation using a pure API-driven 999-method that bypasses modern WAF blocks by dynamically matching browser header signatures.
- **Data Aggregator**: Combine multiple data points (Details, Ranks, Images, Stock, Fulfillment) into a single unified CSV row.

## Project Structure
```text
/AWS/
├── main.py                  # Single unified CLI entry point
├── requirements.txt         # Project dependencies
├── config/                  # Configuration & dynamic states
│   ├── settings.json        # Global timeout/retry settings
│   ├── proxies.txt          # Proxy rotation list
│   └── cookies.json         # (Auto-generated) Cached session data
├── data/                    # I/O directory for CSV files
├── src/                     # Source code
│   ├── core/                # Core logic (networking, session, proxies)
│   │   ├── scraper.py       # Base scraper with self-healing logic
│   │   └── proxy.py         # Proxy rotation manager
│   ├── extractors/          # Task-specific data extractors
│   │   ├── cart_stock.py    # API-driven 999 stock method
│   │   ├── sales.py         # Keyword search scraper
│   │   ├── product_details.py # Detail page scraper
│   │   └── ...              # Other specialized extractors
│   └── utils/               # Shared helpers
│       ├── cookie_helper.py # Browser-based cookie fetcher
│       └── csv_helper.py    # Data persistence logic
└── tests/                   # Debugging scripts and test suits
```

## Installation
Ensure you have Python 3.8+ and a Chrome-based browser installed.
```bash
pip install -r requirements.txt
```

## Configuration
- **`config/settings.json`**: General scraper settings (timeouts, retries).
- **`config/proxies.txt`**: Add your proxy list here (one per line, format: `http://user:pass@ip:port`).
- **`config/cookies.json`**: Automatically generated/updated session data (Safe to ignore, excluded in `.gitignore`).

## Unified Entry Point: `main.py`

### 1. The "Super Aggregator" (Recommended)
Extracts everything related to specific ASINs (Details, Fulfillment, Ranks, Images, Stock, Review Counts) into a single wide CSV.
```bash
python main.py full_asin_details --input data/asins.csv --output data/full_results.csv
```

### 2. Search & Discover ASINs
Search a keyword and save all found ASINs.
```bash
python main.py sales --keyword "curling wand" --pages 3 --output data/asins_found.csv
```

### 3. Check Keyword Rankings
Check where your target ASINs rank for a specific keyword (scans first 3 pages by default).
```bash
python main.py keywords_rank --keyword "hair curler" --input data/asins.csv --output data/rankings.csv
```

### 4. Direct Stock Check
Run only the 999 stock check. Output includes `StockStatus` (Actual vs Limit).
```bash
python main.py stock --input data/asins.csv --output data/stock_check.csv
```

### 5. Other Tasks
- **`reviews`**: Extract the first page of product reviews.
- **`bestsellers`**: Extract ASINs from a Bestseller category URL.
- **`images`**: Extract all product image URLs.
- **`feedback`**: Get seller feedback counts.

## Input CSV Format
For tasks requiring `--input`, the CSV must contain an `ASIN` column:
```csv
ASIN,Notes
B01XXXXXXX,product A
B02YYYYYYY,product B
```

## Anti-Bot Mechanism
The scraper uses a hybrid approach:
1. **Stealth Layer**: `DrissionPage` is used silently at the start to fetch valid `session-id` and WAF tokens.
2. **Speed Layer**: `curl_cffi` performs high-speed requests mimicking modern Chrome TLS fingerprints (JA3/JA4).
3. **Healing Layer**: If Amazon blocks a request, the `AmazonBaseScraper` core automatically triggers a stealth browser to get fresh "keys" and retries without user intervention.
