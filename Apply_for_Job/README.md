# Job Scraper

This project contains scrapers for `job51` and `zhipin`.

## Project Principle

This project is designed to scrape job information from two major recruitment websites: `51job` and `Zhipin`. It uses web scraping techniques to simulate browser behavior, bypass anti-scraping mechanisms, and download job data.

The core technology used is `DrissionPage`, a Python library that integrates browser automation and network request operations.

### Data

The scrapers download the following job data:

-   Job Title
-   Salary
-   Company Name
-   Location
-   Job Description
-   Welfare Benefits
-   Education Requirements
-   Work Experience
-   Update Date
-   Job URL

The data is saved in `.csv` format in the `data` directory.

## File Structure Explanation

```
/Apply_for_Job/
├── .gitignore               # Specifies files to be ignored by Git
├── README.md                # This file, project documentation
├── requirements.txt         # List of Python dependencies for the project
├── main.py                  # Main entry point of the application, handles command-line arguments
├── config/                  # Directory for configuration files
│   └── amapkey.json         # Amap API key for location services
├── data/                    # Directory for data files
│   ├── job51_jobs.csv       # Scraped job data from 51job
│   └── zhipin_jobs.csv      # Scraped job data from Zhipin
└── src/                     # Directory for all source code
    ├── __init__.py          # Makes 'src' a Python package
    ├── job51/               # Code related to the 51job scraper
    │   ├── __init__.py      # Makes 'job51' a Python package
    │   └── scraper.py       # The scraper for 51job
    ├── zhipin/              # Code related to the Zhipin scraper
    │   ├── __init__.py      # Makes 'zhipin' a Python package
    │   └── scraper.py       # The scraper for Zhipin
    └── utils/               # Utility modules shared across the project
        ├── __init__.py      # Makes 'utils' a Python package
        ├── amap.py          # Amap related functionalities (e.g., geocoding)
        ├── client.py        # HTTP client for making requests
        ├── crypto.py        # Cryptographic functions (e.g., signing requests)
        ├── js_engine.py     # For executing JavaScript code
        ├── login.py         # Handles login and cookie acquisition
        └── proxy.py         # Manages proxy IPs
```

## Installation

1.  Clone the repository.
2.  Install the dependencies:

```bash
pip3 install -r requirements.txt
```

## Usage

Use the `main.py` script to run the scrapers.

### 51job

```bash
python3 main.py 51job -k <keyword> -c <city> [-p <pages>]
```

Example:

```bash
python3 main.py 51job -k "python" -c "深圳"
```

### Zhipin

```bash
python3 main.py zhipin -k <keyword> -c <city> [-p <pages>]
```

Example:

```bash
python3 main.py zhipin -k "python" -c "深圳"
```

## Configuration

-   `config/amapkey.json`: Amap API key.
-   `config/51job_cookies.json`: Cookies for 51job.

## Running Tests

```bash
python3 -m unittest discover tests
```
