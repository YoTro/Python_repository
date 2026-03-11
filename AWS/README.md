# Amazon Web Scraper (AWS) V2

A powerful Amazon scraping and analysis tool that supports multi-dimensional data extraction and product similarity analysis based on machine learning.

## 🚀 Key Features

### 1. Data Scraping
*   **Full Dimension Extraction**: ASIN, Title, 5 Features (Bullets), Product Description, Price, Ranks, Review Counts, Sales, etc.
*   **Diverse Tasks**: Supports keyword search scraping, Bestsellers scraping, ASIN details aggregation, stock checking, and more.
*   **Anti-Bot Protection**: Integrated `DrissionPage` for automatic cookie retrieval, TLS fingerprint impersonation via `curl_cffi`, and proxy rotation support.
*   **Network Compatibility**: Specifically optimized for Windows **TUN Mode** (VPN/Proxy environments) to resolve browser automation handshake failures.

### 2. Similarity Analysis
*   **Vectorized Matching**: Uses **TF-IDF** algorithm to convert product text (Title + Features) into high-dimensional vectors.
*   **Cosine Similarity**: Precisely calculates text overlap between products to identify duplicates or highly similar items.
*   **Automated Clustering**: Supporting **K-Means** (fixed groups) and **DBSCAN** (density-based).

---

## 🛠 Installation & Setup

1. **Requirements**: Python 3.8+
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Configuration**: 
   Configure User-Agent lists and scraper settings in `config/settings.json`.

---

## 📖 Usage Guide

### Basic Command Structure
```bash
python main.py <task> --input <input_file> --output <output_file> [options]
```

### Task-Specific Examples

#### 🔍 Keyword & Market Research
*   **Search for ASINs**: Get a list of ASINs for a specific keyword.
    ```bash
    python main.py sales --keyword "outdoor rug" --pages 3 --output data/rug_asins.csv
    ```
*   **Keyword Ranking**: Track where your target ASINs appear in search results for a keyword.
    ```bash
    python main.py keywords_rank --keyword "outdoor rug" --input data/target_asins.csv --pages 5 --output data/rank_results.csv
    ```
*   **Bestsellers**: Extract products from an Amazon Bestsellers category URL.
    ```bash
    python main.py bestsellers --url "https://www.amazon.com/Best-Sellers-..." --output data/bestsellers.csv
    ```

#### 📦 Product Metrics & Details
*   **Product Details**: Extract Title, Bullet Points, and Description.
    ```bash
    python main.py details --input data/asins.csv --output data/details.csv
    ```
*   **Sales & Stock**: Get "Past Month Sales" and current "Cart Stock" levels.
    ```bash
    python main.py past_month_sales --input data/asins.csv --output data/sales_stats.csv
    python main.py stock --input data/asins.csv --output data/stock_levels.csv
    ```
*   **Dimensions & Fulfillment**: Get product weight, size, and FBA/FBM info.
    ```bash
    python main.py dimensions --input data/asins.csv --output data/specs.csv
    python main.py fulfillment --input data/asins.csv --output data/logistics.csv
    ```
*   **Review Analysis**: Get total review counts or fetch actual comment text.
    ```bash
    python main.py review_count --input data/asins.csv --output data/counts.csv
    python main.py reviews --input data/asins.csv --pages 5 --output data/comments.csv
    ```

#### 🏭 Seller & Media Analysis
*   **Seller Feedback**: Get lifetime and recent feedback counts for seller IDs.
    ```bash
    python main.py feedback --input data/seller_ids.csv --output data/seller_stats.csv
    ```
*   **Images & Videos**: Extract all product image URLs or check if a listing has video content.
    ```bash
    python main.py images --input data/asins.csv --output data/images.csv
    python main.py videos --input data/asins.csv --output data/video_check.csv
    ```

#### 💎 Advanced Aggregation & Analysis
*   **Full ASIN Details**: Run ALL extractors at once for a list of ASINs (Comprehensive Report).
    ```bash
    python main.py full_asin_details --input data/asins.csv --output data/master_report.csv
    ```
*   **Similarity Clustering**: Group products by text similarity.
    ```bash
    # KMeans (Macro segments)
    python main.py analyze_similarity --input data/details.csv --output data/clusters.csv --clusters 5
    # DBSCAN (Precision competitors)
    python main.py analyze_similarity --input data/details.csv --output data/clusters.csv --cluster-method dbscan
    ```

---

## 📂 Project Structure

```text
AWS/
├── data/               # Recommended folder for all input/output CSV files
├── config/             # Config files and cookie cache
├── docs/               # System architecture and data schemas
├── src/
│   ├── analysis/       # ML clustering and sales rank regression logic
│   ├── core/           # Core base classes, network handling, and Standard DTO Models
│   ├── extractors/     # Specialized data extractors for various dimensions
│   ├── integrations/   # Third-party service adapters (Sellersprite, Xiyouzhaoci, ERP)
│   ├── tasks/          # Factory/Strategy pattern implementations for CLI commands
│   └── utils/          # Config, CSV, and Cookie utilities
├── tests/              # Integration tests and verification scripts
└── main.py             # Unified CLI entry point routing to TaskFactory
```

---

## 🔬 Clustering Algorithm Comparison

| Algorithm | Use Case | Advantages |
| :--- | :--- | :--- |
| **K-Means** | Market overview, fixed grouping | Simple logic; ensures every product is assigned to a cluster. |
| **DBSCAN** | Finding duplicates, precision competitors | Automatically determines the number of clusters; identifies unique/outlier products. |

## ⚠️ Notes
*   **TUN Mode Fix**: If you encounter `WebSocketBadStatusException` in Windows, this tool has been optimized with randomized debugging ports to bypass loopback issues.
*   **Politeness**: Please respect Amazon's `robots.txt` and avoid aggressive scraping. Use proxies if you need to process large volumes of data.
