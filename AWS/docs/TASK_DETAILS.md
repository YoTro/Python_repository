# Task Functionality Details

A comprehensive reference for all tasks available in the Amazon Web Scraper (AWS) CLI.

| Task Category | Task Name | Function | Internal Logic | Typical Use Case |
| :--- | :--- | :--- | :--- | :--- |
| **Search & Discovery** | `sales` | Keyword-based ASIN harvesting | Navigates search result pages and extracts all organic & sponsored ASINs. | Building a competitor list for a specific niche. |
| | `bestsellers` | Top-tier category scraping | Scrapes a specific Bestsellers category URL for top products. | Monitoring category leaders and trendsetters. |
| | `keywords_rank` | Organic rank tracking | Scans search results to find the specific position of your target ASINs. | Measuring SEO performance for specific keywords. |
| **Data Extraction** | `details` | Deep content extraction | Parses the product page for Title, 5 Bullet Points, and full Description. | Content optimization and input for similarity analysis. |
| | `stock` | Real-time inventory check | Uses cart-addition logic or API hooks to estimate remaining units. | Tracking competitor stock-outs or supply chain health. |
| | `past_month_sales` | Social proof extraction | Fetches the "X+ units bought in past month" text from search result cards. | Rapid validation of market demand and velocity. |
| | `ranks` | Current BSR capture | Extracts the Main Category rank and all sub-category rankings. | Competitive benchmarking snapshot. |
| | `review_count` | Quantitative feedback | Fast extraction of total ratings count and average star rating. | Monitoring review growth velocity. |
| | `reviews` | Qualitative feedback | Scrapes actual customer comment text and review dates. | Sentiment analysis and customer pain-point discovery. |
| | `fulfillment` | Logistics & Seller info | Identifies if a product is FBA (Amazon) or FBM (Merchant). | Understanding competitor logistics strategy. |
| | `dimensions` | Physical specs | Extracts product weight and package/item dimensions. | Calculating FBA fees and shipping overhead. |
| | `images` | Media extraction | Collects all high-resolution product image URLs. | Visual competitive analysis. |
| | `videos` | Media presence check | Checks for the existence of product videos on the listing. | Evaluating listing quality and investment. |
| | `feedback` | Seller reputation | Fetches lifetime and recent feedback counts for a Seller ID. | Assessing merchant reliability and scale. |
| **Advanced Analysis** | `analyze_similarity` | Competitor clustering | Uses TF-IDF and KMeans/DBSCAN to group products by text similarity. | Distinguishing direct competitors from peripheral items. |
| | `analyze_weekly_sales` | Sales forecasting | Implements the UCLA Power-Law formula with seasonality/weekend adjustments. | Benchmarking performance and predicting sales for target ranks. |
| **Aggregation** | `full_asin_details` | Comprehensive Report | Sequentially executes almost all single-dimension extractors for a list of ASINs. | Generating a master spreadsheet for in-depth research. |

---

## 💡 Pro Tips
- **Performance**: For high-volume tasks like `full_asin_details`, always use the `--use-proxy` flag to avoid IP throttling.
- **Data Flow**: Typically, you start with `sales` to get ASINs, then `details` to get text, and finally `analyze_similarity` to group them.
