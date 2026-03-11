# System Architecture

The AWS (Amazon Web Scraper) V2 project recently underwent a major architectural refactoring to embrace Domain-Driven Design (DDD) principles and behavioral design patterns, ensuring high cohesion, low coupling, and massive extensibility.

## 1. Task Routing: Strategy & Simple Factory Pattern
Previously, `main.py` contained a monolithic sequence of `if-elif` statements handling every CLI command. This violated the Open-Closed Principle (OCP).

**Current Architecture:**
*   **`src/tasks/base_task.py`**: Defines a strict `BaseTask` interface with an `execute(args, context)` method.
*   **Task Implementations (`src/tasks/`)**: Every CLI command (e.g., `sales`, `reviews`, `analyze_similarity`) is now encapsulated in its own class (e.g., `SalesTask`, `ReviewsTask`). These classes handle their own validation and data extraction logic independently.
*   **`src/tasks/factory.py`**: A `TaskFactory` centralizes the registration of tasks. `main.py` simply passes the command string to the factory to instantiate the correct task strategy dynamically.
*   **Benefit**: Adding a new CLI command now only requires creating a new class and registering it in the factory, without ever modifying `main.py`.

## 2. External Integrations: Adapter Pattern & High Cohesion
Third-party APIs (like Sellersprite, Xiyouzhaoci, and future ERP systems like Lingxiu) inherently possess different authentication mechanisms, rate limits, and response structures compared to standard web scraping extractors.

**Current Architecture:**
*   Moved out of `src/extractors/` and into `src/integrations/<service_name>/`.
*   Each service directory is a self-contained module containing its specific `auth.py` and `client.py`.
*   **Benefit**: Removing a service or swapping it for a competitor does not affect the core scraper extractors.

## 3. Data Standardization: Data Transfer Object (DTO)
Different data sources return wildly different field names (e.g., a scraper might return `Title`, Sellersprite might return `item_name`, an ERP might return `goods_desc`).

**Current Architecture:**
*   **`src/core/models.py`**: Defines standardized Data Classes (e.g., `StandardProduct`).
*   **Adapter Layer**: External integrations map their raw, heterogeneous JSON/HTML responses into these `StandardProduct` models before passing them to the business logic layer.
*   **Business Logic (`src/analysis/`)**: Modules like `similarity.py` and `sales_rank_regression.py` now operate exclusively on standardized DTOs (or gracefully map legacy column names to the standard internally), making them entirely agnostic to where the data originated.
*   **Benefit**: Total decoupling of data ingestion (Extractors/Integrations) from data processing (Analysis). You can add 10 new data sources without changing a single line of analysis code.