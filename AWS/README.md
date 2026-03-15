# Amazon Web Scraper (AWS) V2 - Hybrid Intelligence Edition

A next-generation Amazon market intelligence platform featuring a robust **Dual-Track Agentic Architecture** and **Model Context Protocol (MCP)** integration. It enables LLMs (Claude, Gemini, etc.) and code-based Workflow Engines to autonomously perform market research, competitor analysis, and listing optimization.

It employs a **Hybrid Intelligence** model: 
- **Workflow Track**: For deterministic, high-throughput batch processing using **Step Tri-primitives** and persistent checkpoints.
- **Agent Track**: For exploratory, high-reasoning market analysis via ReAct loops and **Persistent Session Management**.
- **Unified Gateway**: A centralized API Gateway for identity resolution, rate limiting, and request normalization.

## 📖 Documentation & Guides

*   **[Architecture & Design](docs/ARCHITECTURE.md)**: Deep dive into the Dual-Track and Multi-User Ready system design.
*   **[Testing Guide](docs/TESTING.md)**: Procedures for unit tests, protocol integrity, and full-flow simulation.
*   **[Input & Data Schemas](docs/INPUT_SCHEMA.md)**: Guide to Pydantic models and CLI CSV input formats.
*   **[LLM Usage Guidelines](docs/LLM_GUIDELINES.md)**: Best practices for Prompt Engineering and Model Selection.
*   **[MCP Protocol Usage](docs/MCP_PROTOCOL.md)**: Guide to extending the Model Context Protocol integration.
*   **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)**: Solutions for common scraping and LLM issues.

---

## 🚀 Key Features

### 1. Dual-Track Orchestration (Gateway & Job Manager)
*   **API Gateway**: Centralized entry point for CLI, Feishu, and Cron. Handles Auth (Ext Pt #1), Rate Limiting (Ext Pt #2), and request normalization into `UnifiedRequest`.
*   **Job Manager**: Manages an asynchronous task queue with worker pools. Supports stateful resilience via **Checkpointing** (Workflows) and **Session Management** (Agents).
*   **Hybrid Routing**: Automatically dispatches deterministic pipelines to the Workflow Engine and conversational intents to the MCP Agent.

### 2. Step Tri-primitives & Data Orchestration
*   **Step Primitives**: Workflows are built using `EnrichStep` (Data fetching), `FilterStep` (Rule-based funneling), and `ProcessStep` (AI/Python processing).
*   **Mediated Persistence**: L1 Raw Data Servers (Amazon, Market) write to a centralized **Data Cache**, while L2 Calculation Servers (Finance, Compliance) consume from it, ensuring 100% decoupling.
*   **Unified Registry**: A top-level `Tool Registry` allows both tracks to discover and invoke capabilities across all domain servers.

### 3. Robust Microservice Tool Servers
*   **Amazon Domain**: 16+ focused async scrapers (BSR, Reviews, Stock) using `curl_cffi` for TLS impersonation.
*   **Market & Social**: Integrated adapters for SellerSprite, Lingxing, TikTok, and Meta trends.
*   **Intelligence Router**: Automatically routes tasks between Cloud APIs (Gemini/Claude) and Local LLMs (Llama.cpp/Ollama) based on cost and complexity.

### 4. Advanced Feishu (Lark) Bot Integration
*   **Interactive Commands**: Trigger deterministic workflows and get live progress bars with **Dynamic ETA** updates.
*   **Bitable Reporting**: Automatically generates structured reports in Feishu Multi-dimensional tables.

---

## 🛠 Installation & Setup

1. **Requirements**: Python 3.11+
2. **Install Dependencies**:
   ```bash
   python3.11 -m venv venv311
   source venv311/bin/activate
   pip install -r requirements.txt
   ```
3. **Environment Variables**:
   Create a `.env` file in the project root with your API keys (Gemini, Anthropic, Feishu).

---

## ⚡ Quick Start

### 1. CLI Usage (Single-User Testing)
```bash
# Run a deterministic Workflow (uses defaults from config/workflow_defaults.yaml)
python main.py --workflow product_screening --params '{"keyword": "yoga mat"}'

# Override specific thresholds — unspecified values fall back to workflow_defaults.yaml
python main.py --workflow product_screening --params '{"keyword": "yoga mat", "price_min": 30, "profit_margin_min": 0.35}'

# Talk to the exploratory MCP Agent
python main.py --explore "Analyze the profit margin of massage guns"
```

### 2. Feishu (Lark) Bot Listener
Start the WebSocket listener to receive commands and chat messages from Feishu:
```bash
# Start the bot (defaults to amazon_bot configuration)
PYTHONPATH=. venv311/bin/python src/entry/feishu/bot_listener.py --bot amazon_bot
```
Then in your Feishu group chat, send:
- `获取 Electronics BSR` (Triggers Workflow Track)
- `更新亚马逊 Cookies` (Triggers manual auth refresh)
- `这个类目好做吗？` (Triggers Agent Track Fallback)

### 3. Deployment to Claude Desktop
```bash
# One-click deployment of all L1/L2 tools to Claude Desktop
./scripts/deploy_claude_desktop.sh
```

---

## 📂 Project Structure & Architecture

For a detailed breakdown of the **Domain-Driven Design (DDD)** directory structure and the inter-layer data flow (Gateway -> Job Manager -> MCP Servers), please refer to the **[System Architecture Document (docs/ARCHITECTURE.md)](docs/ARCHITECTURE.md)**.
