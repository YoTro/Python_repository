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
*   **Intelligence Router**: Automatically routes tasks between Cloud APIs (Gemini/Claude) and Local LLMs (Llama.cpp/Ollama) based on cost and complexity, now with full cost transparency.
*   **Precise Cost Tracking**: A universal `PriceManager` provides real-time, per-request cost calculation for all supported cloud LLMs (Gemini, Claude), handling complex tiered pricing and model-specific surcharges.

### 4. Advanced Feishu (Lark) Bot Integration
*   **Interactive Commands**: Trigger deterministic workflows and get live progress bars with **Dynamic ETA** and cumulative **cost** updates.
*   **Robust Bitable Reporting**: Automatically generates structured reports, now with dynamic field creation to prevent schema mismatch errors.

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
   Copy the template below and save it as `.env` in the project root:

   ```dotenv
   # ── LLM Providers ──────────────────────────────────────────
   GEMINI_API_KEY=your_gemini_api_key_here
   ANTHROPIC_API_KEY=your_anthropic_api_key_here   # Required only when using Claude

   # Default provider: "gemini" | "claude"
   DEFAULT_LLM_PROVIDER=gemini

   # Path to local GGUF model file (optional, for offline mode)
   LOCAL_MODEL_PATH=models/llm/your_model.gguf

   # ── Feishu / Lark Bots ─────────────────────────────────────
   # Naming rule: FEISHU_{BOT_NAME_UPPER}_{FIELD}
   # Add as many bots as needed by following the same pattern.

   # amazon_bot (primary bot)
   FEISHU_AMAZON_BOT_APP_ID=
   FEISHU_AMAZON_BOT_APP_SECRET=
   FEISHU_AMAZON_BOT_USER_ACCESS_TOKEN=   # Optional: for user-level Bitable access
   FEISHU_AMAZON_BOT_WEBHOOK_URL=         # Optional: for incoming webhook messages

   # test_bot (secondary / staging bot)
   FEISHU_TEST_BOT_APP_ID=
   FEISHU_TEST_BOT_APP_SECRET=
   FEISHU_TEST_BOT_USER_ACCESS_TOKEN=
   FEISHU_TEST_BOT_WEBHOOK_URL=

   # ── Third-party Services ───────────────────────────────────
   SELLERSPRITE_EMAIL=
   SELLERSPRITE_PASSWORD=
   XIYOU_PHONE=

   # ── Amazon Advertising API (LWA) ───────────────────────────
   AMAZON_ADS_CLIENT_ID=
   AMAZON_ADS_CLIENT_SECRET=
   AMAZON_ADS_DEFAULT_STORE=US
   # Multiple stores supported: AMAZON_ADS_REFRESH_TOKEN_{STORE}, AMAZON_ADS_PROFILE_ID_{STORE}
   AMAZON_ADS_REFRESH_TOKEN_US=
   AMAZON_ADS_PROFILE_ID_US=
   ```

   | Variable | Required | Description |
   |---|---|---|
   | `GEMINI_API_KEY` | When using Gemini | Google AI Studio API key |
   | `ANTHROPIC_API_KEY` | When using Claude | Anthropic Console API key |
   | `DEFAULT_LLM_PROVIDER` | No (default: `gemini`) | Active LLM backend |
   | `LOCAL_MODEL_PATH` | No | Path to GGUF model for offline inference |
   | `FEISHU_*_APP_ID` | For Feishu bot | Lark Open Platform App ID |
   | `FEISHU_*_APP_SECRET` | For Feishu bot | Lark Open Platform App Secret |
   | `FEISHU_*_USER_ACCESS_TOKEN` | No | User-level token for Bitable write access |
   | `FEISHU_*_WEBHOOK_URL` | No | Incoming webhook URL for the bot |
   | `AMAZON_ADS_CLIENT_ID` | For Ads API | Login with Amazon Client ID |
   | `AMAZON_ADS_CLIENT_SECRET` | For Ads API | Login with Amazon Client Secret |
   | `AMAZON_ADS_REFRESH_TOKEN_*` | For Ads API | OAuth2 Refresh Token per store |
   | `AMAZON_ADS_PROFILE_ID_*` | For Ads API | Advertising Profile ID per store |

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
