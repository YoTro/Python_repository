# Job Scraper (Apply_for_Job)

This project contains scrapers for `job51` (前程无忧) and `Zhipin` (BOSS直聘).

## Project Principle

This project is designed to scrape job information from two major recruitment websites. It uses a combination of API requesting (with complex WAF and NoCaptcha slider bypassing) and browser automation (`DrissionPage`) to reliably download job data.

### Scraped Data
The scrapers download the following job data and save it in `.csv` format in the `data` directory:
- Job Title
- Salary
- Company Name
- Location
- Job Description
- Welfare Benefits
- Education Requirements
- Work Experience
- Update Date
- Job URL

---

## File Structure Explanation

```
/Apply_for_Job/
├── .gitignore               # Git 忽略文件配置
├── README.md                # 本文件，项目整体说明
├── requirements.txt         # Python 依赖列表
├── main.py                  # 主程序入口，统一调度 51job 和 Zhipin 抓取任务
├── config/                  # 配置文件目录
│   └── amapkey.json         # 高德地图 API Key (若有地理编码需求)
├── data/                    # 数据结果保存目录
├── src/                     # 核心源码目录
│   ├── job51/               # 51job 抓取模块
│   │   ├── api_scraper.py      # 基于 API 和补环境的抓取脚本
│   │   ├── drission_scraper.py # 基于 DrissionPage 的浏览器自动化备用抓取脚本
│   │   └── nc_env/             # NC 滑块验证 Node.js 补环境核心目录 (用于破解 WAF)
│   ├── zhipin/              # Zhipin 抓取模块
│   │   └── scraper.py          # Zhipin 抓取脚本 (基于 DrissionPage 接管本地浏览器)
│   └── utils/               # 公共工具模块
└── tests/                   # 单元测试代码目录
```

---

## Installation

1. Clone the repository.
2. Install Python dependencies:
   ```bash
   pip3 install -r requirements.txt
   ```
3. Install Node.js (v18+) if you plan to use the 51job API scraper (which requires the `nc_env` Node.js environment to bypass the slider).
4. Install Chrome Browser (Required for both `DrissionPage` fallback and Zhipin scraper).

---

## Usage

Use the `main.py` script to run the scrapers.

**Command Format:**
```bash
python3 main.py [source] [keyword] [city] [pages] [--proxy-url [URL]]
```

### 51job Scraper
```bash
# Basic usage
python3 main.py 51job "python" "深圳" 3

# Use with automatic proxy (fetches from online list/cache)
python3 main.py 51job "python" "深圳" 3 --proxy-url

# Use with a specific proxy
python3 main.py 51job "python" "深圳" 3 --proxy-url http://127.0.0.1:7890
```
*Note: The 51job scraper will first attempt to use the API method (which relies on `nc_env`). If it fails, it will automatically fall back to the `DrissionPage` browser automation scheme.*

### Zhipin Scraper
```bash
# Basic usage
python3 main.py zhipin "Web前端" "上海" 5

# With proxy (Note: For Zhipin, the proxy should ideally be set when starting the browser)
python3 main.py zhipin "Web前端" "上海" 5 --proxy-url
```
*Important Note for Zhipin: You must start a Chrome browser with the remote debugging port open at `9222` before running the Zhipin script.*
**Mac Example:**
```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222
```
*If using a proxy for Zhipin, start Chrome with the `--proxy-server` flag:*
```bash
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222 --proxy-server="http://127.0.0.1:7890"
```

---

## 51job WAF & NC Slider Verification (`nc_env`)

The 51job API is protected by a dual-layer Alibaba Cloud WAF mechanism. To bypass this, the project includes a complex Node.js environment simulation inside `src/job51/nc_env`.

### 1. The Two Layers of WAF
- **Layer 1 (acw_sc__v2 JS Challenge):** 
  The initial request returns a JS challenge. The Python script (`get_challenge.py`) automatically calculates the required `acw_sc__v2` cookie to retry the request.
- **Layer 2 (NC Slider Challenge):**
  If Layer 1 triggers a slider verification, it returns a page containing `requestInfo.token` and `requestInfo.refer`. The `nc_env` Node.js scripts then simulate a browser environment to pass the slider, outputting `u_asession` and `u_asig` tokens to authorize the API access.

### 2. Node.js Browser Environment Emulation (补环境) - Deep Dive
To pass the NC Slider without a real browser, `src/job51/nc_env/js/env.js` implements a sophisticated environment emulation layer. This is not just a simple variable mock, but a recursive, proxy-based simulation designed to withstand deep detection.

#### Core Emulation Strategies
*   **Recursive Proxy Trapping**: 
    Global objects like `window`, `navigator`, and `document` are wrapped in ES6 `Proxy` objects. This allows the environment to:
    - Log every property access (getter/setter) attempted by the WAF scripts (e.g., `fireyejs.js`).
    - Return a "Safe Stub" (a function that returns itself) for any undefined property, preventing `TypeError: ... is not defined` which is a primary detection signal.
    - Mimic native function signatures using `Function.prototype.toString.call` overrides.

*   **Fingerprint & Sensor Forgery**:
    The project injects high-fidelity data into the mocked environment to ensure a "unique but human" identity:
    - **Canvas/WebGL**: Overrides `HTMLCanvasElement.prototype.toDataURL` and `getContext('webgl').getParameter` to return values from `canvas_real.json` and `browser_fingerprint.json`.
    - **AudioContext**: Mocking oscillators and dynamics compressors to return pre-calculated audio hashes.
    - **Hardware Info**: Precise mapping of `deviceMemory`, `hardwareConcurrency`, and screen resolutions.

*   **DOM & BOM API Coverage**:
    - **BOM**: Full implementation of `location` (with protocol/hostname logic), `history`, `screen`, and `performance.now()`.
    - **DOM**: Mocked `createElement`, `getElementById`, and `getElementsByTagName`. Special handling for `<canvas>` and `<script>` tags to track script execution flow.
    - **Events**: A simple event emitter system to handle `addEventListener` and `dispatchEvent`, necessary for the NC slider's internal state machine.

#### Data Synchronization Flow
The bypass operates as a cross-language bridge:
1.  **Python (`api_scraper.py`)**: Detects a 405/WAF block, extracts the `token` and `refer` from the HTML, and writes them to `nc_env/data/challenge.json`.
2.  **Node.js (`simulate_slide.js`)**: 
    - Loads the mocked environment from `env.js`.
    - Reads the challenge parameters.
    - Loads and executes the Alibaba `AWSC` suite (`awsc.js`, `nc.js`, `um.js`, etc.).
    - Simulates the mouse trajectory (interpolated from `trajectory.json`) to trigger the internal "success" callback.
    - Writes the resulting `u_asession` and `u_asig` to `nc_env/data/nc_result.json`.
3.  **Python**: Monitors for the result file, parses the tokens, and retries the original API request with the new authorization headers.

### 3. Manual Preparation for WAF Bypass (Only needed occasionally)
The slider bypass relies on real browser fingerprints and mouse trajectories. You may need to run these scripts in `src/job51/nc_env` manually if the WAF gets updated:

```bash
# 1. Collect real browser fingerprints (Run once)
python3 src/job51/nc_env/python/collect_fingerprints.py

# 2. Collect manual slider trajectories (Requires manual interaction in browser)
python3 src/job51/nc_env/python/collect_trajectory.py
```
*When collecting the trajectory, browse the page for 10-30 seconds, drag the slider to pass the verification, and let the script save the updated trajectories and UMID tokens into `nc_env/data/`.*

The `main.py` script seamlessly integrates with `get_challenge.py` and `simulate_slide.js` to perform these calculations automatically during the scraping process.

---

## Configuration

- `config/amapkey.json`: Amap API key (if needed for location parsing).
- Ensure the JSON data files in `src/job51/nc_env/data/` are populated correctly using the collection scripts if you experience constant API blocks on 51job.

## Running Tests

```bash
python3 -m unittest discover tests
```