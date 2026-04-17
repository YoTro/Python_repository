# Job Scraper & AI Salary Premium Analyzer (Apply_for_Job)

招聘数据采集 + AI 技能薪酬溢价分析工具。支持从前程无忧（51job）和 BOSS 直聘（Zhipin）批量采集招聘信息，并对采集结果进行自动化分析，量化**"会 AI"对不同岗位薪资的提升幅度**。

## 功能概览

| 模块 | 功能 |
|---|---|
| 采集层 | 51job API + WAF 绕过、Zhipin 浏览器自动化，支持代理 |
| 标准化 | 统一双平台字段、解析薪资/经验、岗位名归一化 |
| 技能提取 | AI 技能三级分层（通用工具/数据能力/核心AI）+ 电商专项词表 |
| 溢价估算 | 均值对比 + OLS 回归（控制城市/经验/公司规模）+ PSM 倾向得分匹配（可选）|
| 趋势追踪 | 多次运行快照追加，观察 AI 需求随时间变化 |
| 报告生成 | 控制台摘要 + Markdown 报告 + 可视化图表（PNG）|

### 采集字段
两平台统一输出以下字段至 `data/raw/`：

| 字段 | 说明 |
|---|---|
| job_title | 职位名 |
| salary_mid | 月薪中位数（元，解析后） |
| company | 公司名 |
| location / city_tier | 城市及一/二/三线分级 |
| description | 职位描述（JD 全文）|
| experience / exp_years | 经验要求（原文 + 数字化年限）|
| job_canonical | 归一化岗位名（来自 yaml 词典或搜索关键词）|
| has_ai_skill / ai_skill_tier | AI 技能标记及等级（0-3）|

---

## 目录结构

```
/Apply_for_Job/
├── main.py                      # 主入口：采集 + 分析一体化调度
├── requirements.txt             # Python 依赖
├── config/
│   ├── amapkey.json             # 高德地图 API Key（地理编码需求时使用）
│   └── job_categories.yaml      # 岗位归一化词典（可自由扩展）
├── data/
│   ├── raw/                     # 爬虫原始 CSV（51job_jobs.csv / zhipin_jobs.csv）
│   ├── processed/               # 标准化 + 技能提取后的 CSV + 趋势快照
│   └── reports/                 # Markdown 报告 + 图表 PNG
├── src/
│   ├── job51/                   # 51job 采集模块
│   │   ├── api_scraper.py          # API 方式（含 WAF/NC 绕过）
│   │   ├── drission_scraper.py     # DrissionPage 浏览器自动化备用方案
│   │   └── nc_env/                 # NC 滑块 Node.js 补环境（破解阿里云 WAF）
│   ├── zhipin/                  # Zhipin 采集模块
│   │   └── scraper.py              # DrissionPage 接管本地浏览器
│   ├── analysis/                # 分析层（新）
│   │   ├── normalizer.py           # 字段标准化、薪资/经验解析、岗位归一化
│   │   ├── skill_extractor.py      # AI 技能提取（Tier 1/2/3 + 电商专项）
│   │   ├── premium_estimator.py    # AI 薪酬溢价估算（OLS 回归 + PSM）
│   │   ├── trend_tracker.py        # 多次运行快照追加 + 趋势摘要
│   │   └── report.py               # 控制台 / Markdown / 图表报告生成
│   └── utils/                   # 公共工具（代理等）
└── tests/                       # 单元测试
    ├── test_51job.py
    ├── test_zhipin.py
    ├── test_normalizer.py
    ├── test_skill_extractor.py
    ├── test_premium_estimator.py
    ├── test_trend_tracker.py
    └── test_report.py
```

---

## 安装

1. Clone 本仓库
2. 安装 Python 依赖：
   ```bash
   pip3 install -r requirements.txt
   ```
3. 安装 Node.js（v18+）——仅 51job API 模式需要（用于 NC 滑块补环境）
4. 安装 Chrome 浏览器——DrissionPage 备用方案及 Zhipin 采集均需要

---

## 使用方法

### 采集 + 自动分析（推荐）

```bash
# 单平台采集并生成分析报告
python3 main.py 51job   "amazon运营" 深圳 3
python3 main.py zhipin  "amazon运营" 深圳 5

# 双平台同时采集
python3 main.py both "amazon运营" 深圳 3

# 启用 PSM 倾向得分匹配（控制选择偏差，需 scikit-learn）
python3 main.py zhipin "数据分析师" 上海 5 --psm

# 仅采集，不分析
python3 main.py zhipin "前端开发" 北京 3 --no-analyze

# 不生成图表（无 matplotlib 环境时）
python3 main.py both "算法工程师" 深圳 3 --no-plot
```

### 仅分析已有数据

```bash
python3 main.py analyze \
    --51job  data/raw/51job_jobs.csv \
    --zhipin data/raw/zhipin_jobs.csv \
    --keyword "amazon运营"
```

### 代理支持

```bash
# 自动获取代理
python3 main.py 51job "python" 深圳 3 --proxy-url

# 指定代理地址
python3 main.py 51job "python" 深圳 3 --proxy-url http://127.0.0.1:7890
```

### Zhipin 前置步骤

运行前须先启动带调试端口的 Chrome：

```bash
# 无代理
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222

# 有代理
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
    --remote-debugging-port=9222 --proxy-server="http://127.0.0.1:7890"
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

## 分析层说明

### AI 技能分级（Tier）

| Tier | 含义 | 示例关键词 |
|---|---|---|
| 3 | 核心 AI 技能 | 大模型、RAG、微调、PyTorch、NLP |
| 2 | 数据/自动化能力 | Python、SQL、Power BI、自动化运营 |
| 1 | 通用 AI 工具 | ChatGPT、Claude、AIGC、豆包 |
| 0 | 无 AI 要求 | — |

### 溢价估算方法

- **原始溢价**：有 AI 要求 JD 均薪 − 无 AI 要求 JD 均薪（快速验证）
- **OLS 净溢价**：控制城市等级 / 经验年限 / 公司规模后的回归系数（推荐）
- **PSM 溢价**：倾向得分匹配后的平均处理效应 ATT（`--psm` 开启，消除选择偏差）

### 岗位归一化配置

`config/job_categories.yaml` 控制岗位分组逻辑，支持自由扩展：

```yaml
categories:
  # 新增岗位示例
  游戏策划:
    - 游戏策划
    - game.*design
    - 剧情策划
```

未命中 yaml 的职位标题会自动使用搜索关键词作为分组名，**无需修改代码即可分析任意新岗位**。

---

## 配置

- `config/amapkey.json`：高德地图 API Key（地理编码需求时使用）
- `config/job_categories.yaml`：岗位归一化词典（支持正则，修改后重启生效）
- `src/job51/nc_env/data/`：51job WAF 绕过所需指纹/轨迹数据（遭遇持续 API 封锁时重新采集）

---

## 运行测试

```bash
# 使用项目 venv
venv311/bin/python3 -m pytest tests/ -v

# 或系统 Python
python3 -m pytest tests/ -v
```

当前测试覆盖：

| 测试文件 | 覆盖模块 | 用例数 |
|---|---|---|
| test_normalizer.py | 薪资/经验解析、岗位归一化、双平台适配 | 35 |
| test_skill_extractor.py | AI 技能分级提取、电商专项、DataFrame 批处理 | 18 |
| test_premium_estimator.py | OLS 回归、PSM、边界条件 | 15 |
| test_trend_tracker.py | 快照生成、追加写入、历史加载 | 13 |
| test_report.py | 格式化函数、Markdown 生成、图表渲染 | 17 |
| test_51job.py | 爬虫模块冒烟测试 | 2 |
| test_zhipin.py | 爬虫模块冒烟测试 | 1 |