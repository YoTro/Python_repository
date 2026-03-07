"""
get_challenge.py - 请求 51job 搜索 API，从 WAF 拦截页提取 NC 挑战参数
输出: challenge.json 包含 u_atoken / u_aref / 原始请求信息
      jobs.csv      包含职位列表（若 WAF 直接放行）
"""
import re
import csv
import json
import time
import sys
import os
import requests

OUT_FILE  = os.path.join(os.path.dirname(__file__), 'nc_env/data/challenge.json')
OUT_CSV   = os.path.join(os.path.dirname(__file__), 'nc_env/data/jobs.csv') # This will be changed to data/job_data.csv later

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer': 'https://we.51job.com/',
}

def build_url(keyword='自动化测试', job_area='040000', page=1):
    ts = int(time.time())
    params = {
        'api_key':     '51job',
        'timestamp':   str(ts),
        'keyword':     keyword,
        'searchType':  '2',
        'function':    '',
        'industry':    '',
        'jobArea':     job_area,
        'jobArea2':    '',
        'landmark':    '',
        'metro':       '',
        'salary':      '',
        'workYear':    '',
        'degree':      '',
        'companyType': '',
        'companySize': '',
        'jobType':     '',
        'issueDate':   '',
        'sortType':    '0',
        'pageNum':     str(page),
        'requestId':   '',
        'pageSize':    '20',
        'source':      '1',
        'accountId':   '',
        'pageCode':    'sou|sou|soulb',
        'scene':       '7',
    }
    qs = '&'.join(f'{k}={requests.utils.quote(str(v), safe="")}' for k, v in params.items())
    return 'https://we.51job.com/api/job/search-pc?' + qs

def solve_acw_sc_v2(arg1):
    """计算 acw_sc__v2 cookie 值（固定置换 + XOR）"""
    tr = [0xf,0x23,0x1d,0x18,0x21,0x10,0x1,0x26,0xa,0x9,0x13,0x1f,0x28,0x1b,0x16,0x17,
          0x19,0xd,0x6,0xb,0x27,0x12,0x14,0x8,0xe,0x15,0x20,0x1a,0x2,0x1e,0x7,0x4,
          0x11,0x5,0x3,0x1c,0x22,0x25,0xc,0x24]
    chars = list(arg1)
    shuffled = [''] * 40
    for i, c in enumerate(chars):
        for j, pos in enumerate(tr):
            if pos == i + 1:
                shuffled[j] = c
    shuffled_str = ''.join(shuffled)
    key = '3000176000856006061501533003690027800375'
    result = ''
    for i in range(0, len(shuffled_str), 2):
        a = int(shuffled_str[i:i+2], 16)
        b = int(key[i:i+2], 16)
        result += hex(a ^ b)[2:].zfill(2)
    return result

def clean_text(s):
    if not s:
        return ''
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', '', str(s))).strip()

def save_jobs_csv(items, keyword, output_csv_path):
    """将职位列表保存为 CSV"""
    fieldnames = ['Job', 'Salary', 'Company', 'Location', 'Education',
                  'Experience', 'UpdateDate', 'Welfare', 'JobDetail', 'Href']
    rows = []
    for item in items:
        job_id = item.get('jobId', '')
        href = f'https://we.51job.com/pc/search?jobId={job_id}' if job_id else ''
        rows.append({
            'Job':        item.get('jobName', ''),
            'Salary':     item.get('provideSalaryString', ''),
            'Company':    item.get('fullCompanyName', ''),
            'Location':   item.get('jobAreaString', ''),
            'Education':  item.get('degreeString', ''),
            'Experience': item.get('workYearString', ''),
            'UpdateDate': item.get('updateDateTime', ''),
            'Welfare':    '|'.join(item.get('jobWelfare', [])) if isinstance(item.get('jobWelfare'), list) else item.get('jobWelfare', ''),
            'JobDetail':  clean_text(item.get('jobDescribe', '')),
            'Href':       href,
        })
    file_exists = os.path.exists(output_csv_path)
    with open(output_csv_path, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f'[CSV] 已追加 {len(rows)} 条到 {output_csv_path}')

def fetch_challenge(url):
    session = requests.Session()
    resp = session.get(url, headers=HEADERS, timeout=10)
    html = resp.text

    # 检测 acw_sc__v2 JS 挑战页
    m_arg1 = re.search(r"arg1='([0-9A-Fa-f]{40})'", html)
    if m_arg1:
        arg1 = m_arg1.group(1)
        cookie_val = solve_acw_sc_v2(arg1)
        print(f'[ACW] 求解 acw_sc__v2: arg1={arg1}')
        print(f'[ACW] cookie \u2192 acw_sc__v2={cookie_val}')
        session.cookies.set('acw_sc__v2', cookie_val, domain='.51job.com')
        # 重试（带 cookie）
        resp = session.get(url, headers=HEADERS, timeout=10)
        html = resp.text

    content_type = resp.headers.get('Content-Type', '')

    # 如果是 JSON（未被拦截），解析并保存 CSV
    if 'application/json' in content_type or html.strip().startswith('{'):
        print('[INFO] 未被 WAF 拦截，直接返回 JSON（无需 NC）')
        try:
            data = json.loads(html)
            items = data.get('resultbody', {}).get('job', {}).get('items', [])
            # save_jobs_csv(items, '') # This will be handled by the run function
            return data, None
        except Exception as e:
            print(f'[CSV] 解析失败: {e}')
        return None, None

    return None, html

def parse_request_info(html):
    """从 WAF 拦截页 HTML 提取 requestInfo"""
    m_token  = re.search(r"token\s*:\s*['\"]([^'\"]+)['\"]", html)
    m_refer  = re.search(r"refer\s*:\s*['\"]([^'\"]+)['\"]", html)
    m_appkey = re.search(r"aliyun_captchaid_\w+\s*=\s*['\"]([^'\"]+)['\"]", html)
    m_trace  = re.search(r"aliyun_captchatrace_\w+\s*=\s*['\"]([^'\"]+)['\"]", html)
    m_url    = re.search(r"url\s*:\s*['\"]([^'\"]+)['\"]", html)

    if not m_token or not m_refer:
        print('[ERROR] 未找到 token/refer，HTML 已保存到 waf_response.html')
        with open(os.path.join(os.path.dirname(__file__), 'nc_env/data/waf_response.html'), 'w', encoding='utf-8') as f:
            f.write(html)
        return None

    result = {
        'u_atoken': m_token.group(1),
        'u_aref':   m_refer.group(1),
        'appkey':   m_appkey.group(1) if m_appkey else 'CF_APP_WAF',
        'trace':    m_trace.group(1)  if m_trace  else '',
        'original_url': m_url.group(1) if m_url else '',
    }
    return result

# ----------------------------------------------------------------------
# Merged from nc_env/main.py
# ----------------------------------------------------------------------

def fetch_page_with_session(session, url):
    """
    请求单页，自动处理 acw_sc__v2 挑战。
    返回: (json_data, None) 直接拿到数据
          (None, html)      NC 拦截，html 为拦截页
    """
    resp = session.get(url, headers=HEADERS, timeout=15)
    html = resp.text

    # 第一层：acw_sc__v2 JS 挑战
    m = re.search(r"arg1='([0-9A-Fa-f]{40})'", html)
    if m:
        cookie_val = solve_acw_sc_v2(m.group(1))
        print(f'  [ACW] 求解 acw_sc__v2 \u2192 {cookie_val}')
        session.cookies.set('acw_sc__v2', cookie_val, domain='.51job.com')
        resp = session.get(url, headers=HEADERS, timeout=15)
        html = resp.text

    ct = resp.headers.get('Content-Type', '')
    if 'application/json' in ct or html.strip().startswith('{'):
        return json.loads(html), None

    return None, html   # 第二层：NC 拦截


def fetch_with_nc_params(session, url, nc_params):
    """携带 NC 四参数重试请求"""
    qs = '&'.join(
        f'{k}={requests.utils.quote(str(v), safe="")}'
        for k, v in nc_params.items() if v
    )
    retry_url = url + '&' + qs
    print(f'  [NC-RETRY] {retry_url[:110]}...')
    resp = session.get(retry_url, headers=HEADERS, timeout=15)
    if resp.text.strip().startswith('{'):
        return json.loads(resp.text)
    return None


# \u2192 NC 验证 \u2190
def run_nc_challenge(challenge_info):
    """
    将 challenge_info 写入 challenge.json，
    启动 node js/simulate_slide.js，
    等待 nc_result.json 出现（最多 50 秒）。
    成功返回 nc_result dict，失败返回 None。
    """
    DATA_DIR       = os.path.join(os.path.dirname(__file__), 'nc_env/data')
    CHALLENGE_FILE = os.path.join(DATA_DIR, 'challenge.json')
    NC_RESULT_FILE = os.path.join(DATA_DIR, 'nc_result.json')
    JS_ENTRY       = os.path.join(os.path.dirname(__file__), 'nc_env/js', 'simulate_slide.js')

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(CHALLENGE_FILE, 'w', encoding='utf-8') as f:
        json.dump(challenge_info, f, ensure_ascii=False, indent=2)

    if os.path.exists(NC_RESULT_FILE):
        os.remove(NC_RESULT_FILE)

    print('  [NC] 启动 Node.js 滑块验证...')
    proc = subprocess.Popen(
        ['node', JS_ENTRY],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        cwd=os.path.dirname(__file__), # Run from current directory (src/job51)
    )

    # 实时打印 node 输出，同时等待结果文件
    deadline = time.time() + 50
    result = None
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line:
            print('  [node]', line.rstrip())
        if os.path.exists(NC_RESULT_FILE):
            try:
                with open(NC_RESULT_FILE, encoding='utf-8') as f:
                    result = json.load(f)
                break
            except Exception:
                pass
        if proc.poll() is not None:
            break

    proc.kill()

    if result:
        print(f'  [NC] 成功: u_asession={result.get("u_asession","")[:20]}...')
    else:
        print('  [NC] 超时或失败')
    return result

# \u2192 主流程 \u2190
def run(keyword, city_code, page_num, output_csv_path, session=None, nc_params=None):
    """
    尝试通过 API 方式获取指定页码的职位数据。
    返回 True 表示成功获取数据，False 表示失败（可能需要 DrissionPage 介入）。
    """
    if session is None:
        session = requests.Session()

    url = build_url(keyword, city_code, page_num)
    print(f'[API Scraper - Page {page_num}]')

    # ── 优先用缓存的 NC 参数 ──
    if nc_params:
        data = fetch_with_nc_params(session, url, nc_params)
        if data:
            items = data.get('resultbody', {}).get('job', {}).get('items', [])
            save_jobs_csv(items, keyword, output_csv_path)
            return True, nc_params
        print('  [API Scraper] NC 参数已失效，重新发起请求')
        nc_params = None

    # ── 普通请求（含 acw_sc__v2 处理） ──
    data, html = fetch_page_with_session(session, url)

    if data:
        # 直接拿到 JSON
        items = data.get('resultbody', {}).get('job', {}).get('items', [])
        save_jobs_csv(items, keyword, output_csv_path)
        return True, nc_params

    else:
        # NC 拦截
        print('  [API Scraper] WAF 触发 NC 验证')
        challenge_info = parse_request_info(html)
        if not challenge_info:
            print('  [API Scraper] 无法解析拦截页，API 方式失败')
            return False, nc_params

        nc_result = run_nc_challenge(challenge_info)
        if not nc_result:
            print('[API Scraper] NC 验证失败，API 方式终止')
            return False, nc_params

        nc_params = {
            'u_atoken':   nc_result.get('u_atoken', ''),
            'u_asession': nc_result.get('u_asession', ''),
            'u_asig':     nc_result.get('u_asig', ''),
            'u_aref':     nc_result.get('u_aref', ''),
        }

        # 用 NC 参数重试当前页
        data = fetch_with_nc_params(session, url, nc_params)
        if data:
            items = data.get('resultbody', {}).get('job', {}).get('items', [])
            save_jobs_csv(items, keyword, output_csv_path)
            return True, nc_params
        else:
            print('  [API Scraper] NC 参数重试仍失败，API 方式失败')
            return False, nc_params

