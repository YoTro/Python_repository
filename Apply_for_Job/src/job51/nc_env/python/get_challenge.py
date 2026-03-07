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

OUT_FILE  = os.path.join(os.path.dirname(__file__), '../data/challenge.json')
OUT_CSV   = os.path.join(os.path.dirname(__file__), '../data/jobs.csv')

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

def save_jobs_csv(items, keyword):
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
    file_exists = os.path.exists(OUT_CSV)
    with open(OUT_CSV, 'a', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f'[CSV] 已追加 {len(rows)} 条到 {OUT_CSV}')

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
        print(f'[ACW] cookie → acw_sc__v2={cookie_val}')
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
            if items:
                save_jobs_csv(items, '')
            else:
                print('[CSV] 无职位数据')
        except Exception as e:
            print(f'[CSV] 解析失败: {e}')
        return None

    return html

def parse_request_info(html):
    """从 WAF 拦截页 HTML 提取 requestInfo"""
    m_token  = re.search(r"token\s*:\s*['\"]([^'\"]+)['\"]", html)
    m_refer  = re.search(r"refer\s*:\s*['\"]([^'\"]+)['\"]", html)
    m_appkey = re.search(r"aliyun_captchaid_\w+\s*=\s*['\"]([^'\"]+)['\"]", html)
    m_trace  = re.search(r"aliyun_captchatrace_\w+\s*=\s*['\"]([^'\"]+)['\"]", html)
    m_url    = re.search(r"url\s*:\s*['\"]([^'\"]+)['\"]", html)

    if not m_token or not m_refer:
        print('[ERROR] 未找到 token/refer，HTML 已保存到 waf_response.html')
        with open(os.path.join(os.path.dirname(__file__), '../data/waf_response.html'), 'w', encoding='utf-8') as f:
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

def main():
    keyword  = sys.argv[1] if len(sys.argv) > 1 else 'amazon'
    job_area = sys.argv[2] if len(sys.argv) > 2 else '040000'
    pages    = int(sys.argv[3]) if len(sys.argv) > 3 else 1

    # 清空旧 CSV（首次运行）
    if os.path.exists(OUT_CSV):
        os.remove(OUT_CSV)

    for page in range(1, pages + 1):
        url = build_url(keyword, job_area, page)
        print(f'[INFO] 第 {page}/{pages} 页: {url[:100]}...')

        html = fetch_challenge(url)
        if html is None:
            # JSON 已处理（保存 CSV 或无需 NC）
            if pages > 1:
                time.sleep(1)
            continue

        # 被 NC 拦截，保存 challenge.json 供 simulate_slide.js 使用
        info = parse_request_info(html)
        if info is None:
            return

        print(f'[OK] u_atoken : {info["u_atoken"]}')
        print(f'[OK] u_aref   : {info["u_aref"]}')
        with open(OUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        print(f'[INFO] 已保存: {OUT_FILE}')
        print('[INFO] 需要运行 NC 验证: node js/simulate_slide.js')
        return  # NC 拦截时只处理第一页，后续需先完成验证

if __name__ == '__main__':
    main()
