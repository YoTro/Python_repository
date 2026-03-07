#!/usr/bin/env python3
"""
main.py - 职位采集主流程
统一调度 51job 和 Zhipin 的抓取任务。

用法:
    python3 main.py [source] [keyword] [city] [pages]
示例:
    python3 main.py 51job python 深圳 3
    python3 main.py zhipin "amazon" 深圳 5
"""
import os
import sys
import time
import random
import requests
from src.job51 import api_scraper, drission_scraper
from src.zhipin import scraper as zhipin_scraper

# ── 配置 ──────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, 'data')

# ── Zhipin 城市代码映射 ────────────────────────────────────────────────
ZHIPIN_CITY_MAP = {
    "深圳": "101280600",
    "广州": "101280100",
    "北京": "101010100",
    "上海": "101020100",
}

# ── 51job 流程 ────────────────────────────────────────────────────────
def run_51job(keyword, city, pages, proxy_url=None):
    """执行 51job 的抓取逻辑"""
    output_csv = os.path.join(DATA_DIR, '51job_jobs.csv')
    print(f'[MAIN] 51job 任务启动，结果将保存至: {output_csv}')

    if os.path.exists(output_csv):
        os.remove(output_csv)
        print(f'[MAIN] 已清空旧数据文件: {output_csv}')

    session = requests.Session()
    
    proxies_dict = None
    if proxy_url:
        from src.utils.proxy import proxies
        # 如果 proxy_url 是特定的字符串（由 main 解析得到），传给 proxies
        # 如果 main 传入的是 True（表示开了 flag 但没给地址），则传 None 触发自动获取
        raw_proxies = proxies(None if proxy_url is True else proxy_url)
        
        # 针对 requests.Session，需要将列表转换为单个随机字符串
        selected_http = random.choice(raw_proxies['http']) if isinstance(raw_proxies['http'], list) else raw_proxies['http']
        selected_https = random.choice(raw_proxies['https']) if isinstance(raw_proxies['https'], list) else raw_proxies['https']
        
        proxies_dict = {
            'http': selected_http if selected_http.startswith('http') else f"http://{selected_http}",
            'https': selected_https if selected_https.startswith('http') else f"http://{selected_https}"
        }
        print(f"[MAIN] 启用代理: {proxies_dict}")
        session.proxies.update(proxies_dict)

    nc_params = None

    for page in range(1, pages + 1):
        print(f"\n" + "="*20 + f" 51job: 开始处理第 {page}/{pages} 页 " + "="*20)

        api_success, nc_params = api_scraper.run(
            keyword=keyword,
            city_code=drission_scraper.get_city_code(city),
            page_num=page,
            output_csv_path=output_csv,
            session=session,
            nc_params=nc_params
        )

        if not api_success:
            print("[MAIN] 51job API 方式失败，启动 DrissionPage 备用方案...")
            try:
                dp_proxy = proxies_dict['http'] if proxies_dict else None
                drission_scraper.run_single_page(
                    keyword=keyword, city=city, page_num=page, output_csv_path=output_csv, proxy_url=dp_proxy
                )
                print(f"[MAIN] 51job DrissionPage 方案完成第 {page} 页")
            except Exception as e:
                print(f"[MAIN] 51job DrissionPage 方案在第 {page} 页也失败了: {e}")
                print("[MAIN] 终止抓取。")
                break
        
        if page < pages:
            sleep_time = random.uniform(2, 4)
            print(f"[MAIN] 休眠 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)
    
    print(f'\n[MAIN] 51job 任务完成，结果已保存至 {output_csv}')

# ── Zhipin 流程 ───────────────────────────────────────────────────────
def run_zhipin(keyword, city, pages, proxy_url=None):
    """执行 Zhipin 的抓取逻辑"""
    output_csv = os.path.join(DATA_DIR, 'zhipin_jobs.csv')
    print(f'[MAIN] Zhipin 任务启动，结果将保存至: {output_csv}')

    if os.path.exists(output_csv):
        os.remove(output_csv)
        print(f'[MAIN] 已清空旧数据文件: {output_csv}')

    city_code = ZHIPIN_CITY_MAP.get(city)
    if not city_code:
        print(f"[ERROR] 未找到城市 '{city}' 对应的 Zhipin 城市代码。")
        print(f"         支持的城市: {list(ZHIPIN_CITY_MAP.keys())}")
        return

    print(f'[MAIN] Zhipin 任务启动 (城市代码: {city_code})')
    print("[WARN] 请确保您已在 9222 端口启动了带调试功能的浏览器。")
    print("[WARN] 例如: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222")
    
    dp_proxy = None
    if proxy_url:
        from src.utils.proxy import proxies
        raw_proxies = proxies(None if proxy_url is True else proxy_url)
        selected_http = random.choice(raw_proxies['http']) if isinstance(raw_proxies['http'], list) else raw_proxies['http']
        dp_proxy = selected_http if selected_http.startswith('http') else f"http://{selected_http}"
        print(f"[MAIN] 拟使用代理 (需手动配置浏览器): {dp_proxy}")

    zhipin_scraper.scrape_zhipin(
        query=keyword,
        city_code=city_code,
        output_filename=output_csv,
        max_pages=pages,
        proxy_url=dp_proxy
    )
    print(f'\n[MAIN] Zhipin 任务完成。')

# ── 主流程 ────────────────────────────────────────────────────────────
def main():
    args_without_flags = [arg for arg in sys.argv if not arg.startswith('--')]
    if len(args_without_flags) < 5:
        print("用法: python3 main.py [source] [keyword] [city] [pages] [--proxy-url URL]")
        print("示例: python3 main.py 51job python 深圳 3")
        print("      python3 main.py 51job python 深圳 3 --proxy-url")
        print("      python3 main.py zhipin \"Web前端\" 上海 5 --proxy-url http://127.0.0.1:7890")
        sys.exit(1)

    source = args_without_flags[1].lower()
    keyword = args_without_flags[2]
    city = args_without_flags[3]
    pages = int(args_without_flags[4])
    
    proxy_url = None
    if '--proxy-url' in sys.argv:
        idx = sys.argv.index('--proxy-url')
        if idx + 1 < len(sys.argv) and not sys.argv[idx+1].startswith('--'):
            proxy_url = sys.argv[idx+1]
        else:
            proxy_url = True # 标记为开启代理，但使用自动获取

    print(f'[MAIN] 数据源={source}  关键词={keyword}  城市={city}  页数={pages}  代理={"开启" if proxy_url else "关闭"}')
    os.makedirs(DATA_DIR, exist_ok=True)

    if source == '51job':
        run_51job(keyword, city, pages, proxy_url)
    elif source == 'zhipin':
        run_zhipin(keyword, city, pages, proxy_url)
    else:
        print(f"[ERROR] 不支持的数据源: '{source}'。请选择 '51job' 或 'zhipin'。")
        sys.exit(1)

if __name__ == '__main__':
    main()
