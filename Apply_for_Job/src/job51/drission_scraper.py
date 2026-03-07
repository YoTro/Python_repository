# -*- coding: utf-8 -*-
import time
import os
import sys
import pandas as pd
import json
import random
import urllib.parse
import re
import requests
from DrissionPage import ChromiumPage, ChromiumOptions
from tqdm import tqdm

def run_single_page(keyword, city, page_num, output_csv_path, proxy_url=None):
    """
    使用 DrissionPage 抓取单个页面的数据。
    这是一个备用方案，当 API Scraper 失败时被调用。
    """
    spider = JobSpider(headless=True, proxy_url=proxy_url)
    try:
        spider._scrape_page(keyword, city, page_num, output_csv_path)
    finally:
        spider.page.quit()


def get_city_code(city_name, city_map_cache={}):
    """从 CDN 映射表获取代码，并缓存结果"""
    if city_name in city_map_cache:
        return city_map_cache[city_name]
    url = "https://js.51jobcdn.com/in/js/2023/dd/dd_area_translation.json"
    try:
        resp = requests.get(url, timeout=10)
        data_list = resp.json()
        for item in data_list:
            if item.get('value') == city_name:
                city_map_cache[city_name] = item.get('code')
                return city_map_cache[city_name]
    except Exception:
        pass
    return "000000"


class JobSpider:
    def __init__(self, headless=False, proxy_url=None):
        co = ChromiumOptions()
        co.set_argument('--disable-blink-features=AutomationControlled')
        if proxy_url:
            co.set_proxy(proxy_url)
        if headless:
            co.headless()
        self.page = ChromiumPage(co)

    def get_city_code(self, city_name):
        """从 CDN 映射表获取代码"""
        if city_name in self.city_map: return self.city_map[city_name]
        url = "https://js.51jobcdn.com/in/js/2023/dd/dd_area_translation.json"
        try:
            resp = requests.get(url, timeout=10)
            data_list = resp.json()
            for item in data_list:
                if item.get('value') == city_name:
                    self.city_map[city_name] = item.get('code')
                    return self.city_map[city_name]
        except: pass
        return "000000"

    def clean_text(self, text):
        if not text: return ""
        text = text.replace('<br>', '\n').replace('<br/>', '\n')
        text = re.sub(r'<.*?>', '', text)
        return text.strip()

    def _save_data_to_csv(self, all_datas, output_csv_path):
        """将数据追加到指定的 CSV 文件"""
        if not all_datas:
            return
            
        df = pd.DataFrame(all_datas)
        file_exists = os.path.exists(output_csv_path)
        
        # Append to the CSV
        df.to_csv(output_csv_path, mode='a', index=False, header=not file_exists, encoding='utf-8-sig')
        print(f"\n[Drission Scraper] 成功追加 {len(df)} 条数据到 {output_csv_path}")

    def _scrape_page(self, keyword, city, page_num, output_csv_path):
        city_code = get_city_code(city)
        print(f"[Drission Scraper] 进程启动: {city}({city_code}) - {keyword} - 第 {page_num} 页")
        
        # 1. 启动监听
        self.page.listen.start('search-pc')
        
        # 2. 构建 URL 并访问
        base_url = f'https://we.51job.com/pc/search?keyword={urllib.parse.quote(keyword)}&jobArea={city_code}&searchType=2'
        self.page.get(base_url)
        time.sleep(random.uniform(2, 4)) # 等待初始加载

        # 3. 跳转到目标页面
        if page_num > 1:
            for i in range(1, page_num):
                next_btn = self.page.ele('x://button[contains(@class, "btn-next")]', timeout=5)
                if next_btn and 'disabled' not in next_btn.attr('class'):
                    self.page.scroll.to_see(next_btn)
                    next_btn.click()
                    print(f"[Drission Scraper] 跳转到第 {i+1} 页...")
                    time.sleep(random.uniform(3, 5))
                else:
                    print(f"[Drission Scraper] 未找到或无法点击下一页按钮，目标页面 {page_num} 不可达。")
                    return
        
        # 4. 提取数据
        res = self.page.listen.wait(timeout=15)
        items = []
        
        # --- 方案 A: 尝试从拦截器获取 ---
        if res and res.response.body:
            try:
                body = res.response.body
                if isinstance(body, str):
                    body = json.loads(body)
                items = body.get('resultbody', {}).get('job', {}).get('items', [])
                if items: print(f"[Drission Scraper] 成功从拦截包提取 {len(items)} 条岗位。")
            except Exception: pass
        
        # --- 方案 B: 如果 A 失败，尝试从 DOM 的 window 变量提取 ---
        if not items:
            print("[Drission Scraper] 拦截包内容异常，尝试从页面源码提取...")
            try:
                html = self.page.html
                match = re.search(r'window\.__SEARCH_RESULT__\s*=\s*(\{.*?\});', html)
                if match:
                    json_data = json.loads(match.group(1))
                    items = json_data.get('job', {}).get('items', [])
                    if items: print(f"[Drission Scraper] 成功从页面源码正则提取 {len(items)} 条岗位。")
            except Exception: pass

        # --- 解析数据 ---
        scraped_data = []
        if items:
            for item in items:
                href = item.get('jobHref')
                if not href: continue
                scraped_data.append({
                    'Job': item.get('jobName'),
                    'Salary': item.get('provideSalaryString'),
                    'Company': item.get('fullCompanyName'),
                    'Location': item.get('jobAreaString'),
                    'JobDetail': self.clean_text(item.get('jobDescribe', '')),
                    'Welfare': "|".join(item.get('jobWelfare', [])) if isinstance(item.get('jobWelfare'), list) else item.get('jobWelfare', ''),
                    'Education': item.get('degreeString'),
                    'Experience': item.get('workYearString'),
                    'UpdateDate': item.get('updateDateTime'),
                    'Href': href
                })
        else:
            print(f"[Drission Scraper] 第 {page_num} 页未能提取到任何有效数据。")

        # 5. 保存数据
        self._save_data_to_csv(scraped_data, output_csv_path)
