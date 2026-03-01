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

class JobSpider:
    def __init__(self, headless=False):
        co = ChromiumOptions()
        co.set_argument('--disable-blink-features=AutomationControlled')
        if headless: co.headless()
        self.page = ChromiumPage(co)
        self.city_map = {}

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

    def run(self, keyword, city="深圳", pages=2):
        city_code = self.get_city_code(city)
        print(f"抓取进程启动: {city}({city_code}) - {keyword}")
        
        # 1. 启动监听
        self.page.listen.start('search-pc')
        
        base_url = f'https://we.51job.com/pc/search?keyword={urllib.parse.quote(keyword)}&jobArea={city_code}&searchType=2'
        self.page.get(base_url)
        
        all_datas = []
        seen_links = set()

        for p in range(1, pages + 1):
            print(f"\n--- 正在处理第 {p} 页 ---")
            if p == 1: self.page.refresh()
            
            res = self.page.listen.wait(timeout=10)
            items = []
            
            # --- 方案 A: 尝试从拦截器获取 ---
            if res and res.response.body:
                try:
                    body = res.response.body
                    if isinstance(body, str):
                        body = json.loads(body)
                    items = body.get('resultbody', {}).get('job', {}).get('items', [])
                    if items: print(f"成功从拦截包提取 {len(items)} 条岗位。")
                except: pass
            
            # --- 方案 B: 如果 A 失败，尝试从 DOM 的 window 变量提取 (针对 51job 重大 UI 调整后的备份逻辑) ---
            if not items:
                print("拦截包内容异常，尝试从页面源码提取...")
                try:
                    # 51job 经常将初始 JSON 结果存在脚本变量中
                    html = self.page.html
                    match = re.search(r'window\.__SEARCH_RESULT__\s*=\s*(\{.*?\});', html)
                    if match:
                        json_data = json.loads(match.group(1))
                        items = json_data.get('job', {}).get('items', [])
                        if items: print(f"成功从页面源码正则提取 {len(items)} 条岗位。")
                except: pass

            # --- 解析数据 ---
            if items:
                for item in items:
                    href = item.get('jobHref')
                    if not href or href in seen_links: continue
                    all_datas.append({
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
                    seen_links.add(href)
            else:
                print(f"第 {p} 页未能提取到任何有效数据。")

            # --- 翻页 ---
            if p < pages:
                next_btn = self.page.ele('x://button[contains(@class, "btn-next")]')
                if next_btn and 'disabled' not in next_btn.attr('class'):
                    self.page.scroll.to_see(next_btn)
                    next_btn.click()
                    time.sleep(random.uniform(3, 5))
                else: break

        if all_datas:
            df = pd.DataFrame(all_datas)
            df.to_csv('../../data/job51_jobs.csv', index=False, encoding='utf-8-sig')
            print(f"\n全部抓取完成！保存 {len(df)} 条数据到 job51_jobs.csv")
        else:
            print("\n抓取失败，建议检查是否触发了高频验证码。")
            
        self.page.quit()
