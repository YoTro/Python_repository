# zhipin_scraper.py
# 建议使用火狐浏览器进行调试
import sys
import pandas as pd
from DrissionPage import ChromiumPage, SessionPage
from tqdm import tqdm
from time import sleep
import random
import time
import json

def scrape_zhipin(query: str, city_code: str, output_filename: str, max_pages: int = 20, proxy_url=None):
    """
    使用DrissionPage抓取Boss直聘职位信息

    :param query: 职位关键词，例如 "亚马逊运营"
    :param city_code: 城市代码，例如 "101280600" (深圳)
    :param output_filename: 输出的CSV文件路径
    :param max_pages: 最大抓取页数20
    :param proxy_url: 代理地址
    """
    import os
    os.environ['no_proxy'] = '127.0.0.1,localhost'
    
    if proxy_url:
        print(f"[WARN] Zhipin 模式连接到本地 9222 端口的现有浏览器实例。")
        print(f"[WARN] 无法直接为其设置代理 ({proxy_url})，请在启动浏览器时带上 --proxy-server 参数。")

    job_list = []
    page = ChromiumPage(addr_or_opts='localhost:9222')
    tab = page.new_tab()
    
    if not tab:
        print("错误: 未能创建新的标签页。")
        page.quit()
        return

    try:
        # 启动网络监听
        tab.listen.start('wapi/zpgeek/search/joblist.json')

        # 导航到第一页并进行登录检查
        url = f"https://www.zhipin.com/web/geek/job?query={query}&city={city_code}&page=1"
        tab.get(url)
        print("正在等待第一页数据并检查登录状态...")
        initial_packet = tab.listen.wait(timeout=25)

        # -- 更健壮的检查 --
        if not (initial_packet and initial_packet.response and initial_packet.response.body and 
                initial_packet.response.body.get('zpData', {}).get('jobList')):
            print("\n错误：获取第一页数据失败或登录状态异常。")
            if initial_packet and initial_packet.response and initial_packet.response.body:
                 print(f"API消息: {initial_packet.response.body.get('message')}")
            try:
                tab.save('get_first_page_failed.png')
                print("已保存截图 `get_first_page_failed.png` 供分析。")
            except Exception as e:
                print(f"保存截图失败: {e}")
            return # 直接退出函数

        # 处理第一页数据
        initial_data = initial_packet.response.body
        job_list.extend(initial_data['zpData']['jobList'])
        print("登录状态正常，已获取第一页数据。")
        if not initial_data['zpData'].get('hasMore', True):
            print("API响应表明没有更多职位了，无需滚动。")
            max_pages = 1

        # 滚动加载后续页面
        for i in tqdm(range(2, max_pages + 1), desc="滚动加载列表"):
            tab.scroll.to_bottom()
            sleep(random.uniform(2, 4))
            packet = tab.listen.wait(timeout=25)
            if packet and packet.response.body.get('zpData', {}).get('jobList'):
                data = packet.response.body
                job_list.extend(data['zpData']['jobList'])
                if not data['zpData'].get('hasMore', True):
                    print(f"\n第 {i} 次加载后，API响应表明没有更多职位了，抓取结束。")
                    break
            else:
                print(f"\n在第 {i} 次加载时等待API响应超时或数据异常，列表抓取终止。")
                break

        # --- 使用API获取职位描述 ---
        print(f"\n职位列表抓取完成，共 {len(job_list)} 条。开始通过API获取详细职位描述...")
        # ----------------- 建立映射 -----------------
        job_map = {job['encryptJobId']: job for job in job_list}

        # ----------------- 监听 detail.json -----------------
        tab.listen.start('wapi/zpgeek/job/detail.json')
        cards = tab.eles('.job-card-wrap')
        print(f"检测到 {len(cards)} 张职位卡片")

        for card in tqdm(cards, desc="获取岗位职责"):
            try:
                card.click()  # 触发 detail.json 请求
                packet = tab.listen.wait(timeout=10)
                if not (packet and packet.response and packet.response.body and packet.response.body.get('zpData')):
                    continue

                detail = packet.response.body['zpData']
                job_info = detail.get('jobInfo', {})
                brand_info = detail.get('brandComInfo', {})
                encrypt_id = job_info.get('encryptId')
                if encrypt_id not in job_map:
                    continue
                job_map[encrypt_id]['jobDescription'] = job_info.get('postDescription', '')
                job_map[encrypt_id]['jobAddress'] = job_info.get('address', '')
                job_map[encrypt_id]['brandIntroduce'] = brand_info.get('introduce', '')
            except Exception as e:
                print(f"抓取异常: {e}")

            sleep(random.uniform(0.8, 2.5))

        tab.listen.stop()
        
    finally:
        # 确保浏览器在任何情况下都能被关闭
        print("\n所有操作完成，关闭浏览器。")
        #page.quit()

    # --- 数据处理和保存 ---
    if job_list:
        print(f"正在处理 {len(job_list)} 条数据并保存到 CSV 文件...")
        processed_data = []
        for job in tqdm(job_list, desc="处理最终数据"):
            processed_data.append({
                'jobName': job.get('jobName'),
                'salaryDesc': job.get('salaryDesc'),
                'jobDescription': job.get('jobDescription'), # 新增字段
                'jobAddress': job.get('jobAddress', ''),
                'jobLabels': ','.join(job.get('jobLabels', [])),
                'skills': ','.join(job.get('skills', [])),
                'brandName': job.get('brandName'),
                'brandScaleName': job.get('brandScaleName'),
                'brandIndustry': job.get('brandIndustry'),
                'brandIntroduce': job.get('brandIntroduce', ''),
                'areaDistrict': job.get('areaDistrict'),
                'businessDistrict': job.get('businessDistrict'),
                'jobDegree': job.get('jobDegree'),
                'jobExperience': job.get('jobExperience'),
                'jobDetailUrl': f"https://www.zhipin.com/job_detail/{job.get('encryptJobId')}.html"
            })
        
        df = pd.DataFrame(processed_data)
        df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"数据已成功保存到 {output_filename}")
    else:
        print("\n未能获取到任何职位信息。")
