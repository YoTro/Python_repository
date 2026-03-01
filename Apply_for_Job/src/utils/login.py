# -*- coding: utf-8 -*-
import json
import time
import os
from DrissionPage import ChromiumPage, ChromiumOptions

def get_verified_cookies(output_file='../../config/51job_cookies.json'):
    """
    启动浏览器通过 51job 的 WAF 验证并获取 Cookies
    """
    # 配置浏览器选项
    co = ChromiumOptions()
    # 自动获取空闲端口，避免冲突
    co.auto_port() 
    # 使用无头模式（如果需要看到界面手动滑块，请注释掉这一行）
    # co.headless() 
    # 阻止指纹检测
    co.set_argument('--disable-blink-features=AutomationControlled')
    
    print("正在启动浏览器进行 WAF 验证...")
    page = ChromiumPage(co)
    
    try:
        # 1. 开启拦截
        page.listen.start('cupidjob.51job.com/open/noauth/search-pc')
        
        target_url = 'https://we.51job.com/pc/search?keyword=自动化测试&searchType=2'
        page.get(target_url)
        
        # 2. 检测并处理滑块
        if page.ele('#nc_1_n1z'):
            print("检测到滑块，请在浏览器中手动完成...")
            page.wait.ele_deletion('#nc_1_n1z', timeout=60)
        
        # 等待数据包
        res = page.listen.wait()
        print(f"截获到数据包: {res.url}")
        
        # 提取关键信息
        intercepted = {
            'headers': dict(res.request.headers),
            'params': dict(res.request.params),
            'cookies': {c['name']: c['value'] for c in page.cookies()},
            'user_agent': page.user_agent
        }
        
        # 4. 保存
        with open(output_file, 'w') as f:
            json.dump(intercepted, f)
        print(f"拦截数据已保存到 {output_file}")
        return intercepted

    except Exception as e:
        print(f"验证过程中出错: {e}")
        return {}
    finally:
        page.quit()
