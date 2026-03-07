# -*- coding: utf-8 -*-
import os
import json
import requests
from tqdm import tqdm

def proxies(proxy_url=None):
    if proxy_url:
        return {'http': proxy_url, 'https': proxy_url}

    filename = '../../data/proxies.txt'
    # 如果本地有缓存，直接读取，不再请求网络
    if os.path.exists(filename):
        try:
            with open(filename, 'r') as f:
                return eval(f.read())
        except:
            pass

    url = "http://proxylist.fatezero.org/proxy.list"
    proxies_dic = {'http': [], 'https': []}
    
    print(f"尝试从 {url} 更新代理列表...")
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            for line in response.text.splitlines():
                json_obj = json.loads(line)
                item = f"{json_obj['host']}:{json_obj['port']}"
                if json_obj['type'] == 'http':
                    proxies_dic['http'].append(item)
                else:
                    proxies_dic['https'].append(item)
            
            with open(filename, 'w') as f:
                f.write(str(proxies_dic))
    except Exception as e:
        print(f"无法获取在线代理列表 (跳过): {e}")
    
    # 如果最终还是空的，给个默认值防止程序崩溃
    if not proxies_dic['http']:
        proxies_dic['http'] = ["127.0.0.1:8001"]
    if not proxies_dic['https']:
        proxies_dic['https'] = ["127.0.0.1:8001"]
        
    return proxies_dic
