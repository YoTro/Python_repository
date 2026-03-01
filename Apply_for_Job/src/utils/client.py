# -*- coding: utf-8 -*-
import random
import os
from curl_cffi import requests
from .proxy import proxies as get_all_proxies

class BaseClient:
    """基于 curl_cffi 的基础请求客户端"""
    def __init__(self, impersonate="chrome110", use_proxy=False):
        self.session = requests.Session(impersonate=impersonate)
        self._proxies_pool = None
        self.use_proxy = use_proxy # 默认关闭，让流量走系统（TUN模式）

    @property
    def proxies_pool(self):
        if self._proxies_pool is None:
            self._proxies_pool = get_all_proxies()
        return self._proxies_pool

    def get_random_proxy(self):
        """获取随机代理"""
        if not self.use_proxy:
            return None
            
        p_list = self.proxies_pool.get('https') or self.proxies_pool.get('http')
        # 过滤掉默认占位符 127.0.0.1
        clean_list = [p for p in p_list if not p.startswith("127.0.0.1")]
        
        if not clean_list:
            return None
            
        ip_port = random.choice(clean_list)
        return {"http": f"http://{ip_port}", "https": f"http://{ip_port}"}

    def request(self, method, url, **kwargs):
        """执行请求"""
        # 如果 kwargs 里没传 proxy，且开启了 use_proxy，则尝试获取随机代理
        if 'proxy' not in kwargs and self.use_proxy:
            proxy = self.get_random_proxy()
            if proxy:
                kwargs['proxy'] = proxy
                
        # 默认使用 impersonate 自带的版本，如果需要强制，使用整数常量
        # 1 为 HTTP/1.1, 2 为 HTTP/2
        # if 'http_version' not in kwargs:
        #     kwargs['http_version'] = 1

        # 注意：如果 proxy 为 None，curl_cffi 会自动使用系统环境变量中的代理（符合 TUN/系统代理预期）
        return self.session.request(method, url, **kwargs)
