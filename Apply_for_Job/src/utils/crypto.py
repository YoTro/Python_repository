# -*- coding: utf-8 -*-
import hmac
from hashlib import sha256
from urllib.parse import urlencode

def get_51job_sign(params: dict, key: str) -> str:
    """
    计算51job接口签名
    :param params: 请求参数字典
    :param key: 加密密钥
    :return: 16进制签名字符串
    """
    encoded_params = urlencode(params)
    message = f'/open/noauth/search-pc?{encoded_params}'
    return hmac.new(
        key.encode('utf-8'),
        message.encode('utf-8'),
        sha256
    ).hexdigest()
