import os
import json
import requests
from tqdm import tqdm

def proxies():

  url = "http://proxylist.fatezero.org/proxy.list"

  payload={}
  headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Proxy-Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
  }

  response = requests.request("GET", url, headers=headers, data=payload, timeout=15)
  proxies_http = []
  proxies_https = []
  proxies_dic = {}
  for line in tqdm(response.text.splitlines()):
      json_obj = json.loads(line)
      item = json_obj['host']+':'+str(json_obj['port'])
      proxies={}
      proxies[json_obj['type']]=json_obj['type']+'://'+item
      try:
        headers = {
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
          'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'Cache-Control': 'max-age=0',
          'Connection': 'keep-alive',
          'Upgrade-Insecure-Requests': '1',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
        }
        url = json_obj['type']+'://httpbin.org/ip'
        response = requests.request("GET", url, headers=headers, proxies=proxies, data=payload, timeout=5).json()
        print(proxies, response)
        # 检查匿名性
        if response['origin'] == json_obj['host'] and json_obj['type'] == 'http':
            if item not in proxies_http:
                proxies_http.append(item)
        if response['origin'] == json_obj['host'] and json_obj['type'] == 'https':
            if item not in proxies_https:
                proxies_https.append(item)
      except Exception as e:
        print(e)
        pass
  proxies_dic['http'] = proxies_http
  proxies_dic['https'] = proxies_https
  if proxies_http == []:
    proxies_dic['http'] = ["127.0.0.1:8001"]
  if proxies_https == []:
    proxies_dic['https'] = ["127.0.0.1:8001"]
  print(proxies_dic)
  filename = './proxies.txt'
  if not os.path.exists(filename):
    with open(filename, 'w') as f:
        f.write(str(proxies_dic))
        f.close()
  return proxies_dic
#proxies()