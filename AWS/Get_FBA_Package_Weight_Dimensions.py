import re
import os
import math
import time
import html
import xlrd
import xlwt
import json
import random
import requests
from hyper.contrib import HTTP20Adapter
from Amazon_Utils import excel_bulit, Get_ASINlists

proxies_list80 = [
                "184.60.66.122",
                "34.23.45.223",
                "162.144.236.128",
                "108.170.12.13",
                "64.251.22.20",
                "138.91.159.185",
                "162.144.233.16",
                "167.99.174.59",
                "54.86.198.153",
                "68.183.143.134",
                "45.77.198.163",
                "162.240.75.37",
                "104.45.128.122",
                "191.101.1.116",
                "74.208.177.198",
                "164.92.108.63",
                "93.188.161.84",
                "52.88.105.39",
                "104.225.220.233",
                "143.110.232.177",
                "34.239.204.118",
                "209.126.6.159",
                "45.79.17.203",
                "104.215.127.197",
                "34.75.202.63",
                "147.182.142.189",
                "137.184.232.148",
                "85.239.242.23",
                "192.236.160.186",
                "142.11.222.22",
                "24.199.82.12",
                "129.153.163.10",
                "50.16.22.43",
                "65.109.84.104",
                "74.208.205.5",
                "65.108.9.181",
                "34.239.204.118",
                "103.216.160.163",
                "103.216.160.164",
                "103.216.160.160",
                "34.87.103.220",
                "103.216.160.167"
            ]
proxies_list1994 = ["216.127.188.18",
                    "198.74.98.18",
                    "198.52.105.249",
                    "173.82.102.194",
                    "72.11.130.145",
                    "72.44.76.76",
                    "198.52.114.146",
                    "72.44.68.249",
                    "104.194.232.179",
                    "104.129.41.2",
                    "170.178.193.106",
                    "173.82.20.178",
                    "72.44.67.178",
                    "173.82.46.138",
                    "198.52.115.114",
                    "173.82.43.108",
                    "173.44.42.66",
                    "198.211.55.167"
                    ]
#proxies = {'HTTP': 'HTTP://{}:80'.format(random.choice(proxies_list80))}#, 'HTTPS': 'HTTPS://{}:1994'.format(random.choice(proxies_list1994))}
proxies = random.choice(proxies_list80)
def retry(func):
    def wrap(*args):
        i = 0
        r  = None
        while i<5:
            try:
                r = func(*args)
                if len(r) != 0:
                   i = 5 
            except Exception as e:
                i+=1
        return r
    return wrap

def Get_weight_dimesions(asin):
    a = os.system('''curl -s 'https://sellercentral.amazon.com/rcpublic/getadditionalpronductinfo?countryCode=US&asin={}&fnsku=&searchType=GENERAL&locale=en-US' \
  -H 'authority: sellercentral.amazon.com' \
  -H 'accept: */*' \
  -H 'accept-language: zh-CN,zh;q=0.9,en;q=0.8' \
  -H 'cookie: session-id=132-1594050-2630761; i18n-prefs=USD; ubid-main=133-9093459-6797703; csm-hit=tb:s-RD27W807S27J306PMDRN|1677155166857&t:1677155166871&adb:adblk_yes; session-id-time=2082787201l; lc-main=en_US; session-token=3U342I6z4xu9pIKvXj/jEB8fxtttcgKVe/PTCxrlHNtirjv70uLvc2sWuPCySPtGv07DYrWPFr3zazDtBf/0JXQYxBzGji3UmAZTVJI7qibvrnu00XXRdFdZpA+Ycz4IFhG8ikiALaNMWLvZOUUccCZ9cUBM/30YmsewMY1Zilx+fd7dqeHGaIThYvezLcNtmRhvy07BFd44yJtWpQsMszNXZHyiU+mep8TChN16z4E=' \
  -H 'referer: https://sellercentral.amazon.com/fba/profitabilitycalculator/index?lang=en_US' \
  -H 'sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-origin' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36' \
  --compressed > ./t.json'''.format(asin))
    we = None
    d = None
    if a == 0:
        with open("./t.json", "rb+") as f:
            i = 0
            while i<3:
                try:
                    r = json.load(f)
                    weight, l, w, h = 0, 0, 0, 0
                    weightUnit = 'pounds'
                    dimensionUnit = 'inches'
                    if 'length' in r['data'].keys():
                        l = r['data']['length']
                    if 'width' in r['data'].keys():
                        w = r['data']['width']
                    if 'height' in r['data'].keys():
                        h = r['data']['height']
                    if 'dimensionUnit' in r['data'].keys():
                        dimensionUnit = r['data']['dimensionUnit']
                    if 'weight' in r['data'].keys():
                        weight = r['data']['weight']
                        weightUnit = r['data']['weightUnit']
                    we = [weight, weightUnit]
                    d = [l, w, h, dimensionUnit]
                    i = 5
                except ValueError as e:
                    print(e)
                    i += 1
    return we,d

def Productmatch(asin, proxies):
    url = "https://sellercentral.amazon.com/rcpublic/productmatch?searchKey={}&countryCode=US&locale=en-US".format(asin)
    headers={
        "Host":
        "sellercentral.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Accept":
        "*/*",
        "Accept-Language":
        "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding":
        "gzip, deflate, br",
        "sec-fetch-mode": "cors",
        "sec-ch-ua-platform": "macOS",
        "sec-fetch-site": "same-origin",
        "reffer": "https://sellercentral.amazon.com/fba/profitabilitycalculator/index?lang=en_US",
        "Upgrade-Insecure-Requests":"1"
    }
    params = {
        "countryCode":
        "US",
        "searchKey":
        "{}".format(asin),
        "locale":
        "en-US"
    }
    r = requests.get(url, headers = headers, proxies = proxies, data = params)
    w = {}
    with open('./t.html',"wb+") as f:
        f.write(r.content)
        f.close()
    print(r.status_code)
    if r.status_code == 200:
        w = r.json()['data']
    
    return w

def main():
    file_save='./AMZWD.xls'
    fn = './OR.xls'
    asinlist = Get_ASINlists(fn)
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table = excel_bulit(workbook, '1')
    k = len(asinlist)
    print("{} ASINs".format(k))
    for i in range(k):
        print(asinlist[i])
        weight, dimensions = Get_weight_dimesions(asinlist[i])
        table.write(i, 0, asinlist[i])
        if weight:
            table.write(i, 1, weight[0])
            table.write(i, 2, weight[1])
            table.write(i, 3, dimensions[0]*dimensions[1]*dimensions[2])
            table.write(i, 4, dimensions[3])
    workbook.save(file_save)
    print("Saved to {}".format(file_save))
if __name__ == '__main__':
    main()