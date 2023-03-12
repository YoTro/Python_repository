# -*- coding:UTF-8 -*-
import re
import os
import math
import xlrd
import xlwt
import html
import random
import platform
import requests
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
def is_bash():
    '''判断使用平台'''
    if platform.system().lower() == "windows":
        return 0
    if platform.system().lower() == 'darwin' or platform.system().lower() == 'linux':
        return 1
def is_TTD(url, f):
    '''是否被Amazon屏蔽请求变狗'''
    temp = f
    if(re.findall("(_TTD_\.jpg)", f)):
        if is_bash():
            a = os.system('''curl -s '{}' \
  -H 'authority: www.amazon.com' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
  -H 'accept-language: en-US,en;q=0.9' \
  -H 'sec-ch-ua: "Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "macOS"' \
  -H 'sec-fetch-dest: document' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-site: none' \
  -H 'sec-fetch-user: ?1' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36' \
  --compressed > ./t.html'''.format(url))
        else:
            a = os.system('''curl -s "{}" ^
  -H "authority: www.amazon.com" ^
  -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" ^
  -H "accept-language: en-US,en;q=0.9" ^
  -H "sec-ch-ua: ""Not_A Brand"";v=""99"", ""Google Chrome"";v=""109"", ""Chromium"";v=""109""" ^
  -H "sec-ch-ua-mobile: ?0" ^
  -H "sec-ch-ua-platform: ""macOS""" ^
  -H "sec-fetch-dest: document" ^
  -H "sec-fetch-mode: navigate" ^
  -H "sec-fetch-site: none" ^
  -H "sec-fetch-user: ?1" ^
  -H "upgrade-insecure-requests: 1" ^
  -H "user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36" ^
  --compressed > t.html'''.format(url))
        if a == 0:
            with open('./t.html', 'r') as f:
                temp = f.read()
                f.close()
    return temp
def matchQA(url, headers, f, QA_links, QAs):
    questions_number = re.findall("(\d+) questions", f)
    print(questions_number)
    if len(questions_number) != 0:
        qn = math.ceil(int(questions_number[0])/10)
        for i in range(1, qn+1):
            print(i)
            url_i = url + str(i)
            r = requests.get(url_i, headers = headers, timeout = 5)
            temp = is_TTD(url_i, r.text)
            QA_link = re.findall("askInlineAnswers\" id=\"(.*?)\">", temp)
            QA_links.append(QA_link)
            for i in range(len(QA_link)):
                url_i_i = 'https://www.amazon.com/ask/questions/'+QA_link[i]
                r = requests.get(url_i_i, headers = headers, timeout = 5)
                temp = is_TTD(url_i_i, r.text)
                QA = re.findall("\s+<span>(.*?)<\/span>", temp)
                QAs.append(QA)
    return QA_links, QAs
def retry(func):
    def wrap(*args):
        i = 0
        r  = None
        while i<5:
            try:
                r = func(*args)
                if r:
                   i = 5 
            except Exception as e:
                i+=1
        return r
    return wrap
@retry
def Get_Amazon_QA(asin):
    QAs = []
    QA_links = []
    questions_number = []
    url = "https://www.amazon.com/ask/questions/asin/{}/".format(asin)
    headers={
    "host": "www.amazon.com",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-language": "en-US,en;q=0.9",
    "sec-ch-ua": "\"Not_A Brand\";v=\"99\", \"Google Chrome\";v=\"109\", \"Chromium\";v=\"109\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"macOS\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1"
  }
    proxies = {'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}
    r = requests.get(url, headers = headers, proxies = proxies, timeout = 5)    
    temp = is_TTD(url, r.text)
    QA_links, QAs = matchQA(url, headers, temp, QA_links, QAs)
    return QA_links, QAs

def main():
    file_save='./AMZQA.xls'
    fn = './OR.xls'
    asinlist = Get_ASINlists(fn)
    workbook = xlwt.Workbook(encoding = 'utf-8')
    for i in range(len(asinlist)):
        table = excel_bulit(workbook, asinlist[i])
        print(asinlist[i])
        QA_links, QAs = Get_Amazon_QA(asinlist[i])
        #print(QA_links)
        k = 0
        for j in range(len(QA_links)):
            for m in range(len(QA_links[j])):
                table.write(k,0,QA_links[j][m])
                for n in range(len(QAs[k])):
                    table.write(k,n+1, html.unescape(QAs[k][n]))
                k += 1
    workbook.save(file_save)
    print("Saved to {}".format(file_save))
if __name__ == '__main__':
    main()