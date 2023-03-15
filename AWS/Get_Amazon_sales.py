# -*- coding:UTF-8 -*-
#Python Version:3.8.1
#auth:Toryun
#Date:23/3/15
#Function:Find the sales in the Amazon search,return to the excel
import re
import os
import requests
from Amazon_Utils import xlwt, is_TTD, retry, excel_bulit, Get_ASINlists, Get_Exceldata

def Get_sales(keyword, page):
    url='https://www.amazon.com/s?k={}&page={}'.format(keyword, page)
    headers={
        "Host": "www.amazon.com",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "device-memory": "8",
        "sec-ch-device-memory": "8",
        "dpr": "2",
        "sec-ch-dpr": "2",
        "viewport-width": "1920",
        "sec-ch-viewport-width": "1920",
        "rtt": "50",
        "downlink": "10",
        "ect": "4g",
        "sec-ch-ua": 'Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "macOS",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://www.amazon.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.8"
        }#得到request头部

    r = requests.get(url,headers=headers)
#    with open('./t.html', 'wb+') as f:
#        f.write(r.content)
#        f.close()
    i = 0
    while is_TTD(r.text):
        r = requests.get(url,headers=headers)
        i += 1
        if i == 5:
            break
    m = re.findall('dp\/(B[A-Z0-9]{9}).*?a-row a-size-base\"><span class=\"a-size-base a-color-secondary\">(.*?)<\/span>', r.text)#匹配result
    return m

if __name__ == '__main__':
    keyword = "outdoor rug"
    page = 1
    print(Get_sales(keyword, page))