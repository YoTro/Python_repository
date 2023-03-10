# -*- coding:UTF-8 -*-
#Python Version:2.7.13
#auth:Toryun
#Date:17/8/24
#Function:Find rank of the words in the Amazon search,return to the excel
import re
import os
import time
import requests
from Amazon_Utils import xlwt, retry, excel_bulit, Get_Amazonlists, Get_Exceldata
def is_TTD(f):
    '''是否被Amazon屏蔽请求变狗'''
    temp = f
    if(re.findall("(_TTD_\.jpg)", f)):
        return 1
    else:
        return 0
    return temp
@retry
def get_result(keyword):
    '''返回搜索产品数量'''
    #proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费IP地址*http://www.xicidaili.com*
    url='https://www.amazon.com/s?k={}&page=1'.format(keyword)
    _headers={
        "Host": 
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Referer":
        "https://www.amazon.com/",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":
        "en-US,en;q=0.8",
        "Accept-Encoding":  
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Cache-Control":"max-age=0",
        "Upgrade-Insecure-Requests":"1"
        }#得到request头部

    r=requests.get(url,headers=_headers)#通过代理得到请求内容
#    with open('./t.html', 'wb+') as f:
#        f.write(r.content)
#        f.close()
    if is_TTD(r.text):
        print("Blocked!")
        return None
    else:
        m=re.findall('a-section a-spacing-small a-spacing-top-small\">\s+<span>.*?over (.*?)results for',r.text)#匹配result
        print(m[0]) #返回第一个值
        return m[0]
def get_rank_keyword(asins, keyword, page):
    '''返回关键词的页面排名'''
    url='https://www.amazon.com/s?k={}&page={}'.format(keyword, page)
    _headers={
        "Host": 
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Referer":
        "https://www.amazon.com/",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":
        "en-US,en;q=0.8",
        "Accept-Encoding":  
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Cache-Control":"max-age=0",
        "Upgrade-Insecure-Requests":"1"
        }#得到request头部

    r=requests.get(url,headers=_headers)#通过代理得到请求内容
    if is_TTD(r.text):
        print("Blocked!")
        return None
    else:
        t = {}
        for i in range(len(asins)):
            result = re.findall("data-asin=\"{}\" data-index=\"(\d+)\"".format(asins[i]), r.text)#匹配result
            if len(result) != 0:
                t[asins[i]] = [page, int(result[0])-1]
        return t
    
if __name__ == '__main__':
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table = excel_bulit(workbook, "1")
    table.write(0, 0, "ASIN")
    table.write(0, 1, "keyword")
    table.write(0, 2, "页数")
    table.write(0, 3, "单页排名")
    asins = ["B08B6FJPK5","B08B6FQVZZ","B08HQJ6CV6","B08HQL4Q3G","B08HQPYMH3","B08JLGNPVX","B0BW3Y3W91","B0BW442C9T"]
    file_save = "./keywordrank.xls"
    fn = "./kw.xls"
    keywords = Get_Exceldata(fn, '关键词')
    i = 1
    for keyword in keywords:
        print(keyword)
        t = get_result(keyword)
        if t:
            t = int(t.replace(",",""))
            for page in range(1, (t//48)+2):
                d = get_rank_keyword(asins, keyword, page)
                print(page, d)
                if d:
                    for k in d:
                        table.write(i, 0, k)
                        table.write(i, 1, keyword)
                        table.write(i, 2, d[k][0])
                        table.write(i, 3, d[k][1])
                        i+=1
                if page > 7:
                    print("Result is over 7th page")
                    break
    workbook.save(file_save)
    print("Saved to {}".format(os.path.abspath(file_save)))
