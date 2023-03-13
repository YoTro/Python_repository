# -*- coding:UTF-8 -*-
# Functions: 获取amazon列表每个listing是否含有videos
import re
import os
import xlwt
import requests
import datetime
from Amazon_Utils import Get_Exceldata, excel_bulit

def is_AMZ_V(url):
	header = {
		"Host":
		"www.amazon.com",
		"User-Agent":
		"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
		"Accept":
		"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9v",
		"Accept-Language":
		"zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
		"Accept-Encoding":
		"gzip, deflate, br",
		"Connection":
		"keep-alive",
		"Upgrade-Insecure-Requests":"1"
		}
	proxies = {'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}

	try:
		r = requests.get(url, headers = header, proxies = proxies)
	except requests.exceptions.RequestException as e:
		raise SystemExit(e)
	if(r.status_code == 200):
		is_videos = re.findall("<span class=\"a-size-mini a-color-secondary video-count a-text-bold a-nowrap\">(.*?)<\/span>",r.text)
		print(is_videos)
		return is_videos
	else:
		return "{}".format(r.status_code)
		
def main():
    start=datetime.datetime.now()
    fn="./url.xls"
    file_save = "./amzvide.xls"
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table = excel_bulit(workbook, "1")
    URL = Get_Exceldata(fn, "URL")
    rows = len(URL)
    for i in range(rows):
        try:
            u=URL[i]
            print(i,u)
            is_v = is_AMZ_V(u)
            table.write(i, 0, str(is_v))
            table.write(i, 1, u)
        except Exception as e:
            print(str(e))   
    workbook.save(file_save)
    end=datetime.datetime.now()
    t=end-start
    print('已将照片存入Excel {0}中\n总共用时：{1}s'.format(os.path.abspath(file_save),t))	
if  __name__=="__main__":
	main()
