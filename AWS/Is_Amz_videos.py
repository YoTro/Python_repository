# -*- coding:UTF-8 -*-
# Functions: 获取amazon列表每个listing是否含有videos
import re
import os
import requests
import datetime
import xlrd
import xlsxwriter


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
    fn="/Users/Administrator/Desktop/BSR(Squirrels)-99-US-221213.xls"
    data=xlrd.open_workbook(fn) # 打开工作薄
    sheets=data.sheets()
    fp='/Users/Administrator/Desktop/amz_videos.xls'
    workbook1=xlsxwriter.Workbook(fp)
    sheet1=workbook1.add_worksheet()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print(p,z[p])
    try:
        sheet_index=int(input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print('The digital is wrong,plz input a correct number')
    except Exception as e:
        print(str(e))
        t=1
    table=data.sheet_by_index(t-1) # 读取指定sheet
    cols=table.ncols
    rows=table.nrows
    print("{0}'s rows ,cols are {1},{2}".format(z[t],rows,cols))
    first_sheet=table.row_values(0)
    url_index=first_sheet.index('URL')#返回第一行URL的列数
    URL=table.col_values(url_index) # 读取指定列（该列含有URL）
    for i in range(rows-1):
        try:
            u=URL[i+1]
            print(i,u)
            is_v = is_AMZ_V(u)
            sheet1.write(i, 0, str(is_v))
            sheet1.write(i, 1, u)
        except Exception as e:
            print(str(e))
            
    workbook1.close()
    end=datetime.datetime.now()
    t=end-start
    print('已将照片存入Excel {0}中\n总共用时：{1}s'.format(fp,t))	
if  __name__=="__main__":
	main()
