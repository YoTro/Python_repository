# -*- coding:UTF-8 -*-
#Python Version:2.7.13
#auth:Toryun
#Date:17/8/24
#Function:Find results of the words in the Amazon search,return to the excel
import re,requests,xlwt,xlrd,string,datetime,os,time
from xlutils.copy import copy
def get_url(i,url):
    '''从工作簿中获取Keyword利用代理IP查询Amazon搜索，返回结果'''
    try:
        proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费IP地址*http://www.xicidaili.com*
        amazon='https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords='
        u=str(url)#将列表转换成字符串
        u1=u.replace(' ','+')
        url=amazon+u1
        print i,url
        _headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0",
"Referer":
"https://www.amazon.com/",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Accept-Language":
"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1"
}#得到request头部
        
        r=requests.get(url,headers=_headers,proxies=proxies)#通过代理得到请求内容
        time.sleep(2)#延迟2秒
        m=re.findall(r'a-size-base a-spacing-small a-spacing-top-small a-text-normal">(.*?)<span>',r.content)#匹配result
        print m[0] #返回第一个值
        return m[0]
    except Exception,e:
        print str(e)
def main():
    try:
        curl=str(raw_input("Input positon of  the file you want to search(it's like 'c:\\d.xls'):\n"))#输入关键词文件所在位置
        t=os.path.exists(curl)#判断文件是否存在
        while t==False:
            curl=raw_input("Your url is not correct,plz input a right again:\n")#如果不存在，再次输入
            t=os.path.exists(curl)
    except Exception,x:
        print str(x)
    start=datetime.datetime.now()
    data=xlrd.open_workbook(curl)
    table=data.sheet_by_index(0)
    key=table.row_values(0)
    key_index=key.index('Keyword')
    Keywords=table.col_values(key_index)
    rows=table.nrows
    print "Workbook's rows is %d"%rows
    data1=xlwt.Workbook()
    table1=data1.add_sheet(u'1')
    for i in xrange(rows-1):
        url=Keywords[i+1]
        m=get_url(i,url)
        table1.write(i,0,url)
        table1.write(i,1,m)
    data1_url='c:\\4.xlsx'
    data1.save(data1_url)
    end=datetime.datetime.now()
    t=end-start
    print '您用{0}所搜索的关键词已经存储到{1}\nTotal time: {2} s'.format(curl,data1_url,t)#打印文件原地址和新存储地址，总用时（从读取文件到保存文件）
if __name__=='__main__':
    main()
