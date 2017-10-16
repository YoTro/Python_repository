# -*- coding:UTF-8 -*-
#Python Version:2.7.13
#auth:Toryun
#Date:17/8/24
#Function:Find results of the words in the Amazon search,return to the excel
import re,requests,xlwt,xlrd,string,datetime,os
from xlutils.copy import copy
def get_url(i,url):
    '''利用代理IP查询Amazon搜索，返回结果'''
    try:
        proxies={'HTTP': 'HTTP://183.144.214.132:3128', 'HTTPS': 'HTTPS://219.149.46.151:3129'}#免费IP地址*http://www.xicidaili.com*
        amazon='https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords='
        u=str(url)#将列表转换成字符串
        u1=u.replace(' ','+')
        url=amazon+u1
        print i,url
        _headers=requests.head(url)#得到request头部
        r=requests.get(url,headers=_headers,proxies=proxies)#get
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
    except Exception,x:
        print str(x)
    start=datetime.datetime.now()
    data=xlrd.open_workbook(curl)
    table=data.sheet_by_index(0)
    URL=table.col_values(0)
    rows=table.nrows
    print rows
    data1=xlwt.Workbook()
    table1=data1.add_sheet(u'1')
    for i in range(rows-1):
        url=URL[i+1]
        m=get_url(i,url)
        table1.write(i,0,url)
        table1.write(i,1,m)
    data1.save('c:\\4.xls')
    end=datetime.datetime.now()
    t=end-start
    print 'Total time: {0} s'.format(t)#打印总用时（从读取文件到保存文件）
if __name__=='__main__':
    main()
