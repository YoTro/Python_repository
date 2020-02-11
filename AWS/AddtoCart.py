# -*- conding:UTF-8 -*-
# Author:Toryun
# Python version:2.7.13
# Date:18/3/25
# Function:Add to Cart of Amazon加入购物车，查询商品数量
import os
import re
import sys
import time
import xlwt
import xlrd
import requests
import datetime
import cookielib
import mechanize
from xlutils.copy import copy
#---------------------------------------------------------------------------------
#Register Amazon（Using mechanize library to simulate browser registration）
#注册亚马逊(使用mechanize库模拟浏览器注册）
def AddtoCart(url):
    '''Using the mechanize lib simulation browser to large quantity register Amazon accounts使用mechanize模拟浏览器批量注册亚马逊账号'''
    br = mechanize.Browser()
    cj = cookielib.LWPCookieJar()
    br.set_cookiejar(cj)
    br.set_handle_equiv(True)#handle HTTP-EQUIV headers (HTTP headers embedded in HTML).
    br.set_handle_redirect(True)
    br.set_handle_referer(True)#add Referer (sic) header
    br.set_handle_robots(False)# Ignore robots.txt.  Do not do this without thought and consideration.
    br.set_handle_gzip(False)
    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
    br.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0')]
    proxies={"HTTPS": "HTTPS://122.242.96.30:808"}#proxy代理
    br.set_proxies(proxies)
    for tries in range(10):#解决100610错误问题（代理连接问题）
        try:
            br.open(url)
        except:
            if tries<=10:
                continue
            else:
                break
    br.select_forms(text='addToCart')
    for form in br.forms():
        print form
    br.submit()
    url1='https://www.amazon.com/gp/cart/view.html/ref=lh_cart'
    br.open(url1)
    br.select_forms(name='activeCartViewForm')
    for form in br.forms():
        print form
    response=str(br.response().read())
    quantityBox
    stocks=re.findall(r'
        
#---------------------------------------------------------------------------------
#Reading account accounts and passwords
#读取账户账号和密码
def read_excel(file_path):
    '''Read account and password from workbook读取工作簿中的账号密码'''
    workbook=xlrd.open_workbook(file_path)
    sheets=workbook.sheets()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:
        print str(e)
        t=1
    table=workbook.sheet_by_index(t-1)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print cols,rows
    row_1st=table.row_values(0)#读取第一行
    URL_index=row_1st.index('URL')#返回Asin列的所在列数
    URL=table.col_values(URL_index,1,rows-1)#读取Asin列第二行到最后一行
    data1=copy(workbook)#复制工作簿
    return data1,URL
#---------------------------------------------------------------------------------
def File_path_choice():
    '''选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='d:/documents/downloads'
        print '路径{0}文件夹中的文件和文件夹如下：'.format(file_path)
        file_names=os.listdir(file_path)#列出下载文件夹中的文件名
        for i in range(len(file_names)):
            print i+1,file_names[i]
        file_num=int(raw_input("Default workbook is 1,plz input a number of serial number(default {0}):\nOr Enter a number more than the last option you could input a url of file\n ".format(t)))#默认文件名是t,或者输入比最后选项大的数
        if file_num in range(1,len(file_names)+1):
            file_path=file_path+'/'+file_names[file_num-1]
        if file_num>=len(file_names)+1:
            file_path=str(raw_input("plz input a fileurl (like:D:\\Documents\\Downloads\1.xlsx\n"))
            f=os.path.exists(file_path)
            while f==False:
                file_path=str(raw_input("Your file is not exsits,plz input a fileurl:\n"))
                f=os.path.exists(file_path)
    except Exception,e:
        print str(e)
        file_path=t
    return file_path
#---------------------------------------------------------------------------------   
#Simulate download progress of Lniux 
#模拟Lniux下载进度条
def  progress(i):
    r='\r%s>%d%%' % ('#' * i, i,)
    sys.stdout.write(r)
    sys.stdout.flush()#Refresh progress刷新进度条
#---------------------------------------------------------------------------------   
if __name__=='__main__':
    t1=datetime.datetime.now()
    data,URL=read_excel()
    for i in range(len(URL)):
        url=URL[i]
        stock=AddtoCart(url)
        
    t2=datetime.datetime.now()
    print "\nThe cost time is {0}.\nThe workbook is saved in {1}".format(t2-t1,'C:\\Users\\Administrator\\Desktop\email.xls' ) 
