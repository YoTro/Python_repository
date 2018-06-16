#-*- coding:UTF-8 -*-
# ========================
#Python version is 2.7.13  
# Author: Toryun        
#Date:2017-08- 28
#Function:Get the inventory of amaozn's productions though by post
#========================
import requests
import xlrd
import  datetime
import  os
import sys
import time
def File_path_choice():
    '''Select the file in the folder and return the selected file path.选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='C:/Users/Administrator/Desktop'
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
def requests_url(url):
    '''Get the content of listing web page.获取listing网页内容'''
    headers={"Host":	
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
}#火狐浏览器头部
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    r=requests.get(url,headers=headers,proxies=proxies)
    return r.status_code

def read_workbook(file_path):
    '''Get the Url 获取URL'''
    data=xlrd.open_workbook(file_path)#打开工作簿
    sheets=data.sheets()
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

    table=data.sheet_by_index(t-1)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "{0}'s cols,rows is {1},{2}".format(z[t],cols,rows)#打印该sheet中的列行数
    row_1st=table.row_values(0)#读取第一行
    asin_index=row_1st.index('URL')#返回URL列的所在列数
    URL=table.col_values(asin_index,1,rows-1)#读取URL列第二行到最后一行
    return URL
def progress(i):
    r='\r{0}%{1}'.format(i,'#'*i)
    sys.stdout.write(r)
    sys.stdout.flush()
if __name__=='__main__':
    file_path=File_path_choice()
    URL=read_workbook(file_path)
    b=int(raw_input('plz input the times you want loop:\n'))
    for c in range(b):
        for i in range(len(URL)):
            time.sleep(3)
            url=URL[i]
            status=requests_url(url)
            print status
        progress(c)        
        
