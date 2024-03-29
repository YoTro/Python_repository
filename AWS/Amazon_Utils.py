# -*- coding:UTF-8 -*-
import xlrd
import xlwt
import os
import re
import requests
def is_bash():
    '''判断使用平台'''
    if platform.system().lower() == "windows":
        return 0
    if platform.system().lower() == 'darwin' or platform.system().lower() == 'linux':
        return 1
def is_TTD(f):
    '''是否被Amazon屏蔽请求变狗'''
    temp = f
    if(re.findall("(_TTD_\.jpg)", f)):
        return 1
    else:
        return 0
def retry(func):
    '''装饰器：try最多5次'''
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

def excel_bulit(workbook, asin):
    '''Bulit a excel.构建Excel'''
    table= workbook.add_sheet("{}".format(asin),cell_overwrite_ok=True)
    style = xlwt.XFStyle()#设置样式
    font = xlwt.Font()#设置字体
    font.name = 'SimSun' # 指定“宋体”
    style.font = font
    alignment=xlwt.Alignment()#设置对齐
    alignment.horz=xlwt.Alignment.HORZ_CENTER#单元格字符水平居中
    # 格式: HORZ_GENERAL, HORZ_LEFT, HORZ_CENTER, HORZ_RIGHT, HORZ_FILLED, HORZ_JUSTIFIED, HORZ_CENTER_ACROSS_SEL, HORZ_DISTRIBUTED
    alignment.vert=xlwt.Alignment.VERT_CENTER#单元格字符垂直居中
    #格式: VERT_TOP, VERT_CENTER, VERT_BOTTOM, VERT_JUSTIFIED, VERT_DISTRIBUTED
    style.alignment=alignment#添加至样式
    return table

def Get_ASINlists(fn):
    '''从表格里获取ASINURL'''
    data=xlrd.open_workbook(fn) # 打开工作薄
    sheets=data.sheets()
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
    try:
        url_index=first_sheet.index('ASIN')#返回第一行URL的列数
    except Exception as e:
        print(e)
        return []
    ASINs=table.col_values(url_index) # 读取指定列（该列含有URL）
    ASINs.pop(0)
    return ASINs
def Get_Exceldata(fn, colname):
    '''从表格里获取任意列数组'''
    if not os.path.exists(fn):
        print("Doesn't exist: {}".format(os.path.abspath(fn)))
        return None
    data=xlrd.open_workbook(fn) # 打开工作薄
    sheets=data.sheets()
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
    try:
        url_index=first_sheet.index(colname)#返回第一行URL的列数
    except Exception as e:
        print(e)
        return []
    ASINs=table.col_values(url_index) # 读取指定列（该列含有URL）
    ASINs.pop(0)#去掉表头
    return ASINs

def File_path_choice():
    '''选择文件夹中的文件，返回所选文件路径'''
    t='./stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='.'
        print('当前路径{0}文件夹中的文件和文件夹如下：'.format(os.path.abspath(file_path)))
        file_names=os.listdir(file_path)#列出下载文件夹中的文件名
        for i in range(len(file_names)):
            print(i+1,file_names[i])
        file_num=int(raw_input("Default workbook is 1,plz input a number of serial number(default {0}):\nOr Enter a number more than the last option you could input a url of file\n ".format(t)))#默认文件名是t,或者输入比最后选项大的数
        if file_num in range(1,len(file_names)+1):
            file_path=file_path+'/'+file_names[file_num-1]
        if file_num>=len(file_names)+1:
            file_path=str(raw_input("plz input a fileurl (like:D:\\Documents\\Downloads\1.xlsx or /Users/\{name\}/Projects/1.xls\n"))
            f=os.path.exists(file_path)
            while f==False:
                file_path=str(raw_input("Your file is not exsits,plz input a fileurl:\n"))
                f=os.path.exists(file_path)
    except Exception as e:
        print(str(e))
        file_path=t
    return file_path

def is_Captcha(f, host, asin, session, headers):
    '''Amazon验证码'''
    url = re.findall("(https://images-na.ssl-images-amazon.com/captcha.*?jpg)", f)
    i = 0
    while url:
        captcha = AmazonCaptcha.fromlink(url[0])
        text = captcha.solve()
#        with open('./t.html', 'w') as f:
#            f.write(r)
#            f.close()
        validateCaptcha_url = host+"/errors/validateCaptcha"
        amzn = re.findall("name=\"amzn\" value=\"(.*?)\"", f)[0]
        amzn_r = re.findall("name=\"amzn-r\" value=\"(.*?)\"", f)[0]
        params = {
            "amzn": amzn,
            "amzn-r": html.unescape(amzn_r),
            "field-keywords": text
        }
        validateCaptcha_url = validateCaptcha_url+"?"+ urllib.parse.urlencode(params)
        print(validateCaptcha_url)
        r = session.get(validateCaptcha_url, headers = headers, timeout = 5)#Amazon验证请求
        u = host+asin
        r = session.get(u, headers=headers, timeout = 5)
        f = r.text
        print(url)
        url = re.findall("(https://images-na.ssl-images-amazon.com/captcha.*?jpg)", f)
        i += 1
        if i == 5:
            url = []
    return f, session

def requests_asin(host, asin):
    '''Get the content of Amazon listing web page.获取listing网页内容'''
    headers={
        'Connection':'keep-alive',
        'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"macOS"',
        'Upgrade-Insecure-Requests':'1',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site':'none',
        'Sec-Fetch-Mode':'navigate',
        'Sec-Fetch-User':'?1',
        'Sec-Fetch-Dest':'document',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9'
        }#浏览器头部
    #proxies={'HTTP': 'HTTP://127.0.0.1:1081', 'HTTPS': 'HTTPS://127.0.0.1:1081'}#免费代理IP
    session=requests.Session()
    url = host+'/dp/'+asin
    r = session.get(url, headers=headers, timeout = 5)
    return is_Captcha(r.text, host, asin, session, headers)
