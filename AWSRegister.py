# -*- conding:UTF-8 -*-
# Author:Toryun
# Python version:2.7.13
# Date:18/3/25
# Function:Create a style Excel in special situation生成指定表格,写入随机Email和密码
#Amazom CAPTCHA IMG API  https://www.amazon.com/ap/captcha?appAction=REGISTER&amp;captchaObfuscationLevel=ape:aGFyZA==&amp;captchaType=image
import os
import re
import sys
import time
import xlwt
import xlrd
import random
import requests
import datetime
import cookielib
import mechanize
import  pytesseract
from PIL import Image

#---------------------------------------------------------------------------------
def excel_bulit(file_save):
    #Bulit a excel,Generate a  account  of Amazon 构建Excel，生成亚马逊账号
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table= workbook.add_sheet("data",cell_overwrite_ok=True)
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

    for i in range(0,100):
        t='0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'#chr()
        t1=''.join(random.sample(t,8))
        b1=''.join(random.sample(t,8))
        mail=t1+'@gmail.com'
        table.write(i,0,mail,style)
        table.write(i,1,b1,style)  
    workbook.save(file_save)
    
#---------------------------------------------------------------------------------
#Reading account accounts and passwords
#读取账户账号和密码
def read_excel(i):
    '''Read account and password from workbook读取工作簿中的账号密码'''
    workbook=xlrd.open_workbook('C:\\Users\\Administrator\\Desktop\email.xls')
    table=workbook.sheet_by_index(0)
    account=table.col_values(0)
    password=table.col_values(1)
    a=account[i]
    p=password[i]
    return a,p
#---------------------------------------------------------------------------------
#Register Amazon（Using mechanize library to simulate browser registration）
#注册亚马逊(使用mechanize库模拟浏览器注册）
def register(name,account,password,i):
    '''Using the mechanize lib simulation browser to large quantity register Amazon accounts使用mechanize模拟浏览器批量注册亚马逊账号'''
    url='https://www.amazon.com/ap/register?openid.pape.max_auth_age=0&openid.identity=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&pageId=usflex&ignoreAuthState=1&openid.return_to=https%3A%2F%2Fwww.amazon.com%2F%3Fref_%3Dnav_ya_signin&prevRID=3CBEZNC1DVKVH5BS3CQT&openid.assoc_handle=usflex&openid.mode=checkid_setup&openid.ns.pape=http%3A%2F%2Fspecs.openid.net%2Fextensions%2Fpape%2F1.0&prepopulatedLoginId=&failedSignInCount=0&openid.claimed_id=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0%2Fidentifier_select&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0'
    br = mechanize.Browser()
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
    br.select_form(nr=0)
    br.form['customerName']=name
    br.form['email']=account
    br.form['password']=password
    br.form['passwordCheck']=password
    br.submit()
    response=str(br.response().read())
    try:#如果需要验证进行以下操作
        img_url=re.findall(r'<img alt="Visual CAPTCHA image, continue down for an audio option." src="(.*?)" data-refresh-url',response)#读取验证码图片
        headers={"Host":	
        "opfcaptcha-prod.s3.amazonaws.com",
        "Referer":
        "https://www.amazon.com/ap/register",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":
        "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Accept-Encoding":	
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Upgrade-Insecure-Requests":"1"
        }
        if img_url[0]:
            img_url0=img_url[0].replace("&amp;","&",2)
            print 'Need captcha'
        r=requests.get(img_url0,headers=headers,proxies=proxies)#下载验证码图片
        print r.status_code
        if r.status_code==200:
            _img_0="d:jpg/"+str(i)+".jpg"#存储验证码图片路径
            with open(_img_0,"wb") as _img_1:
                _img_1.write(r.content)
                _img_1.close()
            img = Image.open(_img_0)
            img.show()
            #guess=raw_input('Plz input Type characters you see:\n')
            pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files (x86)\\Tesseract-OCR\\tesseract'#调用环境变量
            guess=pytesseract.image_to_string(img)# type : unicode
            os.rename(_img_0,"d:jpg/{0}.jpg".format(guess))
            command = 'taskkill /F /IM dllhost.exe'#强制终止指定进程名命令
            os.system(command)
            br.select_form(nr=0) 
            br.form['password']=password
            br.form['passwordCheck']=password
            br.form['guess']=guess
            br.submit()

    except Exception,e:
        print str(e)
        continue
#---------------------------------------------------------------------------------   
#Simulate download progress of Lniux 
#模拟Lniux下载进度条
def  progress(i):
    r='\r%s>%d%%' % ('#' * i, i,)
    sys.stdout.write(r)
    sys.stdout.flush()#Refresh progress刷新进度条
def rate_progress(i):
    sys.stdout.write('\r%2d%%')
    sys.stdout.flush()
    
if __name__=='__main__':
    t1=datetime.datetime.now()
    file_save='C:\\Users\\Administrator\\Desktop\email.xls' 
    excel_bulit(file_save)
    print 'email is built,now register amazon account\n完成进度:\n'.decode('utf-8')
    for i in range(0,100):
        a,p=read_excel(i)
        register(name=p,account=a,password=p,i=i)
        time.sleep(random.randint(0,6))#随机休息0到6秒
        progress(i)     
    t2=datetime.datetime.now()
    print "\nThe cost time is {0}.\nThe workbook is saved in {1}".format(t2-t1,file_save )
