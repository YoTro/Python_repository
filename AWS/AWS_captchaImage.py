# -*- conding:UTF-8 -*-
# Author:Toryun
# Python version:2.7.13
#Windows Version:7
# Date:2018-04-05
#Function：Get the captchaImage, Image recognition by pytesseract module获取Amazon验证码图片，使用Google维护的tesseract-ORC识别图片内容
import re
import os
import time
import datetime
import requests
import  pytesseract
#from nt import chdir  ==>os.name=='windows'
from PIL import Image
loop=int(raw_input('Plz input a number what you want to get the captchaImage:\n'))
t0=datetime.datetime.now()
url='https://www.amazon.com/ap/captcha?appAction=REGISTER&amp;captchaObfuscationLevel=ape:aGFyZA==&amp;captchaType=image'
headers1={"Host":	
        "opfcaptcha-prod.s3.amazonaws.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0",
        "Accept":
        "text/html,application",
        "Accept-Language":
        "zh-CN,zh;q=0.8",
        "Accept-Encoding":	
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Upgrade-Insecure-Requests":"1"
        }#验证码图片服务器头
headers2={"Host":	
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*",
        "Accept-Language":
        "zh-CN,zh;q=0.8",
        "Accept-Encoding":	
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Upgrade-Insecure-Requests":"1"
        }#验证码URL服务器头
proxies={"HTTPS": "HTTPS://122.242.96.30:808"}
for i in range(loop):
	r=requests.get(url,headers=headers2,proxies=proxies)
	f=re.findall(r'"captchaImageUrl":"(.*?)","ces',r.content)
	time.sleep(3)
	r1=requests.get(f[0],headers=headers1,proxies=proxies)	
	_img_0="d:\\jpg\\"+str(i)+".jpg"
	with open(_img_0,"wb") as _img_1:

		    _img_1.write(r1.content)
		    _img_1.close()
	img=Image.open(_img_0)
	img.show()
	#num=raw_input('plz input the captchaImage your see:\n')
	#chdir("D:\\jpg\")
	pytesseract.pytesseract.tesseract_cmd = 'C:\\Program Files (x86)\\Tesseract-OCR\\tesseract'#调用环境变量
	num=pytesseract.image_to_string(img)# type : unicode
	print num
	os.rename(_img_0,"d:/jpg/{0}.jpg".format(num))
	command = 'taskkill /F /IM dllhost.exe'#强制终止指定进程名命令
	os.system(command)
t1=datetime.datetime.now()
t=t1-t0
print 'They are saved in {0}\nThe total time is {1}'.format("d:jpg",t)
