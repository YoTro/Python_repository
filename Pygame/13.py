#coding: utf-8
#Author: Toryun
#Date: 2020-06-17 01:13:00

import urllib2
import xmlrpclib
from contextlib import closing

url0 = 'http://www.pythonchallenge.com'
url1 = 'http://www.pythonchallenge.com/pc/return/evil4.jpg'
url2 = 'http://www.pythonchallenge.com/pc/phonebook.php'
#创建默认密码管理对象实例
PasswordMgr1 = urllib2.HTTPPasswordMgrWithDefaultRealm()
#向实例添加密码和用户名,uri,realm(“领域”，其实就是指当前认证的保护范围。)
#例如 /protected_docs就是受限访问对象
#GET /protected_docs HTTP/1.1
#Host: 127.0.0.1:3000
PasswordMgr1.add_password(None, url0, 'huge', 'file')
#实例一个含有密码的基础验证处理对象
auth_handler = urllib2.HTTPBasicAuthHandler(PasswordMgr1)
#Create an opener object from a list of handlers.   
#The opener will use several default handlers, including support
#for HTTP, FTP and when applicable, HTTPS.  
#If any of the handlers passed as arguments are subclasses of the
#default handlers, the default handlers will not be used.
#构建一个含有处理器的打开对象
op = urllib2.build_opener(auth_handler)
#安装这个打开对象
urllib2.install_opener(op)
#打开这个网页并获取内容(自动关闭with..as)
with closing(urllib2.urlopen(url1)) as f:
    print(f.read().decode('utf-8'))
xmlrpc = xmlrpclib.ServerProxy(url2)
print(xmlrpc.phone('Bert'))
