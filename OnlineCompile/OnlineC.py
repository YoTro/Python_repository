#coding:utf-8
#Author: Toryun
#Date: 2020-05-22 01:33:00
#Func: 在线编译C99
#API: https://tpcg.tutorialspoint.com/tpcg.php

import requests

filename = raw_input("please input the path of file which need to complie:(like this /your/file/path/xxx.c\n")
url = "https://tpcg.tutorialspoint.com/tpcg.php"
header = {

"Accept": "*/*",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
"Connection": "keep-alive",
"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
"Host": "tpcg.tutorialspoint.com",
"Origin": "https://www.tutorialspoint.com",
"Referer": "https://www.tutorialspoint.com/compile_c99_online.php",
"Sec-Fetch-Dest": "empty",
"Sec-Fetch-Mode": "cors",
"Sec-Fetch-Site": "same-site",
"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
          }
lang = "c99"
ext = "c"
compile = "gcc -std=c99 -o main *.c"
execute = "main"
mainfile = "main.c"
uid = 9486984
#codefile = open("/Users/jin/Desktop/coinChange.c", 'r').read()
#code = urllib.quote(codefile, safe = "/")
code = open(filename, "r").read()
data = {"lang":lang, "device": "", "code": code, "stdinput": "",  "ext": ext, "compile" : compile, "execute": execute, "mainfile": mainfile, "uid": 9486984}
r = requests.post(url,data, headers = header)
print r.content
