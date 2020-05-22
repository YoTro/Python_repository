#coding:utf-8
#Author: Toryun
#Date: 2020-05-22 01:33:00
#Func: 在线编译包含大部分语言
#API: https://tool.runoob.com/compile2.php

import requests

compilelang = "0: Python\n1: Ruby\n2: C++\n3: PHP\n4: Node.js\n5: Scala\n6: Go\n7: C\n8: Java \n9: RUST\n10: C# \n11: Bash\n12: Erlang\n14: Perl\n15: Python3\n16: Swift\n17: Lua\n18: Pascal\n19: Kotlin\n80: R \n84: VB.NET \n1001: TypeScript\n"
try:
	filename = raw_input("please input the path of file which need to compile:(like this /your/file/path/xxx.c\n")
	print(compilelang)
	lang = int(raw_input("Please input the number of language which you need to compile:\n"))
except Exception as e:
	print("The error:{}, the default language is C".format(e))
	lang = 7
	filename = "/Users/jin/Desktop/coinChange.c"
url = "https://tool.runoob.com/compile2.php"
header = {

"Accept": "*/*",
"Accept-Encoding": "gzip, deflate, br",
"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
"Connection": "keep-alive",
"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
"Host": "tool.runoob.com",
"Origin": "https://c.runoob.com",
"Referer": "https://c.runoob.com/compile/11",
"Sec-Fetch-Dest": "empty",
"Sec-Fetch-Mode": "cors",
"Sec-Fetch-Site": "same-site",
"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
          }

code = open(filename, 'r').read()
#code = urllib.quote(codefile, safe = "/")
token= "4381fe197827ec87cbac9552f14ec62a"
stdin=""
language = lang
fileext = {0: "py", 1: "rb", 2: "cpp", 3: "php", 4: "node.js", 5: "scala", 6: "Go", 7: "c", 8: "java", 9: "rs", 10: "cs",11: "sh", 12: "erl",14: "pl", 15: "py3", 16: "swift", 17: "lua", 18: "pas", 19: "kt", 80: "R", 84: "vb", 1001: "ts"}
#c++ (即cpp)的数字是7
if lang == 2:
	language = 7
data = { "code": code, "token": token, "stdin": "", "language": language, "fileext": fileext[lang]}
r = requests.post(url, data, headers = header)
#如果有汉字可以转换解码
print r.content.decode('unicode_escape')
