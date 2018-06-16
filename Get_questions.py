# -*- conding:UTF-8 -*-
# Author:Toryun
# Python version:2.7.13
# Windows version:7
# Date:2018-4-9
# Function:Get questions ,write in excel,translate into Chinese.获取问题，写入构建的Excel，翻译成中文
import re
import xlwt
import execjs 
import requests
#---------------------------------------------------------------------------------
def excel_bulit():
    '''Bulit a excel.构建Excel'''
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
    return workbook,table
 #---------------------------------------------------------------------------------
'''调用js'''
class Py4Js():  
      
    def __init__(self):  
        self.ctx = execjs.compile(""" 
        function TL(a) { 
        var k = ""; 
        var b = 406644; 
        var b1 = 3293161072; 
         
        var jd = "."; 
        var $b = "+-a^+6"; 
        var Zb = "+-3^+b+-f"; 
     
        for (var e = [], f = 0, g = 0; g < a.length; g++) { 
            var m = a.charCodeAt(g); 
            128 > m ? e[f++] = m : (2048 > m ? e[f++] = m >> 6 | 192 : (55296 == (m & 64512) && g + 1 < a.length && 56320 == (a.charCodeAt(g + 1) & 64512) ? (m = 65536 + ((m & 1023) << 10) + (a.charCodeAt(++g) & 1023), 
            e[f++] = m >> 18 | 240, 
            e[f++] = m >> 12 & 63 | 128) : e[f++] = m >> 12 | 224, 
            e[f++] = m >> 6 & 63 | 128), 
            e[f++] = m & 63 | 128) 
        } 
        a = b; 
        for (f = 0; f < e.length; f++) a += e[f], 
        a = RL(a, $b); 
        a = RL(a, Zb); 
        a ^= b1 || 0; 
        0 > a && (a = (a & 2147483647) + 2147483648); 
        a %= 1E6; 
        return a.toString() + jd + (a ^ b) 
    }; 
     
    function RL(a, b) { 
        var t = "a"; 
        var Yb = "+"; 
        for (var c = 0; c < b.length - 2; c += 3) { 
            var d = b.charAt(c + 2), 
            d = d >= t ? d.charCodeAt(0) - 87 : Number(d), 
            d = b.charAt(c + 1) == Yb ? a >>> d: a << d; 
            a = b.charAt(c) == Yb ? a + d & 4294967295 : a ^ d 
        } 
        return a 
    } 
    """)  
          
    def getTk(self,text):  
        return self.ctx.call("TL",text)
 #---------------------------------------------------------------------------------
def  zh_or_en(sl):
    '''判断中英文，得出翻译结果'''
    s=sl.decode('GB2312')#The rules for translating a Unicode string into a sequence of bytes are called an encoding.                                       
    __zh=re.compile(u'[\u4e00-\u9fa5]+')
    f=__zh.search(s)
    js=Py4Js()
    if f:#如果是中文，则用中译英URL
        q=urllib.quote(s.encode('UTF-8'))
        url="https://translate.google.cn/translate_a/single?client=t&sl=zh-CN&tl=en&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss&dt=t&ie=UTF-8&oe=UTF-8&otf=1&ssel=3&tsel=3&kc=1&"
        tk=js.getTk(q)
    else:
        q=sl
        tk=js.getTk(q)
        url="https://translate.google.cn/translate_a/single?client=t&sl=en&tl=zh-CN&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss&dt=t&ie=UTF-8&oe=UTF-8&otf=2&ssel=3&tsel=6&kc=1&"
    url=url+"tk={0}&q={1}".format(tk,q)
    _headers={
                    "Host":	
            "translate.google.cn",
            "Referer":
            "https://translate.google.cn/",
            "User-Agent":
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3141.7 Safari/537.36",
            "Accept":
            "*/*",
            "Accept-Language":
            "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding":	
            "gzip, deflate",
            "Connection":
            "keep-alive"}
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}
    r=requests.get(url,headers=_headers,proxies=proxies)
    stl=re.findall(r'\["(.*?)",',r.content)
    return stl[0]
 #---------------------------------------------------------------------------------
headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
"Accept-Language":
"zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
"Accept-Encoding":
"gzip, deflate, br",
"Connection":
"keep-alive",
         "Upgrade-Insecure-Requests":"1"
             }
workbook,table=excel_bulit()
j=0
asin=raw_input('Plz input the asin you want to get questions:\n')
url='https://www.amazon.com/ask/questions/asin/B075XLRHNQ/1/ref=ask_ql_psf_ql_hza?isAnswered=true'.format(asin)
r0=requests.get(url,headers=headers)
pages=re.findall(r'ref=ask_ql_psf_ql_hza\?isAnswered=true">(.*?)<\/a><\/li>\s+<li class="a\-last">',r0.content)
page=int(pages[0])+1
for i in range(1,page):
    url='https://www.amazon.com/ask/questions/asin/{0}/{1}/ref=ask_ql_psf_ql_hza?isAnswered=true'.format(asin,str(i))
    r=requests.get(url,headers=headers)
    Questions=re.findall(r'=ask_ql_ql_al_hza">\s+(.*?)\s{45}',r.content)#获取第i页所有问题
    for b in range(len(Questions)):
        question=Questions[b]
        print question
        sl=question
        try:
            stl=zh_or_en(sl)
            table.write(b+j,1,stl)
        except Exception,e:
            print str(e)
        table.write(b+j,0,question)   
    j=j+b
file_save='c:\\questions.xls'
print 'The workbook is saved in {}'.format(file_save)
workbook.save(file_save)

        
        
        
    
