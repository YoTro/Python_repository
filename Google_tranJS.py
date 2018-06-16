#-*-coding:UTF-8-*-
#Python version:2.7.13
#System:window7
#Time:2018-3-13
#Function:python execjs call js snippet
#源码：From https://translate.google.cn/translate/releases/twsfe_w_20180312_RC01/r/js/desktop_module_main.js
#---------------------------------------------------------------------------
#var sq = function (a) {
#return function () {
# return a
#}
#},
#tq = function (a, b) {
#for (var c = 0; c < b.length - 2; c += 3) {
#  var d = b.charAt(c + 2);
#  d = 'a' <= d ? d.charCodeAt(0) - 87 : Number(d);
#  d = '+' == b.charAt(c + 1) ? a >>> d : a << d;
#  a = '+' == b.charAt(c) ? a + d & 4294967295 : a ^ d
#}
#return a
#},
#uq = null,
#vq = function (a) {
#if (null !== uq) var b = uq;
# else {
#  b = sq(String.fromCharCode(84));字符：T
#  var c = sq(String.fromCharCode(75));字符：K
#  b = [
#    b(),
#    b()
#  ];
#  b[1] = c();
#  b = (uq = window[b.join(c())] || '') || ''
#}
#var d = sq(String.fromCharCode(116));字符：t
#c = sq(String.fromCharCode(107));字符：k
#d = [
#  d(),
#  d()
#];
#d[1] = c();
#c = '&' + d.join('') +
#'=';
#d = b.split('.');
#b = Number(d[0]) || 0;
#for (var e = [
#], f = 0, g = 0; g < a.length; g++) {
#  var l = a.charCodeAt(g);
#  128 > l ? e[f++] = l : (2048 > l ? e[f++] = l >> 6 | 192 : (55296 == (l & 64512) && g + 1 < a.length && 56320 == (a.charCodeAt(g + 1) & 64512) ? (l = 65536 + ((l & 1023) << 10) + (a.charCodeAt(++g) & 1023), e[f++] = l >> 18 | 240, e[f++] = l >> 12 & 63 | 128)  : e[f++] = l >> 12 | 224, e[f++] = l >> 6 & 63 | 128), e[f++] = l & 63 | 128)
#}
#a = b;
#for (f = 0; f < e.length; f++) 
#a += e[f],
#a = tq(a, '+-a^+6');
#a = tq(a, '+-3^+b+-f');
#a ^= Number(d[1]) || 0;
#0 > a && (a = (a & 2147483647) + 2147483648);
#a %= 1000000;
#return c + (a.toString() + '.' + (a ^ b))
#};
#---------------------------------------------------------------------------
import execjs  
import requests
import urllib
import re
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
def  zh_or_en(sl):
    s=sl.decode('GB2312')#The rules for translating a Unicode string into a sequence of bytes are called an encoding.                                       
    __zh=re.compile(u'[\u4e00-\u9fa5]+')
    f=__zh.search(s)
    if f:#如果是中文，则用中译英URL
        t=1
        q=urllib.quote(s.encode('UTF-8'))
        url="https://translate.google.cn/translate_a/single?client=t&sl=zh-CN&tl=en&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss&dt=t&ie=UTF-8&oe=UTF-8&otf=1&ssel=3&tsel=3&kc=1&"
    else:
        t=2
        q=sl
        url="https://translate.google.cn/translate_a/single?client=t&sl=en&tl=zh-CN&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss&dt=t&ie=UTF-8&oe=UTF-8&otf=2&ssel=3&tsel=6&kc=1&"
    return q,url,t,s
js=Py4Js()
sl=raw_input('plz input your translate word:\n')#输入要查询的中文
q,url,t,s=zh_or_en(sl)
if t==1:
    q=s
    tk=js.getTk(q)
if t==2:
    tk=js.getTk(q)
print tk
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
print r.status_code,r.content



    
