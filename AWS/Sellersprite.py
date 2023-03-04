# -*- coding:UTF-8 -*-
# 登陆卖家精灵
import json
import execjs
import hashlib
import requests

class dl:
    '''获取卖家精灵tk'''
    def __init__(self):
        self.GOOGLE_TKK_DEFAULT = "446379.1364508470"
        default_EXT_VERSION = "3.4.2"
        self.EXT_VERSION = default_EXT_VERSION.replace(".", "00", 1).replace(".", "0") + ".1364508470"

    def updateTkk(self, e):
        return self.GOOGLE_TKK_DEFAULT

    def tkk(self, e, t):
        try:
            tkk = self.updateTkk(e)
            return self._cal(t, tkk if tkk and tkk != "" else self.GOOGLE_TKK_DEFAULT)
        except Exception as e:
            raise e
    
    def s2Tkk(self, e, t):
        s = []
        a = [e, t]
        for i in range(len(a)):
            if a[i] and a[i] is not None and len(str(a[i])) > 0:
                t = len(s)
                if isinstance(a[i], list):
                    for j in range(len(a[i])):
                        s.append(a[i][j])
                if t == len(s):
                    s.append(str(a[i]))
        return "" if len(s) < 1 else self._cal("".join(s), self.EXT_VERSION)

    def _cal(self, e, t):
        def n(e, t):
            for i in range(0, len(t) - 2, 3):
                r = t[i + 2]
                r = ord(r) - 87 if r >= "a" else int(r)
                r = e >> r if t[i + 1] == "+" else e << r
                e = (e + r) & 4294967295 if t[i] == "+" else e ^ r
            return e

        def cal(e, t):
            r = t.split(".")
            t = int(r[0]) if r[0] else 0
            s = []
            a = 0
            for i in range(len(e)):
                o = ord(e[i])
                if o < 128:
                    s.append(o)
                    a += 1
                else:
                    if o >= 2048:
                        if 64512 == (64512 & o) and i + 1 < len(e) and 56320 == (64512 & ord(e[i + 1])):
                            o = 65536 + ((1023 & o) << 10) + (1023 & ord(e[i + 1]))
                            s.append(o >> 18 | 240)
                            s.append(o >> 12 & 63 | 128)
                            i += 1
                        else:
                            s.append(o >> 12 | 224)
                            s.append(o >> 6 & 63 | 128)
                    else:
                        s.append(o >> 6 | 192)
                        s.append(63 & o | 128)
                    a += 2
            e = t
            for i in range(len(s)):
                e = n(e + s[i], "+-a^+6")
            e = n(e, "+-3^+b+-f")
            e ^= int(r[1]) if r[1] else 0
            if e < 0:
                e = 2147483648 + (2147483647 & e)
            r = e % 1000000
            return str(r) + "." + str(r ^ t)

        return cal(e, t)
def Sellersprite_extension_login(session, email, password, tk):
    '''卖家精灵插件登陆'''
    url = "https://www.sellersprite.com/v2/extension/signin?email={}&password={}&tk={}&version=3.4.2&language=zh_CN&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome".format(email, password, tk)
    headers = {
        "Host": "www.sellersprite.com",
        "Connection": "keep-alive",
        "Accept": "application/json",
        "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
    }
    r = session.get(url, headers = headers)
    token = ""
    if r.status_code == 200:
        token = r.json()['data']['token']
        return session, token
    else:
        print(r.text)
def _tk(email, asin):
    '''从js获取卖家精灵tk'''
    node = execjs.get()
    file = './sellersprite.js'
    ctx = node.compile(open(file, 'r', encoding='utf8').read())
    # 数据源的 tk
    e = email
    t = asin
    # 得到结果, 与我们抓包请求中的 tk 参数结果一致
    # 58497.291017
    tk = ctx.call('s2Tkk', e, t)
    return tk
def keepa(session, asin, Auth_Token, tk):
    '''获取asin keepa排名数据'''
    url = "https://www.sellersprite.com/v2/extension/keepa?station=US&asin={}&tk={}&version=3.4.2&language=zh_CN&extension=lnbmbgocenenhhhdojdielgnmeflbnfb&source=chrome".format(asin, tk)
    headers = {
        "Host": "www.sellersprite.com",
        "Connection": "keep-alive",
        "Accept": "application/json",
        "Random-Token": "6152a0b0-11a4-438e-877e-339c77be509a",
        "Auth-Token": Auth_Token,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
    }
    r = session.get(url, headers = headers)
    response = {'bsr':[],'times':[]}
    if r.status_code == 200:
        response['bsr'] = r.json()['data']['keepa']['bsr']
        response['times'] = r.json()['data']['times']
        print(response)
    else:
        print(r.text)
    return response
def salt_pwd_hash(email, password):
    '''对password加密,返回加密后的password, salt'''
    password_hash = hashlib.md5(password.encode()).hexdigest()
    email_password_hash = email + password_hash
    salt = hashlib.md5(email_password_hash.encode()).hexdigest()
    return password_hash, salt

def Sellersprite_web_login(session, email, pwd, salt):
    '''登陆卖家精灵网页版'''
    url = "https://www.sellersprite.com/w/user/signin"
    headers = {
        "host": "www.sellersprite.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9",
        "cache-control": "max-age=0",
        "content-type": "application/x-www-form-urlencoded",
        "sec-ch-ua": "\"Chromium\";v=\"110\", \"Not A(Brand\";v=\"24\", \"Google Chrome\";v=\"110\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1"
    }
    FormData = {
        "callback": "",
        "password": pwd,
        "email": email,
        "autoLogin": "Y",
        "salt": salt
    }
    r = session.post(url, headers = headers, data = FormData)
#   with open('./t.html', 'w', encoding = "UTF-8") as f:
#       f.write(r.text)
#       f.close()
    return session

def get_keywors_traffic_extend_asin(session, asins):
    '''获取asin数组的扩展流量词'''
    url = "https://www.sellersprite.com/v3/api/traffic/extend/asin"
    headers = {
        "host": "www.sellersprite.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "content-type": "application/json;charset=UTF-8",
#       "sec-ch-ua": "\"Chromium\";v=\"110\", \"Not A(Brand\";v=\"24\", \"Google Chrome\";v=\"110\"",
#       "sec-ch-ua-mobile": "?0",
#       "sec-ch-ua-platform": "\"Windows\"",
#       "sec-fetch-dest": "empty",
#       "sec-fetch-mode": "cors",
#       "sec-fetch-site": "same-origin"
    }
    Payload = {
        "queryVariations":"true",
        "asinList":asins,
        "originAsinList":asins,
        "market":1,
        "page":1,
        "month":"",
        "size":50,
        "orderColumn":12,
        "desc":"true",
        "exactly":"false",
        "ac":"false"
    }
    r = session.post(url, headers = headers, data = json.dumps(Payload))
    keywords = []
    if r.status_code == 200:
        keywordlist = r.json()['data']['items']
        for i in range(len(keywordlist)):
            keywords.append(keywordlist[i]['keywords'])
    else:
        print(r.text)
    return keywords
if __name__ == '__main__':
    email = ""
    password = ""
#   asins = ["B098T9ZFB5","B09JW5FNVX","B0B71DH45N","B07MHHM31K","B08RYQR1CJ"]
    pwd, salt = salt_pwd_hash(email, password)
    #print(type(salt))
    session = requests.Session()
#   session = Sellersprite_login(session, email, pwd, salt)
    #keywords = get_keywors_traffic_extend_asin(session, asins)
    #print(keywords)
    asin = 'B0B71DH45N'
    session, Auth_Token = Sellersprite_extension_login(session,email,pwd, extension_login_tk)
    dl = dl()
    keepa_tk = dl.s2Tkk("", asin)
    extension_login_tk = dl.s2Tkk(email, pwd)
    session, Auth_Token = Sellersprite_extension_login(session,email,pwd, extension_login_tk)
    #brs = keepa(session, asin, Auth_Token, keepa_tk)
    print(extension_login_tk, keepa_tk, _tk("", asin))