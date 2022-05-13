import re,requests
header = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36',
        'Referer': 'https://www.amazon.cn/',
        'Host': 'www.amazon.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive'
        }
r=re.compile(r'.*?<div class="a-section a-spacing-small a-spacing-top-small">\s+<span>1-\d+ of over (\d,{0,1}\d+) results.*?')
url='https://www.amazon.com/s?k='
n=('towel','bar')
j='+'
n=j.join(n)
url=url+n
t=True
print url
#while t:
try:
        session = requests.Session()
        f=session.get(url,headers=header)
        result=f.content
        print f.status_code
        s=r.search(result).group(1)
        print s
        if s is not None:
                t =False
        
except requests.exceptions.RequestException as e:
        raise SystemExit(e)
