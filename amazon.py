import re,urllib2
header = {
'User-Agent': 'Mozilla/5.0 (iPad; U; CPU OS 4_3_4 like Mac OS X; ja-jp) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8K2 Safari/6533.18.5',
'Referer': 'https://www.amazon.cn/',
'Host': 'www.amazon.cn',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
'Connection': 'keep-alive'
}
r=re.compile(r'.*?<h2 id="s-result-count" class="a-size-base .*?1-24 of (\d+) results .*?')
url='https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords='
n=('towel','bar')
j='+'
n=j.join(n)
url=url+n
t=True
print url
while t:
        try:
                f=urllib2.urlopen(url,header)
                result=f.read()
                f.close()
                s=r.search(result).group(1)
                print s
                
        except urllib2.URLError,e:
                print e.reason
