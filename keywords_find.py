import re,requests,xlwt,xlrd,string,datetime
from xlutils.copy import copy
def xlrd_url(i):
    data=xlrd.open_workbook('c:\\3.xls')
    table=data.sheet_by_index(0)
    URL=table.col_values(0)
    return URL[i]
def get_url(i,url):
    try:
        amazon='https://www.amazon.com/s/ref=nb_sb_noss_2?url=search-alias%3Daps&field-keywords='
        u=str(url)
        u1=u.replace(' ','+')
        url=amazon+u1
        print i,url
        _headers=requests.head(url)
        r=requests.get(url,_headers)
        m=re.findall(r'a-size-base a-spacing-small a-spacing-top-small a-text-normal">(.*?)<span>',r.content)
        print m[0]
        return m[0]
    except Exception,e:
        print str(e)
def main():
    start=datetime.datetime.now()
    data1=xlwt.Workbook()
    table1=data1.add_sheet(u'1')
    for i in range(79):
        url=xlrd_url(i)
        m=get_url(i,url)
        table1.write(i,0,url)
        table1.write(i,1,m)
    data1.save('c:\\4.xls')
    end=datetime.datetime.now()
    t=end-start
    print 'Total time: %s s'%(t)
if __name__=='__main__':
    main()
