# -*- conding:UTF-8 -*-
import re,requests,xlrd,datetime,xlwt
from xlutils.copy import copy
# 输入Asin，get网页返回内容和网址
def requests_url(url):
    url='https://www.amazon.com/dp/'+url
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3141.7 Safari/537.36",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
"Accept-Language":
"zh-CN,zh;q=0.8",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1"
}
    proxies={'HTTP': 'HTTP://183.144.214.132:3128', 'HTTPS': 'HTTPS://219.149.46.151:3129'}
    r=requests.get(url)
    return r.content,url
def main():
    start=datetime.datetime.now() # 计算所用时间
    data=xlrd.open_workbook('D:\\Documents\\Downloads\Food_Bins&Canisters_adjust_cell_phone.xls')#打开工作簿
    sheets=data.sheets()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)):
            t=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:
        print str(e)
        t=1

    table=data.sheet_by_index(t-1)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print cols,rows
    row_1st=table.row_values(0)#读取第一行
    asin_index=row_1st.index('Asin')#返回Asin列的所在列数
    URL=table.col_values(asin_index,1,rows)#读取Asin列第二行到最后一行
    data1=copy(data)#复制工作簿
    table1=data1.get_sheet(t-1)
    for i in range(rows-1):
        try:
            r,url=requests_url(URL[i])
            print i,url
            dimensions=re.findall(r'<td class="a-size-base">\s+(.*?)\sinches',r)
            if dimensions:
                print dimensions[0]
            else:
                print 'dimensions is None'                         
            table1.write(i+1,cols-1,dimensions[0])
        except Exception,e:
            print str(e)
    data1.save('c:\\first_Choice_copy.xls') #保存复制表格
    end=datetime.datetime.now()
    t=end-start#总用时
    print 'Total time: %s s.'%(t)

if  __name__== '__main__':
    main()
            
    
    
