# -*- conding:UTF-8 -*-
import re,requests,xlrd,datetime,xlwt,time
from xlutils.copy import copy
# 输入Asin，get网页返回内容和网址
def requests_url(asin):
    url='https://www.amazon.com/dp/'+asin
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
"Accept-Language":
"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1"
}
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    r=requests.get(url，headers=headers,proxies=proxies)
    return r.content,url
def main():
    start=datetime.datetime.now() # 计算所用时间
    data=xlrd.open_workbook('D:\\Documents\\Downloads\Food_Bins&Canisters_adjust_cell_phone.xls')#打开工作簿
    sheets=data.sheets()
    z={}
    p=0#key
    for sheet in sheets:
        p+=1
        z[p]=sheet.name#把工作簿中的sheet名导入字典z
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
    table=data.sheet_by_index(t-1)# 打开第一个sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "%s 's cols,rows is"%(z[t],cols,rows)#打印表名中的行列数
    row_1st=table.row_values(0)#读取第一行
    asin_index=row_1st.index('Asin')#返回Asin列的所在列数
    URL=table.col_values(asin_index,1,rows-1)#读取Asin列第二行到最后一行
    data1=copy(data)#复制工作簿
    table1=data1.get_sheet(t-1)
    for i in range(rows-1):
        try:
            time.sleep(2)
            r,url=requests_url(URL[i])
            print i,url
            rank1=re.findall(r'(#\d+,\d{3}.*?)\(',r)#匹配排名大于百位的主目录排名
            if rank1:
                print rank1
            else:
                rank1=re.findall(r'(#\d{1,3}.*?)\(<a href',r)#匹配排名小于千位的主目录排名
                print rank1
            rank2=re.findall(r'>(#\d+)<\/span>',r)#匹配排名小于千位的小目录排名
            if rank2:
                print rank2
            else:
                rank2=re.findall(r'>(#\d+|#\d+,\d{3})\sin\s<',r)#匹配排名大于百位的小目录排名
                print rank2

            
            
            table1.write(i+1,cols,rank1[0])#写入sheet中
            table1.write(i+1,cols+1,rank2)

        except Exception,e:
            print str(e)
    data1.save('c:\\first_Choice_copy.xls') #保存复制表格
    end=datetime.datetime.now()
    t=end-start#总用时
    print 'Total time: {0} s.'.format(t)

if  __name__== '__main__':
    main()
            
    
    
