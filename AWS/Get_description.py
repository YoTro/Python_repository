# -*- coding:UTF-8 -*-
#Python version is 2.7.13  
#Author: Toryun           
#Time:2017-08- 28         
#Fuction:Get description of product in the list 
'''API：<li><span class="a-list-item">\s+(.*?)<\/span><\/li>


<div id="productDescription" class="a-section a-spacing-small">.*?<p>(.*?)<\/p>'''
import re,requests,xlrd,xlwt,datetime,time,os
from xlutils.copy import copy
'''从表中获取URL查询Amazon商家店铺30天内的feedback'''
def  main():
    start=datetime.datetime.now()
    try:
        filename=raw_input("plz input a filename like c:\\eakd.xlsx (defult filename is d:/Documents/Downloads/Search Term Food Storage Container.xls) :\n")
        t=os.path.exists(filename)
        while t==False:
            filename=raw_input("The path is wrong,plz input a correct filename like c:\\eakd.xlsx:\n")
            t=os.path.exists(filename)
            if filename=="\\n":
                filename='d:/Documents/Downloads/Search Term Food Storage Container.xls'
    except Exception,e:
        print str(e)
        
    data=xlrd.open_workbook(filename) # 打开指定工作薄
    sheets=data.sheets()#获取工作薄所有列表
    print type(sheets)
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p] #返回所有列表名
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:
        print str(e)
        t=1
    table=data.sheet_by_index(t-1) # 打开列表
    nrows=table.nrows # 行数
    cols=table.ncols # 列数
    print '列数：%d, 行数：%d'%(cols,nrows)
    row_1st=table.row_values(0)#读取第一行
    URL_index=row_1st.index('URL')#返回URL列的所在列数
    URL=table.col_values(URL_index) # 读取导入URL数组
    l=len(URL)
    headers={'Host':	
"www.amazon.com",
'User-Agent':	
"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3141.7 Safari/537.36",
'Accept':
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
'Accept-Language':	
"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
'Accept-Encoding':	
"gzip, deflate, br",
'Connection':	
"keep-alive",
'Upgrade-Insecure-Requests':	
"1"}
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费IP地址*http://www.xicidaili.com*
    data2=copy(data) # 复制工作簿
    table2=data2.get_sheet(t-1)
    for i in range(nrows):
        try:
            r=requests.get(URL[i+1],headers=headers,proxies=proxies)
            print i+1,URL[i+1]
            list_description=re.findall(r'<li><span class="a-list-item">\s+(.*?)\s+<\/span><\/li>',r.content) #返回匹配的商品的list详情
            productDescription=re.findall(r'<!-- show up to 2 reviews by default -->\s+<p>(.*)\s+<\/p>',r.content)#返回匹配的商品产品详情
            if list_description:
                print list_description
                table2.write(i+1,cols,str(list_description)
            if productDescription:
                print  productDescription
                table2.write(i+1,cols+1,str(productDescription))
            else:
                print 'None'
  
        except Exception,e:
            print str(e) 
    filepath='d:/Documents/Downloads/best_copy1.xls'
    data2.save(filepath) #保存到新的工作簿
    end=datetime.datetime.now()
    t=end-start 
    print '存储到新的工作簿 {0}\n总用时:{1} s'.format(filepath,t)
if __name__=='__main__':
    main()
