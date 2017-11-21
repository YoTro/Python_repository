# -*- coding:UTF-8 -*-
#========================
#Python version is 2.7.13 
#Author: Toryun        
#Time:2017-08- 28       
#========================
import re,requests,xlrd,xlwt,datetime,time,os
from xlutils.copy import copy 
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
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p] #返回所有列表名
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)):
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
    FBA_index=row_1st.index('FBA')#返回运输方式的所在列数
    URL_index=row_1st.index('URL')#返回URL列的所在列数
    FBA=table.col_values(FBA_index)# 读取导入FBA数组
    URL=table.col_values(URL_index) # 读取导入URL数组
    l=len(URL)
    counts_arry=[]
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
    data2=copy(data) # 复制工作簿
    table2=data2.get_sheet(t-1)
    for i in range(nrows-2):
        try:
            if FBA[i+1]=='FBA': #判断是否为FBA运输方式（因为只有该运输方式和第三方运输可以查店铺月反馈数
                r=requests.get(URL[i+1])
                print i+1,URL[i+1]
                sellerID=re.findall(r'\/gp\/help\/seller\/at-a-glance\.html\/ref=dp_merchant_link\?ie=UTF8&amp;seller=(.*?)&amp;isAmazonFulfilled=1',r.content) #返回第一个匹配的卖家店铺sellerID
                print sellerID[0]
                if sellerID:
                    q=requests.get('https://www.amazon.com/sp?_encoding=UTF8&asin=&isAmazonFulfilled=1&isCBA=&marketplaceID=ATVPDKIKX0DER&orderID=&seller='+sellerID[0]+'&tab=&vasStoreID=') 
                    counts=re.findall(r'Count<\/td><td class=\"a-text-right\"><span>(.*?)<\/span>',q.content)# 匹配30天内反馈数
                    if counts is None:
                        print '匹配失败'
                    else:
                        print counts
                        table2.write(i+1,cols-1,counts[0])# 进行写入操作
                if sellerID[0]==u'':
                    return None
                    print '匹配店铺失败'
                if sellerID==None:
                    print '该店铺无sellerID'
            if FBA[i+1]=='AMZ':
                i+=1
            if FBA[i+1]=='MCH':
                r=requests.get(URL[i+1])
                print i+1,URL[i+1]
                sellerID=re.findall(r'\/gp\/help\/seller\/at-a-glance\.html\/ref=dp_merchant_link\?ie=UTF8&amp;seller=(.*?)\'>',r.content) #返回第一个匹配的卖家店铺sellerID
                print sellerID[0]
                if sellerID:
                    q=requests.get('https://www.amazon.com/sp?_encoding=UTF8&asin=&isAmazonFulfilled=1&isCBA=&marketplaceID=ATVPDKIKX0DER&orderID=&seller='+sellerID[0]+'&tab=&vasStoreID=') 
                    counts=re.findall(r'Count<\/td><td class=\"a-text-right\"><span>(.*?)<\/span>',q.content)# 匹配30天内反馈数
                    if counts is None:
                        print '匹配失败'
                    else:
                        print counts
                        table2.write(i+1,cols-1,counts[0])# 进行写入操作        
        except Exception,e:
            print str(e)
    print len(counts_arry)
    data2.save('d:/Documents/Downloads/best_copy.xls') #保存到新的工作簿
    end=datetime.datetime.now()
    t=end-start 
    print '总用时：%s s'%(t)
if __name__=='__main__':
    main()
        
