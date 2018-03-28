# -*- coding:UTF-8 -*-
# =====================
#==Python version is 2.7.13  
#== Author: Toryun           
#== Time:2017-08- 28         
#======================
import re,requests,xlrd,datetime,time,os
from xlutils.copy import copy
'''从表中获取URL查询Amazon商家店铺30天内的feedback'''
def File_path_choice():
    '''选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='d:/documents/downloads'
        print '路径{0}文件夹中的文件和文件夹如下：'.format(file_path)
        file_names=os.listdir(file_path)#列出下载文件夹中的文件名
        for i in range(len(file_names)):
            print i+1,file_names[i]
        file_num=int(raw_input("Default workbook is 1,plz input a number of serial number(default {0}):\nOr Enter a number more than the last option you could input a url of file\n ".format(t)))#默认文件名是t,或者输入比最后选项大的数
        if file_num in range(1,len(file_names)+1):
            file_path=file_path+'/'+file_names[file_num-1]
        if file_num>=len(file_names)+1:
            file_path=str(raw_input("plz input a fileurl (like:D:\\Documents\\Downloads\1.xlsx\n"))
            f=os.path.exists(file_path)
            while f==False:
                file_path=str(raw_input("Your file is not exsits,plz input a fileurl:\n"))
                f=os.path.exists(file_path)
    except Exception,e:
        print str(e)
        file_path=t
    return file_path
def  main():
    start=datetime.datetime.now()
    filename=File_path_choice()
        
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
    FBA_index=row_1st.index('FBA')#返回运输方式的所在列数
    URL_index=row_1st.index('URL')#返回URL列的所在列数
    FBA=table.col_values(FBA_index)# 读取导入FBA数组
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
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    data2=copy(data) # 复制工作簿
    table2=data2.get_sheet(t-1)
    for i in range(nrows-1):
        try:
            if FBA[i+1]=='FBA': #判断是否为FBA运输方式（因为只有该运输方式和第三方运输可以查店铺月反馈数
                r=requests.get(URL[i+1],headers=headers,proxies=proxies)
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
                        table2.write(i+1,cols,counts[0])# 进行写入操作
            if FBA[i+1]=='MCH':
                r=requests.get(URL[i+1],headers=headers,proxies=proxies)
                print i+1,URL[i+1]
                sellerID=re.findall(r'\/gp\/help\/seller\/at-a-glance\.html\?ie=UTF8&amp;seller=(.*?)&amp;isAmazonFulfilled=1',r.content) #返回第一个匹配的卖家店铺sellerID
                print sellerID[0]
                if sellerID:
                    q=requests.get('https://www.amazon.com/sp?_encoding=UTF8&asin=&isAmazonFulfilled=1&isCBA=&marketplaceID=ATVPDKIKX0DER&orderID=&seller='+sellerID[0]+'&tab=&vasStoreID=') 
                    counts=re.findall(r'Count<\/td><td class=\"a-text-right\"><span>(.*?)<\/span>',q.content)# 匹配30天内反馈数
                    if counts is None:
                        print '匹配失败'
                    else:
                        print counts
                        table2.write(i+1,cols,counts[0])# 进行写入操作
                if sellerID[0]==u'':
                    return None
                    print '匹配店铺失败'
                if sellerID==None:
                    print '该店铺无sellerID'
            if FBA[i+1]=='AMZ':
                i+=1
     
        except Exception,e:
            print str(e) 
    filepath='d:/Documents/Downloads/best_copy.xls'
    data2.save(filepath) #保存到新的工作簿
    end=datetime.datetime.now()
    t=end-start 
    print '存储到新的工作簿 {0}\n总用时:{1} s'.format(filepath,t)
if __name__=='__main__':
    main()
        
