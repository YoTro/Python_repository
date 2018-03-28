#Python version:2.7.13
#Author:Toryun
#Date:2017-11-18
#Function:Get a number products of sellers
'''获取卖家上架产品数和页数'''
import requests,re,datetime,os,xlrd
from xlutils.copy import copy
def post_products(sellerID):
    '''获取卖家上架产品数和页数'''
    post_url='https://www.amazon.com/sp/ajax/products'
    headers={"Host":
    "www.amazon.com",
    "User-Agent":
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Accept":
    "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":
    "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
    "Accept-Encoding":
    "gzip, deflate, br",
    "Referer":
    "https://www.amazon.com/sp?_encoding=UTF8&asin=&isAmazonFulfilled=1&isCBA=&marketplaceID=ATVPDKIKX0DER&orderID=&seller={0}&tab=&vasStoreID=".format(sellerID),
    "Content-Type":
    "application/x-www-form-urlencoded",
    "Content-Length":	
    "313",
    "X-Requested-With":
    "XMLHttpRequest",
    "Cookie":
    "x-wl-uid=1dnnurSt1bfiNTOwNHfLsu0IQMCkCME8HNKE7nQa/V9+Gn9aUz4xNY52zyADhI3uc5rHcRUYm/KA=; session-id-time=2082787201l; session-id=147-6188368-5590659; ubid-main=133-0860109-3446425; session-token=9Vkx3Rqg2Cyc6BtpXvyhmmeBNEN9ukAAN7WPivZz9U87xbRdp4yG36+KbZZXG6AYdqIjjXjwBSUknP0gESudqpwW0GUj69jdYrUZAbePjDk0G3Xx3FT7uqPCXIpEoYsEJAjEgedklWlpWQAn3BQgxKy0XicKNDVnT3uDVuiE/sXySDreYrlmM6EP0hZZdITGDlkI/MlKi7iW2Nz47Ufo30c1TcuDQtzJA3g602ofFtB2LCfA37oCqBh4mUad+apz; s_nr=1503652206665-New; s_vnum=1935652206666%26vn%3D1; s_dslv=1503652206666; lc-main=en_US; skin=noskin",
    "Connection":
    "keep-alive"
             }
    data={"marketplaceID":"ATVPDKIKX0DER",
    "seller":"{0}".format(sellerID),
    "productSearchRequestData":{"marketplace":"ATVPDKIKX0DER","seller":"{0}".format(sellerID),"url":"/sp/ajax/products","pageSize":12,"searchKeyword":"","extraRestrictions":{},"pageNumber":1}}
    r=requests.post(post_url,data=data,headers=headers)
    return r.content
def requests_url(url):
    '''获取listing网页内容'''
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
}#火狐浏览器头部
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    r=requests.get(url,headers=headers,proxies=proxies)
    return r.content
def File_path_choice():
    '''选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='d:/documents/downloads'
        print '路径{0}文件夹中的文件和文件夹如下：'.format(file_path)
        file_names=os.listdir(file_path)#列出下载文件夹中的文件名
        for i in range(len(file_names)):
            print i+1,file_names[i]
        file_num=int(raw_input("Plz input a number of serial number (Default workbook is {0}):\n Or Enter  a number  more than the last option ,then you could  input a url of  file\n".format('stainless steel toilet brush holder.xlsx')))#默认文件名是t,或者输入比最后选项大的数
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
def main():
    '''打开工作簿，选择sheet匹配products数，存储到新的工作簿中'''
    start=datetime.datetime.now()#开始时间
    file_path=File_path_choice()#返回所选文件路径
    data=xlrd.open_workbook(file_path)#打开路径中文件
    sheets=data.sheets()#获取所有sheet (类型list)
    print "{0}'s sheets:\n".format(file_path)
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))#选择工作簿中的sheet
        if sheet_index in range(1,len(sheets)+1):#判断输入数是否超出范围
            sheet_num=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:#如果输入数非法，则默认为sheet1
        print str(e)
        sheet_num=1
    table=data.sheet_by_index(sheet_num-1)
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "{0} 's rows,cols are {1},{2}".format(z[sheet_num],rows,cols)
    rows_1st=table.row_values(0)#读取第一行
    URL_index=rows_1st.index('URL')#读取URL所在位置
    FBA_index=rows_1st.index('FBA')#返回运输方式的所在列数
    URL=table.col_values(URL_index,1,rows)#读取该列从第2行到最后一行
    FBA=table.col_values(FBA_index,1,rows)# 读取导入FBA数组
    data1=copy(data)#xlutils.copy 类里的copy函数
    table1=data1.get_sheet(sheet_num-1)
    for i in xrange(rows-1):
        try:
            if FBA[i+1]=='FBA': #判断是否为FBA运输方式（因为只有该运输方式和第三方运输可以查店铺月反馈数
                r=requests_url(URL[i+1])
                print i+1,URL[i+1]
                sellerID=re.findall(r'\/gp\/help\/seller\/at-a-glance\.html\/ref=dp_merchant_link\?ie=UTF8&amp;seller=(.*?)&amp;isAmazonFulfilled=1',r) #返回第一个匹配的卖家店铺sellerID
                print sellerID[0]
                if sellerID:
                    post=post_products(sellerID[0])
                    products_num=re.findall(r'\"productsTotalCount\":(\d+)',post)
                    print products_num
                    table1.write(i+1,cols,products_num)
                    table1.write(i+1,cols+1,sellerID[0])
                else:
                    print 'None'
        except Exception,e:#<type 'exceptions.ValueError'>
            print str(e)
    filepath='d:/Documents/Downloads/best_copy.xls'
    data1.save(filepath) #保存到新的工作簿
    end=datetime.datetime.now()
    t=end-start 
    print '存储到新的工作簿 {0}\n总用时:{1} s'.format(filepath,t)
if __name__=='__main__':
    main()
