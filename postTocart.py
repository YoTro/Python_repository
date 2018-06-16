#-*- coding:UTF-8 -*-
# ========================
#Python version is 2.7.13  
# Author: Toryun        
#Date:2017-08- 28
#Function:Get the inventory of amaozn's productions though by post
#========================
import os
import re
import time
import xlrd
import xlwt
import datetime
import requests

#-----------------------------------------------------------------------------------------------------------------------------------
def File_path_choice():
    '''Select the file in the folder and return the selected file path.选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='D:\\Documents\\Downloads'
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
#-----------------------------------------------------------------------------------------------------------------------------------
def read_excel(file_path):
    '''读取Excel'''
    data=xlrd.open_workbook(file_path)#打开工作簿
    sheets=data.sheets()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:
        print str(e)
        t=1

    table=data.sheet_by_index(t-1)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "{0}'s cols,rows is {1},{2}".format(z[t],cols,rows)#打印该sheet中的列行数
    row_1st=table.row_values(0)#读取第一行
    asin_index=row_1st.index('Asin')#返回Asin列的所在列数
    Asin=table.col_values(asin_index,1,rows-1)#读取Asin列第二行到最后一行
    return Asin

#-----------------------------------------------------------------------------------------------------------------------------------
def requests_url(url):
    '''Get the content of listing web page.获取listing网页内容'''
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
    session=requests.Session()
    r=session.get(url,headers=headers)
    return r.content,session

#-----------------------------------------------------------------------------------------------------------------------------------
def get_form_parameter(r):
    session_id=re.findall(r'<input type="hidden" id="session-id" name="session-id" value="(.*?)"',r)
    offerListingID=re.findall(r'<input type="hidden" id="offerListingID" name="offerListingID" value="(.*?)">',r)
    merchantID=re.findall(r'<input type="hidden" id="merchantID" name="merchantID" value="(.*?)">',r)   
    tagActionCode=re.findall(r'<input type="hidden" id="tagActionCode" name="tagActionCode" value="(.*?)">',r)
    storeID=re.findall(r'<input type="hidden" id="storeID" name="storeID" value="(.*?)"',r)
    return session_id,offerListingID,merchantID,tagActionCode,storeID
    
#-----------------------------------------------------------------------------------------------------------------------------------
def Post_form_addToCart(session,asin,url,session_id,merchantID,offerListingID,tagActionCode,storeID):
    '''Match the POST parameter and add the product to the shopping cart.匹配post参数，添加产品到购物车'''

    
    post_parameter={'session-id':'{0}'.format(session_id),
'ASIN':'{0}'.format(asin),
'offerListingID':'{0}'.format(offerListingID),
'isMerchantExclusive':'0',
'merchantID':'{0}'.format(merchantID),
'isAddon':'0',
'nodeID':'{}'.format(tagActionCode),
'sellingCustomerID':'{0}'.format(merchantID),
'qid':'',	
'sr':'',
'storeID':'{}'.format(storeID),
'tagActionCode':'{}'.format(tagActionCode),
'viewID':'glance',
'rebateId':'',
'rsid':'{}'.format(session_id),
'sourceCustomerOrgListID':'',	
'sourceCustomerOrgListItemID':'	',
'wlPopCommand':'',
'quantity':'1',
'submit.add-to-cart':'Add+to+Cart',
'dropdown-selection':'add-new'#A2L77EE7U53NWQ
                    }#添加商品到购物车的参数
    host_url='https://www.amazon.com/gp/product/handle-buy-box/ref=dp_start-bbf_1_glance'#请求网址
    headers={'Accept':	
'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
'Accept-Encoding':
'gzip, deflate, br',
'Accept-Language':
'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
'Connection':	
'keep-alive',
'Content-Type':
'application/x-www-form-urlencoded',
'Host':
'www.amazon.com',
'Referer':
'{0}'.format(url),
'User-Agent':	
'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
}#请求头部
    r=session.post(host_url,data=post_parameter,headers=headers)
    empty=re.findall(r'Your Shopping Cart is empty',r.content)
    print empty[0]
    while empty[0]:
        Post_form_addToCart(session,asin,url,session_id,merchantID,offerListingID,tagActionCode,storeID)
        time.sleep(randrom.randint(0,6))
        print empty[0]
    print r.status_code#返回请求状态（成功为200）
#-----------------------------------------------------------------------------------------------------------------------------------
def get_cart_view(session):
    '''Get the shopping cart page and match the key API, such as token, requestID, activeItems...获取购物车页面，匹配token、requestID、activeItems等关键API'''
    url='https://www.amazon.com/gp/cart/view.html/ref=lh_cart'
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",

"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Accept-Language":
"zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
"Accept-Encoding":
"gzip, deflate, br",
"Connection":
"keep-alive"
             }
    r=session.get(url,headers=headers)
    Price=re.findall(r'sc-price-sign a-text-bold">\s+\$(.*?)\s+<\/span>',r.content) 
    token=re.findall(r'<input type="hidden" name="token" value="(.*?)">',r.content)
    requestID=re.findall(r'<input type="hidden" name="requestID" value="(.*?)"',r.content)
    actionItemID=re.findall(r'newItems=(.*?)\'',r.content)
    encodedOffering=re.findall(r'data-encoded-offering="(.*?)"',r.content)
    return Price,token,actionItemID,requestID,encodedOffering
#-----------------------------------------------------------------------------------------------------------------------------------
def update_quantity(session,asin,Price,token,actionItemID,requestID,encodedOffering):
    '''Pass 999 quantity request return inventory.传递999数量请求返回库存量'''
    t=int(time.time())
    url='https://www.amazon.com/gp/cart/view.html/ref=lh_cart'
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
"Referer":"https://www.amazon.com/gp/cart/view.html/ref=lh_cart",
"Accept":
"application/json, text/javascript, */*; q=0.01",
"Accept-Language":
"zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
"Accept-Encoding":
"gzip, deflate, br",
"Connection":
"keep-alive",
"Content-Type":
"application/x-www-form-urlencoded; charset=UTF-8;",
"X-Requested-With":"XMLHttpRequest",
"X-AUI-View":"Desktop"
}
    json_update={'hasMoreItems':'0',
'timeStamp':'{0}'.format(t),
'token':'{0}'.format(token),
'requestID':'{}'.format(requestID),
'activeItemID':'{}'.format(actionItemID),
'activeItems':'{0}|1|0|1|{1}|||0||'.format(actionItemID,Price),
'addressId':'',	
'addressZip':'',	
'closeAddonUpsell':'1',
'flcExpanded':'0',
'quantity.{0}'.format(actionItemID):'999',
'pageAction':'update-quantity',
'submit.update-quantity.{0}'.format(actionItemID):'1',
'actionItemID':'{0}'.format(actionItemID),
'asin':'{0}'.format(asin),
'encodedOffering':'{0}'.format(encodedOffering)
                 }
    r=session.post(url,data=json_update,headers=headers)
    stock=re.findall(r'cartQty:(\d+)',r.content)
    print r.status_code
    return stock
#-----------------------------------------------------------------------------------------------------------------------------------
def delete_quantity(session,asin,Price,token,actionItemID,requestID,stock,encodedOffering):
    '''Delete the products that have been added to the shopping cart.删除已添加到购物车的产品'''
    t=int(time.time())#时间戳
    url='https://www.amazon.com/gp/cart/ajax-update.html/ref=ox_sc_cart_delete_1'
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
"Referer":"https://www.amazon.com/gp/cart/view.html/ref=lh_cart",
"Accept":
"application/json, text/javascript, */*; q=0.01",
"Accept-Language":
"zh-CN,en;q=0.8,zh;q=0.7,zh-TW;q=0.5,zh-HK;q=0.3,en-US;q=0.2",
"Accept-Encoding":
"gzip, deflate, br",
"Connection":
"keep-alive",
"Content-Type":
"application/x-www-form-urlencoded; charset=UTF-8;",
"X-Requested-With":"XMLHttpRequest",
"X-AUI-View":"Desktop"
}
    json_dalete={'hasMoreItems':'0',
'timeStamp':'{0}'.format(t),
'token':'{0}'.format(token),
'requestID':'{0}'.format(requestID),
'activeItems':'{0}|1|0|{1}|{2}|||0||'.format(activeItemID,stock,Price),
'addressId':'',	
'addressZip':'',	
'closeAddonUpsell':'1',
'flcExpanded':'0',
'pageAction':'delete-active',
'submit.delete.{0}'.format(actionItemID):'{0}'.format(stock),
'actionItemID':'{0}'.format(actionItemID),
'asin':'{0}'.format(asin),
'encodedOffering':'{0}'.format(encodedOffering)
}
    r=session.post(url,data=json_dalete,headers=headers)
    print r.status_code
#-----------------------------------------------------------------------------------------------------------------------------------
def write_excel():
    '''Bulit a excel.构建Excel'''
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table= workbook.add_sheet("stock",cell_overwrite_ok=True)
    style = xlwt.XFStyle()#设置样式
    font = xlwt.Font()#设置字体
    font.name = 'SimSun' # 指定“宋体”
    style.font = font
    alignment=xlwt.Alignment()#设置对齐
    alignment.horz=xlwt.Alignment.HORZ_CENTER#单元格字符水平居中
    # 格式: HORZ_GENERAL, HORZ_LEFT, HORZ_CENTER, HORZ_RIGHT, HORZ_FILLED, HORZ_JUSTIFIED, HORZ_CENTER_ACROSS_SEL, HORZ_DISTRIBUTED
    alignment.vert=xlwt.Alignment.VERT_CENTER#单元格字符垂直居中
    #格式: VERT_TOP, VERT_CENTER, VERT_BOTTOM, VERT_JUSTIFIED, VERT_DISTRIBUTED
    style.alignment=alignment#添加至样式
    return workbook,table
    
#-----------------------------------------------------------------------------------------------------------------------------------
def main():
    file_path=File_path_choice()
    Asin=read_excel(file_path)
    workbook,table=write_excel()
    for i in range(len(Asin)):
        asin=Asin[i]
        url='https://www.amazon.com/dp/'+asin
        r,session=requests_url(url)
        session_id,merchantID,offerListingID,tagActionCode,storeID=get_form_parameter(r)
        Post_form_addToCart(session,asin,url,session_id[0],merchantID[0],offerListingID[0],tagActionCode[0],storeID[0])
        Price,token,actionItemID,requestID,encodedOffering=get_cart_view(session)
        
        stock=update_quantity(session,asin,Price[0],token[0],actionItemID[0],requestID[0],encodedOffering[0])
        delete_quantity(session,asin,Price[0],token[0],actionItemID[0],requestID[0],stock[0],encodedOffering[0])
        table.write(i,0,asin)
        table.write(i,1,stock)
    file_save='C:/Users/Administrator/Desktop/stock.xls'
    workbook.save(file_save)
if __name__=='__main__':
        main()
