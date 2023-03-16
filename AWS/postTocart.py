#-*- coding:UTF-8 -*-
# ========================
#Python version: 3.8.1  
#Author: Toryun        
#Update: 2023-03-12
#Function:Get the stocks of amaozn goods by add to Cart 
#========================
import os
import re
import ssl
import time
import xlwt
import html
import urllib
import random
import requests
from io import BytesIO
from amazoncaptcha import AmazonCaptcha
from Amazon_Utils import File_path_choice, Get_ASINlists, is_TTD, retry, excel_bulit, requests_asin

ssl._create_default_https_context = ssl._create_unverified_context


def get_form_parameter(f):
    '''addCart params'''
    offerListingID = re.findall("name=\"offerListingID\" value=\"(.*?)\">", f)
    CSRF = re.findall("name='CSRF' value='(.*?)'", f)
    anti_csrftoken_a2z = re.findall("id=\"anti-csrftoken-a2z\" name=\"anti-csrftoken-a2z\" value=\"(.*?)\"", f)
    session_id = re.findall('id=\"session-id\" name=\"session-id\" value=\"(.*?)\"', f)
    merchantID = re.findall("name=\"merchantID\" value=\"(.*?)\"", f)
    return offerListingID, CSRF, anti_csrftoken_a2z, session_id,merchantID

def Post_form_addToCart(session, asin, offerListingID, CSRF, anti_csrftoken_a2z, session_id,merchantID):
    '''Match the POST parameter and add the product to the shopping cart.匹配post参数，添加产品到购物车'''
    host_url='https://www.amazon.com/gp/product/handle-buy-box/ref=dp_start-bbf_1_glance'#请求网址
    post_parameter={
        "items[0.base][asin]": asin,
        "clientName": "OffersX_OfferDisplay_DetailPage",
        "items[0.base][offerListingId]": offerListingID,
        "CSRF": CSRF,
        "anti-csrftoken-a2z": anti_csrftoken_a2z,
        "offerListingID": offerListingID,
        "session-id": session_id,
        "ASIN": asin,
        "isMerchantExclusive": "0",
        "merchantID": merchantID,
        "isAddon": "0",
        "nodeID": "",
        "sellingCustomerID": "",
        "qid": "",
        "sr": "",
        "storeID": "",
        "tagActionCode": "",
        "viewID": "glance",
        "rebateId": "",
        "ctaDeviceType": "desktop",
        "ctaPageType": "detail",
        "usePrimeHandler": "0",
        "rsid": session_id,
        "sourceCustomerOrgListID": "",
        "sourceCustomerOrgListItemID": "",
        "wlPopCommand": "",
        "items[0.base][quantity]": "1",
        "quantity": "1",
        "submit.add-to-cart": "Add to Cart",
        "dropdown-selection": "add-new",
        "dropdown-selection-ubb": "add-new"
        }#添加商品到购物车的参数
    headers={
        'Connection':'keep-alive',
        'Cache-Control':'max-age=0',
        'device-memory':'8',
        'sec-ch-device-memory':'8',
        'dpr':'2',
        'sec-ch-dpr':'2',
        'viewport-width':'1920',
        'sec-ch-viewport-width':'1920',
        'rtt':'100',
        'downlink':'10',
        'ect':'4g',
        'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"macOS"',
        'Upgrade-Insecure-Requests':'1',
        'Origin':'https://www.amazon.com',
        'Content-Type':'application/x-www-form-urlencoded',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site':'same-origin',
        'Sec-Fetch-Mode':'navigate',
        'Sec-Fetch-User':'?1',
        'Sec-Fetch-Dest':'document',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9'
        }#请求头部
    r = session.post(host_url,data = post_parameter,headers = headers)
    i = 0
    while is_TTD(r.text):#如果变狗则不断请求直到不变狗
        r = session.post(host_url,data = post_parameter,headers = headers)
        time.sleep(random.randint(0,6))
        print(r.status_code)#返回请求状态（成功为200）
        i += 1
        if i == 5:
            break

def get_cart_view(session):
    '''Get the shopping cart page and match the key API, such as token, requestID, activeItems...获取购物车页面，匹配token、requestID、activeItems等关键API'''
    url = "https://www.amazon.com/gp/cart/view.html?ref_=sw_gtc"
    headers = {
        'host':'www.amazon.com',
        'Connection':'keep-alive',
        'Cache-Control':'max-age=0',
        'device-memory':'8',
        'sec-ch-device-memory':'8',
        'dpr':'2',
        'sec-ch-dpr':'2',
        'viewport-width':'683',
        'sec-ch-viewport-width':'683',
        'rtt':'350',
        'downlink':'1.4',
        'ect':'3g',
        'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"macOS"',
        'Upgrade-Insecure-Requests':'1',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site':'same-origin',
        'Sec-Fetch-Mode':'navigate',
        'Sec-Fetch-User':'?1',
        'Sec-Fetch-Dest':'document',
        'Referer':'https://www.amazon.com/cart/smart-wagon?newItems=23a604b9-c6e4-4622-abcf-f35b1456f995,1',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9'
        }
    r = session.get(url, headers=headers, timeout = 5)
    price = re.findall('sc-product-price a-text-bold">\$(.*?)<\/span>', r.text) 
    token = re.findall('name=\'token\' value=\'(.*?)\'', r.text)
    requestID = re.findall('name=\'requestID\' value=\'(.*?)\'', r.text)
    actionItemID = re.findall('data-itemid=\"(.*?)\"', r.text)
    encodedOffering = re.findall('data-encoded-offering=\"(.*?)\"', r.text)
    return price,token,actionItemID,requestID,encodedOffering
@retry
def update_quantity(session, asin, price, token, actionItemID, requestID, encodedOffering):
    '''Pass 999 quantity request return inventory.传递999数量请求返回库存量'''
    t=int(time.time())
    url='https://www.amazon.com/cart/ref=ox_sc_update_quantity_1%7C1%7C999'
    headers={
        'Connection':'keep-alive',
        'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'X-AUI-View':'Desktop',
        'sec-ch-device-memory':'8',
        'sec-ch-viewport-width':'1017',
        'X-Requested-With':'XMLHttpRequest',
        'dpr':'2',
        'downlink':'1.25',
        'sec-ch-ua-platform':'"macOS"',
        'device-memory':'8',
        'rtt':'250',
        'sec-ch-ua-mobile':'?0',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'viewport-width':'1017',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8;',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'sec-ch-dpr':'2',
        'ect':'3g',
        'Origin':'https://www.amazon.com',
        'Sec-Fetch-Site':'same-origin',
        'Sec-Fetch-Mode':'cors',
        'Sec-Fetch-Dest':'empty',
        'Referer':'https://www.amazon.com/gp/cart/view.html?ref_=sw_gtc',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9'
        }
    json_update={
        "quantity.{0}".format(actionItemID): "999",
        "pageAction": "update-quantity",
        "submit.update-quantity.{0}".format(actionItemID): "1",
        "displayedSavedItemNum": "0",
        "actionItemID": actionItemID,
        "actionType": "update-quantity",
        "asin": asin,
        "encodedOffering": encodedOffering,
        "hasMoreItems": "false",
        "addressId": "",
        "addressZip": "",
        "closeAddonUpsell": "1",
        "displayedSavedItemNum": "0",
        "activeItems": [{"itemId":"sc-active-{}".format(actionItemID),"giftable":1,"giftWrapped":0,"quantity":1,"price":price,"incentivizedCartMessage":"","installments":{}}],
        "savedItems": [],
        "timeStamp": t,
        "requestID": requestID,
        "token": token
        }
    r = session.post(url,data=json_update,headers=headers)
    stock = "inf"
    if r.status_code == 200:
        stock=r.json()['features']['nav-cart']['cartQty']
    return stock

def delete_quantity(session, asin, price, token, actionItemID, requestID, stock, encodedOffering):
    '''Delete the products that have been added to the shopping cart.删除已添加到购物车的产品'''
    t=int(time.time())#时间戳
    url='https://www.amazon.com/cart/ref=ox_sc_cart_actions_1'
    headers={
        'Connection':'keep-alive',
        'sec-ch-ua':'"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'X-AUI-View':'Desktop',
        'sec-ch-device-memory':'8',
        'sec-ch-viewport-width':'1017',
        'X-Requested-With':'XMLHttpRequest',
        'dpr':'2',
        'downlink':'1.45',
        'sec-ch-ua-platform':'"macOS"',
        'device-memory':'8',
        'rtt':'300',
        'sec-ch-ua-mobile':'?0',
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'viewport-width':'1017',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8;',
        'Accept':'application/json, text/javascript, */*; q=0.01',
        'sec-ch-dpr':'2',
        'ect':'3g',
        'Origin':'https://www.amazon.com',
        'Sec-Fetch-Site':'same-origin',
        'Sec-Fetch-Mode':'cors',
        'Sec-Fetch-Dest':'empty',
        'Referer':'https://www.amazon.com/gp/cart/view.html?ref_=sw_gtc',
        'Accept-Encoding':'gzip, deflate, br',
        'Accept-Language':'en-US,en;q=0.9'
        }
    json_dalete={
        "submit.cart-actions": "1",
        "pageAction": "cart-actions",
        "actionPayload": [{"type":"DELETE_START","payload":{"itemId":actionItemID,"list":"activeItems","relatedItemIds":[],"isPrimeAsin":"false"}}],
        "hasMoreItems": "false",
        "addressId": "",
        "addressZip": "",
        "closeAddonUpsell": "1",
        "displayedSavedItemNum": "0",
        "activeItems": [{"itemId":"sc-active-{}".format(actionItemID),"giftable":1,"giftWrapped":0,"quantity":stock,"price":price,"incentivizedCartMessage":"","installments":{},"isSelected":1}],
        "savedItems": [],
        "timeStamp": t,
        "requestID": requestID,
        "token": token
        }
    session.post(url, data = json_dalete, headers = headers)
    return session

def main():
    #fn=File_path_choice()
    #ASINs=["B09PVJVS15", "B09V71J4CL"]
    fp = "./asin.xls"
    ASINs = Get_ASINlists(fp)
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table=excel_bulit(workbook, "1")
    for i in range(len(ASINs)):
        asin = ASINs[i]
        stock = "inf"
        table.write(i,0,asin)
        r, session = requests_asin('https://www.amazon.com'+asin)
        if not is_TTD(r):
            offerListingId, CSRF, anti_csrftoken_a2z, session_id,merchantID=get_form_parameter(r)
            if offerListingId[0]=='' and merchantID[0]=='' and len(CSRF)==1:#不在售
                stock = 0
            else:
                Post_form_addToCart(session, asin, offerListingId[0], CSRF[0], anti_csrftoken_a2z[0], session_id[0],merchantID[0])
                price, token, actionItemID, requestID, encodedOffering = get_cart_view(session)       
                stock = update_quantity(session, asin, price[0], token[0], actionItemID[0], requestID[0], encodedOffering[0])
                delete_quantity(session, asin, price[0], token[0], actionItemID[0], requestID[0], stock, encodedOffering[0])   
        else:
            print("Request is blocked: {}".format(url))
        table.write(i,1,stock)
        print(asin, stock)
    file_save='./stock.xls'
    workbook.save(file_save)
    print("Saved to {}".format(os.path.abspath(file_save)))
if __name__=='__main__':
    main()
