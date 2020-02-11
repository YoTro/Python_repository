#-*- encoding: UTF-8 -*-
#Function: Getting the realtime data of Chinese 2019-nCoV
#API: the source we don't who create but thank you 

import requests,re
import json
import time
import csv


url = 'https://service-f9fjwngp-1252021671.bj.apigw.tencentcs.com/release/pneumonia'
html = requests.get(url).text
unicodestr=json.loads(html)  #将json转化为dict
dat = unicodestr["data"].get("statistics")["modifyTime"] #获取data中的内容，取出的内容为str
timeArray = time.localtime(dat/1000)
formatTime = time.strftime("%Y-%m-%d %H:%M", timeArray)

url = 'http://api.map.baidu.com/geocoder/v2/'
output = 'json'
ak = 'XeCfCY777qDMTKSqyc3LTiGPnMA7fqzy'#你的ak

new_list = unicodestr.get("data").get("listByArea")  #获取data中的内容，取出的内容为str

j = 0
print("###############"
      " 读取中   &"
      "&   数据来源：丁香医生 API来源未知"
      "###############")
while j < len(new_list):
    a = new_list[j]["cities"]
    s = new_list[j]["provinceName"].encode('utf-8')

    header = ['时间', '城市', '确诊人数', '疑似病例', '死亡人数', '治愈人数' ,'经度','纬度']
    with open('./'+s+'.csv','w') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(header)
    f.close()

    def save_data(data):
        with open('./'+s+'.csv','a+') as f:#'a+'参数是追加和可读的意思
            f_csv = csv.writer(f)
            f_csv.writerow(data)
        f.close()

    b = len(a)
    i = 0
    while i<b:
        data = (formatTime)
        confirm = (a[i]['confirmed'])
        city = (a[i]['cityName'])
        suspect = (a[i]['suspected'])
        dead = (a[i]['dead'])
        heal = (a[i]['cured'])

        add = a[i]['cityName'].encode('utf-8')
        #uri = url + '?' + 'address=' + add + '&output=' + output + '&ak=' + ak  # 百度地理编码API
        #res = requests.get(uri).text
        #temp = json.loads(res)

        #if temp['status'] == 1:
            #temp["result"] = {'location': {'lng': 0, 'lat': 0}}

        #lon = temp['result']['location']['lng']
        #lat = temp['result']['location']['lat']

        i+=1
        #tap = (data, city.encode('utf-8'), confirm, suspect, dead, heal, lon, lat)
        tap = (data, city.encode('utf-8'), confirm, suspect, dead, heal)
        save_data(tap)
    j += 1
    print(s+"下载结束!")
print("##########数据下载结束#########")
