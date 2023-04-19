# -*- coding: utf-8 -*-
"""
@author:Toryun
@data:2023/4/18
@version:Python3.8
@Function: 获取前程无忧招聘工作数据
"""
import re
import os
import time
import json
import random
import execjs
import pickle
import requests
import pandas as pd
import urllib.parse
import hmac
from hashlib import sha256
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from lxml import etree
from tqdm import tqdm
from proxy import proxies
class Job(object):
    def __init__(self):
        '''初始化参数'''
        self.__baseurl = 'https://cupidjob.51job.com/open/noauth/search-pc' 
        self.__api_key = "51job"
        self.__time = int(time.time())
        self.__keyword = "亚马逊运营"
        self.__urlencode_keyword = urllib.parse.quote("亚马逊运营")
        self.__searchType = 2
        self.__function = ""
        self.__industry = ""
        self.__jobArea = "000000"#默认全国
        self.__jobArea2 = ""
        self.__landmark = ""
        self.__metro = ""
        self.__salary = ""
        self.__workYear = ""
        self.__degree = ""
        self.__companyType = ""
        self.__companySize = ""
        self.__jobType = ""
        self.__issueDate = ""
        self.__sortType = 0
        self.__pageNum = 1
        self.__requestId = ""
        self.__pageSize = 50#20条一页最多请求1000条
        self.__source = 1
        self.__accountId = ""
        self.__pageCode = "sou|sou|soulb"
        self.__key = 'abfc8f9dcf8c3f3d8aa294ac5f2cf2cc7767e5592590f39c3f503271dd68562b'#sign密钥
        self.__proxies = proxies()#{'http': ['20.111.54.16:80', '20.206.106.192:80', '20.111.54.16:8123', '20.210.113.32:8123', '120.197.219.82:9091', '183.237.47.54:9091', '20.24.43.214:8123', '182.106.220.252:9091', '117.40.176.42:9091', '171.34.53.2:9091', '223.84.240.36:9091'], 'https': ['127.0.0.1:8001']}#代理ip

    def get_sign(self, params, key):  
        #sign加密方法
        encoded_params = urlencode(params)
        message =  '/open/noauth/search-pc?'+ encoded_params
        hmac_key = bytes(key, 'utf-8')
        message = bytes(message, 'utf-8')
        signature = hmac.new(hmac_key, message, sha256).hexdigest()
        return signature

    def get_property(self):
        #检测是否登陆属性
        property ='%7B%22partner%22%3A%22%22%2C%22webId%22%3A2%2C%22fromdomain%22%3A%2251job_web%22%2C%22frompageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2F%22%2C%22pageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2Fpc%2Fsearch%3Fkeyword%3D{}%26searchType%3D2%26sortType%3D0%26metro%3D%22%2C%22identityType%22%3A%22%22%2C%22userType%22%3A%22%22%2C%22isLogin%22%3A%22%E5%90%A6%22%2C%22accountid%22%3A%22%22%7D'.format(urllib.parse.quote(self.__urlencode_keyword))
        return property

    def get_uuid(self):
        uuid = requests.get("https://oauth.51job.com/ajax/get_token.php?fromdomain=51job_web").json()['resultBody']['uuid']
        return uuid
    def __citycode__(self, city):
        #获取城市代码,默认全国
        self.__jobArea = "000000"
        url = 'https://vapi.51job.com/resource.php?query=dd&version=400&clientid=000011&accountid=&usertoken=&client_id=000011&property=%7B%22partner%22%3A%22%22%2C%22webId%22%3A2%2C%22fromdomain%22%3A%2251job_web%22%2C%22frompageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2F%22%2C%22pageUrl%22%3A%22https%3A%2F%2Fwe.51job.com%2Fpc%2Fsearch%3Fkeyword%3D%25E4%25BA%259A%25E9%25A9%25AC%25E9%2580%258A%25E8%25BF%2590%25E8%2590%25A5%26searchType%3D2%26sortType%3D0%26metro%3D%22%2C%22identityType%22%3A%22%22%2C%22userType%22%3A%22%22%2C%22isLogin%22%3A%22%E5%90%A6%22%2C%22accountid%22%3A%22%22%7D&dd_name=d_pc_abc_area&path=763cd7c36162daa3d2ed2b48b500e623&sign=53363cff7c255ce47b2c8944787c8196'

        response = requests.get(url)
        json_data = response.json()
        for i in range(len(json_data['resultbody'])):
            for item in json_data['resultbody'][i]['sub']:
              if item['value'] == city:
                  print(item['id'])
                  self.__jobArea = item['id']
                  break

    def __search__(self, job):
        #搜索招聘信息
        try:
            self.__keyword = job
        except:
            self.__keyword = "亚马逊运营"
        payload={
            "api_key":  self.__api_key,
            "timestamp":  self.__time,
            "keyword":  self.__keyword,
            "searchType":  self.__searchType,
            "function":  self.__function,
            "industry":  self.__industry,
            "jobArea": self.__jobArea,
            "jobArea2": self.__jobArea2,
            "landmark": self.__landmark,
            "metro": self.__metro,
            "salary": self.__salary,
            "workYear": self.__workYear,
            "degree": self.__degree,
            "companyType": self.__companyType,
            "companySize": self.__companySize,
            "jobType": self.__jobType,
            "issueDate": self.__issueDate,
            "sortType": self.__sortType,
            "pageNum": self.__pageNum,
            "requestId": self.__requestId,
            "pageSize": self.__pageSize,
            "source": self.__source,
            "accountId": self.__accountId,
            "pageCode": self.__pageCode
        }
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'JSESSIONID=AC424E89E896ACE2581306A5F3162C0B; acw_tc=ac11000116817489511698415e00dd6e33dd767bc22c847a283fd181bca0a6; uid=wKhJP2Q9c9d90Yn04FYjAg==',
            #'Cookie': 'guid=753e3a11c580dd4649d7f95dd88c1d6a; sajssdk_2015_cross_new_user=1; Hm_lvt_1370a11171bd6f2d9b1fe98951541941=1681739056; Hm_lpvt_1370a11171bd6f2d9b1fe98951541941=1681739056; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22753e3a11c580dd4649d7f95dd88c1d6a%22%2C%22first_id%22%3A%221878f74b224cc-04e52bc15a1bf4-1d525634-2073600-1878f74b225404%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTg3OGY3NGIyMjRjYy0wNGU1MmJjMTVhMWJmNC0xZDUyNTYzNC0yMDczNjAwLTE4NzhmNzRiMjI1NDA0IiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiNzUzZTNhMTFjNTgwZGQ0NjQ5ZDdmOTVkZDg4YzFkNmEifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%22753e3a11c580dd4649d7f95dd88c1d6a%22%7D%2C%22%24device_id%22%3A%221878f74b224cc-04e52bc15a1bf4-1d525634-2073600-1878f74b225404%22%7D; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; search=jobarea%7E%60%7C%21recentSearch0%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%D1%C7%C2%ED%D1%B7%D4%CB%D3%AA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21; acw_tc=ac11000116817391419527213e00dded418252606ba0c5d1f6c6c9342cb919; JSESSIONID=7F9E94C3F0C08826EC12AB428F44555C; uid=wKhJP2Q9TYZ9Y4nyzyMiAg==; JSESSIONID=E6F6189DA1387899D42ED8B125F42738',
            'From-Domain': '51job_web',
            'Origin': 'https://we.51job.com',
            'Referer': 'https://we.51job.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
            'account-id': '',
            'partner': '',
            'property': self.get_property(),
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sign': self.get_sign(payload, self.__key),
            'user-token': '',
            'uuid': '753e3a11c580dd4649d7f95dd88c1d6a'
        }
        url = self.__baseurl+'?'+urlencode(payload)
        response = requests.request("GET", url, headers=headers).json()
        #print(response['resultbody']['job']['items'][0])
        return response
    def __jobdetails__(self, url):
        target_url = "_0x48a0dc(_0x319bfa)"
        target_code = "_0x3baf44[_0x3e621b]=_0x30f62c;"
        def get_timestamp(url, proxies, target_url, target_code):
            timestamp__1258 = ""
            try:
                os.environ["EXECJS_RUNTIME"] = "Node"
                headers = {
                  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                  'Accept-Language': 'zh-CN,zh;q=0.9',
                  'Cache-Control': 'max-age=0',
                  'Connection': 'keep-alive',
                  'Cookie': 'acw_tc=ac11000116818008452498924e00e0a4044f563de8a51e045de6ce729d8179',
                  'Sec-Fetch-Dest': 'document',
                  'Sec-Fetch-Mode': 'navigate',
                  'Sec-Fetch-Site': 'same-origin',
                  'Sec-Fetch-User': '?1',
                  'Upgrade-Insecure-Requests': '1',
                  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                  'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                  'sec-ch-ua-mobile': '?0',
                  'sec-ch-ua-platform': '"macOS"'
                }
                response = requests.request("GET", url, headers=headers, proxies = proxies, timeout = 5)
                # 解析HTML内容
                soup = BeautifulSoup(response.text, 'html.parser')
                # 找到<script>标签
                script_tag = soup.find('script')
                # 获取<script>标签中的内容
                script_content = script_tag.string
                # 全局变量返回时间戳, 在目标位置插入新的代码
                insert_code = 'res=_0x30f62c;'
                # 加密需要传入url到此函数
                insert_url = "_0x48a0dc('{}')".format(url)
                first_script_content = script_content.replace(target_code, insert_code + target_code)
                new_script_content = first_script_content.replace(target_url, insert_url)
                # 全局环境
                global_js = '''
                    const jsdom = require("jsdom");
                    const { JSDOM } = jsdom;
                    const dom = new JSDOM(`<!DOCTYPE html><script>`);
                    window = dom.window;
                    document = window.document;
                    XMLHttpRequest = window.XMLHttpRequest;
                    global.navigator = {
                      userAgent: 'node.js'
                    };
                    var res;
                '''
                #编译js
                ctx = execjs.compile(global_js+new_script_content)
                # 返回timestamp__1258
                timestamp__1258 = ctx.eval('res')
                if(timestamp__1258==None):
                    timestamp__1258 = ""
                    print("被检测需要更换IP")
            except Exception as e:
                print(e)
            return timestamp__1258
        http_type = "http"
        proxies = {'https':'https://'+random.choice(self.__proxies[http_type])}
        timestamp__1258 = get_timestamp(url, proxies, target_url, target_code)
        new_url = url + "&timestamp__1258=" + urllib.parse.quote(timestamp__1258) 
        try:
            proxies = {'https':'https://'+random.choice(self.__proxies[http_type])}
            headers = {
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
              'Accept-Language': 'zh-CN,zh;q=0.9',
              'Cache-Control': 'max-age=0',
              'Connection': 'keep-alive',
              'Cookie': 'acw_tc=ac11000116818008452498924e00e0a4044f563de8a51e045de6ce729d8179',
              'Referer': 'https://jobs.51job.com/shenzhen/145527528.html?s=sou_sou_soulb&t=0_0&req=37fc80a0c963972cf0cc9b622ab85802',
              'Sec-Fetch-Dest': 'document',
              'Sec-Fetch-Mode': 'navigate',
              'Sec-Fetch-Site': 'same-origin',
              'Sec-Fetch-User': '?1',
              'Upgrade-Insecure-Requests': '1',
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
              'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"'
            }
            response1 = requests.request("GET", new_url, headers=headers, proxies = proxies, timeout = 5)
            i = 0
            while(target_code in response1.text):
                timestamp__1258 = get_timestamp(url, proxies, target_url, target_code)
                new_url = url + "&timestamp__1258=" + urllib.parse.quote(timestamp__1258) 
                response1 = requests.request("GET", new_url, headers=headers, proxies = proxies, timeout = 5)
                i+=1
                print(i, new_url)
                if i == 5:
                    break
            tree = etree.HTML(response1.text)
            sign = tree.xpath('//input[@id="sign"]/@value')[0]
            data = tree.xpath('//input[@id="data"]/@value')[0]
            url_jobdetails = "https://vapi.51job.com/job.php?apiversion=400&module=jobinfo&clientid=000005"
            payload = "data={}&sign={}".format(urllib.parse.quote(data), sign)
            headers = {
              'Accept': 'application/json, text/javascript, */*; q=0.01',
              'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
              'Connection': 'keep-alive',
              'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
              'Origin': 'https://jobs.51job.com',
              'Referer': 'https://jobs.51job.com/',
              'Sec-Fetch-Dest': 'empty',
              'Sec-Fetch-Mode': 'cors',
              'Sec-Fetch-Site': 'same-site',
              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
              'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
              'Cookie': 'guid=65f3b0e173fdcee5aa1a17fcb7707b50; acw_tc=ac11000116817988482433413e00df9fd1c1a32f77f755ebc6fb656d4bae06'
            }
            response = requests.request("POST", url_jobdetails, headers=headers, data=payload, proxies = proxies, timeout = 5).json()
            address = response['resultbody']['address']
            #提取文字转义换行标签
            cjobinfo = BeautifulSoup(response['resultbody']['cjobinfo'], 'html.parser').get_text(separator='\n')
        except Exception as e:
            print(e)
            cjobinfo = ""
            address = ""
        return cjobinfo, address
    def save_jobs(self, response):
        #以csv和pickle格式保存
        items = response['resultbody']['job']['items']
        datas = []
        for i in tqdm(range(len(items))):
            #公司名
            Company = items[i]['fullCompanyName']
            #工作地点
            Location = items[i]['jobAreaString']
            #岗位
            Job = items[i]['jobName']
            #薪资
            Salary = items[i]['provideSalaryString']
            #发布时间
            UpdateDateTime = items[i]['updateDateTime']
            #详情链接
            JobHref = items[i]['jobHref']
            #岗位职责与任职要求,上班地点
            Jobdetails, Address = self.__jobdetails__(JobHref)
            datas.append([Job, Salary, Company, Location, UpdateDateTime, JobHref, Jobdetails, Address])
        saved_path = './jobs.csv'
        with open('./jobs.pickle', 'wb') as f:
            pickle.dump(datas, f)
            f.close()
        df = pd.DataFrame(datas, columns=['Job', 'Salary', 'Company', 'Location', 'UpdateDateTime', 'JobHref', 'Jobdetails', 'Address'])
        df.to_csv(saved_path, index=False, encoding='utf-8-sig')
        print(os.path.abspath(saved_path))

if __name__=='__main__':
    city = "深圳"
    k = "自动化测试"
    job = Job()
    job.__citycode__(city)
    response = job.__search__(k)
    job.save_jobs(response)