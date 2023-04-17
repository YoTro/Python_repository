# -*- coding: utf-8 -*-
"""
@author:Toryun
@data:2023/4/18
@version:Python3.8
@Function: 获取前程无忧招聘工作数据
"""
import os
import time
import json
import pickle
import requests
import pandas as pd
import urllib.parse
import hmac
from hashlib import sha256
from urllib.parse import urlencode

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
        self.__pageSize = 1000#默认请求50页
        self.__source = 1
        self.__accountId = ""
        self.__pageCode = "sou|sou|soulb"
        self.__key = 'abfc8f9dcf8c3f3d8aa294ac5f2cf2cc7767e5592590f39c3f503271dd68562b'#sign密钥

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

    def save_jobs(self, response):
        #以csv和pickle格式保存
        items = response['resultbody']['job']['items']
        datas = []
        for i in range(len(items)):
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
            datas.append([Job, Salary, Company, Location, UpdateDateTime, JobHref])
        saved_path = './jobs.csv'
        with open('./jobs.pickle', 'wb') as f:
            pickle.dump(datas, f)
            f.close()
        df = pd.DataFrame(datas, columns=['Job', 'Salary', 'Company', 'Location', 'UpdateDateTime', 'JobHref'])
        df.to_csv(saved_path, index=False, encoding='utf-8-sig')
        print(os.path.abspath(saved_path))

if __name__=='__main__':
    city = "深圳"
    k = "自动化测试"
    job = Job()
    job.__citycode__(city)
    response = job.__search__(k)
    job.save_jobs(response)