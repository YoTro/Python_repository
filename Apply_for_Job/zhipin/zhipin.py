import re
import os
import time
import asyncio
import requests
import pandas as pd
import urllib.parse
from tqdm import tqdm
from lxml import etree

class zhipin(object):
    """docstring for zhipin"""
    def __init__(self):
        super(zhipin, self).__init__()
        self.__zp_stoken__ = ""
        self.scene = "1"
        self.query = "亚马逊运营"
        self.city = "101280600"
        self.experience = ""
        self.payType = ""
        self.partTime = ""
        self.degree = ""
        self.industry = ""
        self.scale = ""
        self.stage = ""
        self.position = ""
        self.jobType = ""
        self.salary = ""
        self.multiBusinessDistrict = ""
        self.multiSubway = ""
        self.page = 1
        self.pageSize = 30
    def __citycode__(self, city):
        self.city = "101280600"
        site = {
            "code": 0,
            "message": "Success",
            "zpData": {
                "hotCitySites": [
                    {
                        "name": "全国",
                        "code": 100010000,
                        "url": "/?city=100010000"
                    },
                    {
                        "name": "北京",
                        "code": 101010100,
                        "url": "/beijing/"
                    },
                    {
                        "name": "上海",
                        "code": 101020100,
                        "url": "/shanghai/"
                    },
                    {
                        "name": "广州",
                        "code": 101280100,
                        "url": "/guangzhou/"
                    },
                    {
                        "name": "深圳",
                        "code": 101280600,
                        "url": "/shenzhen/"
                    },
                    {
                        "name": "杭州",
                        "code": 101210100,
                        "url": "/hangzhou/"
                    },
                    {
                        "name": "天津",
                        "code": 101030100,
                        "url": "/tianjin/"
                    },
                    {
                        "name": "西安",
                        "code": 101110100,
                        "url": "/xian/"
                    },
                    {
                        "name": "苏州",
                        "code": 101190400,
                        "url": "/suzhou/"
                    },
                    {
                        "name": "武汉",
                        "code": 101200100,
                        "url": "/wuhan/"
                    },
                    {
                        "name": "厦门",
                        "code": 101230200,
                        "url": "/xiamen/"
                    },
                    {
                        "name": "长沙",
                        "code": 101250100,
                        "url": "/changsha/"
                    },
                    {
                        "name": "成都",
                        "code": 101270100,
                        "url": "/chengdu/"
                    },
                    {
                        "name": "郑州",
                        "code": 101180100,
                        "url": "/zhengzhou/"
                    },
                    {
                        "name": "重庆",
                        "code": 101040100,
                        "url": "/chongqing/"
                    },
                    {
                        "name": "佛山",
                        "code": 101280800,
                        "url": "/foshan/"
                    },
                    {
                        "name": "合肥",
                        "code": 101220100,
                        "url": "/hefei/"
                    },
                    {
                        "name": "济南",
                        "code": 101120100,
                        "url": "/jinan/"
                    },
                    {
                        "name": "青岛",
                        "code": 101120200,
                        "url": "/qingdao/"
                    },
                    {
                        "name": "南京",
                        "code": 101190100,
                        "url": "/nanjing/"
                    },
                    {
                        "name": "东莞",
                        "code": 101281600,
                        "url": "/dongguan/"
                    },
                    {
                        "name": "昆明",
                        "code": 101290100,
                        "url": "/kunming/"
                    },
                    {
                        "name": "南昌",
                        "code": 101240100,
                        "url": "/nanchang/"
                    },
                    {
                        "name": "石家庄",
                        "code": 101090100,
                        "url": "/shijiazhuang/"
                    },
                    {
                        "name": "宁波",
                        "code": 101210400,
                        "url": "/ningbo/"
                    },
                    {
                        "name": "福州",
                        "code": 101230100,
                        "url": "/fuzhou/"
                    }
                ],
                "otherCitySites": [
                    {
                        "name": "南通",
                        "code": 101190500,
                        "url": "/nantong/"
                    },
                    {
                        "name": "无锡",
                        "code": 101190200,
                        "url": "/wuxi/"
                    },
                    {
                        "name": "珠海",
                        "code": 101280700,
                        "url": "/zhuhai/"
                    },
                    {
                        "name": "南宁",
                        "code": 101300100,
                        "url": "/nanning/"
                    },
                    {
                        "name": "常州",
                        "code": 101191100,
                        "url": "/changzhou/"
                    },
                    {
                        "name": "沈阳",
                        "code": 101070100,
                        "url": "/shenyang/"
                    },
                    {
                        "name": "大连",
                        "code": 101070200,
                        "url": "/dalian/"
                    },
                    {
                        "name": "贵阳",
                        "code": 101260100,
                        "url": "/guiyang/"
                    },
                    {
                        "name": "惠州",
                        "code": 101280300,
                        "url": "/huizhou/"
                    },
                    {
                        "name": "太原",
                        "code": 101100100,
                        "url": "/taiyuan/"
                    },
                    {
                        "name": "中山",
                        "code": 101281700,
                        "url": "/zhongshan/"
                    },
                    {
                        "name": "泉州",
                        "code": 101230500,
                        "url": "/quanzhou/"
                    },
                    {
                        "name": "温州",
                        "code": 101210700,
                        "url": "/wenzhou/"
                    },
                    {
                        "name": "金华",
                        "code": 101210900,
                        "url": "/jinhua/"
                    },
                    {
                        "name": "海口",
                        "code": 101310100,
                        "url": "/haikou/"
                    },
                    {
                        "name": "长春",
                        "code": 101060100,
                        "url": "/changchun/"
                    },
                    {
                        "name": "徐州",
                        "code": 101190800,
                        "url": "/xuzhou/"
                    },
                    {
                        "name": "哈尔滨",
                        "code": 101050100,
                        "url": "/haerbin/"
                    },
                    {
                        "name": "乌鲁木齐",
                        "code": 101130100,
                        "url": "/wulumuqi/"
                    },
                    {
                        "name": "嘉兴",
                        "code": 101210300,
                        "url": "/jiaxing/"
                    },
                    {
                        "name": "保定",
                        "code": 101090200,
                        "url": "/baoding/"
                    },
                    {
                        "name": "汕头",
                        "code": 101280500,
                        "url": "/shantou/"
                    },
                    {
                        "name": "烟台",
                        "code": 101120500,
                        "url": "/yantai/"
                    },
                    {
                        "name": "潍坊",
                        "code": 101120600,
                        "url": "/weifang/"
                    },
                    {
                        "name": "江门",
                        "code": 101281100,
                        "url": "/jiangmen/"
                    }
                ]
            }
        }
        for item in site['zpData']['hotCitySites']: 
            if item['name'] == city:
                self.city = item['code']
        for item in site['zpData']['otherCitySites']:
            if item['name'] == city:
                self.city = item['code'] 
        return  self.city
    def getbusinessDistrict(self, citycode):
        # 返回一个字典
        counties = {}
        url = "https://www.zhipin.com/wapi/zpgeek/businessDistrict.json?cityCode={}".format(citycode)
        payload = {}
        headers = {
          'authority': 'www.zhipin.com',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'cache-control': 'no-cache',
          'pragma': 'no-cache',
          'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
          'x-requested-with': 'XMLHttpRequest',
          'Cookie': 'SERVERID=606144fb348bc19e48aededaa626f54e|1682196442|1682193847'
        }
        response = requests.request("GET", url, headers=headers, data=payload, timeout=10)
        if (response.status_code == 200): 
            s = ""
            while(True):
                i = 0
                county = []
                print("\nInput Q or q to back\n")
                for item in response.json()['zpData']['businessDistrict']['subLevelModelList']: 
                    print(i, item['name'])
                    county.append(item['name'])
                    i+=1
                try:
                    s = input("选择序号:")
                    if s == 'q' or s == 'Q':
                        break
                    while(re.match('\d+',s)==None or int(s)>i or int(s)<0):
                        s = input("选择序号:")
                except:
                    s = '0'
                cts = county[int(s)]
                counties[cts]=[]
                subLevelModelList = response.json()['zpData']['businessDistrict']['subLevelModelList'][int(s)]['subLevelModelList']
                while(True):
                    i = 0
                    street = []
                    for p in subLevelModelList:
                        print(i, p['name'])
                        i+=1
                        street.append(p['name'])
                    try:
                        s = input("选择序号(回车默认全选):")
                        if s == 'q' or s == 'Q':
                            break
                        if s == "":
                            counties[cts] = street
                            break 
                        while(re.match('\d+',s)==None or int(s)>i or int(s)<0):
                            s = input("选择序号:")
                    except:
                        s = '0'
                    counties[cts].append(street[int(s)])               
        return counties
    def __businessDistrictcode__(self, citycode, counties):
        self.city = citycode
        countycode = {}
        url = "https://www.zhipin.com/wapi/zpgeek/businessDistrict.json?cityCode={}".format(self.city)
        payload = {}
        headers = {
          'authority': 'www.zhipin.com',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'cache-control': 'no-cache',
          'pragma': 'no-cache',
          'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
          'x-requested-with': 'XMLHttpRequest',
          'Cookie': 'SERVERID=606144fb348bc19e48aededaa626f54e|1682196442|1682193847'
        }
        try:
            response = requests.request("GET", url, headers=headers, data=payload, timeout=5)
            if (response.status_code == 200):
                for item in response.json()['zpData']['businessDistrict']['subLevelModelList']:
                    for county in counties:
                        if item['name'] == county:
                            countycode[item['code']] = []
                            for street in item['subLevelModelList']:
                                for subdistrict in counties[county]:
                                    if street['name'] == subdistrict:
                                        countycode[item['code']].append(street['code'])
        except Exception as e:
            pass
        self.multiBusinessDistrict = ",".join([f"{k}:{'_'.join(map(str, v))}" for k, v in countycode.items()])
        return countycode
    def __getSubwayByCity__(self, cityCode):
        self.city = cityCode
        subways = {}
        url = "https://www.zhipin.com/wapi/zpCommon/data/getSubwayByCity?cityCode={}".format(self.city)
        payload = {}
        headers = {
          'authority': 'www.zhipin.com',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'cache-control': 'no-cache',
          'pragma': 'no-cache',
          'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
          'x-requested-with': 'XMLHttpRequest',
        }

        response = requests.request("GET", url, headers=headers, data=payload, timeout=5)
        if (response.status_code == 200):
            s = ""
            while(True):
                i = 0
                print("\nInput Q or q to back\n")
                for item in response.json()['zpData']['subwayList']:
                    print(i, item['name'], item['code'])
                    i+=1
                try:
                    s = input("选择序号:")
                    if s == 'q' or s == 'Q':
                        break
                    while(re.match('\d+',s)==None or int(s)>i or int(s)<0):
                        s = input("选择序号:")
                except:
                    s = '0'
                subwaycode = response.json()['zpData']['subwayList'][int(s)]['code']
                subways[subwaycode]=[]
                subLevelModelList = response.json()['zpData']['subwayList'][int(s)]['subLevelModelList']
                while(True):
                    i = 0
                    for p in subLevelModelList:
                        print(i, p['name'], p['code'])
                        i+=1
                    try:
                        s = input("选择序号:")
                        if s == 'q' or s == 'Q':
                            break
                        while(re.match('\d+',s)==None or int(s)>i or int(s)<0):
                            s = input("选择序号:")
                    except:
                        s = '0'
                    subways[subwaycode].append(subLevelModelList[int(s)]['code'])
        self.multiSubway = ",".join([f"{k}:{'_'.join(map(str, v))}" for k, v in subways.items()])
        print(self.multiSubway)
        return subways
    def __condition__(self):
        url = "https://www.zhipin.com/wapi/zpgeek/search/job/condition.json"

        payload = {}
        headers = {
          'authority': 'www.zhipin.com',
          'accept': 'application/json, text/plain, */*',
          'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
          'cache-control': 'no-cache',
          'pragma': 'no-cache',
          'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
          'x-requested-with': 'XMLHttpRequest',
        }
        response = requests.request("GET", url, headers=headers, data=payload, timeout=5)

        print(response.json())
    def get_zp_stoken(self, url = "https://toryunbot.com/zp_stoken"):
        '''从本地或者远程服务器获取__zp_stoken__'''
        zp_stoken = ""
        filename = "./zp_stoken.txt"
        if os.path.exists(filename):
            with open(filename, "r") as f:
                zp_stoken  = f.read().strip()
        else:
            try:
                response = requests.request("GET", url, timeout=10)
                zp_stoken = response.text
            except:
                pass
        return zp_stoken
    def Search_jobs(self, scene, queryjob, city, experience, payType, partTime, degree, industry, scale, stage, position, jobType, salary, multiBusinessDistrict, multiSubway, page, pageSize):
        self.query = queryjob
        self.page = page
        # 读取 zp_stoken 的值
        self.__zp_stoken__ = self.get_zp_stoken()
        response = {'zpData':{'jobList':[]}}
        try:
            url_encoded_queryjob= urllib.parse.quote(self.query)
            url = "https://www.zhipin.com/wapi/zpgeek/search/joblist.json?scene={}&query={}&city={}&experience={}&payType={}&partTime={}&degree={}&industry={}&scale={}&stage={}&position={}&jobType={}&salary={}&multiBusinessDistrict={}&multiSubway={}&page={}&pageSize={}".format(self.scene, url_encoded_queryjob, self.city, self.experience, self.payType, self.partTime, self.degree, self.industry, self.scale, self.stage, self.position, self.jobType, self.salary, self.multiBusinessDistrict, self.multiSubway, self.page, self.pageSize)
            headers = {
                  'authority': 'www.zhipin.com',
                  'accept': 'application/json, text/plain, */*',
                  'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                  'cache-control': 'no-cache',
                  'cookie': '__zp_stoken__={}'.format(self.__zp_stoken__),
                  'pragma': 'no-cache',
                  'referer': 'https://www.zhipin.com/web/geek/job?query=%E4%BA%9A%E9%A9%AC%E9%80%8A%E8%BF%90%E8%90%A5&city=100010000',
                  'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                  'sec-ch-ua-mobile': '?0',
                  'sec-ch-ua-platform': '"macOS"',
                  'sec-fetch-dest': 'empty',
                  'sec-fetch-mode': 'cors',
                  'sec-fetch-site': 'same-origin',
                  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                  'x-requested-with': 'XMLHttpRequest'
            }
            response = requests.request("GET", url, headers=headers).json()
            i = 0
            zp_stoken = self.__zp_stoken__ 
            while ('jobList' not in response['zpData'] or len(response['zpData']['jobList'])==0 ):
                    i+=1
                    self.__zp_stoken__ = self.get_zp_stoken()
                    print("New zp_stoken:{}\n".format(self.__zp_stoken__ != zp_stoken))
                    if self.__zp_stoken__ == zp_stoken:
                        time.sleep(20)
                        if i == 5:
                            return {'zpData':{'jobList':[]}}
                    else:
                        headers['cookie'] = '__zp_stoken__={}'.format(self.__zp_stoken__)
                        response = requests.request("GET", url, headers=headers).json()
                        if (response['zpData']['hasMore']==False and response['zpData']['resCount']<=int(self.page)*30):
                            break
        except Exception as e:
            print(e)
            return {'zpData':{'jobList':[]}}
        return response
    def Jobdetails(self, url):
        self.__zp_stoken__ = self.get_zp_stoken()
        secjob = ""
        address = ""  
        try: 
            headers = {
              'authority': 'www.zhipin.com',
              'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
              'accept-language': 'zh-CN,zh;q=0.9',
              'cache-control': 'no-cache',
              'cookie': '__zp_stoken__={}'.format(self.__zp_stoken__),
              'pragma': 'no-cache',
              #'referer': 'https://www.zhipin.com/web/common/security-check.html?seed=NOGJ0aMiI7SZDX5MjHMiKyhkkiuMbJXcn7Gnnri5eGA%3D&name=ec9a6c21&ts=1682019192490&callbackUrl=%2Fjob_detail%2Fff4a47de6154d42e1XN82Nu6FFBR.html&srcReferer=',
              'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
              'sec-fetch-dest': 'document',
              'sec-fetch-mode': 'navigate',
              'sec-fetch-site': 'same-origin',
              'upgrade-insecure-requests': '1',
              'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }

            response = requests.request("GET", url, headers=headers, timeout=10)
            secxpath = "//div[@class='job-sec-text']/text()"
            addressxpath = "//div[@class='location-address']/text()"
            zp_stoken = self.__zp_stoken__
            i = 0 
            while(response.status_code != 200 or "securityCheck" in response.text):
                i+=1
                self.__zp_stoken__ = self.get_zp_stoken()
                print("New zp_stoken:{}\n".format(self.__zp_stoken__ != zp_stoken))
                if self.__zp_stoken__ == zp_stoken:
                    time.sleep(40)
                    if i == 5:
                        return secjob, address    
                    continue
                headers['cookie'] = '__zp_stoken__={}'.format(self.__zp_stoken__)
                response = requests.request("GET", url, headers=headers, timeout=10)
                print("被检测需要验证:{}\n".format("securityCheck" in response.text))
                if ("securityCheck" in response.text):
                    return secjob, address
            html = response.text
            tree = etree.HTML(html)
            for sec in tree.xpath(secxpath):
                secjob += sec + '\n'
            address = tree.xpath(addressxpath)[0]
        except Exception as e:
            pass
        return secjob, address

    def Save_To_Excel(self, response, saved_path = './zhipinjobs.csv'):
        print("-------开始保存-------\n")
        data = []
        joblist = response['zpData']['jobList']
        for job in tqdm(joblist):
            try:
                links = "https://www.zhipin.com/job_detail/"+job['encryptJobId']+".html"
                # 获取职位名称
                titles= job['jobName']
                # 获取工资
                salaries = job['salaryDesc']
                # 获取地点
                city = job['cityName']
                District = job['areaDistrict']
                Street = job['businessDistrict']
                # 获取公司名称
                companies = job['brandName']
                #获取职位详情和工作地址
                secjob, address = self.Jobdetails(links)
                data.append([links, titles, salaries, city, District, Street, companies, secjob, address])
            except Exception as e:
                pass
        df = pd.DataFrame(data, columns=['Links', 'Titles', 'Salaries', 'City', 'District', 'Street', 'Companies', 'Secjob', 'Address'])
        df.to_csv(saved_path, index=False, encoding='utf-8-sig')
        print(os.path.abspath(saved_path))
