import os
import time
import asyncio
import requests
import pandas as pd
import urllib.parse
from tqdm import tqdm
from websocketclient import send_and_receive

async def client(command):
    global zp_stoken
    response = await send_and_receive(command)
    if response == 'refresh':
        with open("./zp_stoken.txt", "r") as f:
            zp_stoken = f.read().strip()
class zhipin(object):
    """docstring for zhipin"""
    def __init__(self):
        super(zhipin, self).__init__()
        self.__zp_stoken__ = ""
        filename = "./zp_stoken.txt"
        if os.path.exists(filename):
            with open(filename, "r") as f:
                self.__zp_stoken__  = f.read().strip()
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
        self.multiBusinessDistrict = "440307"
        self.multiSubway = ""
        self.page = 1
        self.pageSize = 30
        
    def Search_jobs(self, scene, queryjob, city, experience, payType, partTime, degree, industry, scale, stage, position, jobType, salary, multiBusinessDistrict, multiSubway, page, pageSize):
        self.query = queryjob
        self.page = page
        # 从文件中读取 zp_stoken 的值
        with open("./zp_stoken.txt", "r") as f:
            self.__zp_stoken__ = f.read().strip()
        response = {'zpData':{'jobList':[]}}
        try:
            url_encoded_queryjob= urllib.parse.quote(self.query)
            url = "https://www.zhipin.com/wapi/zpgeek/search/joblist.json?scene={}&query={}&city={}&experience={}&payType={}&partTime={}&degree={}&industry={}&scale={}&stage={}&position={}&jobType={}&salary={}&multiBusinessDistrict={}&multiSubway={}&page={}&pageSize={}".format(self.scene, url_encoded_queryjob, self.city, self.experience, self.payType, self.partTime, self.degree, self.industry, self.scale, self.stage, self.position, self.jobType, self.salary, self.multiBusinessDistrict, self.multiSubway, self.page, self.pageSize)
            print(url)
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
            while ('jobList' not in response['zpData'] or len(response['zpData']['jobList'])==0 ):
                    self.__zp_stoken__ = zp_stoken
                    #asyncio.run(client('refresh'))
                    with open("./zp_stoken.txt", "r") as f:
                        zp_stoken = f.read().strip()
                        print("New zp_stoken:{}\n".format(self.__zp_stoken__ == zp_stoken))
                        if self.__zp_stoken__ == zp_stoken:
                            time.sleep(20)
                            continue
                    headers['cookie'] = '__zp_stoken__={}'.format(self.__zp_stoken__)
                    response = requests.request("GET", url, headers=headers).json()
                    i+=1
                    if i == 5:
                        return {'zpData':{'jobList':[]}}
        except Exception as e:
            print(e)
            return {'zpData':{'jobList':[]}}
        return response

    def Save_To_Excel(self, response):
        print("-------开始保存-------\n")
        data = []
        joblist = response['zpData']['jobList']
        for job in tqdm(joblist):
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
            data.append([links, titles, salaries, city, District, Street, companies])
        saved_path = './zhipinjobs.csv'
        df = pd.DataFrame(data, columns=['Links', 'Titles', 'Salaries', 'City', 'District', 'Street', 'Companies'])
        df.to_csv(saved_path, index=False, encoding='utf-8-sig')
        print(os.path.abspath(saved_path))
