#coding=utf-8
"""
@author:Toryun
@data:2020/3/31
@version:Python3.8
@Function: 获取前程无忧招聘工作数据
"""
import xlwt
import re
import requests
from selenium import webdriver
import time
from lxml import etree
from tqdm import tqdm
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib

#通过模拟搜索页面并用xpath筛选岗位情况保存到excel中
class Job(object):
    def __init__(self):
        '''初始化Google驱动'''
        self.__url = 'http://www.51job.com' 
        self.__job = "Python"
        #记录excel中的行数,后面从第二行开始录入数据
        self.__count = 1
        #火狐驱动
        self.__driver = webdriver.Chrome(executable_path=r"/Users/jin/Downloads/chromedriver")
        self.__createSheet()

    def __createSheet(self):
        '''创建工作簿'''
        self.__f = xlwt.Workbook()
        self.__sheet = self.__f.add_sheet("51Job",cell_overwrite_ok=True)
        rowTitle = ['编号','标题','地点','公司名','待遇范围','工作简介','公司介绍','招聘网址']
        for i in range(0,len(rowTitle)):
            self.__sheet.write(0,i,rowTitle[i])
    
    
    def __findWebSite(self):
        '''自动化搜索工作，设置工作地点，月薪等搜索条件'''
        self.__driver.get(self.__url)
        #最大化窗口
        self.__driver.maximize_window()

        self.__driver.find_element_by_xpath('//*[@id="kwdselectid"]').send_keys(self.__job)
        #切换到全国查找，通过js设置没成功，只能换一种
        self.__driver.find_element_by_xpath('//*[@id="work_position_input"]').click()        
        time.sleep(1)
        try:
            #定位当前地方
            self.__driver.find_element_by_xpath('//*[@id="work_position_click_multiple_selected_each_050000"]/em').click()
        except:
            try:
                #可能帮你定到国外，这两种是常见的
                self.__driver.find_element_by_xpath('//*[@id="work_position_click_multiple_selected_each_360000"]/em').click()
            except:
                print("您已设置为全国搜索了")
        self.__driver.find_element_by_xpath('//*[@id="work_position_click_bottom_save"]').click()
        #开始搜索进入到搜索页面
        self.__driver.find_element_by_xpath('/html/body/div[3]/div/div[1]/div/button').click()
        time.sleep(1)
        self.__driver.find_element_by_xpath('//*[@id="filter_issuedate"]/ul/li[2]').click()
        self.__driver.find_element_by_xpath('//*[@id="filter_providesalary"]/ul/li[7]').click()
        self.__js = 'document.querySelector("body > div.dw_wp > div.dw_filter > div.op").click()'
        self.__driver.execute_script(self.__js)

    def __saveDataToExcel(self,jobs):
        '''保存数据到工作簿'''
        for j in range(0,len(jobs)):
            self.__sheet.write(self.__count,j,jobs[j])
        self.__f.save("51Job_xpath.xls")
        self.__count += 1

    def __fitterField(self,title,info,name,salary,detail,address,site):
        '''对数据进行判断，并返回数组'''
        jobs = []
        jobs.append(self.__count)
        title = title[0] if len(title) > 0 else ''
        jobs.append(title.strip())
        print(address)
        address = address if  len(address) > 0 else ''
        jobs.append(address.strip())
        name = name[0] if len(name) > 0 else ''
        jobs.append(name)
        salary = salary[0] if len(salary) > 0 else ''
        jobs.append(salary)
        detail = detail if len(detail) > 0 else ''
        jobs.append(detail.strip())
        info = info[0] if len(info) > 0 else ''
        jobs.append(info)
        site = site[0] if len(site) > 0 else ''
        jobs.append(site)
        return jobs

    def __getJobDetail(self,site):
        '''工作简介职责'''
        try:
            site = site[0] if len(site) > 0 else ''
            res = requests.get(site,timeout=2)
            res.encoding = 'gbk'
            selector = etree.HTML(res.text)
            #有时候是p标签组成的，有时候没有p标签
            jobDetails = selector.xpath('//div[@class="bmsg job_msg inbox"]')
            addresses = selector.xpath('/html/body/div[3]/div[2]/div[3]/div[2]/div')
            detail = jobDetails[0].xpath('string(.)').strip()
            address = addresses[0].xpath('string(.)').strip()
            return detail,address
        except Exception as e:
            return "暂无数据"
            
        
    def getData(self,work='Python'):
        '''获取51job上的数据'''
        self.__job=work
        #先模拟搜索全国Python招聘
        self.__findWebSite()
        t = True
        while t:
            #下拉滚动条
            for i in range(5):
                height = 1000 * i
                self.__driver.execute_script('window.scrollBy(0,'+str(height)+')')
            selector = etree.HTML(self.__driver.page_source)
            divs = selector.xpath('//*[@id="resultList"]/div[@class="el"]')
            for div in divs:
                title = div.xpath('./p/span/a/text()')
                info=div.xpath('./span[2]/text()')
                name=div.xpath('./span[1]/a/@title')
                salary = div.xpath('./span[3]/text()')
                site = div.xpath('./p/span/a/@href')

                detail,address = self.__getJobDetail(site)
                
                jobs = self.__fitterField(title,info,name,salary,detail,address,site)
                #开始存入到excel中
                self.__saveDataToExcel(jobs)
                for i in tqdm(range(len(divs))):
                    time.sleep(0.0001)
            try:
                self.__driver.find_element_by_xpath('//*[@id="resultList"]/div[55]/div/div/div/ul/li[8]/a').click()
                page_id = re.findall(r'<li class=\"on\">(\d+)<\/li>',self.__driver.page_source)
                print(page_id[0])
            except Exception as e:
                print(e,end="\n")
                t = False
        self.__driver.close()
    def Send_attchment():
        '''Send the data to my emailbox'''
        EMAIL_HOST = 'smtp.qq.com'
        EMAIL_HOST_USER = '1329776780@qq.com'
        password = input("Please input the EMAIL_HOST_PASSWORD to login email:\n")
        EMAIL_HOST_PASSWORD = password
        EMAIL_PORT = 587
        EMAIL_USE_TLS = True
        from_email = "1329776780@qq.com"
        from_addr = "Toryun"
        to_addr = ["zhongjin95@gmail.com"]
        subject = "24h jobs for {}".format(self.__job)
        content = "here is the message send from {0}".format(from_addr)
        path_file = "51Job_xpath.xls"     
        message = MIMEMultipart()
        message['From'] = Header(from_addr,'utf-8')
        message['To'] =  Header(to_addr[0],'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        message.attach(MIMEText(content,'plain', 'utf-8'))
        att1 = MIMEText(open(path_file, 'rb').read(), 'base64', 'utf-8')
        att1["Content-Type"] = 'application/octet-stream'
        att1["Content-Disposition"] = 'attachment; filename="covid-19.txt"'
        message.attach(att1)    
        try:
            smtpObj = smtplib.SMTP_SSL(EMAIL_HOST,465)
            smtpObj.login(EMAIL_HOST_USER,EMAIL_HOST_PASSWORD) 
            smtpObj.sendmail(from_email, to_addr, message.as_string())
            print("邮件发送成功")
        except OSError as err:
            print("Error: 无法发送邮件\n{0}".format(err))
        #msg.content_subtype = "txt"
        #msg.attach_file(path_file)
        #msg.send()

if __name__=='__main__':
    job = Job()
    #此处可以写Android等其它岗位
    try:
        s = input("Please in put the jos which you want to search:\n")
    except:
        s = "Python" #默认
    t0 = time.time()
    job.getData(s)
    Send_attchment()
    t1 = time.time()
    T = t1 - t0
    print("Total time is {}".format(T))

