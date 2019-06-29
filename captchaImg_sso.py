# -*- coding:UTF-8 -*-
# Author: Toryun
# Python version: 2.7.16
# Windows version: 10
# Date: 2019-6-15
# Function: Get the sku from the youkeshu when you have permission in Youkeshu获取有颗树sku资料库(服务器资料库或公司统表表格)的成本和价格，进行试算导入表格（适用于产品上传和覆盖产品）
import re
import os
import math
import time
import random
import datetime
import xlrd,xlwt,xlutils
from PIL import Image
import psutil#处理多平台进程的python包
import getpass#获取当前系统用户名的跨平台模块
from CUS_Yuntuexpress import CUS_shippingfee
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms'
SAMPLE_RANGE_NAME = 'Class Data!A2:E'

url='sso.youkeshu.com'#有颗树sku资料库url
discount = {"1":0,"2":0.1,"3":0.15,"4":0.19,"5":0.25,"6":0.30,"7":0.35,"8":0.40,"9":0.45,"10":0.5}#营销折扣:d
exchange_rate = {"1":6,"2":6.1,"3":6.2,"4":6.3,"5":6.4,"6":6.5,"7":6.6,"8":6.7,"9":6.8,"10":6.9,"11":7.0}#美元兑人民币汇率:T
Marginal_Profit_Ratio = {"1":0,"2":0.5,"3":0.10,"4":0.15,"5":0.17}#边际利润率:t
OW = {"1":"US","2":"RU","3":"ES","4":"UK","5":"DE","6":"FR","7":"IT","8":"AU","9":"BR"}#海外仓集合ow
def currency_USDCNY():
        '''Get tbe realtime exchange rate of USD/RMB 获取实时的美元兑人民币汇率'''
        url = 'http://webforex.hermes.hexun.com/forex/quotelist?code=FOREXUSDCNY&column=Code,Price'
        r = requests.get(url)
        rate = re.findall(r'\d+',r.content)
        rate = float(rate[0])/10000
        return rate

def capatchaImg(url):
        '''Get the verification code and enter it manually.获取验证码并手动输入'''
        T=1
        r=0
        while T==1:#如果输入的循环次数出现异常，则再次输入
                try: 
                        loop=int(raw_input('Plz input a number what you want to get the captchaImage from {0}:\n'.format(url)))
                        if r==11:#默认循环一次，如果连续12次输入错误的话
                        loop=1
                        T=0
                except ValueError:
                        r=r+1
                        print r
                        print "Your number is not correct,plz input again.\nYou have {0} times left".format(11-r)

        header0 = {
                "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Connection": "keep-alive",
                "Host": "{0}".format(url),
                "Referer": "{0}".format(url),
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}#验证码URL服务器头
        proxies={"HTTPS": "HTTPS://122.242.96.30:808"}#代理IP
        while T=0:
                try:
                        captchaimg_url = "http://sso.youkeshu.com/include/of/index.php?a=captcha&c=of_base_com_com&key=of_base_sso_main&height=25"#验证码url
                        r=requests.get(captchaimg_url,headers=header0,proxies=proxies)
                        time.sleep(3)
                        print r.status_code
                        if r.status_code==200:
                                sl='Plz kill the cmd.exe if it is still exisit after 4s,then input the characters in the picture\n
                                path_img="d:jpg"
                                _img_0 = path_img+'/'+str(i)+".png"
                                if not os.path.exists(path_img):
                                        os.makedirs(path_img,0755)
                                with open(_img_0,"wb") as _img_1:
                                        _img_1.write(r.content)
                                        _img_1.close()
                                img = Image.open(_img_0)
                                img.show()
                                command = 'taskkill /F /IM cmd.exe  /F /IM conhost.exe/T'#强制终止指定进程名命令
                                os.system(command)
                                num = str(raw_input('Plz input the character in the picture:\n'))
                                pids = psutil.pids()#进程数组,判断加载照片程序的是哪个程序
                                for pid in pids:
                                        p = psutil.Process(pid)#get process name according to pid
                                        process_name = p.nam()
                                        if process_name == 'photolaunch.exe':
                                                command1 = 'taskkill /F /IM photolaunch.exe'
                                                os.system(command1)
                                        elif process_name == 'Microsoft.Photos.exe':
                                                command2 = 'taskkill /F /IM Microsoft.Photos.exe'
                                        else:
                                                process_picture = raw_input("Plz input the process name of the program which process your picture:\n")
                                                command3 = 'taskkill /F /IM {0}'.format(process_picture)
                                print 'The pictures are saved in {0}\n The total time is {1}'.format('d:jpg',t)
                                captcha = int(num)
                                T = 1
                                return captcha
                except IOError,e:
                        print str(e)
                        pass
def login_youkeshu(url,captcha):
        '''Login to a youkeshu SKU database and save sessions.登陆有颗树sku资料库，保存会话'''
        name = 'zhongjin'
        pwd = 'zYKS1234'
        capatcha = captcha
        post_parameter = {
                "name":"{0}".format(name)
                "pwd":"{0}".format(pwd)
                "capatcha":"{0}".format(capatcha)
                }
        headers1 = {Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding":"gzip, deflate",
                "Accept-Language":"zh-CN,zh;q=0.9",
                "Cache-Control":"max-age=0",
                "Connection": "keep-alive",
                "Host": "sso.youkeshu.com",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}
        session = requests.Session()
        r = session.post(url,data=post_parameter,headers=headers1)
        return r,session
def search_sku(sku,session):
        '''Get the sku's weight and cost of the Youkeshu database then return.读取资料库的sku重量和成本价并返回'''
        url="http://192.168.5.5:802/showsku/index?sku={0}".format(sku)
        headers2 = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Host": "192.168.5.5:802",
        "Referer": "{0}".format(url),
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        }
        proxies={"HTTPS": "HTTPS://122.242.96.30:808"}#代理IP
        r = session.get(url,headers=headers2,proxies=proxies) 
        cost=re.findall(r'打包后\)：</td>\s+<td>([1-9]\d*.\d*|0.\d*[1-9]\d*)g<\/td>',r.text)
        weight=re.findall(r'<td> 参考价：<\/td>\s+<td>(.*?)￥<\/td>',r.text)
        if cost:
                cost=int(cost[0])
        else:
                print 'cost is none'
                cost='None'
        if weight:
                weight=int(weight[0])
        else:
                print 'weight is none'
                weight='None'
        return cost,weight
def file_path_choice():
        '''select the file in the folder and return the selected file path.选择文件夹中的文件并返回路径'''
        user = getpass.getuser()#获取当前系统用户名
        f = 'C:\\Users\\{0}}\\Desktop'.format(user)#默认文件夹路径
        t = f+'\\'+'products.xls'
        try:
                file_path = f
                print '路径{0}下的文件和文件夹如下：\n'.format(file_path)
                file_names = os.listdir(file_path)#列出桌面所有文件名
                for i in xrange(len(file_names)):
                        print i+1,file_names[i]
                file_num = int(raw_input('Defualt workbook is 1,plz input a number of this list:\nOr if the file is not in here,plz enter a number big than the last option then you could input a url of the file\n').format(t))#默认文件名是t,或者输入比最后选项大的数
                if file_num in xrange(1,len(file_names)+1):
                        file_path = file_path+'/'+file_names[file_num-1]
                if file_num >= len(file_names)+1:
                        file_path = str(raw_input('Plz input a file url (like:D:\\Documents\\Downloads\1.xlsx\n'))
                        New_file_path = os.path.exists(file_path)
                        while New_file_path == False:#判断该文件是否存在如果不存在，则继续输入
                                file_path = str(raw_input('Plz input a file url (like:D:\\Documents\\Downloads\1.xlsx)\nThe file is not exist\n'))
                                New_file_path = os.path.exists(file_path)
        except Exception,e:
                print str(e)
                file_path = t
        return file_path
def read_sku(file_path):
        '''Get the sku form products.xls.从产品表格里获取产品商家编码和价格信息并返回'''
        data = xlrd.open_workbook(file_path)#打开工作簿
        sheets = data.sheets()
        z = {}
        p = 0
        t = 1
        for sheet in sheets:
                p+=1
                z[p] = sheet.name
                print p,z[p]
        try:
                sheet_index = int(raw_input('Plz input index in the serial numbe(defualt(1):\n'))
                if sheet_index in xrange(1,len(sheets)+1):
                        t = sheet_index
                else:
                        print 'The digital is wrong,plz input a correct number'
        except Exception,e:
                print str(e)
                t == 1
        table = data.sheet_by_index(t-1)#打开sheet
        rows = table.nrows#计算行列数
        cols = table.ncols
        print "{0}'s cols,rows is {1},{2}".format(z[t],cols,rows)
        row_1st = table.row_values(0)#读取表格第一行
        sku_index = row_1st.index('商家编码')#返回商家编码所在列数
        skuArray_index = row_1st.index('价格信息')#返回价格信息所在列数
        skulist = table.col_values(sku_index,1,rows-1)#获取所有的sku
        skuArray = table.col_values(skuArray_index,1,rows-1)#获取价格信息
        return skulist,skuArray


def read_all():
        '''Get all sku, weight and cost in the company product list which is in the local computer if can't get them from the website.读取公司产品统表里的sku，重量和成本价并返回这三个数组（考虑到公司统表过大）'''
        user = getpass.getuser()#获取当前系统用户名
        file_path = 'C:\\Users\\{0}}\\Desktop\公司产品统表.xlsx'.format(user)#默认文件路径  
        if not os.path.exists(file_path):
                try:
                        file_path=raw_input('Plz input the url of 公司产品统表（like:C:\\Users\\{0}}\\Desktop\公司产品统表.xlsx\n'.format(user))      
                except Exception,e:
                        print str(e)

        data = xlrd.open_workbook(file_path)#打开工作簿
        sheets = data.sheets()
        z = {}
        p = 0
        t = 1
        for sheet in sheets:
                p+=1
                z[p] = sheet.name
                print p,z[p]
        try:
                sheet_index = int(raw_input('Plz input index in the serial numbe(defualt(1):\n'))
                if sheet_index in xrange(1,len(sheets)+1):
                        t = sheet_index
                else:
                        print 'The digital is wrong,plz input a correct number'
        except Exception,e:
                print str(e)
                t == 1
        table = data.sheet_by_index(t-1)#打开sheet
        rows = table.nrows#计算行列数
        cols = table.ncols
        print "{0}'s cols,rows is {1},{2}".format(z[t],cols,rows)
        row_1st = table.row_values(0)#读取表格第一行
        sku_index = row_1st.index('SKU')#返回SKU所在列数
        cost_index = row_1st.index('成本价')#返回成本价所在列数
        weight_index = row_1st.index('重量')#返回重量所在列数
        SKU = table.col_values(sku_index,1,rows-1)#获取sku
        costlsit = table.col_values(cost_index,1,rows-1)#获取成本价
        weightlist = table.col_values(weight_index,1,rows-1)#获取重量
        return SKU,costlsit,weightlist
def Search_sku(sku,costlist,weightlist):
	'''
	Get the price and weight of sku from the list then return
	查找sku相对应的重量和成本并返回
	'''
        try:
                index = SKU.index(sku)
                cost,weight = float(costlist[index]),float(weightlist[index])
                return cost,weight
        except Exception,e:
                print str(e)
                print "Plz update the 公司产品统表 to newest version,can't find the sku"

                pass



def pre_price(cost,weight,d,T,f,h,ow):
        '''
        Calculate the price before the discount.计算折前价
       .Choose the promotional discount, marginal profit margin, USD/CNY exchange rate.选择相应的促销折扣、边际利润率，美元兑人民币汇率
        @cost:成本
        @weight:毛重
        @s:运费
        @T:汇率
        @d:营销折扣
        @t:边际利润率
        @f:判断是full_charge还是包邮模式
        @p0:full charge 0%边际利润价格
        @p1:新基准利率0%边际利润价格
        @p:价格字典包含了去海外仓的价格
        @h:判断是否添加海外仓
        @ow:Overseas_warehouse海外仓是一个数组
        '''
        

        t = 0
        p = {}

        if f == 0:#full charge
                p0 = (0.97+1.01*cost)/(0.87*T)
                if p0<2:
                        t = random.uniform(0.0001,0.03)
                elif p0<5:
                        t = random.uniform(0.03,0.05)
                elif p0<8:
                        t = random.uniform(0.05,0.08)
                elif p0<10:
                        t = random.uniform(0.08,0.1)
                else:
                        t = random.uniform(0.1,0.17)                
                p["p_CN"] = (cost*1.01+0.97)/(T*(0.87-t)*(1-d))
        else:#新基准利率价格
                if weight>5:
                        s0 = weight*0.05617+16.17#如果5g<重量w或者属性为406/407(纯电池)
                        s = s0
                else:
                        s1 = 6.72#重量w小于30g
                        s2 = 6.72+(weight-30)*0.06972# 30g<=w<80g
                        s3 = 6.72+50*0.06972+(weight-80)*0.05277#w>=80g
                        s = s1
                p1 = (88*(cost+s)+26.4)/(76.56*T)
                if p1<2:
                        t = random.uniform(0.0001,0.03)
                elif p1<5:
                        t = random.uniform(0.03,0.05)
                elif p1<8:
                        t = random.uniform(0.05,0.08)
                elif p1<10:
                        t = random.uniform(0.08,0.1)
                else:
                        t = random.uniform(0.1,0.17)
                p["p_CN"] = (cost+0.3+s)/(T*(0.87-t)*(1-d))
        if "CUS" in ow:#有美国海外仓

                if weight>9900:
                        return ValueError
                else:
                        s_us = CUS_shippingfee(weight)
                p["p_CUS"] = (1.01*cost+0.57+s_cus)/(T*0.86(1-t)*(1-d))
        elif "CRU" in ow:
                if weight<=100:
                        s_cru == 20
                elif weight<=200:
                        s_cru == 25
                elif weight<=300:
                        s_cru = 30+(weight-300)/400
                else:
                        s_cru = 150+math.ceil((weight-3000)/1000)*43
                p["p_CRU"] = (1.01*cost+0.57+s_cru)/(T*0.86(1-t)*(1-d))
        elif "CES" in ow:
                s_ces = weight*40/1000+16
                p["p_CES"] = (1.01*cost+0.57+s_ces)/(T*0.86(1-t)*(1-d))
        elif "CAU" in ow:
                s_cau = weight*38/1000+29
                p["p_CAU"] = (1.01*cost+0.57+s_cau)/(T*0.86(1-t)*(1-d))
        
        elif "CFR" in ow:
                if weight*0.001<2:
                        s_cfr = weight*0.001*54+25
                else:
                        s_cfr = weight*0.001*50+20
                p["p_CFR"] = (1.01*cost+0.57+s_cfr)/(T*0.86(1-t)*(1-d))
        elif "CUK" in ow:
                s_cuk = weight*40/1000+18
                p["p_CUK"] = (1.01*cost+0.57+s_cuk)/(T*0.86(1-t)*(1-d))
        elif "CIT" in ow:
                if weight*0.001<3:
                        s_cit = weight*0.001*55+25
                else:
                        s_cit = weight**0.001*50+20
                p["p_CIT"] = (1.01*cost+0.57+s_cit)/(T*0.86(1-t)*(1-d))
        elif "CDE" in ow:
                s_cde = weight*45/1000+20
                p["p_CDE"] = (1.01*cost+0.57+s_cde)/(T*0.86(1-t)*(1-d))
        else "CBR" in ow:
                if weight*0.001<30:
                        s_cbr = weight*0.001*80+25
                p["p_CBR"] = (1.01*cost+0.57+s_cbr)/(T*0.86(1-t)*(1-d))
        
        return p

def Oversea_warehouse(sku,p,ow):
        '''如果添加海外仓，怎么合成sku和skuArray'''
        CN = '{"价格":"{0}","属性":\{{0},"库存":"999","商家编码":"{0}"}]'format(sku)
        CUS = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CUS"}]'format(p["p_CUS"],sku)
        CRU = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CRU"}]'format(p["p_CRU"],sku)
        CDE = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CDE"}]'format(p["p_CDE"],sku)
        CAU = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CDE"}]'format(p["p_CAU"],sku)
        CUK = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CDE"}]'format(p["p_CUK"],sku)
        CFR = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CDE"}]'format(p["p_CFR"],sku)
        CIT = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CDE"}]'format(p["p_CIT"],sku)
        CBR = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CBR"}]'format(p["p_CBR"],sku)
        CES = '{"价格":"{0}","属性":\{{0},"库存":"35","商家编码":"{0}_CBR"}]'format(p["p_CES"],sku)
        
        skuArray_ow = {"CN":CN,"CUS":CUS,"CRU":CRU,"CDE":CDE,"CUK":CUK,"CBR":CBR,"CES":CES}   

def Process_sku(sku,p,skuArray):
        '''
        To separate the single attribute of sku and the multi-attribute of sku. PS: similar to delete duplicates in an array.
        对商品编码进行处理，分开单属性sku和多属性sku。PS:类似于删除数组中的重复项

        '''
        i = 0
        t,sku0 = []

        for i in xrange(len(sku)):
                t = re.findall(r'[A-Z]{2}\d+',sku[i])
                for i in t:
                        if not i in sku0:
                                sku0.append(i)
                if len(sku0) == 1:
                        skuArray0 = '{"skuArray": [{"价格":"{0}","库存":"999","商家编码":"{1}"}]}'.format(p,sku[i])
                else:
                        sku_Attributes = re.findall(r'属性\W+\{(.*?),\"库存\W+\d+\W+商家编码\W+[A-Z]{2}\d+\"\}',skuArray[i])
                        skuArray0 = 
                        return sku1

     
        
        
        
        
        
        



            



t0=datetime.datetime.now()
t1 = datetime.datetime.now()
t = t1-t0#测试时间长短

