# -*- coding: UTF-8 -*-
# ========================
#===   Python version is 2.7.13  ==
#==             Author: Toryun           ==
#==              Time:2017-08            ==
#========================
# 使用列表中已有的关键词，到亚马逊搜索results，并存入对应工作薄中
import xlrd,requests,xlwt,re,lxml,os,datetime,urllib2
from xlutils.copy import copy
def open_excel(urlname=' ',sheet=' '):
    '''Input a urlname about a excel abs url,and the excel's sheetname.Then it would open a excel read the data
    [通过绝对路径(urlname)，打开一个现有的Excel表(data)，提取sheet里面的指定列（col)内容,放入数组data_list'''
    try:
        urlname=raw_input("please input a url and name which is the excel's place in the computer like 'c:/js/s.xls':\n")
        t=os.path.exists(urlname)
        print t
        while t==False:
            urlname=raw_input("The filename is wrong! Please input a url and name which is the excel's place in the computer like 'c:/js/s.xls':\n")# 判断该表格是否存在
            print t
        data=xlrd.open_workbook(urlname) #打开一个工作表
        if data.sheets()==None:
            print "The excel has none !" # 判断Excel中的sheet是否为空
        else:
            p=0
            z={}
            for sheet in data.sheets():
                p+=1
                z[p]=sheet.name # 把表中的名字输入到字典中，方便后来用序号查询
                print p,sheet.name# 返回整个Excel中的表名
            try:
                serial_num=int(raw_input("Please input the sheet's serial number  which you want search(defult serial number is 1):\n"))
                if  serial_num in z:
                    table=data.sheet_by_name(serial_num) #通过表序获取
                    rows=table.nrows # 获取列表行数和列数
                    cols=table.ncols
                else:
                    print "Your number is not in the serial number ,please input a correct number."
            except TypeError or IOError,e:
                print str(e)
                serial_num=1
        col=int(raw_input("Please input a col what you want read (defualt col is 0):\n")) #输入想要读取的列
        if not isinstance(col,int) and (col>ncols or col<0): #判断输入列是否超出范围，是否为整型
            col=int(raw_input("Please input a col what you want read:\n"))
    except Exception,e:
        print str(e)
        col=0
    except OSError as errno2:
        print str(errno2)
    data_list=[]#把数据读入数组存储
    start1=datetime.datetime.now()
    for i in range(rows):
        cell=table.cell(i,col).value#读取（i行，col列）单元格的值
        data_list.append(cell)
    end1=datetime.datetime.now()
    if i == rows:
        print "读取工作表%d列用时：%f s"%(col,end1-start1) #计算读取工作表内容时间
    if data_list==None:
        print 'sorry,it have no values!' #判断列是否为空
    return data_list
def key_result(url):
    '''Through the extraction of keywords in the array, query from a website (especially Amazon) , return query_results
    [通过提取到数组(data_list)里的关键词批量到某网站（特指Amazon）查询，返回查询结果(results)放入数组(arr_result)]'''
    try:
        headers=requests.head(url) # 获取信息头部
        arr_result=[]
        start2=datetime.datetime.now() # 计算搜索结果时间
        for a in len(data_list):
            url=url+data_list[a]
            r=requests.get(url,headers=headers)
            html=r.read()
            contents=etree.HTML(html) # 把html 转化为 xml结构
            results=contents.xpath('//*[@id="s-result-count"]/text()') # 把搜索结果导入数组
            arr_result.append(results)
        end2=datetime.datetime.now()
        print "搜索结果用时：%f s"%(end2-start2) 
        print r.status_code
        if r.status_code!='200':
            return r.raise_for_status()
    except HTTPError,e:
        print "The Error code:",e.code
def  Write_sheet():
    '''Import the results of the query (result) into the table column[把查询到的结果（result)导入表格列中]'''
    start3=datetime.datetime.now()
    data=copy(data)
    for i in len(arr_result):
        if table.cell(i,col+1).value!=None:
            col+=1
        table.write(i,col+1,arr_result[i])
    end3=datetime.datetime.now()
    print "写入表格用时：%f s"%(end3-start3) # 计算写入时间
    data.save(urlname)

if __name__=='__main__':
    open_excel()
    key_result(url='https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords=')
    Write_sheet()
    
    
