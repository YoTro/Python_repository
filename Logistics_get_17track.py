#-*- coding:UTF-8 -*-
#Python Version:2.7.13
#Author:Toryun
#Created:2018-01-05
#Function:Query multiple logistics information by 17track
'''通过17track批量查询物流信息，标记妥投信息添加妥投天数'''
import  requests,re,datetime,xlrd,os,json
from xlutils.copy  import copy

def Flie_path_choice():
    '''Select the file in the folder and return the selected file path.选择文件夹中的文件，返回所选文件路径'''
    t='C:\\Users\\Administrator\\Desktop\跟踪物流 (自动保存的).xls'#默认的工作簿地址
    try:
        file_path='C:\\Users\\Administrator\\Desktop'
        print '路径{0}文件夹中的文件和文件夹如下：'.format(file_path)
        file_names=os.listdir(file_path)#列出file_path文件夹中的文件名
        for i in xrange(len(file_names)):
            print i+1,file_names[i]
        file_num=int(raw_input("Purpose:Check the Excel with the tracking number, the program will automatically check the shipping status, and fill in another Excel workbook.\nPlz input a number of serial number (Default workbook is {0}):\n Or Enter  a number  more than the last option ,then you could  input a url of  file\n".format('stainless steel toilet brush holder.xlsx')))#默认文件是t,或者输入任意字符获取输入新文件路径接口
        if  file_num in range(1,len(file_names)+1):
            file_path=file_path+'/'+file_names[file_num-1]
        else:
            file_path=str(raw_input("Plz input a fileurl(e.g. C:\\Users\\Administrator\\Desktop\1.xlsx\n)"))
            f=os.path.exists(file_path)#Check  whether the file is exists检查该文件是否存在
            while f==False:
                file_path=str(raw_input("Your file is not exists,plz input a fileurl:\n"))
                f=os.path.exists(file_path)
    except Exception,e:#<type 'exceptions.ValueError'>
        print str(e)
        file_path=t#Defualt path of the file默认文件路径
    return file_path

def Get_TrackID(file_path):
    """Get a list TrackID from workbook and return获取工作簿和一组运单号并返回 """
    data=xlrd.open_workbook(file_path)#打开路径中文件
    sheets=data.sheets()#获取所有sheet(类型是list)
    print "{0}'s sheets:\n".format(file_path)
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.file_name
        print p,z[p]
    try:
        sheet_index=int(raw_input("Plz input index in the serial number(defualt 1):]n"))#选择工作簿中的sheet
        if sheet_index in xrange(1,len(sheets)+1):#判断输入数是否超出范围
            sheet_num=sheet_index
        else:
            print 'The number is wrong,plz input a correct number'
    except Exception,e:#<type 'exceptions.ValueError'>
        print str(e)
        sheet_num=2
    table=data.sheet_by_index(sheet_num-1)
    rows=table.nrows#计算列、行数
    cols=table.ncols
    print "{0}'s rows,cols are {1},{2}".format(z[sheet_num],rows,cols)
    rows_1st=table.row_values(0)#Read the first row读取第一行
    TrackID_index=rows_1st.index('TrackID')#Obtain the number of the Tracking number获取运单号所在列数
    TrackID=table.col_values(TrackID_index,1,rows-1)#Read 2nd row  to the last row读取第2行到最后一行
    return cols,sheet_num,data,TrackID

def Pickle_to_transit(i,e,cols,sheet_num,data,TrackID):
    """Copy from Original Workbook to New workbook,write the transit statues.复制原工作簿到新工作簿，把查询结果写入新的工作簿"""
    file_path="C:\\Users\\Administrator\\Desktop\Post_17track.xlsx"
    data1=copy(data)#xlutils.copy类里的copy函数
    table1=data1.get_sheet(sheet_num-1)
    z={'T':3,'F':2,'C':22,'Not taken':4,'Alert':5}#Check the dictionary, choose to populate the background color of the Excel cell查字典选择填充Excel单元格背景颜色
    bg_color=z[e]
    pattern=xlwt.Pattern()#Create the pattern
    pattern.pattern_fore_colou=bg_color#背景色May be: 8 through 63. 0 = Black, 1 = White, 2 = Red, 3 = Green, 4 = Blue, 5 = Yellow, 6 = Magenta, 7 = Cyan, 16 = Maroon, 17 = Dark Green, 18 = Dark Blue, 19 = Dark Yellow , almost brown), 20 = Dark Magenta, 21 = Teal, 22 = Light Gray, 23 = Dark Gray,
    style=xlwt.XFStyle()#Create the style
    style.pattern=pattern#Add Pattern to style
    table1.write(i+1,cols,e,style)
    
def RE_match_Track(r):
    """Match the return values get the Transit Status匹配返回值确定运单状态"""
   r=json.loads(r)
   e=r["e"]
   f=r["f"]
   z={'10':'
    return e,f    
def requests_url(TrackID):
    """Get a url webpage获取网页内容"""
    Data_requests={"guid":"","data":[{"num":"{0}"}]}.format(TrackID)#A single tracking number in request body单个运单号请求主体
    url="https://t.17track.net/restapi/track"
   headers={"Host":
    "t.17track.net",
    "User-Agent":
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
    "Accept":
    "application/json, text/javascript, */*; q=0.01",
    "Accept-Language":
    "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding":
    "gzip, deflate, br",
    "Referer":
    "https://t.17track.net/en",
    "Connection":
    "keep-alive",
    "Content-Type":
    "application/x-www-form-urlencoded; charset=UTF-8",
    "Upgrade-Insecure-Requests":"1",
    "X-Requested-With":"XMLHttpRequest"
    }#Firefox browser  requests_header火狐浏览器头部
    proxies={'HTTP':'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#Free proxy IP免费代理IP
    r=requests.post(url,headers=headers,proxies=proxies,data=Data_requests)
    return r.content

def main():
    """Packaging functions封装函数"""
    
if __name__=='__main__':
    main()
