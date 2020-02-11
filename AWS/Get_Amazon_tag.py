# -*- conding:UTF-8 -*-
#Author:Toryun
#Python version:2.7.13
#Date:17/11/11
#Function:Get the keywords in Read reviews that mention
import requests,re,xlrd,os,datetime,time
from xlutils.copy import copy
def requests_url(url):
    '''获取网页内容'''
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:55.0) Gecko/20100101 Firefox/55.0",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
"Accept-Language":
"zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1"
}#火狐浏览器头部
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    r=requests.get(url,headers=headers,proxies=proxies)
    return r.content
def File_path_choice():
    '''选择文件夹中的文件，返回所选文件路径'''
    t='D:\\Documents\\Downloads\stainless steel toilet brush holder.xlsx'#默认工作簿地址
    try:
        file_path='d:/documents/downloads'
        print '路径{0}文件夹中的文件和文件夹如下：'.format(file_path)
        file_names=os.listdir(file_path)#列出下载文件夹中的文件名
        for i in range(len(file_names)):
            print i+1,file_names[i]
        file_num=int(raw_input("Default workbook is 1,plz input a number of serial number(default {0}):\nOr Enter a number more than the last option you could input a url of file\n ".format(t)))#默认文件名是t,或者输入比最后选项大的数
        if file_num in range(1,len(file_names)+1):
            file_path=file_path+'/'+file_names[file_num-1]
        if file_num>=len(file_names)+1:
            file_path=str(raw_input("plz input a fileurl (like:D:\\Documents\\Downloads\1.xlsx\n"))
            f=os.path.exists(file_path)
            while f==False:
                file_path=str(raw_input("Your file is not exsits,plz input a fileurl:\n"))
                f=os.path.exists(file_path)
    except Exception,e:
        print str(e)
        file_path=t
    return file_path
def main():
    '''打开工作簿，选择sheet匹配标签获得个数，存储到新的工作簿中'''
    start=datetime.datetime.now()#开始时间
    file_path=File_path_choice()#返回所选文件路径
    data=xlrd.open_workbook(file_path)#打开路径中文件
    sheets=data.sheets()#获取所有sheet (类型list)
    print "{0}'s sheets:\n".format(file_path)
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))#选择工作簿中的sheet
        if sheet_index in range(1,len(sheets)+1):#判断输入数是否超出范围
            sheet_num=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:#如果输入数非法，则默认为sheet1
        print str(e)
        sheet_num=1
    table=data.sheet_by_index(sheet_num-1)
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "{0} 's rows,cols are {1},{2}".format(z[sheet_num],rows,cols)
    rows_1st=table.row_values(0)#读取第一行
    URL_index=rows_1st.index('URL')#读取URL所在位置
    URL=table.col_values(URL_index,1,rows-1)#读取该列从第2行到最后一行
    data1=copy(data)#xlutils.copy 类里的copy函数
    table1=data1.get_sheet(sheet_num-1)
    for i in xrange(rows):
        Amazon_='https://www.amazon.com'
        try:
            r=requests_url(URL[i])
            print i,URL[i]
            filter_tag_url=re.findall(r'data-reviews-state-param class=\"a-link-normal\" href=\"(.*?)"><span id=\"'
,r)
            if  filter_tag_url:
                l=len(filter_tag_url)#标准为20个标签
                for j in range(l+1):
                    time.sleep(2)#延迟2秒
                    r1=requests_url(Amazon_+filter_tag_url[j])
                    filter_tag=re.findall(r'<span class=\"a-size-base\">\(containing \"(.*?)\"\)',r1)
                    reviews_num=re.findall(r'<div class="a-section a-spacing-medium"><span class="a-size-base">(.*?) reviews',r1)
                    
                    table1.write(i+1,cols+j,str(filter_tag)+':'+str(reviews_num))#写入表格
            else:
                print "None"
        except Exception,e:
               print str(e)
    filepath='d:/Documents/Downloads/Amaozn_tag.xls'
    data1.save(filepath)#保存到新的工作簿
    end=datetime.datetime.now()
    s=end-start
    print '存储到新的工作簿{0}\n总用时：{1} s'.format(filepath,s)
if __name__ =='__main__':
    main()
            
    
    
