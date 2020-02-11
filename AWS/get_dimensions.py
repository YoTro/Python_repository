# -*- conding:UTF-8 -*-
import re,requests,xlrd,datetime,xlwt,os,time
from xlutils.copy import copy
# 输入Asin，get网页返回内容和网址
def requests_url(Asin):
    url='https://www.amazon.com/dp/'+Asin
    headers={"Host":	
"www.amazon.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3141.7 Safari/537.36",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
"Accept-Language":
"zh-CN,zh;q=0.8",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1"
}
    proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费代理IP
    r=requests.get(url,headers=headers,proxies=proxies)
    return r.content,url

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
    start=datetime.datetime.now() # 计算所用时间
    file_path=File_path_choice()
    data=xlrd.open_workbook(file_path)#打开工作簿
    sheets=data.sheets()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print p,z[p]
    try:
        sheet_index=int(raw_input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print 'The digital is wrong,plz input a correct number'
    except Exception,e:
        print str(e)
        t=1

    table=data.sheet_by_index(t-1)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    print "{0}'s cols,rows is {1},{2}".format(z[t],cols,rows)#打印该sheet中的列行数
    row_1st=table.row_values(0)#读取第一行
    asin_index=row_1st.index('Asin')#返回Asin列的所在列数
    URL=table.col_values(asin_index,1,rows-1)#读取Asin列第二行到最后一行
    data1=copy(data)#复制工作簿
    table1=data1.get_sheet(t-1)
    for i in range(rows-1):
        try:
            r,url=requests_url(URL[i])
            time.sleep(3)
            print i,url
            dimensions1=re.findall(r'<td class="a-size-base">\s+(.*?)\sinches',r)
            dimensions2=re.findall('Product Dimensions:\s+<\/b>\s+(.*?)\s+inches',r)
            price=re.findall(r'class="a-size-medium a-color-price">\$(.*?)<\/span>',r)
            if dimensions1:
                print dimensions1[0]
                print price[0]
                table1.write(i+1,cols,dimensions1[0])
            elif  dimensions2:
                print dimensions2[0]
                print price[0]
                table1.write(i+1,cols,dimensions2[0])
            else:
                print 'dimensions is None'
            table1.write(i+1,cols+1,price[0])
        except Exception,e:
            print str(e)
    u='c:\\first_Choice_copy.xls'
    data1.save(u) #保存复制表格
    end=datetime.datetime.now()
    t=end-start#总用时
    print 'It save in {0}.\nTotal time: {1} s.'.format(u,t)

if  __name__== '__main__':
    main()
            
    
    
