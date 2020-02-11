#-*- coding:UTF-8 -*-
# ========================
#===   Python version is 2.7.13  
#==             Author: Toryun        
#==         Time:2017-08- 28        
#========================
import re,requests,xlrd,datetime,os,xlsxwriter
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
def get_image_url_and_filename(text):
    '''匹配图片地址，返回地址和命名'''
    url=re.findall(r'id=\"landingImage\" data-a-dynamic-image=\"{&quot;(.*?)\&quot;:',text)
    r=url[0]
    return r
def save_url_image_to_file(url,filename):
    '''请求图片URL，并保存到指定文件夹'''
    r=requests.get(url)
    with open("d:jpg/"+filename+".jpg","wb") as f:
        f.write(r.content)
        f.close()
def main():
    start=datetime.datetime.now()
    fn=File_path_choice()
    fp='c:\\image_xls.xls'
    workbook1=xlsxwriter.Workbook(fp)
    sheet1=workbook1.add_worksheet()
    data=xlrd.open_workbook(fn) # 打开工作薄
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
    table=data.sheet_by_index(t-1) # 读取指定sheet
    cols=table.ncols
    rows=table.nrows
    print "{0}'s rows ,cols are {1},{2}".format(z[t],rows,cols)
    first_sheet=table.row_values(0)
    url_index=first_sheet.index('URL')#返回第一行URL的列数
    asin_index=first_sheet.index('Asin')#返回第一行asin的列数
    URL=table.col_values(url_index) # 读取指定列（该列含有URL）
    asin=table.col_values(asin_index)
    for i in range(rows-1):
        try:
            u=URL[i+1]
            print i,u
            img_headers={
                "Host":	
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3141.7 Safari/537.36",
        "Accept":
        "*/*",
        "Accept-Language":
        "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding":	
        "gzip, deflate",
        "Connection":
        "keep-alive"}
            proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费IP地址*http://www.xicidaili.com*
            r=requests.get(u,headers=img_headers,proxies=proxies) # 获取listing
            url=get_image_url_and_filename(r.content) # 获取网页内容中的图片地址和命名
            filename=asin[i+1]
            save_url_image_to_file(url,filename) # 保存到文件夹去
            sheet1.write(i,0,filename)
            sheet1.insert_image(i,1,"d:jpg/"+filename+".jpg",{'x_scale':0.2,'y_scale':0.2})#把图片按长宽原来比例0.2的插入Excel中
        except Exception,e:
            print str(e)
            
    workbook1.close()
    end=datetime.datetime.now()
    t=end-start
    print '已将照片存入Excel {0}中\n总共用时：{1}s'.format(fp,t)

if __name__=='__main__':
    main()
