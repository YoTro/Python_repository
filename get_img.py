#-*- coding:UTF-8 -*-
# ========================
#===   Python version is 2.7.13  ==
#==             Author: Toryun           ==
#==         Time:2017-08- 28         ==
#========================
import re,requests,xlrd,datetime,os,xlsxwriter
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
    fn='d:/Documents/Downloads/Storefront.xlsx'
    fp='c:\\image_xls.xls'
    workbook1=xlsxwriter.Workbook(fp)
    sheet1=workbook1.add_worksheet()
    data=xlrd.open_workbook(fn) # 打开工作薄
    table=data.sheet_by_index(0) # 读取指定sheet
    cols=table.ncols
    rows=table.nrows
    first_sheet=table.row_values(0)
    url_index=first_sheet.index('URL')#返回第一行URL的列数
    asin_index=first_sheet.index('Asin')#返回第一行asin的列数
    URL=table.col_values(url_index) # 读取指定列（该列含有URL）
    asin=table.col_values(asin_index)
    l=len(URL)
    print l
    for i in range(rows-1):
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
        r=requests.get(u,headers=img_headers) # 获取listing
        url=get_image_url_and_filename(r.content) # 获取网页内容中的图片地址和命名
        filename=asin[i+1]
        save_url_image_to_file(url,filename) # 保存到文件夹去
        sheet1.write(i,0,filename)
        sheet1.insert_image(i,1,"d:jpg/"+filename+".jpg",{'x_scale':0.2,'y_scale':0.2})
    workbook1.close()
    end=datetime.datetime.now()
    t=end-start
    print '总共用时：{0}s'.format(t)

if __name__=='__main__':
    main()
