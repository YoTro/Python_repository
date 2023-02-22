import re
import os
import math
import time
import html
import xlrd
import xlwt
import random
import requests

def retry(func):
    def wrap(*args):
        i = 0
        r  = None
        while i<5:
            try:
                r = func(*args)
                if r:
                   i = 5 
            except Exception as e:
                i+=1
        return r
    return wrap
@retry
def Get_Amazon_QA(asin):
    url = "https://www.amazon.com/ask/questions/asin/{}/".format(asin)
    headers={
        "Host":
        "www.amazon.com",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":
        "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding":
        "gzip, deflate, br",
        "Connection":
        "keep-alive",
        "Upgrade-Insecure-Requests":"1"
        }
    r = requests.get(url, headers = headers)
    QAs = []
    QA_links = []
    questions_number = []
    if re.findall("(_TTD_\.png)", r.text):
        return QA_links, QAs
    else:
        questions_number = re.findall("(\d+) questions", r.text)
        if len(questions_number) != 0:
            qn = math.ceil(int(questions_number[0])/10)
            for i in range(1, qn+1):
                url_i = url + str(i)
                r = requests.get(url_i, headers = headers)
                QA_link = re.findall("askInlineAnswers\" id=\"(.*?)\">", r.text)
                QA_links.append(QA_link)
                for j in range(len(QA_link)):
                    r = requests.get('https://www.amazon.com/ask/questions/'+QA_link[j],headers = headers)
                    QA = re.findall("\s+<span>(.*?)<\/span>", r.text)
                    QAs.append(QA)
    return QA_links, QAs
def excel_bulit(workbook, asin):
    '''Bulit a excel.构建Excel'''
    table= workbook.add_sheet("{}".format(asin),cell_overwrite_ok=True)
    style = xlwt.XFStyle()#设置样式
    font = xlwt.Font()#设置字体
    font.name = 'SimSun' # 指定“宋体”
    style.font = font
    alignment=xlwt.Alignment()#设置对齐
    alignment.horz=xlwt.Alignment.HORZ_CENTER#单元格字符水平居中
    # 格式: HORZ_GENERAL, HORZ_LEFT, HORZ_CENTER, HORZ_RIGHT, HORZ_FILLED, HORZ_JUSTIFIED, HORZ_CENTER_ACROSS_SEL, HORZ_DISTRIBUTED
    alignment.vert=xlwt.Alignment.VERT_CENTER#单元格字符垂直居中
    #格式: VERT_TOP, VERT_CENTER, VERT_BOTTOM, VERT_JUSTIFIED, VERT_DISTRIBUTED
    style.alignment=alignment#添加至样式
    return table

def Get_Amazonlists(fn):
    '''从表格里获取ASINURL'''
    data=xlrd.open_workbook(fn) # 打开工作薄
    sheets=data.sheets()
    z={}
    p=0
    for sheet in sheets:
        p+=1
        z[p]=sheet.name
        print(p,z[p])
    try:
        sheet_index=int(input("plz input index in the serial number(default 1):\n"))
        if sheet_index in range(1,len(sheets)+1):
            t=sheet_index
        else:
            print('The digital is wrong,plz input a correct number')
    except Exception as e:
        print(str(e))
        t=1
    table=data.sheet_by_index(t-1) # 读取指定sheet
    cols=table.ncols
    rows=table.nrows
    print("{0}'s rows ,cols are {1},{2}".format(z[t],rows,cols))
    first_sheet=table.row_values(0)
    try:
        url_index=first_sheet.index('ASIN')#返回第一行URL的列数
    except Exception as e:
        print(e)
        return []
    ASINs=table.col_values(url_index) # 读取指定列（该列含有URL）
    ASINs.pop(0)
    return ASINs
def main():
    file_save='./AMZQA.xls'
    fn = './OR.xls'
    asinlist = Get_Amazonlists(fn)
    workbook = xlwt.Workbook(encoding = 'utf-8')
    for i in range(len(asinlist)):
        print("第{}个ASIN".format(i+1))
        table = excel_bulit(workbook, asinlist[i])
        QA_links, QAs = Get_Amazon_QA(asinlist[i])
        k = len(QA_links)
        #print(QAs)
        if(k != 0):
            m = 0
            for j in range(k):
                print("第{}页".format(j+1))
                for n in range(len(QA_links[j])):
                    print(m, n)
                    table.write(m, 0, QA_links[j][n])
                    for a in range(len(QAs[m])):
                        table.write(m, a+1, html.unescape(QAs[m][a]))
                    m += 1
    workbook.save(file_save)
    print("Save to {}".format(os.path.abspath(file_save)))
if __name__ == '__main__':
    main()