# -*- coding:UTF-8 -*-
import xlrd
import xlwt
import os
def retry(func):
    '''装饰器：try最多5次'''
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
def Get_Exceldata(fn, colname):
    '''从表格里获取ASINURL'''
    if not os.path.exists(fn):
        print("Doesn't exist: {}".format(os.path.abspath(fn)))
        return None
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
        url_index=first_sheet.index(colname)#返回第一行URL的列数
    except Exception as e:
        print(e)
        return []
    ASINs=table.col_values(url_index) # 读取指定列（该列含有URL）
    ASINs.pop(0)
    return ASINs