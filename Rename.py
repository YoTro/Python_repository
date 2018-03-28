#-*- coding:UTF-8 -*-
# ========================
#===   Python version is 2.7.13  
#==             Author: Toryun        
#==         Time:2017-08- 28        
#========================
import re,xlrd,datetime,os,xlsxwriter
def main():
    '''把文件中下载的图片写入新的Excel表中'''
    start=datetime.datetime.now() 
    fn='d:/Documents/Downloads/Bestsellers in Touch On Kitchen Sink Faucets.xlsx'
    fp='c:\\image_xls.xls'
    workbook1=xlsxwriter.Workbook(fp)
    sheet1=workbook1.add_worksheet()
    data=xlrd.open_workbook(fn) # 打开工作薄
    sheets=data.sheets()
    table=data.sheet_by_index(0) # 读取指定sheet
    cols=table.ncols
    rows=table.nrows
    print "{0}'s rows ,cols are {1},{2}".format(sheets[0],rows,cols)
    first_sheet=table.row_values(0)
    url_index=first_sheet.index('URL')#返回第一行URL的列数
    asin_index=first_sheet.index('Asin')#返回第一行asin的列数
    URL=table.col_values(url_index) # 读取指定列（该列含有URL）
    asin=table.col_values(asin_index)
    for i in range(rows-1):
        try:
            filename=asin[i+1]
            sheet1.write(i,0,filename)
            sheet1.insert_image(i,1,"d:jpg/"+filename+".jpg",{'x_scale':0.2,'y_scale':0.2})#把图片按长宽原来比例0.2的插入Excel中
        except Exception,e:
            print str(e)
    print '已经存入到新的工作簿{0}\n总用时：{1} s'.format(fp,t)
    workbook1.close()
    end=datetime.datetime.now()
    t=end-start
if __name__=='__main__':
    main()
