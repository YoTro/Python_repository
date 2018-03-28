# -*- coding: UTF-8 -*-
# ========================
#===   Python version is 2.7.13
#==         Operator: window7
#==             Author: Toryun          
#==              Time:2018-3-26           
#========================
# 调用csvAPI，合并多个CSV文件写入Excel
import csv
import os
import re
import xlwt
import datetime
def workbook_create():
    '''Create sheet in workbook by xlwt创建表格'''
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table1= workbook.add_sheet("data",cell_overwrite_ok=True)#取名data，可重写单元格
    table2= workbook.add_sheet("data1",cell_overwrite_ok=False)
    style = xlwt.XFStyle()#设置样式
    font = xlwt.Font()#设置字体
    font.name = 'SimSun' # 指定“宋体”
    style.font = font#把字体传递给样式
    alignment=xlwt.Alignment()#设置对齐
    alignment.horz=xlwt.Alignment.HORZ_CENTER#单元格字符水平居中
    # 格式: HORZ_GENERAL, HORZ_LEFT, HORZ_CENTER, HORZ_RIGHT, HORZ_FILLED, HORZ_JUSTIFIED, HORZ_CENTER_ACROSS_SEL, HORZ_DISTRIBUTED
    alignment.vert=xlwt.Alignment.VERT_CENTER#单元格字符垂直居中
    #格式: VERT_TOP, VERT_CENTER, VERT_BOTTOM, VERT_JUSTIFIED, VERT_DISTRIBUTED
    style.alignment=alignment#添加至样式
    return table1,table2,workbook
        
def main():
    file_path="C:\\Users\\Administrator\\Desktop\\1"#工作路径
    file_save='C:\\csv_1st.xls'#保存路径
    d=int(raw_input('Plz input a datetime of day\n'))
    m=int(raw_input('Plz input a datetime of month\n'))
    T=True
    while T:
        if m not in range(1,13):
            print 'the month is incorrect,plz input again\n'
            m=int(raw_input('Plz input a datetime of month\n'))
        else:
            T=False
    
    t1=datetime.datetime.now()
    table1,table2,workbook=workbook_create()
    
    file_list=os.listdir(file_path)#Get filename with extension name列出文件
    f=[]#Get old_filename获取旧文件名
    n=[]
    t=0#row行数
    l=0#parameter迭代参数
    for i in range(len(file_list)):
        for oldname in file_list:
            os.chdir(file_path)#编译时防止出现Python找不到工作目录错误
            f.append(oldname)
            newname=oldname.split(' ')[-1]
            if os.path.exists(file_path+os.sep+newname):
                pass
            else:
                os.rename(oldname,newname)
            n.append(newname)
        print n[i]
        new_url=file_path+os.sep+'('+str(i+1)+')'+'.csv'
        f_in=open(new_url, 'rb')
        text=csv.reader(f_in,dialect='excel')
        for line in text:
            
            #通过双重循环获取单个单元信息
            r=1
            for x in line:
                table1.write(l,0,'2018/{0}/{1}'.format(m,str(i+d)))
                table1.write(l,r,x)
                table2.write(l,r,x)
                r+=1
            l=l+1
    t2=datetime.datetime.now()      
    print 'The workbook is save in {0}\nThe time is {1}'.format(file_save,t2-t1)
    workbook.save(file_save)
if __name__=='__main__':
    main()
