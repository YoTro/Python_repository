#*--coding:UTF-8--*
#Function:Create a style Excel in special situation生成指定表格
#练习xlwt lib
#桌面文件夹1中的多个表格名字换成日期号写入一个工作簿
import xlwt,compileall
import xlrd
import os,sys
import datetime
'''调用xlwt设置样式制作一个可用的表格'''
t0=datetime.datetime.now()
workbook = xlwt.Workbook(encoding = 'utf-8')
table1= workbook.add_sheet("data",cell_overwrite_ok=False)#取名data，可重写单元格
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
'''读取文件夹中的文件，获取相应的文件数、关键词个数'''
file_path="C:\\Users\\Administrator\\Desktop\\1"
os.chdir(file_path)#编译时出现Python找不到工作目录
file_list=os.listdir(file_path)
f=[]#获取旧文件名
n=[]#获取新文件路径
for oldname in file_list:
    f.append(oldname)
try:
    for i in range(0,len(file_list)):
        
        old=file_path+os.sep+f[i]
        n.append(new)
        if os.path.exists(new):
            pass
        else:
            os.rename(old,new)#把工作簿名换成日期，解决文件中文编码问题
        print n[i]
except Exception,e:
    print str(e)    
file_num=len(f)
print '完成进度：',file_num
t=0
unqiue_key=[]
for i in xrange(file_num):
    data=xlrd.open_workbook(n[i])#打开工作簿
    table=data.sheet_by_index(0)# 打开sheet
    rows=table.nrows#计算表中列数和行数
    cols=table.ncols
    keywords=table.col_values(0,rows-1)#读取关键词列第1行到最后一行
    len_key=len(keywords)
    for k in xrange(0,rows-1):
        row_1st=table.row_values(k)#读取第关键词行
        table1.write(0+t,0,n[i],style)
        for row in range(cols): 
            table1.write(t,row+1,row_1st[row],style)
    t=t+rows
    print '#'*i,"{0}%".format(int(i/file_num)*100)
for x in range(len(keywords)):#去除重复关键词，写入新data1
    if  keywords[x]  not in unqiue_key:
        unqiue_key.append(keywords[x])
    table2.write(x,0,unqiue_key[x],style)
save_path='c:\\seller_data.xls'
workbook.save(save_path)
compileall.compile_file(r'c:\\Users\\Administrator\\Desktop\xt.py')
t1=datetime.datetime.now()
t3=t1-t0
print 'The workbook is save in {0}\nThe time is used {1}'.format(save_path,t3)


