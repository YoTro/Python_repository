#coding:UTF-8
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import platform
import re
import os
import sys
import datetime,time
import random
import tkinter as tk
from tkinter import messagebox
from tkinter.filedialog import (askopenfilename, 
                                    askopenfilenames, 
                                    askdirectory, 
                                    asksaveasfilename)


url = "https://www.cleverpdf.com/cn/pdf-to-epub"

def system_info():
    '''Read system information then download the corresponding chromedriver
    读取系统信息，下载对应的chromedriver'''
    sysstr = platform.system()
    if sysstr =="Darwin":
        url = "https://chromedriver.storage.googleapis.com/80.0.3987.106/chromedriver_mac64.zip"
        name = "chromedriver_mac64.zip"
        os.system("wget "+url+" && tar -xzvf "+zip_name)
        driver_url = os.getcwd()+"/chromedriver"
    elif sysstr == "windows":
        url = "https://chromedriver.storage.googleapis.com/80.0.3987.106/chromedriver_win32.zip"
        name = "chromedriver_win32.zip"
        os.system("wget "+url+" && tar -xzvf "+zip_name)
    elif sysstr == "Linux":
        url = "https://chromedriver.storage.googleapis.com/80.0.3987.106/chromedriver_linux64.zip"
        name = "chromedriver_linux64.zip"
        os.system("wget "+url+" && unzip "+zip_name+" && mv chromedriver /usr/bin/")
        driver_url = "/usr/bin/chromedriver"
    else:
        print("Sorry, we can't identify the system you use")
        
    return driver_url
def readfilepath():
    '''Read the folder path of the PDF to be converted, and return the path and filename
    读取需要转换的pdf所在文件夹路径,并返回路径和文件名'''
    try:
        file_path = raw_input("Please raw_input the path folder which has pdf:\n")
        if file_path == '':
            #file_path = askdirectory()
            file_path = "/Users/jin/Documents/GitHub/Python_Source_Code/Tutorial_pdf"
            print file_path
            #close_window()
        file_names = os.listdir(file_path)#列出下载文件夹中的文件名
        names = []
        for i in range(len(file_names)-1):
            file_format = os.path.splitext(file_names[i])[-1]
            print i+1,file_names[i]
            names.append(file_names[i])
            file_names[i] = file_path + "/"+file_names[i]
            if file_format != ".pdf":
                file_names.pop(i)#剔除不是pdf格式的文件
                names.pop(i)
                names.append(file_names[i])
                file_names[i] = file_path + "/"+file_names[i]
        return file_names,names

    except Exception as e:
        print(str(e))

def close_window():
    root = tk.Tk()
    top = tk.Toplevel()
    top.destroy()
    root.destroy()
    root.mainloop()


#打开chromedriver，如果没有，则下载
if __name__ == '__main__':
    path_driver = '/Users/jin/Downloads/chromedriver'
    #path_driver = askopenfilename(title = "Please choose the chromedriver in the directories",initialdir='/')
    #close_window()
    if os.path.exists(path_driver):
        print "Chromedriver is in {}".format(path_driver)
    else:
        path_driver = system_info()
    options = webdriver.ChromeOptions()
    options.add_argument('disable-infobars')
    driver = webdriver.Chrome(path_driver,chrome_options = options)
    driver.get(url)
    s ,names= readfilepath()
    failed = [] #如果失败，则存入数组最后再次尝试
    waits = [] #如果等待时间过长，则刷新页面
    t0 = time.time()
    for i in range(len(s)):
        a = 1
        while a<10:
            openPdf = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.ID,"openPdf")))
            driver.find_element_by_id("openPdf").send_keys(s[i])
            r = re.findall(names[i],driver.page_source)
            if r:
                del waits[:]
                driver.find_element_by_xpath('//*[@id="choosefile_title"]').click()
                try:
                    download = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.ID,"fileDownload")))
                    if download:
                        time.sleep(random.randrange(5))
                        driver.find_element_by_id("fileDownload").click()
                        driver.find_element_by_xpath('//*[@id="convert_file_two"]').click()
                        a = 10
                        print "Success"
                except:
                    driver.get(url)
                    time.sleep(random.randrange(5))
                    failed.append(names[i])
            else:
                print "Please wait seconds"
                time.sleep(random.randrange(3))
                waits.append(a)
            if len(waits)>15:
                driver.get(url)
                del waits[:]
                failed.append(names[i])
                break
    t1 = time.time()
    T = t1-t0
    print("总用时:{0}\n成功{1}份,失败{2},其中失败的文件有：\n{3}".format(T,len(names)-len(failed),len(failed),failed))
    driver.close()
    driver.quit()

    
    
