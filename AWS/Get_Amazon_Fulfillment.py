import os
import re
import ssl
import requests
from Amazon_Utils import xlwt, requests_asin, is_TTD, excel_bulit, Get_ASINlists

ssl._create_default_https_context = ssl._create_unverified_context

def FulfilledBy(host, asin):
    f, session = requests_asin(host, asin)
#    with open('./t.html', 'w') as s:
#        s.write(f)
#        s.close()
    Fulfilled = []
    if not is_TTD(f):
        Fulfilled = re.findall("Fulfilled by (.*?)<\/span><\/a><span>(.*?).[\s]?<\/span>", f)
    return Fulfilled

if __name__ == '__main__':
    host = "https://www.amazon.ca"
    fp = "./asin.xls"
    file_save = "./fulfilledby.xls"
    ASINs = Get_ASINlists(fp)
    workbook = xlwt.Workbook(encoding = 'utf-8')
    table=excel_bulit(workbook, "1")
    table.write(0, 0, "ASIN")
    table.write(0, 1, "Fulfilled")
    for i in range(len(ASINs)):
        try:
            asin = ASINs[i]
            table.write(i+1, 0, asin)
            m = FulfilledBy(host, asin)
            if m:
                table.write(i+1, 1, m[0][0]+m[0][1])
            print(asin, m)
        except Exception as e:
            print(e)
            pass
    workbook.save(file_save)
    print("Saved to {}".format(os.path.abspath(file_save)))