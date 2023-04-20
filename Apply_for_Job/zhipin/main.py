#import csv
#from login_zhipin import login_zhipin
#from selenium_search import __search, match_job, save_To_csv
from zhipin import zhipin
from tqdm import tqdm

if __name__ == '__main__':
    scene = "1"
    queryjob = "亚马逊运营"
    city = "101280600"
    experience = ""
    payType = ""
    partTime = ""
    degree = ""
    industry = ""
    scale = ""
    stage = ""
    position = ""
    jobType = ""
    salary = ""
    multiBusinessDistrict = "440307"
    multiSubway = ""
    page = 1
    pageSize = 30
    data = {'zpData':{'jobList':[]}}
    boss = zhipin()
    print("-------开始抓取-------\n")
    # 最长10页数据
    for i in tqdm(range(1, 11)):
        try:
            response = boss.Search_jobs(scene, queryjob, city, experience, payType, partTime, degree, industry, scale, stage, position, jobType, salary, multiBusinessDistrict, multiSubway, str(i), pageSize)
            data['zpData']['jobList']+=response['zpData']['jobList']
            if(response['zpData']['jobList'] == []):
                print("抓取失败:第{}页".format(i))
        except Exception as e:
            print(e)

    boss.Save_To_Excel(data)
        #login_zhipin(phonenumber)
#    with open("./jobs.csv", "a", newline="", encoding="utf-8") as f:
#        writer = csv.writer(f)
#        writer.writerow(["职位", "公司", "工资", "工作地点", "详情页链接"])
#    for page in range(1, 2):
#        print("当前页数:{}".format(page))
#        html,cookies = __search(queryjob, city, page)
#        datas = match_job(html)
#        save_To_csv(datas)


