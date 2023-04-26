#import csv
#from login_zhipin import login_zhipin
#from selenium_search import __search, match_job, save_To_csv
from zhipin import zhipin
from tqdm import tqdm

if __name__ == '__main__':
    scene = "1"#场景
    queryjob = "亚马逊运营"#岗位关键词
    city = "深圳"#城市
    experience = ""#工作经验
    payType = ""#工资结算周期
    partTime = ""#兼职时间
    degree = ""#学历要求
    industry = ""#公司行业
    scale = ""#公司规模
    stage = ""#融资阶段
    position = ""#职位类型
    jobType = ""#求职类型(全职,兼职)
    salary = ""#薪资待遇
    multiBusinessDistrict = ""#区,县
    multiSubway = ""#地铁线与站点
    page = 1#页数
    pageSize = 30#默认一页最多30条招聘信息
    data = {'zpData':{'jobList':[]}}
    boss = zhipin()
    citycode = boss.__citycode__(city)
    counties = boss.getbusinessDistrict(citycode)
    print(counties)
    for c in counties:
        for p in counties[c]:
            saved_path = './zhipinjobs_{}.csv'.format(p)
            print(saved_path)
            countiescode = boss.__businessDistrictcode__(citycode, {c:[p]})
            print("-------开始抓取-------\n")
            # 最长10页数据
            for i in tqdm(range(1, 11)):
                try:
                    response = boss.Search_jobs(scene, queryjob, citycode, experience, payType, partTime, degree, industry, scale, stage, position, jobType, salary, multiBusinessDistrict, multiSubway, str(i), pageSize)
                    # 不够10页直接停止
                    if (response['zpData']['hasMore']==False and response['zpData']['resCount']<=i*30):
                        break
                    data['zpData']['jobList']+=response['zpData']['jobList']
                    # 为空抓取失败
                    if(response['zpData']['jobList'] == []):
                        print("抓取失败:第{}页".format(i))
                except Exception as e:
                    pass
            boss.Save_To_Excel(data, saved_path)



