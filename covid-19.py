#-*- coding:utf-8 -*-
import re,json,requests
def load_dxy_data():
    '''get a dict of covid-19 data'''
    url = 'https://3g.dxy.cn/newh5/view/pneumonia'
    raw_html = requests.get(url).content.decode('utf8')
    match = re.search('window.getAreaStat = (.*?)}catch', raw_html)
    raw_json = match.group(1)
    result = json.loads(raw_json, encoding='utf8')
    return result
def count_data(result):
'''Caculate the data of covid-19'''
    deadCount = 0 #init the data
    confirmedCount = 0
    curedCount = 0
    for x in xrange(len(result)):
        d = result[x][u'deadCount']
        deadCount += d
        for x in xrange(len(result[1][u'cities'])):
            conf = result[1][u'cities'][x][u'confirmedCount']
            confirmedCount += conf
            cured = result[1][u'cities'][x][u'curedCount']
            curedCount += cured
    return confirmedCount,deadCount,curedCount
def input_txt(*args):
'''write the data to txt then send to us'''
    with open('/Users/jin/Desktop/hhg.txt','w') as f:
        l0 = 'confirmedCount deadCount curedCount\n'
        l1 = [str(confirmedCount),' ',str(deadCount),' ',str(curedCount)]
        f.writelines(l0)
        f.writelines(l1)
        f.close()
if __name__ == '__main__':
    result = load_dxy_data()
    confirmedCount,deadCount,curedCount = count_data(result)
    input_txt(confirmedCount,deadCount,curedCount)
