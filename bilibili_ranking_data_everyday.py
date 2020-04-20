import requests
import datetime
import re
import pandas as pd
import random
import time
import json

url = 'https://www.bilibili.com/ranking?spm_id_from=333.851.b_7072696d61727950616765546162.3'
r = requests.get(url)
s = re.findall(r'<li class=\"rank-item\"><div class=\"num\">(\d+)<\/div><div class=\"content\"><div class=\"img\"><a href=\"(.*?)\" target=\"_blank\"><div class=\"lazy-img cover\"><img alt=\"(.*?)\" src=.*?<i class=\"b\-icon play\"><\/i>(.*?)<\/span><span class=\"data-box\"><i class=\"b-icon view\"><\/i>(.*?)<\/span><a target=\"_blank\" href=\"(.*?)\"><span class=\"data-box\"><i class=\"b-icon author\"><\/i>(.*?)<\/span><\/a>',r.content)
head = ['ranking','url_videos_bilibili','title','played','views','up_space','author']
like, favorite, share, reply, danmaku, his_rank = [],[],[],[],[],[]
for i in range(len(s)):
    bvid = s[i][1][-12:]
    url1 = 'https://api.bilibili.com/x/web-interface/view?bvid={}'.format(bvid)
    json_bvid = requests.get(url1)
    like.append( json_bvid.json()[ u'data'][u'stat'][u'like'])
    favorite.append( json_bvid.json()[ u'data'][u'stat'][u'favorite'])
    share.append( json_bvid.json()[ u'data'][u'stat'][u'share'])
    reply.append( json_bvid.json()[ u'data'][u'stat'][u'reply'])
    danmaku.append( json_bvid.json()[ u'data'][u'stat'][u'danmaku'])
    his_rank.append( json_bvid.json()[ u'data'][u'stat'][u'his_rank'])

df = pd.DataFrame(s, columns = head)
df['点赞'] = like
   '收藏',
   '转发',
   '评论',
   '弹幕',
   '单日全站排名'] = , favorite, share, reply, danmaku, his_rank
d = datetime.datetime.today()
df.to_csv('/Users/jin/Documents/bilibili/data_rank_bilibili_{}.csv'.format(d), encoding='utf-8')
