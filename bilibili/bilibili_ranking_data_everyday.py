import requests
import datetime
import re
import pandas as pd
import random
import time
import progressbar  

def json_bvid(bvid, like, coin, favorite, share, reply, danmaku, his_rank):
    '''获取单个视频的数据'''
    url1 = 'https://api.bilibili.com/x/web-interface/view?bvid={}'.format(bvid)
    json_bvid = requests.get(url1)
    like.append(json_bvid.json()[ u'data'][u'stat'][u'like'])
    coin.append(json_bvid.json()[ u'data'][u'stat'][u'coin'])
    favorite.append(json_bvid.json()[ u'data'][u'stat'][u'favorite'])
    share.append(json_bvid.json()[ u'data'][u'stat'][u'share'] )
    reply.append(json_bvid.json()[ u'data'][u'stat'][u'reply']) 
    danmaku.append(json_bvid.json()[ u'data'][u'stat'][u'danmaku'] )
    his_rank.append(json_bvid.json()[ u'data'][u'stat'][ u'his_rank'])


url = 'https://www.bilibili.com/ranking?spm_id_from=333.851.b_7072696d61727950616765546162.3'
headers={"Host":	
"api.bilibili.com",
"User-Agent":
"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0",
"Referer":
"https://space.bilibili.com/96654548/video",
"Accept":
"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
"Accept-Language":
"zh-CN,zh;q=0.9,en;q=0.8",
"Accept-Encoding":	
"gzip, deflate, br",
"Connection":
"keep-alive",
"Cache-Control":"max-age=0",
"Upgrade-Insecure-Requests":"1",
"Cookie": 
"_uuid=500B1F19-1AD8-2AF1-FA34-A76D73F5C98913672infoc; buvid3=2EF6859F-2258-4D84-81BF-165ACE9CD69F53934infoc"
}
proxies={'HTTP': 'HTTP://122.242.96.30:808', 'HTTPS': 'HTTPS://122.242.96.30:808'}#免费IP地址*http://www.xicidaili.com*
r = requests.get(url)
s = re.findall(r'<li class=\"rank-item\"><div class=\"num\">(\d+)<\/div><div class=\"content\"><div class=\"img\"><a href=\"(.*?)\" target=\"_blank\"><div class=\"lazy-img cover\"><img alt=\"(.*?)\" src=.*?<i class=\"b\-icon play\"><\/i>(.*?)<\/span><span class=\"data-box\"><i class=\"b-icon view\"><\/i>(.*?)<\/span><a target=\"_blank\" href=\"(.*?)\"><span class=\"data-box\"><i class=\"b-icon author\"><\/i>(.*?)<\/span><\/a>',r.content)
head = ['ranking','url_videos_bilibili','title','played','views','up_space','author']
df = pd.DataFrame(s, columns = head)
like, coin, favorite, share, reply, danmaku, his_rank = [],[],[],[],[],[],[]
T_likes, T_views, followers, following = [],[],[],[]
widgets = ['Progress: ',progressbar.Percentage(), ' ', progressbar.Bar('#'),' ', progressbar.Timer(),  
           ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]  
pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(s)).start() 
for i in range(len(s)):
    bvid = s[i][1][-12:]
    json_bvid(bvid, like, coin, favorite, share, reply, danmaku, his_rank)
    uuid = re.findall(r'(\d+)',s[i][5])
    upstat = requests.get('https://api.bilibili.com/x/space/upstat?mid={}'.format(uuid[0]),headers = headers,proxies = proxies)
    stat = requests.get('https://api.bilibili.com/x/relation/stat?vmid={}'.format(uuid[0]))
    T_likes.append(upstat.json()[u'data'][u'likes'])
    T_views.append(upstat.json()[u'data'][u'archive'][u'view'])
    followers.append(stat.json()[u'data'][u'follower'])
    following.append(stat.json()[u'data'][u'following'])
    
    pbar.update( i + 1) 
df['点赞'] = like
df['投币'] = coin
df['收藏'] = favorite
df['转发'] = share
df['评论'] = reply
df['弹幕'] = danmaku
df['单日全站排名'] = his_rank
df['总获赞数'] = T_likes
df['粉丝数'] = followers
df['关注数'] = following
df['总播放数'] = T_views

d = datetime.datetime.today()
df.to_csv('/Users/jin/Documents/bilibili/data_rank_bilibili_{}.csv'.format(d), encoding='utf-8')
