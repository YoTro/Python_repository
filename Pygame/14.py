#coding: utf-8
#Author: Toryun
#Date: 2020-06-17 15:13:00

import urllib2
from contextlib import closing
from PIL import Image,ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True
'''

url0 = 'http://www.pythonchallenge.com'
url1 = 'http://www.pythonchallenge.com/pc/return/wire.png'

PasswordMgr1 = urllib2.HTTPPasswordMgrWithDefaultRealm()
PasswordMgr1.add_password(None, url0, 'huge', 'file')
auth_handler = urllib2.HTTPBasicAuthHandler(PasswordMgr1)
op = urllib2.build_opener(auth_handler)
urllib2.install_opener(op)

with closing(urllib2.urlopen(url1)) as f:
    b = open('/Users/jin/Desktop/wire.png','wb')
    b.write(f.read())
'''

t = '/Users/jin/Desktop/wire.png'
t0 = '/Users/jin/Desktop/14.png'
im = Image.open(t)

data = list(im.getdata())

#create new image object
im0 = Image.new(im.mode, (100,100))
#Allocates storage for the image and loads the pixel data. In normal cases, you don’t need to call this method, since the Image class automatically loads an opened image when it is accessed for the first time. This method will close the file associated with the image.
data0 = im0.load()

allsteps = [[i, i-1, i-1, i-2] for i in range(100, 0, -2)]
#把二维数组变成一维数组
nsteps  = reduce(lambda x, y: x+y, allsteps)
#右,上,左,下
directions = [(1,0), (0,1), (-1,0), (0,-1)]
direction = 0
pos = 0
#使起始点为(0,0)
pos0 = (-1, 0)

for i in nsteps:
    #前进i步
    for j in range(i):
        #控制方向
        pos0 = tuple(map(lambda x,y: x+y, pos0, directions[direction]))
        #将wire的像素值填入新的图片中
        data0[pos0] = data[pos]
        pos += 1
    #换方向
    direction = (direction + 1) % 4
im0.save(t0)
