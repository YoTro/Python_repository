#coding:utf-8
#Author: Toryun
#Date: 2020-05-03 12:00:00
#Function: Download files on Mac from DingTalk automaticly

import random
import pyautogui as pt
def __pinyin():
    pt.moveTo(1183, 2, 2)
    pt.click()
    pt.moveTo(1190, 56,3)
    pt.click()

t = random.randrange(2,3)
pt.FAILSAFE = True
pt.click(1343, 9)
pt.write('dingding', interval = 0.25)
pt.press(['enter'])
pt.moveTo(158, 61, 10)
pt.click()
pt.write('wuliu')
pt.press(['enter'])
pt.moveTo(150, 200, 5)
pt.click()
pt.moveTo(1417, 179, random.randrange(5,10))
pt.click()
pt.moveTo(1280, 159, t)
pt.click()
pt.moveTo(1092, 121, t)
pt.click()
s = '-2.1'
pt.write(s, interval = 0.25)
for i in range(205,693,61):
    
    pt.moveTo(1321, i, 2)
    pt.click()

print("Download Success!")

