#coding:UTF-8
#Author: Toryun
#Date: 2020-04-23 01:31:00
#Function: Zeller's congruence
#latex:\begin{align} D &= \left[ \frac{c}{4} \right] - 2c + y + \left[ \frac{y}{4} \right] + \left[ \frac{13(m+1)}{5} \right] + d - 1 \\[2ex]  W &= D \bmod 7 \end{align} \\

import math
import datetime

def Zeller(y, m, d):
    '''
    c:centry
    y:year
    m:month
    d:day
    w:week
    '''
    c = y/100%100
    y = y/10%10*10+y%10
    M = [0, 13, 14, 3, 4, 5, 6, 7, 8, 9, 10,11,12]
    m = M[m]
    w = ((c/4) - 2*c + y + (y/4) + (13*(m+1)/5) + d - 1)%7
    print "((c/4) - 2*c + y + (y/4) + (13*(m+1)/5) + d - 1)%7\n=({}-{}+{}+{}+{}-{})%7\n={}".format(c/4 ,2*c, y ,(y/4) ,(13*(m+1)/5) ,d,w)
    return w

if __name__ == '__main__':
    day0 = datetime.datetime.now()
    year0 = day0.year
    month0 = day0.month
    day0 = day0.day
    print year0, month0, day0
    w = Zeller(year0, month0, day0)
    print("今日是星期{}".format(w))
