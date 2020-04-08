#coding:UTF-8
#Author: Toryun
#Date: 2020-04-06 23:44:00
#Function: Merge Sort

import math
import time
import os
from random import *

def Merge(left,right):
    t = 1
    res = []
    while t<10:
        if left[0]<right[0]:
            res.append(left.pop(0))
        elif left[0] == right[0]:
            res.append(left.pop(0))
            res.append(right.pop(0))
        else:
            res.append(right.pop(0))
        if len(left) == 0 and len(right) == 0:
            t = 10
        elif len(left) == 0 and len(right) > 0:
            res += right
            t = 10
        elif len(left)  > 0 and len(right) == 0:
            res += left
            t = 10
    #print res   
    return res

def Merge_sort(arr):
    if len(arr) <= 1:
        return arr
    else:
        m = len(arr)//2
        left = arr[:m]
        right = arr[m:]
        #print m,left,right
        left = Merge_sort(left)
        print left,right
        right = Merge_sort(right)
        print left,right
    return Merge(left, right)
                
if __name__ == '__main__':
    arr = [i for i in range(0,100)]
    shuffle(arr)
    print arr
    t0 = time.time()
    Merge_sort(arr)
    t1 = time.time()
    T = t1 - t0
    print "Merge Sort total time is {}".format(T)
    

