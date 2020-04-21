#coding:UTF-8
#Author: Toryun
#Date: 2020-04-21 20:05:00
#Function: Radix Sort

import time
import math
import random
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import gc
import numpy as np

def Radix_sort(arr,count):
    '''根据数组元素的个位来进行桶排序'''
    k = [10**c for c in range(count)]
    print k
    while len(k)>0:
        b = 10
        c = k.pop(0)
        l = len(arr)
        buckets = [0]*b
        arr_new = [0]*l
        for i in range(l):
            n = arr[i]/c%b
            buckets[n] += 1
        for i in range(9):
            buckets[i+1] += buckets[i]
        #print(buckets)
        for i in range(l-1,-1,-1):
            n = arr[i]/c%b
            buckets[n] -= 1
            arr_new[buckets[n]] = arr[i]
        arr = arr_new
        
    return arr
def digital(num):
    '''获取数位'''
    x = num
    count = 1
    while x >= 10:
        #print x,count
        x /= 10
        count += 1
    return count
if __name__ == '__main__':
    num_bins = 100
    count = digital(num_bins-1)
    print("数组最大位为{}".format(count))
    arr = np.arange(num_bins)
    random.shuffle(arr)
    print("原数组array:\n{}".format(arr))
    t0 = time.time()
    arr1 = Radix_sort(arr,count)    
    print("排序后:\n{}".format(arr1))
    t1 = time.time()
    T = t1 - t0
    print "Heap Sort total time is {}".format(T)
