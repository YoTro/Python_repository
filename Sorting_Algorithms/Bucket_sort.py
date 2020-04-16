#coding:UTF-8
#Author: Toryun
#Date: 2020-04-15 14:55:00
#Function: Bucket Sort

from random import *
import numpy as np
import time

def Bucket_sort(arr):
    '''把数组中的元素按照arr[i]*len(arr)/max(arr[i])放入另外一个数组中【桶】，最后依次把非空桶中元素按大小重新存入数组'''
    assert len(arr) > 0 and isinstance(arr,list)
    l = len(arr)
    max_i = 0
    min_i = 0
    for i in range(l):
        if arr[i]>arr[max_i]:
            max_i = i
        if arr[i]<arr[min_i]:
            min_i = i
    bucket_counts = arr[max_i]-arr[min_i]+1   
    arr_bucket = [[] for i in range(l)]
    i = 0
    while i < l:
        #print(arr[i],arr[i]*l/arr[max_i])
        arr_bucket[arr[i]*l/(arr[max_i]+1)].append(arr[i])
        i += 1
    #print l,arr[max_i]
    #print arr_bucket
    for i in range(l):
        #对每个桶内进行排序，可以简化成一个函数min(arr_tmp[i])
        if len(arr_bucket[i]) == 0:
            i += 1
        else:
            t0 = time.time()
            arr_bucket[i].sort()           
            '''tmp = 0
            while tmp < len(arr_bucket[i]):
                #print tmp
                for j in range(len(arr_bucket[i])):
                    #print j
                    if arr_bucket[i][j] < arr_bucket[i][tmp]:
                        #print(arr_bucket[i][j], arr_bucket[i][tmp])
                        arr_bucket[i][j], arr_bucket[i][tmp] = arr_bucket[i][tmp], arr_bucket[i][j]
                tmp += 1'''
                #print tmp
    #print arr_bucket
    i = 0
    while i < l:
        n = arr_bucket.pop(0)
        #print i
        if len(n) > 0:
            for j in range(len(n)):
                arr[i] = n[j]
                i += 1            
    return arr
if __name__ == '__main__':
    arr = [i for i in range(100)]
    shuffle(arr)
    t0 = time.time()
    #print(arr)
    a = Bucket_sort(arr)
    t1 = time.time()
    #print(a)
    T = t1 - t0
    print "Merge Sort total time is {}".format(T)
    
