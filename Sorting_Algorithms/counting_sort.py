#coding:UTF-8
#Author: Toryun
#Date: 2020-04-07 21:42:00
#Function: Counting Sort
from random import *
import time

def Counting_sort(arr):
    '''增加两个数组[arr_tmp]和[arr_new]，把数组arr中的元素存放在相应下标处，相应arr_tmp[i] += 1，存储完成后再把数组元素值两两相加得到顺序大小，最后根据元素值arr_tmp[i]作为arr_new的下标进行存储'''
    assert len(arr) > 0 and isinstance(arr,list)
    l = len(arr)
    tmp_a = 0 #最大值
    tmp_b = 0 #最小值
    for i in range(l):
        if arr[tmp_a] < arr[i]:
            tmp_a = i
        if arr[tmp_b] > arr[i]:
            tmp_b = i
    #print(arr[tmp])
    arr_tmp = [0]*(arr[tmp_a]+1)
    for i in arr:
        # print i
        arr_tmp[i] += 1
    print(arr_tmp)
    for i in range(l-1):
        arr_tmp[i+1] += arr_tmp[i]
    print(arr_tmp) 
    arr_new = [0]*l
    for i in range(l-1,-1,-1):
        #倒序保持算法稳定性
        #print(i,arr[i],arr_tmp[arr[i]])
        arr_tmp[arr[i]] -= 1
        arr_new[arr_tmp[arr[i]]] = arr[i]
        
     
    return arr_new
if __name__ == '__main__':
    arr = [i for i in range(100)]
    shuffle(arr)
    #print arr
    t0 = time.time()
    a = Counting_sort(arr)
    t1 = time.time()
    T = t0 - t1
    print(a)
    print "Insertion sort total time is {}".format(T)
