#coding:UTF-8
#Author: Toryun
#Date: 2020-04-07 22:44:00
#Function: Quick Sort

import time
from random import *

def Quick_sort(arr):
    if len(arr) < 1:
        return arr
    else:
        
        left, right = quick(arr)
        m = right.pop(0)
        return Quick_sort(left)+[m]+Quick_sort(right)#分治法+递归
        
    return arr
def quick(arr):
    tmp = 0
    i = 1
    j = len(arr)-1
    while i < j+1:
        
        if arr[i] > arr[tmp] and arr[j]<=arr[tmp]:
            #if arr[j]<=arr[tmp]:
            arr[j], arr[i] = arr[i], arr[j]
            i += 1
            j -= 1
        elif arr[i] > arr[tmp] and arr[j] > arr[tmp]:
            j -= 1
        elif arr[i] <= arr[tmp] and arr[j] <= arr[tmp]:
            i += 1
        elif arr[i] <= arr[tmp] and arr[j] > arr[tmp]:
            i += 1
            j -= 1
            
        if i > j:
            
            arr[tmp], arr[j] = arr[j], arr[tmp]
        #print i,j
    #print arr
    return arr[:j],arr[j:]

if __name__ == '__main__':
    arr = [i for i in range(100)]
    shuffle(arr)
    print arr
    t0 = time.time()
    arr = Quick_sort(arr)
    #left, right = quick(arr)
    print arr
    t1 = time.time()
    T = t1 - t0
    print "Quick sort total time is {}".format(T)


            
    
