#coding:UTF-8
#Author: Toryun
#Date: 2020-04-15 14:55:00
#Function: Select Sort

from random import *
import time

def Select_sort(arr):
    assert len(arr)>0 and isinstance(arr,list)
    tmp = 0
    l = len(arr)
    while tmp < l:
        for i in range(tmp,l):
            if arr[tmp] > arr[i]:
                arr[tmp], arr[i] = arr[i], arr[tmp]
            
        tmp += 1
    return arr

if __name__ == '__main__':
    arr = [i for i in range(100)]
    shuffle(arr)
    a = Select_sort(arr)
    print(a)
